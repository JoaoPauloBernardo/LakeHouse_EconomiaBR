"""
DAG principal: orquestra o pipeline inteiro, mensalmente.

    ingest_bcb   ──▶ silver_bcb    ─┐
    ingest_comex ──▶ silver_comex  ─┼──▶ gold ──▶ publish_databricks
    ingest_ibge  ──▶ silver_ibge   ─┘

Tres trilhas independentes que so se encontram na gold. Nao existe aresta
entre elas, entao o scheduler paraleliza sozinho - dependencia no Airflow
nao e desenho bonito, e o que define o paralelismo.

CONCEITOS QUE ESTE DAG DEMONSTRA
--------------------------------

1. DATA LOGICA = BACKFILL DE GRACA.
   O ano vem de {{ data_interval_start.year }}: template Jinja que o Airflow
   preenche com a data da execucao LOGICA, nao com datetime.now(). Como os
   jobs sao idempotentes (MERGE / replaceWhere - ver adr-002):

       airflow dags backfill -s 2020-01-01 -e 2023-12-31 lakehouse_pipeline

   reprocessa 4 anos com seguranca. Se o job usasse now(), as 4 execucoes
   ingeririam 2026 e o backfill seria impossivel.

   As duas metades sao necessarias: data logica sem idempotencia duplica;
   idempotencia sem data logica nao sabe qual periodo processar.

2. TRES CAMADAS DE RESILIENCIA, PRA PROBLEMAS DIFERENTES.
   - tenacity, dentro do job ......... 502 passageiro, conexao caiu (segundos)
   - retries do Airflow, na task ..... governo fora do ar (minutos/horas)
   - AirflowFailException ............ dado ruim: NAO tenta de novo

   A terceira e a mais sutil e vem dos quality gates. Falha de qualidade e
   DETERMINISTICA: se a silver tem duplicata na chave, tentar de novo em 10
   minutos encontra a mesma duplicata. O retry so queima tempo, polui o log
   e falha igual. Os jobs sinalizam isso saindo com EXIT_QUALIDADE (3), e o
   _run() abaixo traduz pra uma falha seca, sem retry.
   Ver src/common/exit_codes.py.

3. FEATURE FLAG NA ULTIMA TASK.
   publish_databricks sai com codigo 0 se DATABRICKS_HOST nao estiver
   setado. Quem clonou o repo sem conta no Databricks roda o DAG inteiro
   verde. Ver adr-003.

POR QUE PythonOperator E NAO BashOperator
-----------------------------------------
BashOperator so sabe "exit 0 = ok, resto = falha" - nao da pra reagir
diferente conforme o codigo. Como precisamos distinguir falha transiente
de deterministica, o operator roda o subprocess e mapeia o exit code na
excecao certa do Airflow.
"""

import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator

# Mesmo layout do container `app` (ver docker/Dockerfile.airflow): o comando
# roda identico pelo `make` ou pelo Airflow. Isso nao e detalhe - e o que
# evita o "funciona na minha maquina" entre os dois caminhos de execucao.
PROJECT_DIR = "/opt/app"

# Espelha src/common/exit_codes.py. Duplicado de proposito: o DAG e o unico
# ponto do projeto que NAO importa de src/ (o scheduler parseia este arquivo
# a cada poucos segundos; import pesado aqui e custo de scheduler).
EXIT_QUALIDADE = 3

default_args = {
    "owner": "joao-paulo",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _run(module: str, args: str, **context) -> None:
    """Roda `python -m <module> <args>` e traduz o exit code pra excecao.

    stdout/stderr sao herdados, entao o log do job aparece inteiro no log
    da task do Airflow - sem capture_output, que engoliria o progresso ate
    o processo terminar.
    """
    cmd = f"cd {PROJECT_DIR} && python -m {module} {args}".strip()
    print(f"$ {cmd}")
    rc = subprocess.run(["bash", "-lc", cmd]).returncode

    if rc == 0:
        return

    if rc == EXIT_QUALIDADE:
        # Falha seca: marca a task como failed e NAO agenda retry.
        raise AirflowFailException(
            f"{module}: falha de quality gate (exit {rc}). "
            "Deterministica - retry nao resolve, o dado precisa ser investigado."
        )

    # Qualquer outra coisa: pode ser transiente, deixa o retry agir.
    raise AirflowException(f"{module}: falhou com exit {rc}")


def job(task_id: str, module: str, args: str = "") -> PythonOperator:
    """Helper: toda task e `python -m <modulo>` rodando em /opt/app.

    Encapsular aqui evita repetir o boilerplate em 7 tasks e - mais
    importante - torna a migracao pro padrao de producao um edit em UM
    lugar: trocar por DatabricksSubmitRunOperator / KubernetesPodOperator
    mexe nesta funcao, nao nas 7 tasks.
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=_run,
        op_kwargs={"module": module, "args": args},
    )


with DAG(
    dag_id="lakehouse_pipeline",
    description="BCB + Comex Stat + IBGE -> raw -> bronze -> silver -> gold",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",          # MDIC e BCB divulgam mensalmente
    catchup=False,                # backfill e ato deliberado, via CLI
    default_args=default_args,
    tags=["lakehouse", "economia"],
) as dag:

    # --- Trilha BCB: serie inteira todo run (bronze append-only) ---------
    ingest_bcb = job("ingest_bcb", "src.ingestion.bcb_sgs")
    silver_bcb = job("silver_bcb", "src.transform.bronze_to_silver", "--datasets bcb")

    # --- Trilha Comex: o dataset pesado, por ano ------------------------
    ingest_comex = job(
        "ingest_comex",
        "src.ingestion.comex_stat",
        "--years {{ data_interval_start.year }} --flows EXP IMP",
    )
    silver_comex = job(
        "silver_comex",
        "src.transform.bronze_to_silver",
        "--datasets comex --years {{ data_interval_start.year }}",
    )

    # --- Trilha IBGE ----------------------------------------------------
    # Janela de 2 anos pra tras, de proposito: o PIB municipal do IBGE sai
    # com ~2 anos de defasagem e a estimativa de populacao e revisada.
    # Pedir o ano corrente devolveria vazio; pedir uma janela pega tanto o
    # dado novo quanto as revisoes do ano anterior (o MERGE da silver
    # resolve a sobreposicao sem duplicar - adr-002).
    # Nota: o CLI do ibge_sidra usa --anos (portugues), nao --years.
    ingest_ibge = job(
        "ingest_ibge",
        "src.ingestion.ibge_sidra",
        "--anos {{ data_interval_start.year - 2 }} {{ data_interval_start.year - 1 }}",
    )
    silver_ibge = job("silver_ibge", "src.transform.ibge_to_silver")

    # --- Convergencia: gold depende das TRES silvers ---------------------
    # A gold e overwrite total (nao replaceWhere): e 100% derivada, entao
    # recalcular do zero elimina risco de particao velha com logica antiga.
    gold = job("gold", "src.transform.silver_to_gold")

    # --- Publicacao (feature flag) --------------------------------------
    publish_databricks = job("publish_databricks", "src.publish.databricks_publish")

    ingest_bcb >> silver_bcb
    ingest_comex >> silver_comex
    ingest_ibge >> silver_ibge

    [silver_bcb, silver_comex, silver_ibge] >> gold >> publish_databricks
