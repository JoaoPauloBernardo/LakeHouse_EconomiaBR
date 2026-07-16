"""
Publicacao: GOLD local -> Databricks (Unity Catalog).

POR QUE ESSE JOB E "AO CONTRARIO" DO QUE PARECE OBVIO
-----------------------------------------------------
A tentacao e escrever direto do Spark do Databricks lendo o nosso MinIO.
Nao funciona, por dois motivos concretos:
  1. O compute do Databricks roda na nuvem; o MinIO roda na sua maquina.
     Ele nao tem rota pra localhost:9000.
  2. O Free Edition e serverless e restringe saida de rede a um conjunto
     de dominios confiaveis - mesmo com IP publico nao ia sair.

Entao o fluxo e o inverso: o LOCAL empurra os dados pra cima.

    [1] export    Spark local le a gold (Delta) -> escreve Parquet local
    [2] upload    Databricks CLI sobe o Parquet pra um UC Volume
    [3] register  Sessao Databricks cria a tabela gerenciada a partir do Volume

Isso e feature flag, nao dependencia dura: sem DATABRICKS_HOST setado o job
avisa e sai com codigo 0. O pipeline local nunca quebra por causa disso.

PRE-REQUISITOS (so pra quem vai publicar):
    pip install -r requirements-databricks.txt   # em OUTRO venv! ver adr-003
    databricks auth login --host https://<seu-workspace>
    export DATABRICKS_HOST=https://<seu-workspace>
    export DATABRICKS_CATALOG=workspace DATABRICKS_SCHEMA=economia_br
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from src.common.config import settings

GOLD_TABELA = "exportacao_uf_ano"
STAGING_LOCAL = Path("/tmp/databricks_staging") / GOLD_TABELA


def _volume_uri() -> str:
    return (
        f"/Volumes/{settings.databricks_catalog}"
        f"/{settings.databricks_schema}/{settings.databricks_volume}/{GOLD_TABELA}"
    )


def export_gold() -> Path:
    """[1] Le a gold local e exporta pra Parquet num diretorio de staging.

    Por que Parquet e nao Delta: o que sobe pro Volume e so um veiculo de
    transporte. A tabela final no Unity Catalog e criada como Delta
    gerenciado pelo proprio Databricks no passo [3] - nao faz sentido
    carregar o _delta_log daqui pra la.

    coalesce(1) porque a gold e pequena (1 linha por uf/ano, ~800 linhas):
    um arquivo unico simplifica o upload. Se um dia crescer, tirar isso.
    """
    from src.common.spark_session import get_spark  # import local: pesa

    spark = get_spark(app_name="databricks-export")
    gold_path = f"{settings.gold_zone}/{GOLD_TABELA}"

    df = spark.read.format("delta").load(gold_path)
    n = df.count()

    if STAGING_LOCAL.exists():
        shutil.rmtree(STAGING_LOCAL)  # staging e descartavel, sempre limpo

    df.coalesce(1).write.mode("overwrite").parquet(f"file://{STAGING_LOCAL}")
    print(f"[1/3] export ok: {n:,} linhas -> {STAGING_LOCAL}")
    spark.stop()
    return STAGING_LOCAL


def upload_to_volume(staging: Path) -> str:
    """[2] Sobe o staging pro UC Volume via Databricks CLI.

    CLI e nao SDK porque `databricks fs cp -r` ja resolve upload recursivo
    com retry; reimplementar isso em Python seria reinventar a roda.
    """
    destino = _volume_uri()
    print(f"[2/3] upload -> {destino}")
    subprocess.run(
        ["databricks", "fs", "cp", "-r", "--overwrite", str(staging), f"dbfs:{destino}"],
        check=True,
    )
    return destino


def register_table(volume_path: str) -> None:
    """[3] Cria/atualiza a tabela gerenciada no Unity Catalog.

    CREATE OR REPLACE TABLE ... AS SELECT: idempotente por construcao.
    Mesmo principio das camadas silver/gold (adr-002) - rodar 2x da o
    mesmo estado final.

    Repare que aqui get_spark() devolve DatabricksSession, nao Spark local:
    e o RUNTIME_ENV=databricks agindo. Mesmo codigo, plataforma diferente.
    """
    import os

    os.environ["RUNTIME_ENV"] = "databricks"
    from src.common.spark_session import get_spark

    spark = get_spark()
    fqn = f"{settings.databricks_catalog}.{settings.databricks_schema}.{GOLD_TABELA}"

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS "
        f"{settings.databricks_catalog}.{settings.databricks_schema}"
    )
    spark.sql(
        f"CREATE OR REPLACE TABLE {fqn} AS "
        f"SELECT * FROM parquet.`{volume_path}`"
    )
    print(f"[3/3] tabela registrada: {fqn}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publica a gold no Databricks")
    parser.add_argument(
        "--step",
        choices=["export", "upload", "register", "all"],
        default="all",
        help="rodar um passo isolado ajuda a debugar credencial/permissao",
    )
    args = parser.parse_args()

    # Feature flag: sem host configurado, nao e erro - so nao publica.
    # E isso que permite a task existir no DAG sem quebrar quem clonou
    # o repo e nao tem conta no Databricks.
    if not settings.databricks_enabled:
        print("DATABRICKS_HOST nao configurado - publicacao pulada (isso nao e erro).")
        sys.exit(0)

    if args.step in ("export", "all"):
        staging = export_gold()
    else:
        staging = STAGING_LOCAL

    if args.step in ("upload", "all"):
        volume_path = upload_to_volume(staging)
    else:
        volume_path = _volume_uri()

    if args.step in ("register", "all"):
        register_table(volume_path)

    print(">>> Publicacao concluida.")


if __name__ == "__main__":
    main()
