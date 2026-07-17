"""
Smoke test do ambiente: `make smoke`.

NAO e teste unitario (isso e o pytest, em tests/). Isso aqui responde uma
pergunta so, na hora que voce sobe o ambiente:

    "a plumbing esta de pe?"

Ou seja: o Spark sobe, os JARs do Delta/S3A baixaram, o MinIO responde,
as credenciais estao certas e o que ja foi ingerido e legivel.

Por que isso vale um arquivo proprio: sem ele, o primeiro sinal de que o
S3A esta mal configurado vem no meio de um job de ingestao de 40 minutos.
Smoke test falha em 20 segundos e diz exatamente qual peca quebrou.

Exit code != 0 quando algo essencial falha -> serve de gate em CI depois.
"""

from __future__ import annotations

import sys

from py4j.protocol import Py4JJavaError

from src.common.config import settings
from src.common.spark_session import get_spark

# (nome logico, caminho) das tabelas que o pipeline produz.
# Tabela ausente NAO e falha: no dia 1 nada foi ingerido ainda.
TABELAS = [
    ("bronze/bcb_sgs", f"{settings.bronze_zone}/bcb_sgs"),
    ("bronze/comex_stat", f"{settings.bronze_zone}/comex_stat"),
    ("bronze/ibge_sidra", f"{settings.bronze_zone}/ibge_sidra"),
    ("silver/bcb_sgs", f"{settings.silver_zone}/bcb_sgs"),
    ("silver/comex_stat", f"{settings.silver_zone}/comex_stat"),
    ("silver/ibge_sidra", f"{settings.silver_zone}/ibge_sidra"),
    ("gold/exportacao_uf_ano", f"{settings.gold_zone}/exportacao_uf_ano"),
]


def main() -> None:
    print("=" * 62)
    print(f"SMOKE TEST | runtime={settings.runtime_env} | bucket={settings.bucket}")
    print(f"           | spark_master={settings.spark_master}")
    print(f"           | minio={settings.minio_endpoint}")
    print("=" * 62)

    # ---------------------------------------------------------------
    # 1) Spark sobe? (se os JARs do Delta/S3A nao baixaram, morre aqui)
    # ---------------------------------------------------------------
    spark = get_spark(app_name="smoke-test")
    print(f"[ok] SparkSession de pe  (Spark {spark.version})")

    # ---------------------------------------------------------------
    # 2) Delta responde? Escreve e le uma tabela descartavel.
    #    Isso exercita a cadeia inteira de uma vez: Spark -> Delta ->
    #    S3A -> MinIO -> credenciais -> bucket. Se passar, a plumbing
    #    esta boa; qualquer erro depois e do DADO, nao do ambiente.
    # ---------------------------------------------------------------
    probe_path = f"s3a://{settings.bucket}/_smoke/probe"
    try:
        df = spark.createDataFrame([(1, "ok")], ["id", "status"])
        df.write.format("delta").mode("overwrite").save(probe_path)
        lido = spark.read.format("delta").load(probe_path).collect()
        assert lido[0]["status"] == "ok"
        print(f"[ok] Delta + S3A + MinIO escrevendo e lendo ({probe_path})")
    except Py4JJavaError as exc:
        print("[FALHA] nao consegui escrever/ler Delta no MinIO.")
        print("        Suspeitos de sempre:")
        print("        - credenciais do .env != as do MinIO")
        print("        - bucket inexistente (o minio-init rodou?)")
        print("        - fs.s3a.path.style.access ausente")
        print(f"        Erro Java: {str(exc)[:300]}")
        sys.exit(1)

    # ---------------------------------------------------------------
    # 3) Inventario: o que ja existe no lakehouse?
    #    Tabela que ainda nao foi criada aparece como "-", sem falhar.
    # ---------------------------------------------------------------
    print("-" * 62)
    print(f"{'TABELA':<28} {'LINHAS':>12}   STATUS")
    print("-" * 62)
    for nome, path in TABELAS:
        try:
            n = spark.read.format("delta").load(path).count()
            print(f"{nome:<28} {n:>12,}   ok")
        except Exception:
            print(f"{nome:<28} {'-':>12}   ainda nao ingerida")

    print("-" * 62)
    print(">>> Ambiente saudavel.")
    spark.stop()


if __name__ == "__main__":
    main()
