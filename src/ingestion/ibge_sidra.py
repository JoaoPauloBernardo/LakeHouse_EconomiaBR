"""
Ingestão: API SIDRA/Agregados do IBGE -> RAW (JSON cru) -> BRONZE (Delta).

Dois datasets, mesma API (agregados IBGE), nível município (N6):
- população: agregado 6579, variável 9324 ("População residente estimada")
- pib_municipal: agregado 5938, variável 37 ("PIB a preços correntes")

Segue o mesmo contrato raw -> bronze do comex_stat.py:
1. RAW: bytes exatamente como a fonte devolveu, gravados via boto3, sem
   nenhuma transformação. Se o parse da bronze quebrar, reprocessa do raw
   sem bater na API de novo.
2. BRONZE: Spark lê o raw com schema EXPLÍCITO (nunca inferSchema) e grava
   Delta com colunas de linhagem (_source_url, _raw_path, _run_id,
   _ingested_at).

Por que replaceWhere (por dataset+ano) em vez de append puro na bronze:
o IBGE revisa estimativas populacionais e o PIB municipal é publicado com
defasagem e também sofre revisão. Sem idempotência por partição, rodar a
ingestão 2x pro mesmo ano duplicaria linha - mesmo raciocínio do Comex
(a unidade natural de reprocessamento é o dataset-ano inteiro, não a
linha individual).

API: https://servicodados.ibge.gov.br/api/docs/agregados
"""
from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone

import boto3
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from tenacity import retry, stop_after_attempt, wait_exponential

from src.common.config import settings
from src.common.spark_session import get_spark

API_URL = (
    "https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}"
    "/periodos/{periodo}/variaveis/{variavel}?localidades=N6[all]"
)
TIMEOUT = 60

# Catálogo dos datasets que alimentam o projeto.
# Catálogo completo de agregados: https://servicodados.ibge.gov.br/api/docs/agregados
DATASETS = {
    "populacao": {"agregado": 6579, "variavel": 9324},
    "pib_municipal": {"agregado": 5938, "variavel": 37},
}

# Schema explícito da resposta da API de agregados (mesma forma pros dois
# datasets). "serie" tem uma chave por ano pedido -> MapType, não StructType,
# porque o conjunto de anos é dinâmico (decidido em tempo de request).
BRONZE_RAW_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("variavel", StringType()),
        StructField("unidade", StringType()),
        StructField(
            "resultados",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "series",
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            "localidade",
                                            StructType(
                                                [
                                                    StructField("id", StringType()),
                                                    StructField("nome", StringType()),
                                                ]
                                            ),
                                        ),
                                        StructField(
                                            "serie", MapType(StringType(), StringType())
                                        ),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
    ]
)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_user,
        aws_secret_access_key=settings.minio_password,
    )


def _periodo_arg(anos: list[int]) -> str:
    """IBGE aceita anos separados por '|' na URL (não precisa ser um range)."""
    return "|".join(str(ano) for ano in sorted(anos))


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=5, min=5, max=60))
def fetch_dataset(agregado: int, variavel: int, periodo: str) -> bytes:
    """Baixa o JSON cru do agregado IBGE para o(s) ano(s) pedido(s)."""
    url = API_URL.format(agregado=agregado, periodo=periodo, variavel=variavel)
    resp = requests.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.content


def write_raw(dataset: str, periodo: str, payload: bytes, run_id: str) -> str:
    """Grava o JSON cru no bucket do lakehouse, camada raw. Byte a byte, sem tocar."""
    key = f"raw/ibge_sidra/{dataset}/periodo={periodo.replace('|', '-')}/{run_id}.json"
    _s3_client().put_object(
        Bucket=settings.bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
    )
    raw_path = f"s3a://{settings.bucket}/{key}"
    print(f"[ibge {dataset} {periodo}] raw gravado: {raw_path}")
    return raw_path


def raw_to_bronze(
    spark: SparkSession, dataset: str, periodo: str, raw_path: str, run_id: str
) -> None:
    """Lê o raw com Spark (schema explícito) e grava a partição bronze do dataset/ano."""
    source_url = API_URL.format(
        agregado=DATASETS[dataset]["agregado"],
        periodo=periodo,
        variavel=DATASETS[dataset]["variavel"],
    )
    ingested_at = datetime.now(timezone.utc).isoformat()

    raw = (
        spark.read.schema(BRONZE_RAW_SCHEMA)
        # multiLine: a resposta da API é um único array JSON, não JSON-lines
        .option("multiLine", "true")
        .json(raw_path)
    )

    df = (
        raw.select("unidade", F.explode("resultados").alias("resultado"))
        .select("unidade", F.explode("resultado.series").alias("s"))
        # explode de MapType devolve as colunas key/value
        .select(
            "unidade",
            F.col("s.localidade.id").alias("municipio_id"),
            F.col("s.localidade.nome").alias("municipio_nome"),
            F.explode("s.serie"),
        )
        .withColumnRenamed("key", "ano")
        .withColumnRenamed("value", "valor")
        .withColumn("dataset", F.lit(dataset))
        .withColumn("ano", F.col("ano").cast(IntegerType()))
        # Linhagem - mesmo padrão do projeto inteiro:
        .withColumn("_source_url", F.lit(source_url))
        .withColumn("_raw_path", F.lit(raw_path))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.lit(ingested_at))
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        # replaceWhere = substitui só a partição deste dataset/período,
        # preservando o resto da tabela -> idempotente por dataset-ano.
        .option(
            "replaceWhere",
            f"dataset = '{dataset}' AND ano IN ({periodo.replace('|', ',')})",
        )
        .partitionBy("dataset", "ano")
        .save(f"{settings.bronze_zone}/ibge_sidra")
    )
    print(f"[ibge {dataset} {periodo}] bronze gravado (partição dataset={dataset})")


def ingest_dataset(spark: SparkSession, dataset: str, anos: list[int], run_id: str) -> None:
    cfg = DATASETS[dataset]
    periodo = _periodo_arg(anos)
    payload = fetch_dataset(cfg["agregado"], cfg["variavel"], periodo)
    raw_path = write_raw(dataset, periodo, payload, run_id)
    raw_to_bronze(spark, dataset, periodo, raw_path, run_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestão IBGE SIDRA -> raw -> bronze")
    parser.add_argument("--anos", nargs="+", type=int, required=True)
    parser.add_argument(
        "--datasets", nargs="+", default=list(DATASETS), choices=list(DATASETS)
    )
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:12]
    spark = get_spark(app_name="ibge-sidra-ingestion")
    print(f">>> Ingestão IBGE SIDRA | anos={args.anos} datasets={args.datasets} run_id={run_id}")

    failures: list[str] = []
    for dataset in args.datasets:
        try:
            ingest_dataset(spark, dataset, args.anos, run_id)
        except Exception as exc:  # um dataset ruim não derruba o resto
            print(f"[ERRO] {dataset}: {exc}")
            failures.append(dataset)

    if failures:
        raise SystemExit(f">>> Concluído COM FALHAS: {failures}")
    print(">>> Concluído sem falhas.")


if __name__ == "__main__":
    main()