"""
Ingestao: API SGS do Banco Central -> RAW (JSON cru) -> BRONZE (Delta).

Decisao de arquitetura importante:
POR QUE O DOWNLOAD NAO USA SPARK?

Porque ingestao de API e um problema de I/O, nao de processamento.
Sao poucos MB por serie. Subir um cluster distribuido pra baixar JSON
e usar bazuca pra matar mosquito. Python puro + boto3 resolve. O Spark
so entra depois, pra ler o raw e escrever a bronze tipada em Delta -
e ai que ele brilha (transformar dados em escala, silver/gold tambem).

Principios da camada bronze (mesmo contrato de comex_stat.py):
- Delta, append-only: cada execucao reappenda a serie inteira, porque
  o BCB revisa valores retroativamente e a API nao expoe "o que mudou".
  A silver (bronze_to_silver.py) resolve isso com dedup + MERGE por
  (serie, data) - ver ADR-0002. E por isso que bronze aqui NAO usa
  replaceWhere feito o Comex: a unidade de reprocessamento e histórica
  por natureza, nao por particao fechada.
- Schema explicito, nunca inferSchema.
- Colunas de linhagem: _source_url, _raw_path, _run_id, _ingested_at.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime, timezone

import boto3
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from tenacity import retry, stop_after_attempt, wait_exponential

from src.common.config import settings
from src.common.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingestion.bcb_sgs")

# Series do SGS que alimentam o projeto.
# Catalogo completo: https://www3.bcb.gov.br/sgspub
#
# "diaria": True identifica series de periodicidade diaria (cambio, Selic).
# Isso importa porque a API do BCB REJEITA (406) consulta de serie diaria
# sem parametro dataInicial, e limita a janela a no maximo 10 anos - regra
# de negocio da API, nao bug nosso (a mensagem de erro explica isso). Series
# mensais (IPCA, PIB) nao tem essa restricao, pedimos o historico inteiro.
SERIES = {
    1: {"nome": "cambio_dolar_venda", "diaria": True},
    432: {"nome": "selic_meta", "diaria": True},
    433: {"nome": "ipca_variacao_mensal", "diaria": False},
    4380: {"nome": "pib_mensal_ibcbr", "diaria": False},
}

API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json"
TIMEOUT = 30
JANELA_DIARIA_ANOS = 10  # limite imposto pela própria API do BCB


def _url_serie(code: int, diaria: bool) -> str:
    url = API_URL.format(code=code)
    if diaria:
        data_inicial = date.today().replace(year=date.today().year - JANELA_DIARIA_ANOS)
        url += f"&dataInicial={data_inicial:%d/%m/%Y}"
    return url


# A API devolve um array json no formato [{"data": "dd/mm/yyyy", "valor": "x.xx"}, ...]
BRONZE_RAW_SCHEMA = StructType(
    [
        StructField("data", StringType()),
        StructField("valor", StringType()),
    ]
)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_user,
        aws_secret_access_key=settings.minio_password,
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=5, min=5, max=60))
def fetch_series(code: int, diaria: bool) -> bytes:
    """Baixa uma serie completa (ou os ultimos N anos, se diaria) do SGS."""
    url = _url_serie(code, diaria)
    log.info("Baixando serie %s: %s", code, url)
    resp = requests.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    # status 200 nao garante corpo valido: o BCB as vezes devolve uma pagina
    # HTML de erro de um sistema legado com status 200. Sem essa checagem o
    # tenacity nunca reage (so retenta em cima de exception) e lixo vira bronze.
    try:
        json.loads(resp.content)
    except ValueError as exc:
        raise ValueError(f"serie {code}: resposta não é JSON válido") from exc
    return resp.content


def write_raw(code: int, payload: bytes, run_id: str) -> str:
    """Grava o JSON exatamente como veio da API. Zero transformacao."""
    ingestion_date = date.today().isoformat()
    key = f"raw/bcb_sgs/serie={code}/ingestion_date={ingestion_date}/{run_id}.json"
    _s3_client().put_object(
        Bucket=settings.bucket,
        Key=key,
        Body=payload,
        ContentType="application/json",
    )
    raw_path = f"s3a://{settings.bucket}/{key}"
    log.info("Raw OK -> %s", raw_path)
    return raw_path


def raw_to_bronze(
    spark: SparkSession, code: int, diaria: bool, raw_path: str, run_id: str
) -> None:
    """Le o raw com Spark (schema explicito) e faz APPEND na bronze da serie."""
    source_url = _url_serie(code, diaria)
    ingested_at = datetime.now(timezone.utc).isoformat()

    raw = (
        spark.read.schema(BRONZE_RAW_SCHEMA)
        .option("multiLine", "true")
        .json(raw_path)
    )

    df = (
        raw.withColumn("serie", F.lit(code))
        # Linhagem - mesmo padrao do projeto inteiro:
        .withColumn("_source_url", F.lit(source_url))
        .withColumn("_raw_path", F.lit(raw_path))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.lit(ingested_at))
    )

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("serie")
        .save(f"{settings.bronze_zone}/bcb_sgs")
    )
    log.info("Bronze OK -> %s (serie=%s, %d linhas)", f"{settings.bronze_zone}/bcb_sgs", code, df.count())


def ingest_series(spark: SparkSession, code: int, diaria: bool, run_id: str) -> None:
    payload = fetch_series(code, diaria)
    raw_path = write_raw(code, payload, run_id)
    raw_to_bronze(spark, code, diaria, raw_path, run_id)


def run() -> None:
    """Ingesta todas as series do catalogo. Falha de uma nao derruba as outras."""
    run_id = uuid.uuid4().hex[:12]
    spark = get_spark(app_name="bcb-sgs-ingestion")

    failures = []
    for code, meta in SERIES.items():
        try:
            ingest_series(spark, code, meta["diaria"], run_id)
        except Exception:
            log.exception("Falha na serie %s (%s)", code, meta["nome"])
            failures.append(code)

    if failures:
        # Levantar erro no final = o orquestrador (Airflow, depois) marca
        # a task como falha e alerta, mas as series boas ja foram salvas.
        raise RuntimeError(f"Falha nas series: {failures}")


if __name__ == "__main__":
    run()
