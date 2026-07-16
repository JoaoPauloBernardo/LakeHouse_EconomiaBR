"""
Ingestão: Comex Stat (MDIC) -> RAW (CSV cru) -> BRONZE (Delta).

Este é o dataset PESADO do projeto: um arquivo EXP_YYYY.csv tem centenas
de MB; a série 1997-hoje soma dezenas de GB e centenas de milhões de
linhas. É ele que justifica Spark.

Diferenças-chave vs a ingestão do BCB (bcb_sgs.py):

1. DOWNLOAD EM STREAMING. O arquivo não cabe (confortavelmente) em RAM.
   `requests` com stream=True + upload multipart do boto3 movem o arquivo
   da fonte pro MinIO em pedaços de 16MB — uso de memória constante,
   não importa se o arquivo tem 100MB ou 5GB.

2. SPARK LÊ DO RAW, NÃO DA FONTE. O bronze é construído a partir do
   arquivo já no MinIO. Se o parse falhar, reprocessa do raw sem baixar
   de novo (o download é a parte lenta e frágil).

3. PARTICIONAMENTO POR ano + fluxo no bronze. Consultas típicas filtram
   por ano — partition pruning faz o Spark ler só os arquivos daquele
   ano em vez de escanear a base inteira.

4. INGESTÃO INCREMENTAL POR ANO. Cada execução processa uma lista de
   anos. O modo padrão substitui a partição do ano (replaceWhere), o que
   torna o job IDEMPOTENTE por ano: rodar 2x o mesmo ano não duplica.
   (Exceção ao "bronze é append-only" do BCB — aqui a unidade natural de
   reprocessamento é o ano-arquivo inteiro, e replaceWhere é a forma
   idempotente de expressar isso. Vale um ADR.)

Fonte oficial (página "Base de dados bruta" do MDIC):
https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta
Layout NCM: CO_ANO;CO_MES;CO_NCM;CO_UNID;CO_PAIS;SG_UF_NCM;CO_VIA;CO_URF;QT_ESTAT;KG_LIQUIDO;VL_FOB
Separador ';', encoding latin-1.
"""
from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone

import boto3
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
from tenacity import retry, stop_after_attempt, wait_exponential

from src.common.config import settings
from src.common.spark_session import get_spark

# ----------------------------------------------------------------------------
# Constantes
# ----------------------------------------------------------------------------
# ATENÇÃO: confirme o padrão de URL na página oficial antes da 1ª execução —
# governo muda host/path de tempos em tempos. Padrão vigente:
BASE_URL = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/{flow}_{year}.csv"

FLOWS = ("EXP", "IMP")           # exportação e importação
CHUNK_SIZE = 16 * 1024 * 1024    # 16MB por parte do multipart upload

# Schema explícito, tudo string no bronze (tipagem forte é na silver).
# Nomes idênticos aos do CSV pra facilitar conferência com a fonte.
COMEX_COLUMNS = [
    "CO_ANO", "CO_MES", "CO_NCM", "CO_UNID", "CO_PAIS", "SG_UF_NCM",
    "CO_VIA", "CO_URF", "QT_ESTAT", "KG_LIQUIDO", "VL_FOB",
]
BRONZE_SCHEMA = StructType(
    [StructField(c, StringType(), nullable=True) for c in COMEX_COLUMNS]
)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_user,
        aws_secret_access_key=settings.minio_password,
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=5, min=5, max=60))
def download_to_raw(flow: str, year: int, run_id: str) -> str:
    """Baixa o CSV da fonte direto pro MinIO em streaming multipart.

    O arquivo nunca fica inteiro em memória nem em disco local:
    fonte --(16MB por vez)--> MinIO. Isso escala pra qualquer tamanho.
    """
    url = BASE_URL.format(flow=flow, year=year)
    # Prefixo "raw/" dentro do bucket único do lakehouse (settings.bucket) -
    # não existe bucket "raw" separado no MinIO, só o "lakehouse" criado
    # pelo minio-init (mesmo padrão de bronze_zone/silver_zone/gold_zone).
    key = f"raw/comex_stat/{flow}/{year}/{run_id}.csv"
    s3 = _s3_client()

    with requests.get(url, stream=True, timeout=(30, 300)) as resp:
        resp.raise_for_status()

        mpu = s3.create_multipart_upload(Bucket=settings.bucket, Key=key)
        parts, part_number, buffer = [], 1, b""
        try:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                buffer += chunk
                if len(buffer) >= CHUNK_SIZE:
                    part = s3.upload_part(
                        Bucket=settings.bucket, Key=key, UploadId=mpu["UploadId"],
                        PartNumber=part_number, Body=buffer,
                    )
                    parts.append({"ETag": part["ETag"], "PartNumber": part_number})
                    part_number += 1
                    buffer = b""
            if buffer:  # última parte (multipart aceita a final < 5MB)
                part = s3.upload_part(
                    Bucket=settings.bucket, Key=key, UploadId=mpu["UploadId"],
                    PartNumber=part_number, Body=buffer,
                )
                parts.append({"ETag": part["ETag"], "PartNumber": part_number})

            s3.complete_multipart_upload(
                Bucket=settings.bucket, Key=key, UploadId=mpu["UploadId"],
                MultipartUpload={"Parts": parts},
            )
        except Exception:
            # Aborta upload parcial pra não deixar lixo cobrável/órfão no storage
            s3.abort_multipart_upload(Bucket=settings.bucket, Key=key, UploadId=mpu["UploadId"])
            raise

    raw_path = f"s3a://{settings.bucket}/{key}"
    print(f"[{flow} {year}] raw gravado: {raw_path}")
    return raw_path


def raw_to_bronze(flow: str, year: int, raw_path: str, run_id: str) -> None:
    """Lê o CSV do raw com Spark e grava a partição bronze do ano/fluxo."""
    spark = get_spark(app_name=f"bronze-comex-{flow}-{year}")

    df = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("encoding", "latin1")      # dado público BR raramente é UTF-8
        .schema(BRONZE_SCHEMA)             # nunca inferSchema em produção
        .csv(raw_path)
        .withColumn("fluxo", F.lit(flow))
        .withColumn("ano", F.col("CO_ANO").cast(IntegerType()))
        # Linhagem — mesmo padrão do projeto inteiro:
        .withColumn("_source_url", F.lit(BASE_URL.format(flow=flow, year=year)))
        .withColumn("_raw_path", F.lit(raw_path))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.lit(datetime.now(timezone.utc).isoformat()))
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        # replaceWhere = "overwrite cirúrgico": substitui SÓ a partição
        # deste ano/fluxo, preservando todo o resto da tabela.
        # É isso que torna o job idempotente e o backfill trivial.
        .option(
            "replaceWhere", f"ano = {year} AND fluxo = '{flow}'"
        )
        .partitionBy("ano", "fluxo")
        .save(f"{settings.bronze_zone}/comex_stat")
    )
    print(f"[{flow} {year}] bronze gravado (partição ano={year}/fluxo={flow})")


def ingest_year(flow: str, year: int, run_id: str) -> None:
    raw_path = download_to_raw(flow, year, run_id)
    raw_to_bronze(flow, year, raw_path, run_id)


def main() -> None:
    # CLI: python -m src.ingestion.comex_stat --years 2022 2023 2024 --flows EXP
    # Anos como argumento (e não hardcoded) = mesmo código serve pra
    # carga inicial (1997-2026), backfill de um ano ou carga mensal.
    parser = argparse.ArgumentParser(description="Ingestão Comex Stat -> raw -> bronze")
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--flows", nargs="+", default=list(FLOWS), choices=FLOWS)
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:12]
    print(f">>> Ingestão Comex Stat | anos={args.years} fluxos={args.flows} run_id={run_id}")

    failures: list[str] = []
    for year in args.years:
        for flow in args.flows:
            try:
                ingest_year(flow, year, run_id)
            except Exception as exc:  # um ano ruim não derruba o backfill inteiro
                print(f"[ERRO] {flow} {year}: {exc}")
                failures.append(f"{flow}_{year}")

    if failures:
        raise SystemExit(f">>> Concluído COM FALHAS: {failures}")
    print(">>> Concluído sem falhas.")


if __name__ == "__main__":
    main()