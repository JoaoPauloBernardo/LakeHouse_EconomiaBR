"""
Ingestao: API SGS do Banco Central -> camada BRONZE.

Decisao de arquitetura importante:
POR QUE A INGESTAO NAO USA SPARK?

Porque ingestao de API é um problema de I/O, nao de processamento.
Sao poucos MB por serie. Subir um cluster distribuido pra baixar JSON
é usar bazuca pra matar mosquito. Python puro + boto3 resolve, e o
Spark entra onde ele brilha: transformar dados EM ESCALA (silver/gold).

Principios da camada bronze:
- Guarda o dado CRU, byte a byte como veio da fonte. Zero transformacao.
- Imutavel e append-only: nunca sobrescreve, so adiciona.
- Particionado por data de ingestao -> reprocessamento e auditoria faceis.
- Se o silver quebrar, a bronze e o "backup da verdade" pra reprocessar
  sem bater na API de novo.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone

import boto3
import requests

from src.common.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingestion.bcb_sgs")

# Series do SGS que alimentam o projeto.
# Catalogo completo: https://www3.bcb.gov.br/sgspub
SERIES = {
    1: "cambio_dolar_venda",      # diaria
    432: "selic_meta",            # diaria
    433: "ipca_variacao_mensal",  # mensal
    4380: "pib_mensal_ibcbr",     # mensal
}

API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json"
TIMEOUT = 30


def fetch_series(code: int) -> list[dict]:
    """Baixa uma serie completa do SGS. Retorna o JSON cru da API."""
    url = API_URL.format(code=code)
    log.info("Baixando serie %s: %s", code, url)
    resp = requests.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    log.info("Serie %s: %d registros", code, len(data))
    return data


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_user,
        aws_secret_access_key=settings.minio_password,
    )


def write_bronze(code: int, name: str, payload: list[dict]) -> str:
    """
    Escreve o JSON cru no bronze com envelope de metadados.

    Layout (particionamento hive-style, o Spark le as particoes sozinho):
      bronze/bcb_sgs/serie=432/ingestion_date=2026-07-15/data.json

    O envelope (_meta) registra QUANDO e DE ONDE o dado veio - linhagem.
    Sem isso, seis meses depois ninguem sabe se o dado ta velho.
    """
    ingestion_date = date.today().isoformat()
    key = f"bronze/bcb_sgs/serie={code}/ingestion_date={ingestion_date}/data.json"

    envelope = {
        "_meta": {
            "source": API_URL.format(code=code),
            "series_code": code,
            "series_name": name,
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "record_count": len(payload),
        },
        "records": payload,
    }

    _s3_client().put_object(
        Bucket=settings.bucket,
        Key=key,
        Body=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    log.info("Bronze OK -> s3://%s/%s", settings.bucket, key)
    return key


def run() -> None:
    """Ingesta todas as series do catalogo. Falha de uma nao derruba as outras."""
    failures = []
    for code, name in SERIES.items():
        try:
            payload = fetch_series(code)
            write_bronze(code, name, payload)
        except Exception:
            log.exception("Falha na serie %s (%s)", code, name)
            failures.append(code)

    if failures:
        # Levantar erro no final = o orquestrador (Airflow, depois) marca
        # a task como falha e alerta, mas as series boas ja foram salvas.
        raise RuntimeError(f"Falha nas series: {failures}")


if __name__ == "__main__":
    run()
