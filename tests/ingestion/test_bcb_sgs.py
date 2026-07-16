from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from chispa import assert_df_equality
from pyspark.sql import Row

from src.ingestion import bcb_sgs

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_url_serie_diaria_inclui_janela_de_dez_anos():
    """Regressão do bug real: sem dataInicial a API do BCB devolve 406
    pras séries diárias (câmbio, Selic) - só aceita janela <= 10 anos."""
    url = bcb_sgs._url_serie(1, diaria=True)
    assert "dataInicial=" in url
    assert "bcdata.sgs.1" in url


def test_url_serie_mensal_nao_tem_janela():
    """IPCA/PIB mensal não tem a restrição de janela - pede o histórico inteiro."""
    url = bcb_sgs._url_serie(433, diaria=False)
    assert "dataInicial" not in url


def test_fetch_series_rejeita_resposta_que_nao_e_json():
    """Regressão do bug real: o BCB às vezes devolve uma página HTML de erro
    com status 200. Sem essa checagem, a lixo vira linha de bronze em
    silêncio (tenacity só reage a exception, não a payload inválido).
    Chama a função SEM o decorator @retry (via __wrapped__) pra não
    esperar o backoff de verdade (5-60s) num teste unitário."""
    resp = Mock(content=b"<html>Requisi\xc3\xa7\xc3\xa3o inv\xc3\xa1lida!</html>")
    resp.raise_for_status = Mock()
    with patch("src.ingestion.bcb_sgs.requests.get", return_value=resp):
        with pytest.raises(ValueError):
            bcb_sgs.fetch_series.__wrapped__(432, diaria=True)


def test_raw_to_bronze_parseia_serie_e_carimba_linhagem(spark, local_zones):
    raw_path = str(FIXTURES / "bcb_serie_sample.json")

    bcb_sgs.raw_to_bronze(spark, 1, True, raw_path, "run-teste")

    bronze = spark.read.format("delta").load(f"{local_zones}/bronze/bcb_sgs")
    actual = bronze.select("serie", "data", "valor").orderBy("data")

    expected = spark.createDataFrame(
        [
            Row(1, "02/01/2024", "4.9000"),
            Row(1, "03/01/2024", "4.9100"),
            Row(1, "04/01/2024", "4.8950"),
        ],
        "serie int, data string, valor string",
    ).orderBy("data")

    assert_df_equality(actual, expected, ignore_nullable=True)
    assert bronze.collect()[0]._run_id == "run-teste"


def test_raw_to_bronze_e_append_only(spark, local_zones):
    """Diferente do Comex (replaceWhere), a bronze do BCB é append-only de
    propósito: rodar a ingestão 2x pra mesma série NÃO deve substituir, e
    sim empilhar (a silver é quem deduplica via MERGE)."""
    raw_path = str(FIXTURES / "bcb_serie_sample.json")

    bcb_sgs.raw_to_bronze(spark, 1, True, raw_path, "run-1")
    bcb_sgs.raw_to_bronze(spark, 1, True, raw_path, "run-2")

    bronze = spark.read.format("delta").load(f"{local_zones}/bronze/bcb_sgs")
    assert bronze.count() == 6  # 3 linhas x 2 execuções, nada foi substituído
