import pytest

from src.quality.expectations import (
    DataQualityError,
    expect_column_between,
    expect_not_null,
    expect_unique,
    run_gate,
)


def test_expect_not_null_passa_sem_nulo(spark):
    df = spark.createDataFrame([(1,), (2,)], ["valor"])
    r = expect_not_null(df, "valor")
    assert r.passed


def test_expect_not_null_falha_acima_da_tolerancia(spark):
    df = spark.createDataFrame([(1,), (None,), (None,)], "valor int")
    r = expect_not_null(df, "valor", max_null_ratio=0.1)
    assert not r.passed
    assert "2/3" in r.detail


def test_expect_not_null_respeita_tolerancia_configurada(spark):
    df = spark.createDataFrame([(1,), (2,), (None,)], "valor int")
    r = expect_not_null(df, "valor", max_null_ratio=0.5)
    assert r.passed  # 1/3 nulo, dentro dos 50% permitidos


def test_expect_unique_passa_sem_duplicata(spark):
    df = spark.createDataFrame([("a", 1), ("b", 2)], ["chave", "valor"])
    r = expect_unique(df, ["chave"])
    assert r.passed


def test_expect_unique_falha_com_duplicata(spark):
    df = spark.createDataFrame([("a", 1), ("a", 2)], ["chave", "valor"])
    r = expect_unique(df, ["chave"])
    assert not r.passed
    assert "1 linha" in r.detail


def test_expect_column_between_falha_abaixo_do_minimo(spark):
    df = spark.createDataFrame([(-5,), (10,)], "valor int")
    r = expect_column_between(df, "valor", min_value=0)
    assert not r.passed


def test_expect_column_between_ignora_nulo(spark):
    """Null não é 'fora de faixa' - quem checa presença de dado é expect_not_null."""
    df = spark.createDataFrame([(None,), (10,)], "valor int")
    r = expect_column_between(df, "valor", min_value=0, max_value=100)
    assert r.passed


def test_run_gate_levanta_quando_algum_check_falha(spark):
    df = spark.createDataFrame([(-5,), (10,)], "valor int")
    with pytest.raises(DataQualityError):
        run_gate(df, [lambda d: expect_column_between(d, "valor", min_value=0)], stage="teste")


def test_run_gate_nao_levanta_quando_tudo_passa(spark):
    df = spark.createDataFrame([(5,), (10,)], "valor int")
    run_gate(df, [lambda d: expect_column_between(d, "valor", min_value=0)], stage="teste")
