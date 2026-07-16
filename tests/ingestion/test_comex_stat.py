from pathlib import Path

from chispa import assert_df_equality
from pyspark.sql import Row

from src.ingestion import comex_stat

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_raw_to_bronze_parseia_csv_e_deriva_ano_fluxo(spark, local_zones):
    raw_path = str(FIXTURES / "comex_exp_sample.csv")

    comex_stat.raw_to_bronze("EXP", 2023, raw_path, "run-teste")

    bronze = spark.read.format("delta").load(f"{local_zones}/bronze/comex_stat")
    actual = bronze.select("ano", "fluxo", "SG_UF_NCM", "VL_FOB").orderBy("SG_UF_NCM")

    expected = spark.createDataFrame(
        [
            Row(2023, "EXP", "RJ", "15000"),
            Row(2023, "EXP", "SP", "25000"),
        ],
        "ano int, fluxo string, SG_UF_NCM string, VL_FOB string",
    ).orderBy("SG_UF_NCM")

    assert_df_equality(actual, expected, ignore_nullable=True)


def test_raw_to_bronze_e_idempotente_via_replacewhere(spark, local_zones):
    """Diferente do BCB (append-only), o Comex usa replaceWhere por
    ano+fluxo: reprocessar o mesmo ano/fluxo NÃO duplica linha."""
    raw_path = str(FIXTURES / "comex_exp_sample.csv")

    comex_stat.raw_to_bronze("EXP", 2023, raw_path, "run-1")
    comex_stat.raw_to_bronze("EXP", 2023, raw_path, "run-2")

    bronze = spark.read.format("delta").load(f"{local_zones}/bronze/comex_stat")
    assert bronze.count() == 2  # não dobrou pra 4
