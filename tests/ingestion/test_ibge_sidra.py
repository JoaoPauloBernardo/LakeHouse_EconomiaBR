from pathlib import Path

from chispa import assert_df_equality
from pyspark.sql import Row

from src.ingestion import ibge_sidra

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_periodo_arg_ordena_anos_e_junta_com_pipe():
    assert ibge_sidra._periodo_arg([2022, 2020, 2021]) == "2020|2021|2022"


def test_periodo_arg_ano_unico():
    assert ibge_sidra._periodo_arg([2021]) == "2021"


def test_raw_to_bronze_parseia_json_aninhado_da_api(spark, local_zones):
    """A API do IBGE devolve município aninhado dentro de resultados[].series[]
    com o ano como CHAVE de um map ("serie"). Esse é o parsing que
    justifica o schema explícito com MapType em vez de inferSchema."""
    raw_path = str(FIXTURES / "ibge_populacao_sample.json")

    ibge_sidra.raw_to_bronze(spark, "populacao", "2021", raw_path, "run-teste")

    bronze = spark.read.format("delta").load(f"{local_zones}/bronze/ibge_sidra")
    actual = bronze.select(
        "dataset", "municipio_id", "municipio_nome", "ano", "valor", "unidade"
    ).orderBy("municipio_id")

    expected = spark.createDataFrame(
        [
            Row("populacao", "1100015", "Alta Floresta D'Oeste - RO", 2021, "22516", "Pessoas"),
            Row("populacao", "3304557", "Rio de Janeiro - RJ", 2021, "6775561", "Pessoas"),
            Row("populacao", "3550308", "São Paulo - SP", 2021, "12396372", "Pessoas"),
        ],
        "dataset string, municipio_id string, municipio_nome string, ano int, valor string, unidade string",
    ).orderBy("municipio_id")

    assert_df_equality(actual, expected, ignore_nullable=True)

    # linhagem: confere que colunas de _run_id/_raw_path foram carimbadas
    linhagem = bronze.filter(bronze.municipio_id == "3550308").collect()[0]
    assert linhagem._run_id == "run-teste"
    assert linhagem._raw_path == raw_path
