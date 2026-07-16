"""
Transformação: SILVER -> GOLD.

Junta três silvers de origens diferentes numa única tabela pronta pra
consumo: exportação por UF/ano, cruzada com câmbio médio anual (BCB) e
população/PIB per capita (IBGE).

Grão da tabela: 1 linha por (uf, ano).

Por que overwrite total (sem replaceWhere) aqui, diferente da silver:
gold é 100% DERIVADA das silvers - não tem identidade própria, é só uma
pergunta de negócio pré-calculada. Reprocessar tudo do zero a cada run é
mais simples e não corre risco de ficar com partição velha calculada com
uma versão antiga da lógica de agregação (o problema que replaceWhere
resolveria não existe aqui, porque a fonte da verdade são as silvers).

Fontes (silver):
- comex_stat: 1 linha por registro estatístico de comércio exterior.
  Filtra fluxo='EXP' e soma vl_fob por (uf, ano).
- bcb_sgs: série 1 = câmbio dólar venda, diária. Média por ano
  (sem dimensão de UF - é o mesmo valor nacional pra todo UF no ano).
- ibge_sidra: população e PIB municipal, por município. Soma por
  (uf, ano) pra cada dataset, depois pivota lado a lado.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import settings
from src.common.spark_session import get_spark

COMEX_SILVER = f"{settings.silver_zone}/comex_stat"
BCB_SILVER = f"{settings.silver_zone}/bcb_sgs"
IBGE_SILVER = f"{settings.silver_zone}/ibge_sidra"
GOLD_PATH = f"{settings.gold_zone}/exportacao_uf_ano"

BCB_SERIE_CAMBIO_DOLAR_VENDA = 1


def _exportacao_por_uf_ano(spark: SparkSession):
    comex = spark.read.format("delta").load(COMEX_SILVER)
    return (
        comex.filter(F.col("fluxo") == "EXP")
        .groupBy("uf", "ano")
        .agg(F.sum("vl_fob").alias("exportacao_fob_usd"))
    )


def _cambio_medio_por_ano(spark: SparkSession):
    bcb = spark.read.format("delta").load(BCB_SILVER)
    return (
        bcb.filter(F.col("serie") == BCB_SERIE_CAMBIO_DOLAR_VENDA)
        .withColumn("ano", F.year("data"))
        .groupBy("ano")
        .agg(F.avg("valor").alias("cambio_medio_venda"))
    )


def _populacao_pib_por_uf_ano(spark: SparkSession):
    ibge = spark.read.format("delta").load(IBGE_SILVER)
    agregado = (
        ibge.groupBy("uf", "ano")
        .pivot("dataset", ["populacao", "pib_municipal"])
        .agg(F.sum("valor"))
        .withColumnRenamed("populacao", "populacao")
        .withColumnRenamed("pib_municipal", "pib_total_mil_reais")
    )
    # PIB per capita em reais: PIB municipal vem em Mil Reais (IBGE),
    # populacao em pessoas -> *1000 converte pra reais antes de dividir.
    return agregado.withColumn(
        "pib_per_capita_reais",
        F.round((F.col("pib_total_mil_reais") * 1000) / F.col("populacao"), 2),
    )


def build_gold(spark: SparkSession) -> None:
    exportacao = _exportacao_por_uf_ano(spark)
    cambio = _cambio_medio_por_ano(spark)
    populacao_pib = _populacao_pib_por_uf_ano(spark)

    gold = (
        exportacao.join(cambio, on="ano", how="left")
        .join(populacao_pib, on=["uf", "ano"], how="left")
        .select(
            "uf",
            "ano",
            "exportacao_fob_usd",
            "cambio_medio_venda",
            "populacao",
            "pib_total_mil_reais",
            "pib_per_capita_reais",
        )
    )

    (
        gold.write.format("delta")
        .mode("overwrite")
        .partitionBy("ano")
        .save(GOLD_PATH)
    )
    print(f"[gold] exportacao_uf_ano gravada: {gold.count()} linhas")


def main() -> None:
    spark = get_spark(app_name="silver-to-gold")
    build_gold(spark)
    print(">>> Silver -> Gold concluído")


if __name__ == "__main__":
    main()
