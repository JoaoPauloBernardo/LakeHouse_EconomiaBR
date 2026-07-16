"""
Transformação: BRONZE -> SILVER.

A silver é onde o dado vira CONFIÁVEL: tipado, deduplicado, validado e
— a palavra-chave — IDEMPOTENTE. Rodar este job 1x ou 10x produz
exatamente o mesmo estado final. Sem isso, todo retry do Airflow vira
uma fonte de duplicata (classe de bug clássica em pipeline).

Duas estratégias de idempotência DIFERENTES, de propósito
(cada dataset pede a ferramenta certa — isso é o ADR-0002):

1. BCB -> MERGE (upsert linha a linha)
   O bronze do BCB é append-only: cada execução appenda a série INTEIRA
   de novo. Logo a silver recebe muita linha repetida e, às vezes,
   valores REVISADOS (o BCB revisa índices retroativamente).
   Chave de negócio: (serie, data).
   MERGE: se a chave existe -> atualiza; se não existe -> insere.
   Resultado: 1 linha por (serie, data), sempre com o valor mais recente.

2. Comex -> substituição de partição (replaceWhere)
   O Comex não tem chave natural de linha (são registros estatísticos
   agregados). A unidade de reprocessamento é o ARQUIVO-ANO inteiro.
   MERGE aqui seria caríssimo (comparar centenas de milhões de linhas)
   e sem sentido. Idempotência por partição: reescreve ano/fluxo inteiro.

Lição pra entrevista: "idempotência" não é uma técnica, é uma PROPRIEDADE.
MERGE e replaceWhere são duas formas de alcançá-la, com custos diferentes.
"""
from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

from src.common.config import settings
from src.common.spark_session import get_spark
from src.quality.expectations import expect_column_between, expect_not_null, expect_unique, run_gate

BCB_BRONZE = f"{settings.bronze_zone}/bcb_sgs"
BCB_SILVER = f"{settings.silver_zone}/bcb_sgs"
COMEX_BRONZE = f"{settings.bronze_zone}/comex_stat"
COMEX_SILVER = f"{settings.silver_zone}/comex_stat"


# ============================================================================
# BCB: bronze append-only -> silver via MERGE
# ============================================================================
def transform_bcb(spark: SparkSession) -> None:
    bronze = spark.read.format("delta").load(BCB_BRONZE)

    # ------------------------------------------------------------------
    # 1) TIPAGEM. Bronze é tudo string; aqui vira dado de verdade.
    #    - data: "dd/MM/yyyy" -> DateType
    #    - valor: string -> double
    #    Valores não-parseáveis viram NULL (e não explodem o job) —
    #    a gente CONTA esses nulls e loga: silêncio é o inimigo.
    # ------------------------------------------------------------------
    typed = bronze.select(
        F.col("serie"),
        F.to_date("data", "dd/MM/yyyy").alias("data"),
        F.col("valor").cast(DoubleType()).alias("valor"),
        F.col("_ingested_at"),
        F.col("_run_id"),
    )

    bad_dates = typed.filter(F.col("data").isNull()).count()
    bad_values = typed.filter(F.col("valor").isNull()).count()
    print(f"[bcb] linhas com data inválida: {bad_dates} | valor inválido: {bad_values}")
    typed = typed.filter(F.col("data").isNotNull())

    # ------------------------------------------------------------------
    # 2) DEDUP INTRA-BATCH. O bronze tem a mesma (serie, data) repetida
    #    N vezes (uma por execução da ingestão). Antes do MERGE, reduzimos
    #    o batch a 1 linha por chave — a mais recente (_ingested_at max).
    #    row_number() sobre janela particionada pela chave de negócio é
    #    O padrão canônico de dedup em Spark. Decore esse idioma.
    # ------------------------------------------------------------------
    w = Window.partitionBy("serie", "data").orderBy(F.col("_ingested_at").desc())
    latest = (
        typed.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ------------------------------------------------------------------
    # 3) MERGE (upsert). Primeira execução: tabela não existe -> cria.
    #    Demais execuções: MERGE pela chave (serie, data).
    #    whenMatchedUpdateAll  -> chave já existe: atualiza (pega revisões)
    #    whenNotMatchedInsertAll -> chave nova: insere
    # ------------------------------------------------------------------
    # Gate de qualidade - nota: "valor" é contado como inválido acima mas
    # NÃO era descartado (só "data" era). Esse gate vira a rede de segurança
    # que falta: se algum dia aparecer valor nulo de verdade, o job para em
    # vez de deixar passar silencioso pra silver.
    run_gate(
        latest,
        [
            lambda d: expect_not_null(d, "valor"),
            lambda d: expect_unique(d, ["serie", "data"]),
        ],
        stage="bcb_silver",
    )

    if not DeltaTable.isDeltaTable(spark, BCB_SILVER):
        latest.write.format("delta").partitionBy("serie").save(BCB_SILVER)
        print(f"[bcb] silver criada: {latest.count()} linhas")
        return

    silver = DeltaTable.forPath(spark, BCB_SILVER)
    (
        silver.alias("s")
        .merge(
            latest.alias("b"),
            "s.serie = b.serie AND s.data = b.data",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("[bcb] MERGE concluído")


# ============================================================================
# Comex: bronze particionado -> silver por substituição de partição
# ============================================================================
def transform_comex(spark: SparkSession, years: list[int]) -> None:
    for year in years:
        bronze = (
            spark.read.format("delta")
            .load(COMEX_BRONZE)
            # Filtro na coluna de partição -> partition pruning:
            # o Spark lê SÓ os arquivos de ano=YYYY, não a tabela toda.
            .filter(F.col("ano") == year)
        )

        typed = bronze.select(
            F.col("ano"),
            F.col("fluxo"),
            F.col("CO_MES").cast(IntegerType()).alias("mes"),
            F.col("CO_NCM").alias("ncm"),
            F.col("CO_UNID").alias("cod_unidade"),
            F.col("CO_PAIS").alias("cod_pais"),
            F.col("SG_UF_NCM").alias("uf"),
            F.col("CO_VIA").cast(IntegerType()).alias("cod_via"),
            F.col("CO_URF").alias("cod_urf"),
            F.col("QT_ESTAT").cast(LongType()).alias("qt_estatistica"),
            F.col("KG_LIQUIDO").cast(LongType()).alias("kg_liquido"),
            F.col("VL_FOB").cast(LongType()).alias("vl_fob"),
            F.col("_run_id"),
            F.col("_ingested_at"),
        )

        # Guard-rails mínimos (a semana 4 do Fabio industrializa isso):
        # mês válido e valor FOB não-negativo. Linhas ruins são contadas
        # e descartadas — nunca descartadas em silêncio.
        valid = typed.filter(
            F.col("mes").between(1, 12) & (F.col("vl_fob") >= 0)
        )
        dropped = typed.count() - valid.count()
        if dropped:
            print(f"[comex {year}] linhas descartadas por validação: {dropped}")

        run_gate(
            valid,
            [
                lambda d: expect_not_null(d, "vl_fob"),
                lambda d: expect_column_between(d, "vl_fob", min_value=0),
                lambda d: expect_column_between(d, "mes", min_value=1, max_value=12),
            ],
            stage=f"comex_silver_{year}",
        )

        (
            valid.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"ano = {year}")
            .partitionBy("ano", "fluxo")
            .save(COMEX_SILVER)
        )
        print(f"[comex {year}] silver gravada (partição ano={year})")


def main() -> None:
    # --years só afeta o Comex (BCB é pequeno, processa inteiro sempre)
    parser = argparse.ArgumentParser(description="Bronze -> Silver")
    parser.add_argument("--years", nargs="+", type=int, default=[])
    parser.add_argument(
        "--datasets", nargs="+", default=["bcb", "comex"], choices=["bcb", "comex"]
    )
    args = parser.parse_args()

    spark = get_spark(app_name="bronze-to-silver")

    if "bcb" in args.datasets:
        transform_bcb(spark)
    if "comex" in args.datasets:
        if not args.years:
            raise SystemExit("Comex requer --years (ex: --years 2023 2024)")
        transform_comex(spark, args.years)

    print(">>> Bronze -> Silver concluído")


if __name__ == "__main__":
    main()