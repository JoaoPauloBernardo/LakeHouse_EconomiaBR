"""
Transformação: BRONZE -> SILVER do IBGE SIDRA (população + PIB municipal).

Mesma ideia do BCB em bronze_to_silver.py: bronze é append/replace por
partição, então pode ter registro repetido entre execuções -> dedup +
MERGE por chave de negócio (dataset, municipio_id, ano) garante idempotência
(rodar 1x ou 10x dá o mesmo estado final).

Decisão de schema: adiciona a coluna "uf" derivada do código do município
(2 primeiros dígitos do código IBGE de 7 dígitos), via mapa explícito -
não por parsing do nome ("Ariquemes - RO"), porque o nome é texto livre e
pode ter " - " dentro dele por acidente; o código IBGE é a fonte confiável.
É essa coluna que o gold vai usar pra cruzar com o UF do Comex.

Valores "-" / ".." / "X" (convenção do IBGE pra zero/não disponível/sigiloso)
viram NULL no cast pra double - contamos e logamos, igual ao BCB.
"""
from __future__ import annotations

import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.common.config import settings
from src.common.spark_session import get_spark
from src.common.exit_codes import EXIT_QUALIDADE
from src.quality.expectations import (
    DataQualityError,
    expect_column_between,
    expect_not_null,
    expect_unique,
    run_gate,
)

IBGE_BRONZE = f"{settings.bronze_zone}/ibge_sidra"
IBGE_SILVER = f"{settings.silver_zone}/ibge_sidra"

# Código IBGE de UF (2 primeiros dígitos do código de 7 dígitos do município)
# -> sigla. Fonte: https://www.ibge.gov.br/explica/codigos-dos-municipios.php
UF_POR_CODIGO = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP",
    "17": "TO", "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB",
    "26": "PE", "27": "AL", "28": "SE", "29": "BA", "31": "MG", "32": "ES",
    "33": "RJ", "35": "SP", "41": "PR", "42": "SC", "43": "RS", "50": "MS",
    "51": "MT", "52": "GO", "53": "DF",
}


def transform_ibge(spark: SparkSession) -> None:
    bronze = spark.read.format("delta").load(IBGE_BRONZE)

    uf_map = F.create_map([F.lit(x) for pair in UF_POR_CODIGO.items() for x in pair])

    typed = bronze.select(
        F.col("dataset"),
        F.col("municipio_id"),
        F.col("municipio_nome"),
        F.col("ano"),
        F.col("valor").cast(DoubleType()).alias("valor"),
        F.col("unidade"),
        uf_map[F.substring("municipio_id", 1, 2)].alias("uf"),
        F.col("_ingested_at"),
        F.col("_run_id"),
    )

    bad_valores = typed.filter(F.col("valor").isNull()).count()
    print(f"[ibge] linhas com valor não numérico (-, .., X): {bad_valores}")
    typed = typed.filter(F.col("valor").isNotNull())

    # Dedup intra-batch: mesma (dataset, municipio_id, ano) pode aparecer em
    # execuções diferentes do bronze -> fica só a mais recente antes do MERGE.
    w = Window.partitionBy("dataset", "municipio_id", "ano").orderBy(
        F.col("_ingested_at").desc()
    )
    latest = (
        typed.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Gate de qualidade ANTES de gravar - falha o job se o dado não bate
    # com as garantias que a silver promete (sem isso, uma revisão futura
    # no parsing do bronze podia furar silenciosamente e ninguém notar).
    run_gate(
        latest,
        [
            lambda d: expect_not_null(d, "valor"),
            lambda d: expect_not_null(d, "uf"),
            lambda d: expect_unique(d, ["dataset", "municipio_id", "ano"]),
            lambda d: expect_column_between(d, "valor", min_value=0),
        ],
        stage="ibge_silver",
    )

    if not DeltaTable.isDeltaTable(spark, IBGE_SILVER):
        latest.write.format("delta").partitionBy("dataset").save(IBGE_SILVER)
        print(f"[ibge] silver criada: {latest.count()} linhas")
        return

    silver = DeltaTable.forPath(spark, IBGE_SILVER)
    (
        silver.alias("s")
        .merge(
            latest.alias("b"),
            "s.dataset = b.dataset AND s.municipio_id = b.municipio_id AND s.ano = b.ano",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("[ibge] MERGE concluído")


def main() -> None:
    spark = get_spark(app_name="ibge-bronze-to-silver")

    # Ver src/common/exit_codes.py: qualidade e falha deterministica,
    # sai com codigo 3 pro orquestrador nao gastar retry a toa.
    try:
        transform_ibge(spark)
    except DataQualityError as exc:
        print(f"[FALHA DE QUALIDADE] {exc}")
        sys.exit(EXIT_QUALIDADE)

    print(">>> IBGE Bronze -> Silver concluído")


if __name__ == "__main__":
    main()
