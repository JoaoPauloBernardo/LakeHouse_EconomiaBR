"""
Harness de benchmark: os 4 experimentos de tuning.  `make benchmark`

Roda cada experimento em configuracoes diferentes, mede o tempo e imprime
uma tabela pronta pra colar em docs/tuning.md.

PRE-REQUISITO: silver do Comex com VARIOS anos. Com 1 ano os numeros ficam
parecidos e o benchmark mente - o efeito de shuffle/skew/broadcast so
aparece com volume. (A silver ja tem 2016-2025 carregado: serve.)

METODOLOGIA (vale tanto quanto os experimentos - isso e o que se defende
em entrevista)
--------------------------------------------------------------------------

1. write.format("noop") pra forcar execucao.
   Spark e LAZY: df.filter().join() nao executa nada ate uma action. Mas
   count() mente: o otimizador percebe que voce so quer o numero de linhas
   e pula colunas inteiras (column pruning) - voce mede um plano que nao e
   o seu. `noop` executa o plano COMPLETO e joga o resultado fora. E o
   jeito canonico de benchmarkar Spark.

2. clearCache() entre medicoes + descartar a 1a rodada.
   A JVM tem warm-up (JIT compila os hot paths). A primeira execucao e
   sempre mais lenta por motivo que nao tem nada a ver com a sua config.

3. UMA variavel por vez.
   Repare que os experimentos 2 e 4 DESLIGAM o AQE. Se ele ficar ligado, ele
   conserta o problema em runtime e os numeros ficam todos iguais - voce
   mediria o AQE, nao o que queria medir. Metodo cientifico aplicado a Spark.
"""

from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import settings
from src.common.spark_session import get_spark

COMEX_SILVER = f"{settings.silver_zone}/comex_stat"

WARMUP = 1   # rodadas descartadas por medicao (JIT warm-up)
RODADAS = 2  # rodadas medidas; fica a melhor (menos ruido de vizinho)

RESULTS: list[tuple[str, str, float]] = []


def timed(spark: SparkSession, label: str, config: str, build) -> None:
    """Executa o plano com write noop, N vezes, e registra o melhor tempo.

    `build` e uma funcao que constroi o DataFrame do zero a cada rodada -
    e nao um DataFrame pronto - pra garantir que cada medicao parta do
    mesmo ponto, sem plano ja resolvido de rodada anterior.
    """
    tempos = []
    for i in range(WARMUP + RODADAS):
        spark.catalog.clearCache()
        t0 = time.perf_counter()
        build().write.format("noop").mode("overwrite").save()
        dt = time.perf_counter() - t0
        if i >= WARMUP:
            tempos.append(dt)
    melhor = min(tempos)
    RESULTS.append((label, config, melhor))
    print(f"  [{label} | {config}] {melhor:.2f}s")


# ============================================================================
# EXP 1: AQE ligado vs desligado
# AQE (Adaptive Query Execution) re-otimiza o plano EM RUNTIME usando
# estatisticas reais do shuffle: junta particoes vazias (coalesce), corrige
# skew de join, troca sort-merge por broadcast se descobrir que a tabela e
# pequena. Default ON no Spark 3.x - o experimento mostra O QUANTO ele salva.
# ============================================================================
def exp1_aqe(spark: SparkSession) -> None:
    print("\n=== EXP 1: AQE off vs on ===")

    def build():
        return (
            spark.read.format("delta").load(COMEX_SILVER)
            .groupBy("ano", "uf", "ncm")
            .agg(F.sum("vl_fob").alias("vl_fob_total"))
        )

    for enabled in ("false", "true"):
        spark.conf.set("spark.sql.adaptive.enabled", enabled)
        timed(spark, "aqe", f"adaptive={enabled}", build)
    spark.conf.set("spark.sql.adaptive.enabled", "true")  # restaura o default


# ============================================================================
# EXP 2: shuffle partitions (8 / 64 / 200)
# Cada shuffle divide o dado em N particoes. Poucas = particoes gigantes
# (spill pra disco, OOM). Muitas = milhares de micro-tasks (overhead de
# scheduling maior que o trabalho). O numero certo depende do VOLUME - por
# isso nao se chuta, se mede. (200 e default por heranca historica.)
# ============================================================================
def exp2_shuffle_partitions(spark: SparkSession) -> None:
    print("\n=== EXP 2: shuffle partitions ===")
    spark.conf.set("spark.sql.adaptive.enabled", "false")  # senao o AQE mascara

    def build():
        return (
            spark.read.format("delta").load(COMEX_SILVER)
            .groupBy("ano", "cod_pais", "ncm")
            .agg(F.sum("vl_fob").alias("total"), F.sum("kg_liquido").alias("kg"))
        )

    for n in ("8", "64", "200"):
        spark.conf.set("spark.sql.shuffle.partitions", n)
        timed(spark, "shuffle", f"partitions={n}", build)

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "8")


# ============================================================================
# EXP 3: broadcast join vs sort-merge join
# Join de tabela GIGANTE (comex) com tabela PEQUENA (dim de paises).
#  - Sort-merge: embaralha AS DUAS pelas chaves. A gigante se move. Caro.
#  - Broadcast: manda a pequena INTEIRA pra cada executor; a gigante NAO se
#    move. Ordens de magnitude mais barato.
# Fato gigante x dimensao pequena -> sempre broadcast.
# ============================================================================
def exp3_broadcast(spark: SparkSession) -> None:
    print("\n=== EXP 3: broadcast vs sort-merge join ===")
    fato = spark.read.format("delta").load(COMEX_SILVER)
    # Dim sintetica de paises (na vida real: tabela PAIS.csv do MDIC)
    dim_pais = (
        fato.select("cod_pais").distinct()
        .withColumn("regiao", F.expr("concat('regiao_', cod_pais % 5)"))
    )

    def build_sortmerge():
        return (
            spark.read.format("delta").load(COMEX_SILVER)
            .join(dim_pais, "cod_pais")
            .groupBy("regiao").agg(F.sum("vl_fob").alias("total"))
        )

    def build_broadcast():
        return (
            spark.read.format("delta").load(COMEX_SILVER)
            .join(F.broadcast(dim_pais), "cod_pais")
            .groupBy("regiao").agg(F.sum("vl_fob").alias("total"))
        )

    # threshold -1 desabilita o broadcast automatico -> forca sort-merge
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    timed(spark, "join", "sort-merge", build_sortmerge)

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    timed(spark, "join", "broadcast", build_broadcast)

    # Os PLANOS sao evidencia tao boa quanto os tempos: cole os dois no
    # docs/tuning.md mostrando SortMergeJoin vs BroadcastHashJoin.
    print("\n--- plano sort-merge (procure por SortMergeJoin) ---")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    build_sortmerge().explain()
    print("\n--- plano broadcast (procure por BroadcastHashJoin) ---")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
    build_broadcast().explain()


# ============================================================================
# EXP 4: skew + salting  (o mais bonito de mostrar)
# Comercio exterior e NATURALMENTE enviesado: China e EUA concentram uma
# fracao enorme das linhas. groupBy("cod_pais") manda todas as linhas da
# China pra UMA task. As outras 200 terminam em segundos; a da China roda
# por minutos. O JOB DEMORA O TEMPO DA TASK MAIS LENTA.
#
# SALTING: a chave `China` vira `China_0`..`China_15`. Agora 16 tasks
# dividem o trabalho; depois re-agrega os parciais. Duas agregacoes
# pequenas < uma agregacao travada num executor so.
# ============================================================================
def exp4_skew_salting(spark: SparkSession) -> None:
    print("\n=== EXP 4: skew (groupBy direto vs salting) ===")
    spark.conf.set("spark.sql.adaptive.enabled", "false")  # isola o efeito

    # Evidencia do skew: as chaves mais quentes. Printar isso e o que
    # transforma "achei que tinha skew" em "medi o skew".
    print("--- top 5 paises por volume de linhas (a fonte do skew) ---")
    (
        spark.read.format("delta").load(COMEX_SILVER)
        .groupBy("cod_pais").count().orderBy(F.desc("count")).limit(5).show()
    )

    def build_direto():
        return (
            spark.read.format("delta").load(COMEX_SILVER)
            .groupBy("cod_pais").agg(F.sum("vl_fob").alias("total"))
        )

    def build_salted():
        return (
            spark.read.format("delta").load(COMEX_SILVER)
            .withColumn("_salt", (F.rand() * 16).cast("int"))
            .groupBy("cod_pais", "_salt")
            .agg(F.sum("vl_fob").alias("parcial"))
            .groupBy("cod_pais")
            .agg(F.sum("parcial").alias("total"))
        )

    timed(spark, "skew", "groupBy direto", build_direto)
    timed(spark, "skew", "salting (16)", build_salted)

    spark.conf.set("spark.sql.adaptive.enabled", "true")


def main() -> None:
    spark = get_spark(app_name="benchmark-tuning")
    n = spark.read.format("delta").load(COMEX_SILVER).count()
    print(f"Fonte: {COMEX_SILVER}")
    print(f"Linhas na silver: {n:,}")
    if n < 1_000_000:
        print("\n*** AVISO: menos de 1M de linhas. Os numeros vao ser ruido.")
        print("*** Carregue mais anos do Comex antes de tirar conclusao.\n")

    exp1_aqe(spark)
    exp2_shuffle_partitions(spark)
    exp3_broadcast(spark)
    exp4_skew_salting(spark)

    print("\n\n=== RESUMO (cole no docs/tuning.md) ===")
    print("| Experimento | Configuração | Tempo (s) |")
    print("|---|---|---|")
    for label, config, elapsed in RESULTS:
        print(f"| {label} | {config} | {elapsed:.2f} |")

    print("\nNao esqueca: printe o Spark UI (localhost:4040) na aba Stages")
    print("mostrando a task enviesada do EXP 4. Aquela barra gigante conta")
    print("a historia sozinha melhor que qualquer tabela.")


if __name__ == "__main__":
    main()
