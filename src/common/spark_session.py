"""
Factory da SparkSession.

Por que um factory em vez de criar a sessao em cada script?
1. As configs de S3A + Delta sao chatas e ficam em UM lugar so.
2. Quando portar pro Databricks, e SO este arquivo que muda
   (la a sessao ja vem pronta; aqui a gente monta na mao).
3. Facilita teste: os jobs recebem `spark` como argumento,
   entao da pra injetar uma sessao local minusculo no pytest.
"""

from pyspark.sql import SparkSession

from src.common.config import settings

# JARs que o Spark baixa sozinho na primeira execucao (cache no volume ivy):
# - delta: formato de tabela ACID que usamos em todas as camadas
# - hadoop-aws + aws-sdk: conector s3a:// pra falar com MinIO/S3
_PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
)


def get_spark(app_name: str | None = None) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name or settings.app_name)
        .master(settings.spark_master)
        # --- Delta Lake ---
        .config("spark.jars.packages", _PACKAGES)
        .config("spark.jars.ivy", "/opt/ivy")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # --- Conexao com MinIO via protocolo S3A ---
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_password)
        # path-style: MinIO usa http://host/bucket, nao http://bucket.host (AWS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        # --- Sanidade em dev ---
        # 200 particoes de shuffle (default) e overkill pra dados de dev.
        # Na semana de tuning a gente mede e justifica o valor certo.
        .config("spark.sql.shuffle.partitions", "8")
    )
    return builder.getOrCreate()
