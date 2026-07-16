"""
Configuracao central do projeto.

Regra de ouro: NENHUM caminho ou credencial hardcoded no resto do codigo.
Tudo vem de variavel de ambiente com default sensato pra dev local.
Isso e o que permite o mesmo codigo rodar no Docker local hoje e no
Databricks depois - so muda o ambiente, nao o codigo.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # --- Ambiente de execucao: "local" ou "databricks" ---
    runtime_env: str = os.getenv("RUNTIME_ENV", "local")

    # --- Storage (MinIO local fingindo ser S3) ---
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_user: str = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password: str = os.getenv("MINIO_ROOT_PASSWORD", "admin12345")
    bucket: str = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")

    # --- Spark ---
    spark_master: str = os.getenv("SPARK_MASTER_URL", "local[*]")
    app_name: str = os.getenv("SPARK_APP_NAME", "lakehouse-economia-br")

    # --- DATABRICKS ---
    # tem motivo pra estar vazio
    databricks_host: str = os.getenv("DATABRICKS_HOST", "")
    databricks_catalog: str = os.getenv("DATABRICKS_CATALOG", "workspace")
    databricks_schema: str = os.getenv("DATABRICKS_SCHEMA", "economia_br")
    databricks_volume: str = os.getenv("DATABRICKS_VOLUME", "staging")

    # --- Camadas do lakehouse (arquitetura medallion) ---
    @property
    def raw_zone(self) -> str:
        return f"s3a://{self.bucket}/raw"

    @property
    def bronze_zone(self) -> str:
        return f"s3a://{self.bucket}/bronze"

    @property
    def silver_zone(self) -> str:
        return f"s3a://{self.bucket}/silver"

    @property
    def gold_zone(self) -> str:
        return f"s3a://{self.bucket}/gold"

    @property
    def databricks_enabled(self) -> bool:
        return bool(self.databricks_host)


settings = Settings()
