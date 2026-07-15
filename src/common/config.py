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
    # --- Storage (MinIO local fingindo ser S3) ---
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_user: str = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password: str = os.getenv("MINIO_ROOT_PASSWORD", "admin12345")
    bucket: str = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")

    # --- Spark ---
    spark_master: str = os.getenv("SPARK_MASTER_URL", "local[*]")
    app_name: str = os.getenv("SPARK_APP_NAME", "lakehouse-economia-br")

    # --- Camadas do lakehouse (arquitetura medallion) ---
    @property
    def bronze(self) -> str:
        return f"s3a://{self.bucket}/bronze"

    @property
    def silver(self) -> str:
        return f"s3a://{self.bucket}/silver"

    @property
    def gold(self) -> str:
        return f"s3a://{self.bucket}/gold"


settings = Settings()
