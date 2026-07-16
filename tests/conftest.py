"""
Fixtures compartilhados.

Por que usar get_spark() nos testes (e não um SparkSession.builder solto):
é regra do projeto - "todo job Spark pega a sessão via get_spark()". Vale
pros testes também, senão a suíte testa uma sessão diferente da que roda
em produção (configs de Delta/S3A diferentes).

Por que monkeypatch em Settings.bronze_zone/silver_zone/gold_zone (e não
mexer no MinIO real): essas properties resolvem pra "s3a://..." fixo -
sem isso todo teste bateria no MinIO de verdade (lento, precisa do
ambiente Docker de pé, e um teste não deveria depender de infra externa).
Patch no atributo de CLASSE funciona mesmo Settings sendo um dataclass
frozen, porque troca o descriptor no tipo, não tenta escrever na instância.
"""

import pytest

from src.common.config import Settings
from src.common.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    return get_spark(app_name="pytest")


@pytest.fixture
def local_zones(tmp_path, monkeypatch):
    """Redireciona bronze_zone/silver_zone/gold_zone pra um diretório local."""
    base = str(tmp_path)
    monkeypatch.setattr(Settings, "bronze_zone", property(lambda self: f"{base}/bronze"))
    monkeypatch.setattr(Settings, "silver_zone", property(lambda self: f"{base}/silver"))
    monkeypatch.setattr(Settings, "gold_zone", property(lambda self: f"{base}/gold"))
    return base
