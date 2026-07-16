"""
Quality gates: checks de qualidade rodados ANTES de gravar silver/gold.

Por que hand-rolled em vez de uma lib tipo Great Expectations:
lib de qualidade de verdade traz config YAML, data context, checkpoints -
peso desproporcional pro escopo de portfólio. O que importa demonstrar
aqui é QUE checks fazer e ONDE plugar no pipeline, não a API de uma
ferramenta. Cada check é uma função pura DataFrame -> resultado: fácil
de testar isolado com chispa, fácil de adicionar um novo sem aprender
framework nenhum (ver ADR-0003).

Filosofia: falhar o job é melhor que gravar dado ruim. Silver/gold
alimentam decisão (dashboard, entrevista técnica); um número errado
silencioso é pior que um pipeline vermelho no Airflow.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


class DataQualityError(Exception):
    """Levantado quando um ou mais checks falham - para o job de propósito."""


Check = Callable[[DataFrame], CheckResult]


def expect_not_null(df: DataFrame, column: str, max_null_ratio: float = 0.0) -> CheckResult:
    """Falha se a fração de nulos na coluna passar de max_null_ratio.

    Por que ratio configurável (não "zero nulo" fixo): algumas colunas
    derivadas (ex: pib_per_capita quando falta população do ano) podem
    ter null legítimo - quem chama decide a tolerância, o check só mede.
    """
    total = df.count()
    if total == 0:
        return CheckResult(f"not_null({column})", True, "dataframe vazio, nada a checar")

    nulls = df.filter(F.col(column).isNull()).count()
    ratio = nulls / total
    passed = ratio <= max_null_ratio
    detail = f"{nulls}/{total} nulos ({ratio:.1%}), limite {max_null_ratio:.1%}"
    return CheckResult(f"not_null({column})", passed, detail)


def expect_unique(df: DataFrame, columns: list[str]) -> CheckResult:
    """Falha se houver linha duplicada pela combinação de colunas (chave de negócio)."""
    total = df.count()
    distintos = df.select(*columns).distinct().count()
    duplicatas = total - distintos
    passed = duplicatas == 0
    chave = ",".join(columns)
    detail = f"{duplicatas} linha(s) duplicada(s) em ({chave}) - {total} total, {distintos} distintos"
    return CheckResult(f"unique({chave})", passed, detail)


def expect_column_between(
    df: DataFrame,
    column: str,
    min_value: float | None = None,
    max_value: float | None = None,
) -> CheckResult:
    """Falha se algum valor NÃO-NULO da coluna estiver fora de [min_value, max_value]."""
    fora_da_faixa = F.lit(False)
    if min_value is not None:
        fora_da_faixa = fora_da_faixa | (F.col(column) < F.lit(min_value))
    if max_value is not None:
        fora_da_faixa = fora_da_faixa | (F.col(column) > F.lit(max_value))

    ruins = df.filter(F.col(column).isNotNull() & fora_da_faixa).count()
    passed = ruins == 0
    faixa = f"[{min_value if min_value is not None else '-inf'}, {max_value if max_value is not None else '+inf'}]"
    detail = f"{ruins} valor(es) de '{column}' fora de {faixa}"
    return CheckResult(f"range({column})", passed, detail)


def run_gate(df: DataFrame, checks: list[Check], *, stage: str) -> None:
    """Roda todos os checks e levanta DataQualityError se algum falhar.

    Roda TODOS antes de decidir (não para no primeiro) - assim o log
    mostra o quadro completo de uma vez, útil pra debugar um pipeline
    que falhou de manhã sem precisar rodar de novo a cada check corrigido.
    """
    results = [check(df) for check in checks]
    for r in results:
        status = "OK" if r.passed else "FALHOU"
        print(f"[quality:{stage}] {status} - {r.name}: {r.detail}")

    failures = [r for r in results if not r.passed]
    if failures:
        resumo = "; ".join(f"{r.name} ({r.detail})" for r in failures)
        raise DataQualityError(f"[{stage}] {len(failures)} check(s) falharam: {resumo}")
