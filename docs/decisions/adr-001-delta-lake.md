# ADR 001 — Delta Lake como formato de tabela

**Status:** aceito · **Data:** 2026-07

## Contexto

As camadas silver e gold precisam de escrita idempotente (reprocessar um dia
não pode duplicar dados), evolução de schema e auditoria de mudanças. Parquet
puro não oferece nada disso: é só um formato de arquivo, sem transação.

## Decisão

Usar Delta Lake (open source) em todas as camadas silver/gold.

## Consequências

**Positivas**
- `MERGE INTO` permite upsert idempotente — reprocessamento seguro.
- Transações ACID: leitura nunca vê escrita pela metade.
- Time travel: auditar/reverter versões quando dado contaminado entra.
- Mesmo formato do Databricks → portabilidade direta pra nuvem.

**Negativas / trade-offs**
- Dependência de JARs extras e matriz de compatibilidade com a versão do Spark.
- Pequeno overhead de metadados (_delta_log) irrelevante nesta escala.

## Alternativas consideradas

- **Parquet puro:** sem ACID/MERGE; descartado.
- **Apache Iceberg:** equivalente tecnicamente; Delta escolhido pela
  integração nativa com Databricks (alvo de portabilidade do projeto).
