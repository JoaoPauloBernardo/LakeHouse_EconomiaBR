# ADR 003 — Portabilidade para Databricks: local é a sala de máquinas, Databricks é a vitrine

**Status:** aceito · **Data:** 2026-07

## Contexto

O escopo do projeto sempre previu Databricks. O `config.py` e o
`spark_session.py` existem exatamente por isso: centralizar ambiente e
abstrair a origem da sessão, para que o mesmo código rode nos dois lugares.

Restava decidir **o que** roda onde. As restrições do Databricks Free
Edition (a conta que usamos) são concretas e moldaram a decisão:

- compute é **serverless-only**: configuração custom de compute não é
  suportada, e a maioria das configs de Spark é ignorada;
- **saída de rede restrita** a um conjunto de domínios confiáveis;
- quotas por conta: estourou, o compute é desligado pelo resto do dia.

## Decisão

Divisão em duas camadas, por capacidade — não por preferência:

```
Docker local (Spark OSS + MinIO)          ← sala de máquinas
  ingestão · bronze · silver · gold · tuning
        ↓ src/publish/databricks_publish.py
Databricks (Unity Catalog)                ← vitrine
  tabela gold gerenciada · consulta · dashboard
```

**Ingestão fica no local, obrigatoriamente.** O Free Edition não tem saída
de internet para `api.bcb.gov.br` nem `balanca.economia.gov.br`. Não é
escolha de arquitetura: é o que a plataforma permite.

**Tuning fica no local, deliberadamente.** O serverless decide
`shuffle.partitions`, AQE e estratégia de join por conta própria — é ótimo
para produtividade e péssimo para *aprender*, porque esconde exatamente as
decisões que queremos medir (ver `docs/tuning.md`).

**Publicação empurra de baixo para cima.** O compute do Databricks não
enxerga o MinIO local, então o fluxo é: Spark local exporta a gold para
Parquet → CLI sobe para um UC Volume → sessão Databricks cria a tabela
gerenciada. Nunca o contrário.

**`RUNTIME_ENV` é o interruptor.** `get_spark()` devolve Spark OSS local ou
`DatabricksSession` sem que o job saiba a diferença. Migrar um job de
plataforma é variável de ambiente, não rewrite.

**Publicação é feature flag.** Sem `DATABRICKS_HOST` configurado, o job
avisa e sai com código 0. Quem clona o repo sem conta no Databricks roda o
pipeline inteiro normalmente.

## Consequências

- (+) A promessa de portabilidade do `config.py` passa a ser verificável:
  existe um caminho de código que usa o outro runtime.
- (+) Cada plataforma faz o que faz bem: OSS ensina e dá controle fino;
  Databricks entrega catálogo, governança e consulta sem manutenção.
- (+) O pipeline local não depende do Databricks para funcionar.
- (−) `databricks-connect` embute o próprio pyspark e **conflita** com o
  pacote `pyspark`. Por isso vive em `requirements-databricks.txt`, num
  venv separado — o container `app` não o instala. Custo real de
  manutenção, aceito conscientemente.
- (−) A gold existe em dois lugares (local e UC). Aceitável: a fonte da
  verdade é a local; a cópia no UC é derivada e recriável a qualquer
  momento com `CREATE OR REPLACE`.

## Alternativas descartadas

- **Rodar tudo no Databricks.** Impossível no Free Edition (sem saída de
  rede para as fontes) e mataria a semana de tuning.
- **Storage em S3 real para os dois enxergarem.** Resolveria elegantemente,
  mas custa dinheiro e o projeto é de portfólio.
- **Só citar Databricks no README.** É o que existia antes deste ADR: o
  README prometia portabilidade que o código não entregava. Claim que não
  se sustenta ao abrir o arquivo é pior que não ter a claim.
