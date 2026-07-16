# ADR 002 — Duas estratégias de idempotência (MERGE vs replaceWhere)

**Status:** aceito · **Data:** 2026-07

## Contexto

Todo job do pipeline pode rodar mais de uma vez: retry do Airflow, backfill,
correção de bug. Se rodar 2x duplicar dado, o pipeline é uma bomba-relógio.
Idempotência (rodar 1x ou N vezes → mesmo estado final) é requisito, não luxo.

Mas as três fontes têm naturezas diferentes, e uma única técnica não serve
para todas:

| Fonte | Chave natural por linha? | Unidade de reprocessamento | Revisa histórico? |
|---|---|---|---|
| BCB SGS | sim: `(serie, data)` | a série inteira | sim, retroativamente |
| Comex Stat | não (registros estatísticos) | o arquivo-ano | não |
| IBGE SIDRA | sim: `(dataset, municipio_id, ano)` | o dataset-ano | sim (estimativas/PIB) |

## Decisão

**Bronze do BCB → append-only.** Cada execução reappenda a série inteira.
A duplicata é *intencional*: bronze responde "o que a fonte me deu", e a
fonte mandou de novo. Deduplicar aqui destruiria a trilha de auditoria.

**Bronze do Comex e do IBGE → `replaceWhere`.** A fonte publica arquivos
fechados por ano; a unidade natural de reprocessamento é a partição inteira.
`mode("overwrite") + replaceWhere("ano = X")` substitui só aquela partição
e preserva o resto da tabela.

**Silver do BCB e do IBGE → dedup + `MERGE`.** Chave de negócio existe, e
as fontes **revisam valores retroativamente** (o IPCA de março pode ser
corrigido em maio). `whenMatchedUpdateAll` captura a revisão;
`whenNotMatchedInsertAll` insere o que é novo. O dedup intra-batch com
`row_number()` é obrigatório *antes* do MERGE: o Delta falha se a mesma
chave aparecer duas vezes no batch de origem.

**Silver do Comex → `replaceWhere`.** Sem chave natural, um `MERGE` sobre
centenas de milhões de linhas seria caríssimo e sem ganho.

## Consequências

- (+) Cada dataset usa a técnica mais barata para a sua natureza.
- (+) Backfill de um ano específico do Comex é trivial e seguro.
- (+) Revisões do BCB/IBGE chegam à silver sem duplicar linha.
- (−) Dois padrões de código a manter em vez de um; mitigado por este ADR,
  para que a diferença seja lida como decisão e não como inconsistência.

## Regra que sai daqui

> Idempotência não é uma técnica, é uma **propriedade**. `MERGE` e
> `replaceWhere` são dois caminhos para ela, com custos diferentes.
> Escolhe-se pelo formato do dado, não por preferência.
