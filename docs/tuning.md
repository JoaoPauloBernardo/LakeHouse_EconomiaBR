# Performance Tuning — metodologia e resultados

> Objetivo: **medir** (não chutar) o efeito de AQE, shuffle partitions,
> broadcast join e tratamento de skew sobre a silver do Comex Stat.
> Harness: [`src/tuning/benchmark.py`](../src/tuning/benchmark.py) · `make benchmark`

## Metodologia

- **Execução forçada com `write.format("noop")`.** Spark é lazy; `count()`
  mente (o otimizador aplica column pruning e mede um plano que não é o
  nosso). `noop` executa o plano completo e descarta o resultado.
- **Cache limpo entre medições**, 1 rodada de warm-up descartada (JIT da
  JVM) e 2 rodadas medidas — fica a melhor, para reduzir ruído de vizinho.
- **Uma variável por vez.** AQE é desligado nos experimentos 2 e 4: ligado,
  ele corrige o problema em runtime e mascara exatamente o efeito medido.
- Ambiente: Docker local, `--scale spark-worker=2`, 2 cores / 2 GB por
  worker. <!-- confirmar/ajustar -->

## Contexto do dado

<!-- preencher com a saída da primeira linha do benchmark -->

| | |
|---|---|
| Tabela | `s3a://lakehouse/silver/comex_stat` |
| Anos carregados | 2016–2025 (fluxo EXP) |
| Linhas | _preencher_ |

## Resultados

<!-- Colar aqui a tabela que o benchmark imprime no final -->

| Experimento | Configuração | Tempo (s) |
|---|---|---|
| aqe | adaptive=false | |
| aqe | adaptive=true | |
| shuffle | partitions=8 | |
| shuffle | partitions=64 | |
| shuffle | partitions=200 | |
| join | sort-merge | |
| join | broadcast | |
| skew | groupBy direto | |
| skew | salting (16) | |

## Análise

### 1. AQE

<!-- 2-4 linhas: o que o número mostra e POR QUÊ. -->

### 2. Shuffle partitions

<!-- Qual valor ganhou e por quê. Relacionar com o volume real da tabela:
     partições grandes demais = spill; pequenas demais = overhead. -->

### 3. Broadcast vs sort-merge

<!-- Colar os trechos relevantes dos dois planos (o benchmark imprime os
     dois com .explain()): SortMergeJoin vs BroadcastHashJoin. -->

### 4. Skew e salting

<!-- Colar a tabela "top 5 países por volume de linhas" que o benchmark
     imprime — é a evidência de que o skew existe e não é suposição.
     Anexar o print do Spark UI (localhost:4040 > Stages) com a barra
     gigante da task enviesada. Essa imagem conta a história sozinha. -->

## Decisões aplicadas ao pipeline

<!-- A seção mais importante do documento.
     O benchmark tem que virar decisão: "fixamos shuffle.partitions=64 no
     silver_to_gold porque a medição X mostrou Y", "broadcast hint nas dims
     porque Z".
     Sem esta seção, o tuning é curiosidade acadêmica.
     Com ela, é engenharia. Medição que não vira decisão é desperdício. -->

| Decisão | Onde foi aplicada | Evidência |
|---|---|---|
| | | |
