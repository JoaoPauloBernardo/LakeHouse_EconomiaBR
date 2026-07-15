# Guia de estudo — Lakehouse Economia BR (parte João Paulo)

fiz esse documento pois o objetivo deste projeto é que eu aprenda pyspark, docker e
consolide meus conhecimentos em airflow e para isso preciso de um guia
decidi deixa-lo no github para caso alguem se interesse em aprender usando esse projeto assim como eu fiz

> Documento de referência: o que cada peça faz, **onde está no código**, e
> por que foi feita assim. Escrito pra ser lido antes de entrevista e pra
> te permitir reescrever qualquer parte do zero.

---

## Índice

1. [O mapa mental do projeto](#1-o-mapa-mental-do-projeto)
2. [O fluxo de um dado, do começo ao fim](#2-o-fluxo-de-um-dado-do-começo-ao-fim)
3. [Camada 0 — Infraestrutura (Docker)](#3-camada-0--infraestrutura-docker)
4. [Camada 1 — Fundação do código (config + spark_session)](#4-camada-1--fundação-do-código)
5. [Camada 2 — Ingestão (BCB e Comex)](#5-camada-2--ingestão)
6. [Camada 3 — Transformação (bronze → silver)](#6-camada-3--transformação-bronze--silver)
7. [Camada 4 — Orquestração (Airflow)](#7-camada-4--orquestração-airflow)
8. [Camada 5 — Tuning](#8-camada-5--tuning)
9. [Os 8 idiomas que você precisa saber escrever de cabeça](#9-os-8-idiomas-que-você-precisa-saber-escrever-de-cabeça)
10. [Banco de perguntas de entrevista](#10-banco-de-perguntas-de-entrevista)

---

## 1. O mapa mental do projeto

Antes do código: **as 4 tecnologias e o papel de cada uma**. Se você
confundir os papéis, a entrevista acaba ali.

```
┌─────────────────────────────────────────────────────────────────┐
│  DOCKER          "onde tudo roda"                               │
│  ─────────────────────────────────────────────────────────────  │
│  Não processa nada. Só garante que MinIO, Spark e Airflow       │
│  existam, se enxerguem e tenham as versões certas.              │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  MINIO         "onde o dado mora"                         │  │
│  │  Storage de objetos S3-compatible. Burro de propósito:    │  │
│  │  só guarda bytes. Não sabe o que é uma tabela.            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  DELTA LAKE    "o que transforma bytes em tabela"         │  │
│  │  Uma convenção: arquivos Parquet + um log de transações   │  │
│  │  (_delta_log). É o log que dá ACID, MERGE, time travel.   │  │
│  │  NÃO é um banco. É um formato em cima do storage.         │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  SPARK         "quem processa"                            │  │
│  │  Motor de processamento distribuído. Lê do MinIO,         │  │
│  │  transforma, escreve no MinIO. Não guarda nada.           │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  AIRFLOW       "quem manda rodar e quando"                │  │
│  │  Orquestrador. Não processa dado. Dispara jobs, controla  │  │
│  │  ordem, retry e data lógica.                              │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**A frase que resume:** *Docker hospeda, MinIO guarda, Delta organiza,
Spark processa, Airflow comanda.*

### O mapa do repositório

```
lakehouse-economia-br/
│
├── docker-compose.yml          ← [CAMADA 0] define os 6 serviços
├── docker/
│   ├── Dockerfile.app          ← [CAMADA 0] imagem onde os jobs rodam
│   └── Dockerfile.airflow      ← [CAMADA 0] imagem do orquestrador
├── requirements.txt            ← [CAMADA 0] versões travadas
├── Makefile                    ← [CAMADA 0] atalhos (make up, make shell)
│
├── src/
│   ├── common/
│   │   ├── config.py           ← [CAMADA 1] toda config vem daqui
│   │   └── spark_session.py    ← [CAMADA 1] ⭐ o coração da portabilidade
│   ├── ingestion/
│   │   ├── bcb_sgs.py          ← [CAMADA 2] API pequena → raw → bronze
│   │   └── comex_stat.py       ← [CAMADA 2] ⭐ arquivo gigante, streaming
│   └── transform/
│       └── bronze_to_silver.py ← [CAMADA 3] ⭐ MERGE + dedup + tipagem
│
├── dags/
│   └── lakehouse_pipeline.py   ← [CAMADA 4] o DAG
│
├── scripts/
│   └── benchmark_tuning.py     ← [CAMADA 5] os 4 experimentos
│
└── docs/
    ├── decisions/ADR-0001…     ← por que medallion + Delta
    ├── decisions/ADR-0002…     ← por que duas idempotências
    └── tuning.md               ← metodologia + números
```

As ⭐ são os 3 arquivos que você **tem** que saber explicar linha a linha.

---

## 2. O fluxo de um dado, do começo ao fim

Vamos seguir **uma linha do IPCA** atravessando o pipeline inteiro. Se
você entender essa jornada, entendeu o projeto.

```
[1] API do BCB devolve JSON
    {"data": "01/03/2024", "valor": "0.16"}
         │
         │  src/ingestion/bcb_sgs.py :: _fetch()
         │  requests.get + retry (tenacity)
         ▼
[2] RAW — o byte cru, intocado
    s3a://raw/bcb_sgs/ipca_mensal/2026-07-15/a3f9c1.json
         │
         │  src/ingestion/bcb_sgs.py :: _write_raw()
         │  boto3.put_object — SEM Spark (é só um PUT de arquivo)
         │
         │  src/ingestion/bcb_sgs.py :: ingest_serie()
         │  Spark cria DataFrame + adiciona linhagem
         ▼
[3] BRONZE — estruturado, ainda tudo string, APPEND-ONLY
    s3a://bronze/bcb_sgs/serie=ipca_mensal/
    ┌────────────┬───────┬─────────────┬──────────┬──────────────┐
    │ data       │ valor │ serie       │ _run_id  │ _ingested_at │
    │ 01/03/2024 │ 0.16  │ ipca_mensal │ a3f9c1   │ 2026-07-15…  │  ← run 1
    │ 01/03/2024 │ 0.16  │ ipca_mensal │ b7e2d4   │ 2026-07-16…  │  ← run 2 (duplicata! e tá certo)
    │ 01/03/2024 │ 0.18  │ ipca_mensal │ c1a8f2   │ 2026-07-17…  │  ← run 3 (BCB REVISOU o valor!)
    └────────────┴───────┴─────────────┴──────────┴──────────────┘
         │
         │  src/transform/bronze_to_silver.py :: transform_bcb()
         │  (a) tipagem: string → DateType/DoubleType
         │  (b) dedup: Window + row_number → fica só a mais recente (0.18)
         │  (c) MERGE: upsert por (serie, data)
         ▼
[4] SILVER — tipado, deduplicado, 1 linha por chave
    s3a://silver/bcb_sgs/serie=ipca_mensal/
    ┌────────────┬───────┬─────────────┐
    │ data       │ valor │ serie       │
    │ 2024-03-01 │ 0.18  │ ipca_mensal │  ← DateType, DoubleType, valor revisado
    └────────────┴───────┴─────────────┘
         │
         │  (Fabio) src/transform/silver_to_gold.py
         ▼
[5] GOLD — agregado, pronto pra consumo
```

**Repare no ponto crítico:** o bronze *deixa* a duplicata entrar. Não é
desleixo — é design. Bronze responde "o que a fonte me deu"; silver
responde "qual é a verdade". Separar essas duas perguntas é a arquitetura
medallion inteira em uma frase.

---

## 3. Camada 0 — Infraestrutura (Docker)

📁 **`docker-compose.yml`**

Seis serviços, três papéis:

| Serviço | Papel | Porta | Detalhe que importa |
|---|---|---|---|
| `minio` | storage | 9000 (API), 9001 (console) | tem `healthcheck` — os outros esperam ele ficar pronto |
| `minio-init` | setup | — | roda `mc mb`, cria os 4 buckets, **morre** |
| `spark-master` | coordenador | 8080 (UI), 7077 (RPC) | não executa task, só distribui |
| `spark-worker` | executor | — | `--scale spark-worker=2` = 2 executores |
| `app` | driver | 4040 (Spark App UI) | onde SEU código roda |
| `airflow` | orquestrador | 8081 | 8081 porque 8080 já é do Spark Master |

**Os 3 detalhes que valem explicar em entrevista:**

**(a) `minio-init` — infra declarada, não clicada.**
```yaml
entrypoint: >
  /bin/sh -c "
  mc alias set local http://minio:9000 $$MINIO_ROOT_USER $$MINIO_ROOT_PASSWORD &&
  mc mb --ignore-existing local/raw local/bronze local/silver local/gold
  "
```
`--ignore-existing` faz o container ser **idempotente**: subir o ambiente
10x não dá erro. Esse é o mesmo princípio de idempotência que rege o
pipeline inteiro — aparece até na infra.

**(b) Dependência com condição:**
```yaml
depends_on:
  minio-init:
    condition: service_completed_successfully
```
`depends_on` puro só espera o container *iniciar*. Com `condition`, o
`app` só sobe depois do `minio-init` **terminar com sucesso** (buckets
criados). Sem isso, o primeiro job falha com "bucket does not exist" em
condição de corrida — bug que só acontece às vezes, o pior tipo.

**(c) Volume de código, não COPY:**
```yaml
volumes:
  - ./src:/app/src
```
Você edita no VSCode, roda no container, **sem rebuild**. O código não
está *dentro* da imagem — está montado. É o que torna o ciclo de dev
rápido.

📁 **`docker/Dockerfile.app`**

```dockerfile
FROM python:3.11-slim
RUN apt-get install -y openjdk-17-jre-headless   # ← o "porquê" abaixo
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=/app
```

**Pergunta clássica: "por que Java num container Python?"**
Porque **PySpark não é Spark**. PySpark é uma casca Python que conversa
com a JVM via py4j. O driver Spark roda em Java **mesmo quando você
escreve Python**. Sem JRE, `get_spark()` explode.

**`PYTHONPATH=/app`** é o que faz `python -m src.ingestion.bcb_sgs`
funcionar de qualquer diretório.

📁 **`requirements.txt`** — a armadilha das versões:
```
pyspark==3.5.1
delta-spark==3.2.0
```
Essas duas versões são **casadas** pela matriz de compatibilidade oficial
do Delta. Subir uma sem a outra não quebra no `pip install` — quebra em
**runtime**, com erro de classe Java que não diz nada. E a versão do
PySpark tem que bater com a do cluster (`bitnami/spark:3.5.1`), senão dá
erro de serialização.

---

## 4. Camada 1 — Fundação do código

📁 **`src/common/config.py`**

```python
@dataclass(frozen=True)
class Settings:
    runtime_env: str = os.getenv("RUNTIME_ENV", "local")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    raw_zone: str = "s3a://raw"
    ...
settings = Settings()
```

Regra: **zero path/credencial/endpoint hardcoded nos jobs.** `frozen=True`
= imutável, ninguém muda config em runtime por acidente.

Isso não é preciosismo: é o que permite o **mesmo código** rodar local e
no Databricks trocando só variável de ambiente. Configuração é dado, não
código.

📁 **`src/common/spark_session.py`** ⭐ **(o arquivo mais importante)**

```python
def get_spark(app_name="lakehouse-economia-br") -> SparkSession:
    if settings.runtime_env == "databricks":
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    return _build_local_session(app_name)
```

**Todo job chama `get_spark()` e não sabe onde está rodando.** Se um dia
o projeto migra pro Databricks, muda-se **uma função**, não 40 jobs.
Isso é o padrão *factory* aplicado a infra.

As configs de `_build_local_session()`, em 3 blocos:

**Bloco 1 — ensinar Delta pro Spark OSS:**
```python
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```
No Databricks, Delta é nativo. No Spark open source, você **pluga** a
extensão. Sem essas duas linhas, `.format("delta")` dá "data source not
found".

**Bloco 2 — os JARs (a parte que quebra todo mundo):**
```python
.config("spark.jars.packages", ",".join([
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]))
```
O PySpark do pip **não traz** os conectores. `hadoop-aws` é o "driver" do
protocolo `s3a://`. Versão 3.3.4 porque é o Hadoop embutido no Spark
3.5 — misturar versão de Hadoop é erro de `NoSuchMethodError` clássico.

**Bloco 3 — a config que engana todo mundo:**
```python
.config("spark.hadoop.fs.s3a.path.style.access", "true")
```
🔥 **Decore isso.** S3 real endereça `bucket.dominio.com` (virtual-host
style). MinIO endereça `dominio.com/bucket` (path style). Sem essa config,
o erro é **"bucket does not exist"** — com o bucket existindo na sua
frente. Já vi gente perder meio dia nisso.

---

## 5. Camada 2 — Ingestão

Dois arquivos, **duas estratégias diferentes**, porque os dados são
diferentes. Entender *por que são diferentes* é o ponto.

| | `bcb_sgs.py` | `comex_stat.py` |
|---|---|---|
| Tamanho | KBs (JSON) | centenas de MB por ano (CSV) |
| Download | `requests.get()` inteiro na RAM | **streaming multipart** |
| Bronze | **append-only** | **replaceWhere** por ano |
| Parâmetro | nenhum (série toda) | `--years` (CLI) |
| Chave | `(serie, data)` | não tem (partição ano) |

### 5.1 `src/ingestion/bcb_sgs.py`

**Estrutura em 4 funções:**

```python
SERIES = {"selic_meta": 432, "ipca_mensal": 433, ...}   # constantes no topo
BRONZE_SCHEMA = StructType([...])                        # schema explícito

@retry(stop=stop_after_attempt(4), wait=wait_exponential(...))
def _fetch(url) -> list[dict]:        # API → memória, com retry
def _write_raw(payload, ...) -> str:  # memória → MinIO (boto3, sem Spark)
def ingest_serie(name, code, run_id): # orquestra: fetch → raw → bronze
def main():                            # loop nas séries, gera run_id
```

**As 4 decisões defensáveis aqui:**

**(1) Raw não passa pelo Spark.**
```python
s3.put_object(Bucket="raw", Key=key, Body=json.dumps(payload)...)
```
Raw é um PUT de arquivo. Usar Spark pra isso é matar mosca com canhão —
Spark entra quando há **transformação distribuída**. Saber *quando não
usar* a ferramenta é sinal de maturidade.

**(2) Schema explícito, nunca `inferSchema`.**
```python
BRONZE_SCHEMA = StructType([
    StructField("data", StringType(), nullable=True),
    StructField("valor", StringType(), nullable=True),
])
```
Inferência lê uma amostra e **adivinha**. Se hoje a amostra tem só
inteiros e amanhã vem `"1.5"`, o tipo muda sozinho e o pipeline quebra —
ou pior, converte errado **em silêncio**. Schema explícito = contrato.
(É tudo `StringType` de propósito: bronze guarda o que veio; tipagem é
responsabilidade da silver.)

**(3) Colunas de linhagem:**
```python
.withColumn("_source_url", F.lit(url))
.withColumn("_raw_path", F.lit(raw_path))
.withColumn("_run_id", F.lit(run_id))
.withColumn("_ingested_at", F.lit(ingested_at.isoformat()))
```
Toda linha do bronze responde: **de onde vim, quando cheguei, qual
arquivo raw me gerou, qual execução me trouxe.** Isso é o que torna
auditoria possível. O `_run_id` (mesmo pra todas as séries de uma
execução) é o que liga raw ↔ bronze ↔ log.

**(4) Retry com backoff exponencial:**
```python
@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=2, max=30))
```
Espera 2s, 4s, 8s, 16s... API pública cai. Backoff **exponencial** (não
fixo) porque se o servidor está sobrecarregado, martelar de 1 em 1 segundo
piora o problema.

### 5.2 `src/ingestion/comex_stat.py` ⭐

**O coração é o streaming multipart** — a única forma de mover um arquivo
de 500MB+ sem estourar RAM:

```python
with requests.get(url, stream=True, timeout=(30, 300)) as resp:
    mpu = s3.create_multipart_upload(Bucket="raw", Key=key)
    parts, part_number, buffer = [], 1, b""
    try:
        for chunk in resp.iter_content(chunk_size=1024*1024):   # 1MB da rede
            buffer += chunk
            if len(buffer) >= CHUNK_SIZE:                        # acumulou 16MB?
                part = s3.upload_part(..., PartNumber=part_number, Body=buffer)
                parts.append({"ETag": part["ETag"], "PartNumber": part_number})
                part_number += 1
                buffer = b""                                     # ← libera a RAM
        if buffer:  # resto final
            ...
        s3.complete_multipart_upload(..., MultipartUpload={"Parts": parts})
    except Exception:
        s3.abort_multipart_upload(...)   # ← 🔥 não esqueça
        raise
```

**O que está acontecendo:** o arquivo nunca existe inteiro em lugar
nenhum. Ele flui `fonte → buffer de 16MB → MinIO`, em loop. Uso de RAM
**constante**, seja o arquivo de 100MB ou 5GB.

**Por que `abort_multipart_upload` no `except`:** upload que falha no meio
deixa partes **órfãs** no storage — ocupando espaço pra sempre, e na AWS,
**cobrando pra sempre**. Esse `abort` é o tipo de detalhe que separa
código de portfólio de código de produção. Menciona isso em entrevista.

**Por que `timeout=(30, 300)`:** tupla = (timeout de conexão, timeout de
leitura). 30s pra conectar, 300s entre chunks. Timeout único não serve:
um download legítimo de 10 minutos não pode morrer, mas um servidor que
não responde em 30s tem que.

**O segundo conceito: Spark lê do RAW, não da fonte.**
```python
def raw_to_bronze(flow, year, raw_path, run_id):
    df = spark.read.option("sep", ";").option("encoding", "latin1")
             .schema(BRONZE_SCHEMA).csv(raw_path)   # ← lê do MinIO, não do MDIC
```
Download é lento e frágil; parse é rápido. Separando os dois, um bug de
parsing se reprocessa **do raw em segundos**, sem rebater no servidor do
governo. É por isso que a camada raw existe.

(`encoding="latin1"` porque dado público brasileiro raramente é UTF-8. Se
aparecer `SÃ£o Paulo`, é aí que se mexe.)

**O terceiro: `replaceWhere` — o overwrite cirúrgico:**
```python
df.write.format("delta")
  .mode("overwrite")
  .option("replaceWhere", f"ano = {year} AND fluxo = '{flow}'")
  .partitionBy("ano", "fluxo")
  .save(...)
```
`mode("overwrite")` sozinho apagaria a **tabela inteira**. Com
`replaceWhere`, ele substitui **só a partição** daquele ano/fluxo e
preserva o resto. Resultado: rodar o mesmo ano 2x **não duplica**
(idempotente) e backfill de um ano é trivial.

**O quarto: anos via CLI, não hardcoded:**
```python
parser.add_argument("--years", nargs="+", type=int, required=True)
```
O **mesmo código** serve pra carga inicial (`--years 1997 ... 2026`),
backfill de um ano (`--years 2019`) e carga mensal (o Airflow injeta o
ano). Parametrizar em vez de hardcodar é o que torna o job reutilizável —
e é o que permite o Airflow fazer backfill (seção 7).

**O quinto: falha isolada não derruba o batch:**
```python
for year in args.years:
    for flow in args.flows:
        try:
            ingest_year(flow, year, run_id)
        except Exception as exc:
            failures.append(f"{flow}_{year}")   # coleta e continua
if failures:
    raise SystemExit(f">>> Concluído COM FALHAS: {failures}")
```
Num backfill de 30 anos, um ano com 404 não pode matar os outros 29. Mas
o job **falha no final** (exit code ≠ 0) — senão o Airflow marca sucesso
com dado faltando, que é o pior dos mundos: erro silencioso.

---

## 6. Camada 3 — Transformação (bronze → silver)

📁 **`src/transform/bronze_to_silver.py`** ⭐ **(a joia da coroa)**

Aqui o dado vira **confiável**. Três operações, nesta ordem:
**tipagem → dedup → merge**.

### 6.1 Tipagem — e a falha barulhenta

```python
typed = bronze.select(
    F.col("serie"),
    F.to_date("data", "dd/MM/yyyy").alias("data"),      # string → DateType
    F.col("valor").cast(DoubleType()).alias("valor"),   # string → Double
    ...
)

bad_dates = typed.filter(F.col("data").isNull()).count()
bad_values = typed.filter(F.col("valor").isNull()).count()
print(f"[bcb] linhas com data inválida: {bad_dates} | valor inválido: {bad_values}")
typed = typed.filter(F.col("data").isNotNull())
```

**O conceito:** no Spark, `cast` que falha **não levanta exceção — vira
NULL**. Silenciosamente. É assim que dado contaminado nasce.

Por isso o job **conta e loga** antes de filtrar. A regra: *linha ruim
pode ser descartada, mas nunca em silêncio.* (Na semana 4 o Fabio
transforma esses prints em gates formais que derrubam o job.)

### 6.2 Dedup — o idioma canônico do Spark

```python
w = Window.partitionBy("serie", "data").orderBy(F.col("_ingested_at").desc())
latest = (
    typed.withColumn("_rn", F.row_number().over(w))
         .filter(F.col("_rn") == 1)
         .drop("_rn")
)
```

**Leia em português:** "particiona por chave de negócio, ordena por
recência decrescente, numera as linhas dentro de cada grupo, fica com a
número 1."

Isso é **80% dos problemas de duplicata em Spark**. Decore o padrão.

**Por que fazer isso ANTES do MERGE:** o MERGE do Delta **falha** se o
batch tiver a mesma chave duas vezes (ele não sabe qual das duas usar —
erro `multiple source rows matched`). O bronze é append-only, então a
mesma `(serie, data)` aparece N vezes. Dedup intra-batch primeiro,
MERGE depois. **Essa ordem não é opcional.**

### 6.3 MERGE — o upsert

```python
if not DeltaTable.isDeltaTable(spark, BCB_SILVER):
    latest.write.format("delta").partitionBy("serie").save(BCB_SILVER)
    return                                    # primeira execução: só cria

silver = DeltaTable.forPath(spark, BCB_SILVER)
(silver.alias("s")
    .merge(latest.alias("b"), "s.serie = b.serie AND s.data = b.data")
    .whenMatchedUpdateAll()      # chave existe → atualiza (pega revisão!)
    .whenNotMatchedInsertAll()   # chave nova → insere
    .execute())
```

**Traduzindo:** "pra cada linha do batch, procura a chave na silver. Achou?
atualiza. Não achou? insere."

**Por que `whenMatchedUpdateAll` e não ignorar:** o BCB **revisa índices
retroativamente**. O IPCA de março pode ser corrigido em maio. Se você
só inserisse novas chaves, a silver ficaria com o valor **errado** pra
sempre. O update é o que captura a revisão.

**Por que o `if isDeltaTable`:** `DeltaTable.forPath` numa tabela que não
existe explode. Primeiro run cria, demais fazem merge.

### 6.4 A decisão que vale o ADR-0002

`transform_comex()` **não usa MERGE** — usa `replaceWhere`, igual à
ingestão:

```python
bronze = spark.read.format("delta").load(COMEX_BRONZE).filter(F.col("ano") == year)
...
valid.write.format("delta").mode("overwrite") \
     .option("replaceWhere", f"ano = {year}").partitionBy("ano", "fluxo").save(...)
```

**Por quê?** Três motivos:
1. Comex **não tem chave natural** por linha (são registros estatísticos
   agregados — nada identifica unicamente uma linha).
2. A unidade real de reprocessamento é o **arquivo-ano inteiro**.
3. MERGE em centenas de milhões de linhas = comparar tudo com tudo.
   Caríssimo, e sem ganho nenhum.

🔥 **A frase de entrevista:** *"Idempotência não é uma técnica, é uma
propriedade. MERGE e replaceWhere são duas formas de alcançá-la, com
custos diferentes. Escolhi cada uma pela natureza do dado."*

Isso é literalmente a diferença entre "sei usar MERGE" e "sei **quando**
usar MERGE".

**Bônus escondido nesse arquivo — partition pruning:**
```python
.filter(F.col("ano") == year)   # filtro na COLUNA DE PARTIÇÃO
```
Como o bronze foi escrito com `partitionBy("ano", "fluxo")`, o dado está
fisicamente em pastas `ano=2024/fluxo=EXP/`. Filtrar na coluna de partição
faz o Spark **ler só aquela pasta** — não escaneia a tabela inteira.
Você vai ver isso no `explain()` como `PartitionFilters`.

---

## 7. Camada 4 — Orquestração (Airflow)

📁 **`dags/lakehouse_pipeline.py`**

```python
ingest_bcb   >> silver_bcb
ingest_comex >> silver_comex
```

Duas trilhas. **Sem aresta entre elas** = o scheduler paraleliza sozinho.
Dependência no Airflow não é decoração: é o que define o paralelismo.

### 7.1 O conceito de ouro: data lógica

```python
bash_command=(
    "python -m src.ingestion.comex_stat "
    "--years {{ data_interval_start.year }} --flows EXP IMP"
)
```

`{{ ... }}` é **Jinja**, preenchido pelo Airflow em runtime.
`data_interval_start` é a **data lógica da execução** — não é
`datetime.now()`.

**Por que isso muda tudo:** quando você roda
```bash
airflow dags backfill -s 2020-01-01 -e 2023-12-31 lakehouse_pipeline
```
o Airflow executa a DAG 4 vezes, e em cada uma `data_interval_start.year`
vale 2020, 2021, 2022, 2023. **O mesmo código faz o backfill de 4 anos.**

Se você tivesse escrito `datetime.now().year` dentro do job, todas as 4
execuções ingeririam 2026. O backfill seria impossível.

🔥 **A frase:** *"Orquestrador com data lógica + jobs idempotentes =
backfill de graça."* As duas metades são necessárias: data lógica sem
idempotência duplica dado; idempotência sem data lógica não sabe qual
período processar.

### 7.2 Duas camadas de retry (não é redundância)

```python
default_args = {"retries": 2, "retry_delay": timedelta(minutes=10)}
```

| Camada | Onde | Cobre | Escala de tempo |
|---|---|---|---|
| `tenacity` | dentro do job (`_fetch`) | 502 passageiro, conexão caiu | segundos |
| Airflow `retries` | na task | site do governo fora do ar | minutos/horas |

São problemas **diferentes**. Retry de rede não resolve "MDIC em
manutenção por 2 horas"; retry do Airflow não deveria ser acionado por um
timeout de 3 segundos.

### 7.3 O trade-off que você TEM que saber defender

📁 **`docker/Dockerfile.airflow`** instala Java. Por quê? Porque neste
setup o Airflow roda os jobs com `BashOperator` → ele **é** o driver
Spark.

**Isso é anti-padrão em produção.** Em produção:
- Airflow **orquestra**, não processa
- Você usaria `DatabricksSubmitRunOperator`, `KubernetesPodOperator` ou
  `SparkSubmitOperator` apontando pra um cluster **externo**
- Motivo: o worker do Airflow não deve competir por memória com o Spark;
  se o job estoura RAM, ele derruba o scheduler junto

**Por que aceitável aqui:** ambiente local, simplifica a infra, e — o
ponto importante — **não muda o código dos jobs**. A migração pro padrão
de produção é trocar o operator no DAG. Nada em `src/` muda.

Saber que é anti-padrão, saber por que, e saber o caminho da migração:
isso é o que te separa de quem copiou tutorial. Se numa entrevista
perguntarem "por que o Airflow tem Java?", a resposta errada é "porque
precisa"; a certa é essa análise inteira.

---

## 8. Camada 5 — Tuning

📁 **`scripts/benchmark_tuning.py`** + 📁 **`docs/tuning.md`**

**Esta é a semana que todo mundo pula e a que mais te contrata.** Qualquer
um escreve `df.groupBy().agg()`. Quem diz *"tinha skew no NCM, salguei a
chave, caiu de 12min pra 3min"* — esse é contratado.

### 8.1 A metodologia (vale tanto quanto os experimentos)

```python
def timed(spark, label, config, df):
    spark.catalog.clearCache()                          # (2)
    start = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()    # (1)
    elapsed = time.perf_counter() - start
```

**(1) `write.format("noop")` — por que não `count()`:**
Spark é **lazy**. `df.filter().join()` não executa **nada** até uma
*action*. Mas `count()` mente: o otimizador percebe que você só quer o
número de linhas e **pula colunas inteiras** (column pruning) — você mede
um plano que não é o seu. `noop` executa o plano **completo** e joga o
resultado fora. É o jeito canônico de benchmarkar Spark.

**(2) `clearCache()` + descartar a 1ª execução:** JVM tem warm-up (JIT
compila os hot paths). Primeira rodada sempre é mais lenta por motivo que
não tem nada a ver com sua config.

**(3) Uma variável por vez:** repare que os experimentos 2 e 4 **desligam
o AQE**:
```python
spark.conf.set("spark.sql.adaptive.enabled", "false")   # isola o efeito
```
Se você deixar o AQE ligado medindo shuffle partitions, ele **conserta o
problema em runtime** e os números ficam todos iguais. Você mediria o AQE,
não o que queria medir. Isso é método científico aplicado a Spark.

### 8.2 Os 4 experimentos

**EXP 1 — AQE (Adaptive Query Execution) on/off**
```python
spark.conf.set("spark.sql.adaptive.enabled", enabled)
```
AQE re-otimiza o plano **em runtime** usando estatísticas reais do
shuffle: junta partições vazias (coalesce), corrige skew de join, troca
sort-merge por broadcast se descobrir que a tabela é pequena. Default ON
no Spark 3.x. O experimento mostra **o quanto** ele te salva.

**EXP 2 — shuffle partitions (8 / 64 / 200)**
```python
spark.conf.set("spark.sql.shuffle.partitions", n)
```
Cada shuffle divide o dado em N partições. Poucas = partições gigantes
(spill pra disco, OOM). Muitas = milhares de micro-tasks (overhead de
scheduling maior que o trabalho). **O número certo depende do volume** —
por isso não se chuta, se mede. (Default 200 é uma herança histórica que
raramente é o ideal.)

**EXP 3 — broadcast vs sort-merge join**
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # força sort-merge
timed(..., fato.join(dim_pais, "cod_pais")...)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
timed(..., fato.join(F.broadcast(dim_pais), "cod_pais")...)   # broadcast explícito
```
- **Sort-merge:** embaralha **as duas** tabelas pelas chaves. A tabela
  gigante se move pela rede. Caro.
- **Broadcast:** manda a tabela pequena **inteira** pra cada executor. A
  gigante **não se move**. Ordens de magnitude mais barato.

Fato gigante × dimensão pequena → sempre broadcast. Rode `df.explain()`
nos dois e cole os planos no `tuning.md`: `BroadcastHashJoin` vs
`SortMergeJoin`. Isso é evidência visual.

**EXP 4 — skew + salting (o mais bonito)**
```python
# baseline: 1 task gigante (a da China) segura o job inteiro
direto = fato.groupBy("cod_pais").agg(F.sum("vl_fob"))

# salting: espalha a chave quente em 16 sub-chaves
salted = (fato.withColumn("_salt", (F.rand() * 16).cast("int"))
              .groupBy("cod_pais", "_salt").agg(F.sum("vl_fob").alias("parcial"))
              .groupBy("cod_pais").agg(F.sum("parcial").alias("total")))
```

**O problema:** comércio exterior é **naturalmente enviesado**. China e
EUA concentram uma fração enorme das linhas. `groupBy("cod_pais")` manda
todas as linhas da China pra **uma única task**. As outras 200 terminam
em segundos; a da China roda por 10 minutos. **O job demora o tempo da
task mais lenta.**

**A solução (salting):** chave `China` vira `China_0` … `China_15`. Agora
16 tasks dividem o trabalho. Depois re-agrega os 16 parciais. **Duas
agregações pequenas < uma agregação travada num executor só.**

📸 **Tira print do Spark UI (localhost:4040)** mostrando a barra gigante
da task enviesada. Essa imagem conta a história sozinha — vai pro README
e pro LinkedIn.

### 8.3 A seção que transforma curiosidade em engenharia

No `docs/tuning.md`, a última seção é **"Decisões aplicadas ao pipeline"**.

Sem ela, o benchmark é curiosidade acadêmica. Com ela — *"fixamos
`shuffle.partitions=64` no silver_to_gold porque a medição X mostrou Y"* —
vira engenharia. **Medição que não vira decisão é desperdício.**

---

## 9. Os 8 idiomas que você precisa saber escrever de cabeça

Se te mandarem escrever código na entrevista, é isso que vai cair:

**1. Dedup por recência**
```python
w = Window.partitionBy("chave").orderBy(F.col("timestamp").desc())
df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
```

**2. MERGE (upsert)**
```python
DeltaTable.forPath(spark, path).alias("t").merge(
    batch.alias("s"), "t.k1 = s.k1 AND t.k2 = s.k2"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**3. Overwrite de partição**
```python
df.write.format("delta").mode("overwrite") \
  .option("replaceWhere", "ano = 2024").partitionBy("ano").save(path)
```

**4. Leitura com schema explícito**
```python
df = spark.read.option("header","true").option("sep",";") \
          .option("encoding","latin1").schema(MEU_SCHEMA).csv(path)
```

**5. Broadcast join**
```python
fato.join(F.broadcast(dim), "chave")
```

**6. Salting (skew)**
```python
(df.withColumn("_salt", (F.rand()*16).cast("int"))
   .groupBy("chave","_salt").agg(F.sum("v").alias("p"))
   .groupBy("chave").agg(F.sum("p").alias("total")))
```

**7. Benchmark honesto**
```python
spark.catalog.clearCache()
t0 = time.perf_counter()
df.write.format("noop").mode("overwrite").save()
print(time.perf_counter() - t0)
```

**8. Task do Airflow com data lógica**
```python
BashOperator(
    task_id="ingest",
    bash_command="python -m src.ingestion.x --year {{ data_interval_start.year }}",
)
```

---

## 10. Banco de perguntas de entrevista

**"Por que Delta e não Parquet puro?"**
Parquet é formato de **arquivo**; Delta é formato de **tabela**. O
`_delta_log` (log de transações) dá: ACID (leitor nunca vê escrita
parcial), MERGE (upsert declarativo), time travel (auditar/reverter carga
ruim), schema enforcement (barra contaminação silenciosa). Com Parquet
puro, upsert vira "lê tudo, junta na mão, reescreve tudo".

**"Por que 4 camadas? Não é overhead?"**
Cada camada responde **uma pergunta diferente**. Raw: "o que a fonte me
deu?" (auditoria, reprocessável sem rebater na API). Bronze: "o que
entrou, quando, de onde?" (linhagem). Silver: "qual é a verdade?" (dedup,
tipagem). Gold: "o que o negócio consome?". Storage é barato; descobrir
que o dado tá errado e não ter o original é caro.

**"Bronze duplica dado. Isso não é bug?"**
Não, é design. Bronze é append-only porque a pergunta dele é "o que a
fonte mandou?" — e a fonte mandou duas vezes. Deduplicar no bronze
destruiria a trilha de auditoria. A silver responde "qual é a verdade" e
é lá que o dedup mora. Uma responsabilidade por camada.

**"Por que MERGE no BCB e replaceWhere no Comex?"**
[a resposta inteira da seção 6.4 — a frase da idempotência como
propriedade]

**"Como você garante que um retry não duplica dado?"**
Idempotência em duas frentes: os jobs (MERGE por chave / replaceWhere por
partição) e o orquestrador (data lógica em vez de `now()`). As duas juntas
= retry e backfill seguros.

**"Como você descobriu o skew?"**
Spark UI (localhost:4040), aba Stages: uma task com duração e volume de
shuffle read absurdamente maiores que a mediana. Confirmei com
`groupBy("cod_pais").count().orderBy(desc("count"))`. Resolvi com salting
e medi o antes/depois — está documentado no `docs/tuning.md`.

**"Por que Java num container Python?"**
PySpark é casca sobre a JVM (py4j). O driver Spark roda em Java mesmo com
código Python.

**"Por que o Airflow do seu projeto tem Java?"**
[o trade-off inteiro da seção 7.3 — e a menção de que é anti-padrão em
prod, com o caminho de migração]

**"Esse projeto roda em produção?"**
Não, roda local via Docker — mas o código é portável por design:
`get_spark()` abstrai o runtime, `config.py` centraliza ambiente, e o
storage é S3-compatible. Migrar pra Databricks/EMR é trocar variável de
ambiente e o operator do DAG, não reescrever job.

---

## Checklist final antes de subir pro GitHub

- [ ] `docs/tuning.md` preenchido com **números reais** (não é opcional —
      é a seção que mais vende)
- [ ] Print do Spark UI mostrando a task enviesada
- [ ] Print do `explain()` com `BroadcastHashJoin` vs `SortMergeJoin`
- [ ] Prova de idempotência: rodar ingestão 2x, mostrar contagem do bronze
      (duplicado) vs silver (1 por chave) — **antes e depois**
- [ ] README com o diagrama no topo e links pros ADRs
- [ ] Seção "Contribuições" (você × Fabio)
