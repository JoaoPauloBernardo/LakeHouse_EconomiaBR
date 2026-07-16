# Container de desenvolvimento e execução dos jobs
# Escolhi o python pro codigo + Java pq o pyspark é uma casca em cima do JVM
FROM python:3.11-slim-bookworm

# Java 17 (Spark 3.5 suporta o 8/11/17) + utilitarios minimos
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# balanca.economia.gov.br (fonte do Comex Stat) não manda o certificado
# intermediário na negociação TLS - problema do servidor deles, não nosso,
# mas sem isso todo download do Comex falha com "unable to get local issuer
# certificate". O intermediário em si é público e assinado por uma CA
# (Sectigo) que já é confiável por padrão; só falta ele na cadeia.
COPY docker/certs/sectigo-public-server-auth-ov-r36.pem /usr/local/share/ca-certificates/sectigo-public-server-auth-ov-r36.crt
RUN update-ca-certificates

# A lib "requests" do Python NÃO usa o trust store do SO por padrão - ela usa
# o bundle embutido do pacote "certifi", que ignora o update-ca-certificates
# acima. REQUESTS_CA_BUNDLE aponta o requests pro bundle do sistema (que já
# tem o intermediário extra), sem precisar mexer no certifi.
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

WORKDIR /opt/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Cache de dependencias JVM (delta, hadoop-aws) fora do home efemero
ENV IVY_HOME=/opt/.ivy
COPY src/ ./src/

ENV PYTHONPATH=/opt/app