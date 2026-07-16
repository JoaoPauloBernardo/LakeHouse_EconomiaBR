# Container de desenvolvimento e execução dos jobs
# Escolhi o python pro codigo + Java pq o pyspark é uma casca em cima do JVM
FROM python:3.11-slim-bookworm

# Java 17 (Spark 3.5 suporta o 8/11/17) + utilitarios minimos
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /opt/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Cache de dependencias JVM (delta, hadoop-aws) fora do home efemero
ENV IVY_HOME=/opt/.ivy
COPY src/ ./src/

ENV PYTHONPATH=/opt/app