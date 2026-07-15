# Comandos do dia a dia. `make help` lista tudo.

.PHONY: help up down ps logs ingest-bcb smoke shell spark-ui minio-ui

help:
	@echo "make up          - sobe o ambiente (minio + spark + app)"
	@echo "make down        - derruba tudo"
	@echo "make ps          - status dos containers"
	@echo "make ingest-bcb  - roda ingestao BCB SGS -> bronze"
	@echo "make smoke       - smoke test (Spark lendo o bronze)"
	@echo "make shell       - abre um bash dentro do container app"
	@echo "make spark-ui    - lembra a URL da UI do Spark"
	@echo "make minio-ui    - lembra a URL do console do MinIO"

up:
	docker compose up -d --build --scale spark-worker=2

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

ingest-bcb:
	docker compose exec app python -m src.ingestion.bcb_sgs

smoke:
	docker compose exec app python -m src.smoke_test

shell:
	docker compose exec app bash

spark-ui:
	@echo "Spark Master UI: http://localhost:8080"

minio-ui:
	@echo "MinIO Console:   http://localhost:9001 (user/senha do .env)"
