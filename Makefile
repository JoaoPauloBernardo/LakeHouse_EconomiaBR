# Comandos do dia a dia. `make help` lista tudo.

.PHONY: help up down ps logs ingest-bcb ingest-ibge smoke shell spark-ui minio-ui dashboard

help:
	@echo "make up          - sobe o ambiente (minio + spark + app)"
	@echo "make down        - derruba tudo"
	@echo "make ps          - status dos containers"
	@echo "make ingest-bcb  - roda ingestao BCB SGS -> bronze"
	@echo "make ingest-ibge - roda ingestao IBGE SIDRA -> bronze"
	@echo "make smoke       - smoke test (Spark lendo o bronze)"
	@echo "make shell       - abre um bash dentro do container app"
	@echo "make spark-ui    - lembra a URL da UI do Spark"
	@echo "make minio-ui    - lembra a URL do console do MinIO"
	@echo "make dashboard   - sobe o dashboard Streamlit (http://localhost:8501)"

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

# uso: make ingest-ibge ANOS="2021 2022"
ANOS ?= 2021 2022
ingest-ibge:
	docker compose exec app python -m src.ingestion.ibge_sidra --anos $(ANOS)

smoke:
	docker compose exec app python -m src.smoke_test

dashboard:
	docker compose exec app streamlit run dashboards/app.py --server.address 0.0.0.0

shell:
	docker compose exec app bash

spark-ui:
	@echo "Spark Master UI: http://localhost:8080"

minio-ui:
	@echo "MinIO Console:   http://localhost:9001 (user/senha do .env)"
