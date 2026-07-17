# Comandos do dia a dia. `make help` lista tudo.

.PHONY: help up down ps logs ingest-bcb ingest-ibge ingest-comex silver gold smoke test benchmark dashboard shell spark-ui minio-ui airflow-ui airflow-password publish-databricks

help:
	@echo "-- ambiente --"
	@echo "make up           - sobe o ambiente (minio + spark + app + airflow)"
	@echo "make down         - derruba tudo"
	@echo "make ps           - status dos containers"
	@echo "make smoke        - smoke test (Spark + Delta + S3A + MinIO)"
	@echo "make shell        - abre um bash dentro do container app"
	@echo ""
	@echo "-- pipeline (uso: make silver ANOS=\"2023 2024\") --"
	@echo "make ingest-bcb   - ingestao BCB SGS -> bronze"
	@echo "make ingest-ibge  - ingestao IBGE SIDRA -> bronze  (ANOS=...)"
	@echo "make ingest-comex - ingestao Comex Stat -> bronze  (ANOS=...)"
	@echo "make silver       - bronze -> silver, todas as fontes (ANOS=...)"
	@echo "make gold         - silver -> gold"
	@echo ""
	@echo "-- qualidade e entrega --"
	@echo "make test         - roda a suite pytest"
	@echo "make benchmark    - experimentos de tuning (precisa de varios anos na silver)"
	@echo "make dashboard    - dashboard Streamlit (http://localhost:8501)"
	@echo "make publish-databricks - publica a gold no Unity Catalog"
	@echo ""
	@echo "-- UIs --"
	@echo "make spark-ui     - lembra a URL da UI do Spark"
	@echo "make minio-ui     - lembra a URL do console do MinIO"
	@echo "make airflow-ui   - lembra a URL do Airflow"
	@echo "make airflow-password - mostra a senha do admin do Airflow"

up:
	docker compose up -d --build --scale spark-worker=2

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
ingest-bcb:
	docker compose exec app python -m src.ingestion.bcb_sgs

# uso: make ingest-ibge ANOS="2022 2023"
ingest-ibge:
	docker compose exec app python -m src.ingestion.ibge_sidra --anos $(ANOS)

# uso: make ingest-comex ANOS="2023 2024"
ingest-comex:
	docker compose exec app python -m src.ingestion.comex_stat --years $(ANOS)

# BCB e IBGE processam a fonte inteira; Comex precisa dos anos
silver:
	docker compose exec app python -m src.transform.bronze_to_silver --datasets bcb
	docker compose exec app python -m src.transform.bronze_to_silver --datasets comex --years $(ANOS)
	docker compose exec app python -m src.transform.ibge_to_silver

gold:
	docker compose exec app python -m src.transform.silver_to_gold

# ---------------------------------------------------------------------------
# Qualidade e entrega
# ---------------------------------------------------------------------------
smoke:
	docker compose exec app python -m src.smoke_test

test:
	docker compose exec app python -m pytest tests/ -v

benchmark:
	docker compose exec app python -m src.tuning.benchmark

dashboard:
	docker compose exec app streamlit run dashboards/app.py --server.address 0.0.0.0

publish-databricks:
	docker compose exec app python -m src.publish.databricks_publish

shell:
	docker compose exec app bash

# ---------------------------------------------------------------------------
# UIs
# ---------------------------------------------------------------------------
spark-ui:
	@echo "Spark Master UI: http://localhost:8080"
	@echo "Spark App UI:    http://localhost:4040 (so enquanto um job roda)"

minio-ui:
	@echo "MinIO Console:   http://localhost:9001 (user/senha do .env)"

airflow-ui:
	@echo "Airflow UI:      http://localhost:8081 (user: admin)"

airflow-password:
	docker compose logs airflow 2>/dev/null | grep -i "password" | tail -1
