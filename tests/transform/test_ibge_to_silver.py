from src.transform import ibge_to_silver as mod

BRONZE_COLS = [
    "dataset", "municipio_id", "municipio_nome", "ano", "valor",
    "unidade", "_ingested_at", "_run_id",
]


def _seed_bronze(spark, path, rows):
    spark.createDataFrame(rows, BRONZE_COLS).write.format("delta").mode(
        "overwrite"
    ).partitionBy("dataset").save(path)


def _patch_zones(monkeypatch, tmp_path):
    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    monkeypatch.setattr(mod, "IBGE_BRONZE", bronze_path)
    monkeypatch.setattr(mod, "IBGE_SILVER", silver_path)
    return bronze_path, silver_path


def test_transform_ibge_deriva_uf_e_descarta_valor_invalido(spark, tmp_path, monkeypatch):
    bronze_path, silver_path = _patch_zones(monkeypatch, tmp_path)

    _seed_bronze(
        spark,
        bronze_path,
        [
            ("populacao", "3550308", "São Paulo - SP", 2021, "12396372", "Pessoas", "2026-01-01T00:00:00", "run1"),
            # "-" é a convenção do IBGE pra dado indisponível -> vira NULL no cast -> descartado
            ("populacao", "1100015", "Alta Floresta D'Oeste - RO", 2021, "-", "Pessoas", "2026-01-01T00:00:00", "run1"),
        ],
    )

    mod.transform_ibge(spark)

    silver = spark.read.format("delta").load(silver_path)
    assert silver.count() == 1
    row = silver.collect()[0]
    assert row.uf == "SP"  # derivado do código 35xxxxx, não do texto do nome
    assert row.valor == 12396372.0


def test_transform_ibge_merge_e_idempotente_pega_revisao(spark, tmp_path, monkeypatch):
    """MERGE por (dataset, municipio_id, ano): reprocessar a mesma chave
    atualiza o valor (revisão do IBGE) em vez de duplicar linha."""
    bronze_path, silver_path = _patch_zones(monkeypatch, tmp_path)

    _seed_bronze(
        spark,
        bronze_path,
        [("populacao", "3550308", "São Paulo - SP", 2021, "12396372", "Pessoas", "2026-01-01T00:00:00", "run1")],
    )
    mod.transform_ibge(spark)  # cria a silver

    _seed_bronze(
        spark,
        bronze_path,
        [("populacao", "3550308", "São Paulo - SP", 2021, "12400000", "Pessoas", "2026-02-01T00:00:00", "run2")],
    )
    mod.transform_ibge(spark)  # MERGE

    silver = spark.read.format("delta").load(silver_path)
    assert silver.count() == 1  # não duplicou
    assert silver.collect()[0].valor == 12400000.0  # ficou com a revisão
