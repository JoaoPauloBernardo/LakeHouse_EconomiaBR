"""
Testes pro bronze_to_silver.py (BCB e Comex) - módulo do João Paulo, mas
"testes pros módulos de ingestão e transformação" é literalmente a tarefa 3,
então cobre os dois, não só o que eu escrevi.
"""

from src.transform import bronze_to_silver as mod

BCB_COLS = ["serie", "data", "valor", "_ingested_at", "_run_id"]
COMEX_COLS = [
    "ano", "fluxo", "CO_MES", "CO_NCM", "CO_UNID", "CO_PAIS", "SG_UF_NCM",
    "CO_VIA", "CO_URF", "QT_ESTAT", "KG_LIQUIDO", "VL_FOB",
    "_run_id", "_ingested_at",
]


def _patch_zones(monkeypatch, tmp_path):
    monkeypatch.setattr(mod, "BCB_BRONZE", str(tmp_path / "bronze" / "bcb_sgs"))
    monkeypatch.setattr(mod, "BCB_SILVER", str(tmp_path / "silver" / "bcb_sgs"))
    monkeypatch.setattr(mod, "COMEX_BRONZE", str(tmp_path / "bronze" / "comex_stat"))
    monkeypatch.setattr(mod, "COMEX_SILVER", str(tmp_path / "silver" / "comex_stat"))


def _seed_bcb_bronze(spark, rows):
    spark.createDataFrame(rows, BCB_COLS).write.format("delta").mode(
        "overwrite"
    ).partitionBy("serie").save(mod.BCB_BRONZE)


def _comex_row(ano, fluxo, mes, uf, vl_fob, run_id="run1", ingested="2026-01-01T00:00:00"):
    return (ano, fluxo, mes, "12019000", "10", "249", uf, "1", "0817600", "100", "5000", vl_fob, run_id, ingested)


def _seed_comex_bronze(spark, rows):
    spark.createDataFrame(rows, COMEX_COLS).write.format("delta").mode(
        "overwrite"
    ).partitionBy("ano", "fluxo").save(mod.COMEX_BRONZE)


def test_transform_bcb_dedup_pega_valor_mais_recente(spark, tmp_path, monkeypatch):
    """Bronze do BCB pode ter (serie, data) repetida entre execuções -
    a mais recente por _ingested_at é quem vence antes do MERGE."""
    _patch_zones(monkeypatch, tmp_path)

    _seed_bcb_bronze(
        spark,
        [
            (1, "15/06/2021", "5.10", "2026-01-01T00:00:00", "run1"),
            (1, "15/06/2021", "5.15", "2026-01-02T00:00:00", "run2"),  # revisão
        ],
    )

    mod.transform_bcb(spark)

    silver = spark.read.format("delta").load(mod.BCB_SILVER)
    assert silver.count() == 1
    assert silver.collect()[0].valor == 5.15


def test_transform_bcb_descarta_data_invalida(spark, tmp_path, monkeypatch):
    _patch_zones(monkeypatch, tmp_path)

    _seed_bcb_bronze(
        spark,
        [
            (1, "15/06/2021", "5.10", "2026-01-01T00:00:00", "run1"),
            (1, "31/13/2021", "9.99", "2026-01-01T00:00:00", "run1"),  # mês 13 não existe
        ],
    )

    mod.transform_bcb(spark)

    silver = spark.read.format("delta").load(mod.BCB_SILVER)
    assert silver.count() == 1  # a linha com data inválida foi descartada


def test_transform_comex_filtra_mes_invalido(spark, tmp_path, monkeypatch):
    """Guard-rail: mês fora de 1-12 é descartado, não quebra o job."""
    _patch_zones(monkeypatch, tmp_path)

    _seed_comex_bronze(
        spark,
        [
            _comex_row(2023, "EXP", "6", "SP", "25000"),
            _comex_row(2023, "EXP", "13", "RJ", "15000"),  # mês inválido
        ],
    )

    mod.transform_comex(spark, [2023])

    silver = spark.read.format("delta").load(mod.COMEX_SILVER)
    assert silver.count() == 1
    assert silver.collect()[0].uf == "SP"


def test_transform_comex_replacewhere_e_idempotente(spark, tmp_path, monkeypatch):
    _patch_zones(monkeypatch, tmp_path)

    _seed_comex_bronze(spark, [_comex_row(2023, "EXP", "6", "SP", "25000")])

    mod.transform_comex(spark, [2023])
    mod.transform_comex(spark, [2023])  # reprocessa o mesmo ano

    silver = spark.read.format("delta").load(mod.COMEX_SILVER)
    assert silver.count() == 1  # não duplicou
