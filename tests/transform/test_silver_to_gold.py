from datetime import date

from src.transform import silver_to_gold as mod


def _patch_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(mod, "COMEX_SILVER", str(tmp_path / "silver" / "comex_stat"))
    monkeypatch.setattr(mod, "BCB_SILVER", str(tmp_path / "silver" / "bcb_sgs"))
    monkeypatch.setattr(mod, "IBGE_SILVER", str(tmp_path / "silver" / "ibge_sidra"))
    monkeypatch.setattr(mod, "GOLD_PATH", str(tmp_path / "gold" / "exportacao_uf_ano"))


def _seed(spark, tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)

    comex = spark.createDataFrame(
        [
            ("SP", 2021, "EXP", 100),
            ("SP", 2021, "IMP", 999),  # não deve entrar na soma (só EXP)
            ("RJ", 2021, "EXP", 50),
        ],
        ["uf", "ano", "fluxo", "vl_fob"],
    )
    comex.write.format("delta").mode("overwrite").save(mod.COMEX_SILVER)

    bcb = spark.createDataFrame(
        [
            (1, date(2021, 1, 4), 5.0),
            (1, date(2021, 6, 15), 5.2),
            (1, date(2021, 12, 30), 5.4),
            (432, date(2021, 6, 15), 13.75),  # selic - não deve entrar (só serie 1)
        ],
        ["serie", "data", "valor"],
    )
    bcb.write.format("delta").mode("overwrite").save(mod.BCB_SILVER)

    ibge = spark.createDataFrame(
        [
            ("populacao", "SP", 2021, 1000.0),
            ("pib_municipal", "SP", 2021, 50000.0),
            ("populacao", "RJ", 2021, 500.0),
            ("pib_municipal", "RJ", 2021, 20000.0),
        ],
        ["dataset", "uf", "ano", "valor"],
    )
    ibge.write.format("delta").mode("overwrite").save(mod.IBGE_SILVER)


def test_build_gold_junta_as_tres_fontes_por_uf_ano(spark, tmp_path, monkeypatch):
    _seed(spark, tmp_path, monkeypatch)

    mod.build_gold(spark)

    gold = spark.read.format("delta").load(mod.GOLD_PATH)
    rows = {r.uf: r for r in gold.collect()}

    assert gold.count() == 2

    sp = rows["SP"]
    assert sp.exportacao_fob_usd == 100  # só o EXP, não o IMP de 999
    assert sp.cambio_medio_venda == (5.0 + 5.2 + 5.4) / 3  # só a série 1
    assert sp.populacao == 1000.0
    assert sp.pib_total_mil_reais == 50000.0
    assert sp.pib_per_capita_reais == 50000.0  # 50000 mil reais * 1000 / 1000 hab

    rj = rows["RJ"]
    assert rj.exportacao_fob_usd == 50
    assert rj.pib_per_capita_reais == 40000.0  # 20000 mil reais * 1000 / 500 hab


def test_build_gold_e_particionado_por_ano(spark, tmp_path, monkeypatch):
    _seed(spark, tmp_path, monkeypatch)

    mod.build_gold(spark)

    from delta.tables import DeltaTable

    detail = DeltaTable.forPath(spark, mod.GOLD_PATH).detail().collect()[0]
    assert detail.partitionColumns == ["ano"]
