"""
Dashboard: exportação por UF/ano cruzada com câmbio (camada gold).

Por que lê a gold com get_spark() e só vira pandas na borda (não desde
o início): é a mesma regra do resto do projeto - nenhum job instancia
SparkSession direto, e o Streamlit não precisa saber se o dado veio do
MinIO local ou do Databricks depois. A conversão pra pandas só acontece
DEPOIS da leitura, porque a gold já é pequena (1 linha por UF/ano) -
processar isso em Spark seria bazuca pra matar mosquito, mas LER com
Spark mantém o dashboard portável junto com o resto do pipeline.

Rodar: streamlit run dashboards/app.py
(dentro do container: docker compose exec app streamlit run dashboards/app.py --server.address 0.0.0.0)
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from src.common.config import settings
from src.common.spark_session import get_spark

GOLD_PATH = f"{settings.gold_zone}/exportacao_uf_ano"


@st.cache_data(ttl=300)
def load_gold() -> pd.DataFrame:
    spark = get_spark(app_name="dashboard")
    return spark.read.format("delta").load(GOLD_PATH).toPandas()


def main() -> None:
    st.set_page_config(page_title="Comex x Câmbio x IBGE", layout="wide")
    st.title("Exportação por UF/ano x câmbio x população/PIB per capita")

    df = load_gold()
    if df.empty:
        st.warning(f"Gold vazia em {GOLD_PATH} - rode o pipeline (silver_to_gold) primeiro.")
        return

    anos = sorted(df["ano"].unique())
    anos_selecionados = st.sidebar.multiselect("Ano", anos, default=anos)
    ufs_selecionadas = st.sidebar.multiselect(
        "UF", sorted(df["uf"].unique()), default=sorted(df["uf"].unique())
    )
    filtrado = df[df["ano"].isin(anos_selecionados) & df["uf"].isin(ufs_selecionadas)]

    if filtrado.empty:
        st.info("Nenhuma linha para os filtros selecionados.")
        return

    st.subheader("Exportação (FOB USD) por UF")
    exportacao_por_uf = (
        filtrado.groupby("uf")["exportacao_fob_usd"].sum().sort_values(ascending=False)
    )
    st.bar_chart(exportacao_por_uf)

    col1, col2 = st.columns(2)

    # Câmbio/população/PIB podem faltar pra um recorte de ano/UF sem ser bug
    # (limite real da fonte - ver docstring do silver_to_gold.py). Em vez de
    # desenhar gráfico vazio/quebrado, escondemos a seção inteira com um
    # aviso, e a tabela bruta no fim esconde só a(s) coluna(s) sem dado.
    tem_cambio = filtrado["cambio_medio_venda"].notna().any()

    with col1:
        st.subheader("Câmbio médio por ano")
        if not tem_cambio:
            st.caption("Sem câmbio (BCB) para os anos selecionados.")
        else:
            cambio_por_ano = (
                filtrado.dropna(subset=["cambio_medio_venda"])
                .drop_duplicates("ano")
                .set_index("ano")["cambio_medio_venda"]
            )
            st.line_chart(cambio_por_ano.sort_index())

    with col2:
        st.subheader("Exportação x câmbio médio (correlação)")
        if not tem_cambio:
            st.caption("Sem câmbio (BCB) para os anos selecionados.")
        else:
            com_cambio = filtrado.dropna(subset=["cambio_medio_venda"])
            st.scatter_chart(com_cambio, x="cambio_medio_venda", y="exportacao_fob_usd", color="uf")
            # Câmbio médio é por ANO (não varia entre UF dentro do mesmo ano) -
            # com 1 ano só, a "correlação" sai ~0 por falta de variância, não
            # porque não haja relação. pd.notna() sozinho não pega esse caso
            # (o resultado não é NaN, é só sem sentido estatístico).
            if com_cambio["ano"].nunique() < 2:
                st.caption(
                    "Correlação indisponível: só há 1 ano com câmbio nos dados "
                    "filtrados (câmbio não varia dentro de um único ano)."
                )
            else:
                correlacao = com_cambio["cambio_medio_venda"].corr(com_cambio["exportacao_fob_usd"])
                if pd.notna(correlacao):
                    st.metric("Correlação (Pearson)", f"{correlacao:.3f}")
                else:
                    st.caption("Correlação indisponível (variação insuficiente nos dados filtrados).")

    st.subheader("PIB per capita x exportação por UF")
    com_pib = filtrado.dropna(subset=["pib_per_capita_reais"])
    if com_pib.empty:
        st.caption(
            "Sem população/PIB para os anos selecionados - o IBGE tem hiato "
            "de população em 2022-2023 (pós-Censo) e PIB municipal atrasa ~2 anos."
        )
    else:
        st.scatter_chart(com_pib, x="pib_per_capita_reais", y="exportacao_fob_usd", color="uf")

    st.subheader("Dado bruto (gold)")
    # esconde coluna 100% vazia pro recorte atual (ex: populacao some se só
    # anos sem estimativa do IBGE estiverem selecionados)
    tabela = filtrado.dropna(axis=1, how="all")
    st.dataframe(tabela.sort_values(["ano", "exportacao_fob_usd"], ascending=[True, False]))


if __name__ == "__main__":
    main()
