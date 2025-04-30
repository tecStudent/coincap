import streamlit as st
import pandas as pd
import altair as alt

from conectionPostgres import read_table
st.title("Radar Cripto")

st.subheader("Analise das Criptomoedas")
df = read_table("history_coin")
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date')

# filtros de período e coin (igual ao anterior)...
start_date, end_date = st.date_input("Período", value=(df['date'].dt.date.min(), df['date'].dt.date.max()))
selected = st.multiselect("Coins", options=df['coin'].unique(), default=df['coin'].unique())

mask = (
    (df['date'].dt.date >= start_date) &
    (df['date'].dt.date <= end_date) &
    (df['coin'].isin(selected))
)
df_filtrado = df[mask]

# pivot para wide-format
df_wide = (
    df_filtrado
    .pivot(index='date', columns='coin', values='priceUsd')
    .fillna(method='ffill')
)

st.subheader("Evolução de Preço")
st.line_chart(df_wide, use_container_width=True)

##############################################################################################

df_rank_coin = read_table("rank_coin")

df_rank_coin = df_rank_coin.dropna(subset=['maxSupply']).copy()


df_rank_coin['pct_nao_emitida'] = ( (df_rank_coin['maxSupply'] - df_rank_coin['supply']) / df_rank_coin['maxSupply'] * 100 ).round(2).clip(lower=0)

st.subheader("% de Moedas Não Emitidas")

# filtro interativo
symbols = df_rank_coin['symbol'].unique().tolist()
sel = st.multiselect("Selecione ativos", symbols, default=symbols[:5])

chart_df = df_rank_coin[df_rank_coin['symbol'].isin(sel)].set_index('symbol')['pct_nao_emitida']

if not chart_df.empty:
    st.bar_chart(chart_df)
else:
    st.info("Selecione ao menos um ativo.")


df_rank_coin = df_rank_coin.drop(columns=['id','explorer','dateRequest'])

df_rank_coin = df_rank_coin.rename(columns={
    'symbol':               'Símbolo',
    'name':                 'Nome',
    'supply':               'Quantidade em circulação',
    'maxSupply':            'Quantidade máxima possível',
    'marketCapUsd':         'Capitalização de mercado (USD)',
    'volumeUsd24Hr':        'Últimas 24h (USD)',
    'priceUsd':             'Preço unitário (USD)',
    'changePercent24Hr':    'Variação percentual 24h',
    'vwap24Hr':             'Preço médio 24h'
})




st.subheader("Tabela de métricas diárias")
st.dataframe(df_rank_coin, use_container_width=True, hide_index=True)




