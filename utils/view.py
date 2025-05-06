import streamlit as st
import pandas as pd
from .conection import read_table 


def display_price_evolution(DATABASE_URL: str):
    df = read_table("history_coin", DATABASE_URL=DATABASE_URL)
    
    df['date'] = pd.to_datetime(df['date'])
    
    df = df.drop_duplicates(subset=['date', 'coin'])
    
    df = df.sort_values('date')

    start_date, end_date = st.date_input(
        "Período", 
        value=(df['date'].dt.date.min(), df['date'].dt.date.max())
    )
    selected_coins = st.multiselect(
        "Coins", 
        options=df['coin'].unique(), 
        default=list(df['coin'].unique())
    )

    mask = (
        (df['date'].dt.date >= start_date) &
        (df['date'].dt.date <= end_date) &
        (df['coin'].isin(selected_coins))
    )
    df_filtrado = df.loc[mask]
    
    if not df_filtrado.empty:
        df_wide = df_filtrado.pivot_table(
            index='date', 
            columns='coin', 
            values='priceUsd',
            aggfunc='mean' 
        ).fillna(method='ffill')
        
        st.subheader("Evolução de Preço")
        st.line_chart(df_wide, use_container_width=True)
    else:
        st.info("Nenhum dado disponível para o período e moedas selecionados.")


def display_pct_nao_emitida(DATABASE_URL: str):

    df_rank = read_table("rank_coin", DATABASE_URL=DATABASE_URL)
    df_rank = df_rank.dropna(subset=['maxSupply']).copy()
    df_rank['pct_nao_emitida'] = (
        (df_rank['maxSupply'] - df_rank['supply']) / df_rank['maxSupply'] * 100
    ).round(2).clip(lower=0)

    st.subheader("% de Moedas Não Emitidas")
    symbols = df_rank['symbol'].unique().tolist()
    sel = st.multiselect("Selecione ativos", symbols, default=symbols[:5])
    chart_df = df_rank[df_rank['symbol'].isin(sel)].set_index('symbol')['pct_nao_emitida']
    if not chart_df.empty:
        st.bar_chart(chart_df)
    else:
        st.info("Selecione ao menos um ativo.")


def display_metrics_table(DATABASE_URL: str):
    df_rank = read_table("rank_coin", DATABASE_URL=DATABASE_URL)
    df_rank = df_rank.dropna(subset=['maxSupply']).copy()
    df_rank['pct_nao_emitida'] = (
        (df_rank['maxSupply'] - df_rank['supply']) / df_rank['maxSupply'] * 100
    ).round(2).clip(lower=0)

    df_table = (
        df_rank
        .drop(columns=['id', 'explorer', 'dateRequest'])
        .rename(columns={
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
    )
    st.subheader("Tabela de métricas diárias")
    st.dataframe(df_table, use_container_width=True, hide_index=True)


def render_dashboard(DATABASE_URL: str):

    st.title("Radar Cripto")
    st.subheader("Análise das Criptomoedas")
    display_price_evolution(DATABASE_URL=DATABASE_URL)
    display_pct_nao_emitida(DATABASE_URL=DATABASE_URL)
    display_metrics_table(DATABASE_URL=DATABASE_URL)