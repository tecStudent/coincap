import logging
import requests 
import pandas as pd
import datetime
from pandas import DataFrame


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def fetch_data(endpoint: str, api_key: dict) -> dict:
    try:
        
        response = requests.get(endpoint, params=api_key, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info("Recebido %d chaves", len(data))
        return data
    except requests.exceptions.RequestException as err:
        logger.error("Erro de requisição em %s: %s", endpoint, err)
        raise
    except ValueError as err:
        logger.error("Resposta JSON inválida de %s: %s", endpoint, err)
        raise

def etl_rank_coin(endpoint: str, api_key: dict) -> DataFrame:
    try:
        data = fetch_data(endpoint=endpoint, api_key=api_key)
        df = DataFrame(data.get('data', []))
        num_cols = [
            'supply', 'maxSupply', 'marketCapUsd',
            'volumeUsd24Hr', 'priceUsd',
            'changePercent24Hr', 'vwap24Hr'
        ]
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce').round(2)
        timestamp = data.get('timestamp')
        dt = datetime.datetime.fromtimestamp(timestamp / 1000) if timestamp else datetime.datetime.utcnow()
        df['dateRequest'] = dt.strftime('%Y-%m-%d')
        logger.info("ETL de rank_coin finalizado com %d registros", len(df))
        return df
    except Exception as err:
        logger.exception("Erro em etl_rank_coin: %s", err)
        raise

def get_top_5(df: DataFrame) -> list:
    try:
        top5 = list(df['id'].head(5))
        logger.info("Top 5 moedas: %s", top5)
        return top5
    except KeyError as err:
        logger.error("Coluna 'id' não encontrada no DataFrame: %s", err)
        raise
    except Exception as err:
        logger.exception("Erro em get_top_5: %s", err)
        raise

def etl_history_coin(list_coin: list, endpoint: str, api_key: dict) -> DataFrame:
    try:
        params = api_key.copy()
        params['interval'] = 'd1'
        dfs = []
        for coin in list_coin:
            url = f"{endpoint}/{coin}/history"
            logger.info("Buscando histórico para %s", coin)
            data = fetch_data(endpoint=url, api_key=params)
            df = DataFrame(data.get('data', []))
            df = df[['date', 'priceUsd']].copy()
            df['priceUsd'] = pd.to_numeric(df['priceUsd'], errors='coerce').round(2)
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')
            df['coin'] = coin
            dfs.append(df)
        df_final = pd.concat(dfs, axis=0, ignore_index=True)
        logger.info("ETL de history_coin finalizado com %d registros", len(df_final))
        return df_final
    except Exception as err:
        logger.exception("Erro em etl_history_coin: %s", err)
        raise
