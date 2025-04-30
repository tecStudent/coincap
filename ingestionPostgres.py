import requests 
import pandas as pd
import datetime
import os
import json
from pandas import DataFrame
from dotenv import load_dotenv

from conectionPostgres import create_table, drop_table

load_dotenv()

key = os.getenv("API_KEY")

api_key = json.loads(key)

RAILWAY_PRIVATE_DOMAIN = os.getenv("RAILWAY_PRIVATE_DOMAIN")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PGUSER = os.getenv("PGUSER")
PGPORT = os.getenv("PGPORT")
PGDATABASE = os.getenv("PGDATABASE")



url = f'https://rest.coincap.io/v3/assets'


def fetch_data(endpoint: str, api_key: dict) -> dict:
    response = requests.get(endpoint,params=api_key)
    response.raise_for_status()  
    return response.json()

def etl_rank_coin(endpoint: str, api_key: dict) -> DataFrame:

    data = fetch_data(endpoint=endpoint,api_key=api_key)

    df = DataFrame(data['data'])
    num_cols = ['supply','maxSupply','marketCapUsd','volumeUsd24Hr','priceUsd','changePercent24Hr',	'vwap24Hr']

    df[num_cols] = df[num_cols].apply(pd.to_numeric)

    df = df.round(2)

    dt = datetime.datetime.fromtimestamp(data['timestamp'] / 1000)

    df['dateRequest'] = dt.strftime('%Y-%m-%d')

    return df

def get_top_5(df: DataFrame) -> list:
    return list(df['id'].head(5))

def etl_history_coin(list_coin: list, endpoint: str, api_key: dict) -> DataFrame:

    api_key['interval'] = 'd1'

    dfs = []

    for coin in list_coin:
        df = DataFrame()

        url = f"{endpoint}/{coin}/history"

        data = fetch_data(endpoint=url,api_key=api_key)

        df = DataFrame(data['data'])

        df = df[['date','priceUsd']]

        df['priceUsd'] = df['priceUsd'].apply(pd.to_numeric)
        df = df.round(2)

        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        df['coin'] = coin

        dfs.append(df)

    df_final = pd.concat(dfs, axis=0, ignore_index=True)
    return df_final




df_rank_coin = etl_rank_coin(endpoint=url,api_key=api_key)

list_coin = get_top_5(df_rank_coin)

df_history_coin = etl_history_coin(list_coin=list_coin,endpoint=url,api_key=api_key)


print(df_history_coin.sample(10))


drop_table("rank_coin")
drop_table("history_coin")


create_table(
    df=df_rank_coin,
    table_name="rank_coin",
    if_exists="fail", 
)

create_table(
    df=df_history_coin,
    table_name="history_coin",
    if_exists="fail",  
)
