import os
import json

from dotenv import load_dotenv
from utils.ingestion import etl_rank_coin, get_top_5, etl_history_coin
from utils.conection import create_table, drop_table
from utils.view import render_dashboard

load_dotenv()

key = os.getenv("API_KEY")

api_key = json.loads(key)


# — Configuração centralizada da URL do banco
DB_USER = os.getenv("PGUSER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("RAILWAY_PRIVATE_DOMAIN")
DB_PORT = os.getenv("PGPORT")
DB_NAME = os.getenv("PGDATABASE")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


url = f'https://rest.coincap.io/v3/assets'


def run_etl():

    df_rank = etl_rank_coin(endpoint=url, api_key=api_key)
    top5 = get_top_5(df_rank)
    df_hist = etl_history_coin(list_coin=top5, endpoint=url, api_key=api_key)

    drop_table("rank_coin", DATABASE_URL=DATABASE_URL)
    drop_table("history_coin", DATABASE_URL=DATABASE_URL)

    create_table(df=df_rank,     table_name="rank_coin",    DATABASE_URL=DATABASE_URL)
    create_table(df=df_hist,     table_name="history_coin", DATABASE_URL=DATABASE_URL)


run_etl()
render_dashboard(DATABASE_URL=DATABASE_URL)
