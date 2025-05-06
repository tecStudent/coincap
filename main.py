import os
import json
import pandas as pd

from dotenv import load_dotenv
from utils.ingestion import etl_rank_coin, get_top_5, etl_history_coin
from utils.conection import create_table, check_table_exists, read_table
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

    # Verificar se as tabelas existem
    rank_exists = check_table_exists("rank_coin", DATABASE_URL=DATABASE_URL)
    hist_exists = check_table_exists("history_coin", DATABASE_URL=DATABASE_URL)
    
    # Usar "replace" para rank_coin (dados diários que são substituídos)
    create_table(df=df_rank, table_name="rank_coin", DATABASE_URL=DATABASE_URL, 
                 if_exists="replace" if rank_exists else "fail")
    
    # Para history_coin, vamos tratar diferente para evitar duplicatas
    if hist_exists:
        # Ler a tabela existente
        existing_hist = read_table("history_coin", DATABASE_URL=DATABASE_URL)
        # Converter para o mesmo formato que os novos dados
        existing_hist['date'] = pd.to_datetime(existing_hist['date']).dt.strftime('%Y-%m-%d')
        
        # Concat com os novos dados
        combined_hist = pd.concat([existing_hist, df_hist], ignore_index=True)
        # Remover duplicatas baseadas em date e coin
        combined_hist = combined_hist.drop_duplicates(subset=['date', 'coin'])
        
        # Recriar a tabela com os dados combinados
        create_table(df=combined_hist, table_name="history_coin", DATABASE_URL=DATABASE_URL, if_exists="replace")
    else:
        # Se não existe, criar nova
        create_table(df=df_hist, table_name="history_coin", DATABASE_URL=DATABASE_URL, if_exists="fail")


run_etl()
render_dashboard(DATABASE_URL=DATABASE_URL)