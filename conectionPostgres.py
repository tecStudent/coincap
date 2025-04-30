import os
import logging

from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import Engine

# — Carrega variáveis de ambiente do .env (se estiver em dev)
load_dotenv()

# — Logger configurado
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# — Configuração centralizada da URL do banco
DB_USER = os.getenv("PGUSER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("RAILWAY_PRIVATE_DOMAIN")
DB_PORT = os.getenv("PGPORT")
DB_NAME = os.getenv("PGDATABASE")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_engine() -> Engine:
    """
    Retorna uma instância de Engine, usando future=True para comportamentos mais previsíveis.
    """
    return create_engine(DATABASE_URL, echo=False, future=True)

def create_table(
    df: DataFrame,
    table_name: str,
    if_exists: str = "fail",
) -> None:
    """
    Grava um DataFrame no banco usando pandas.to_sql.
    
    Parâmetros:
    - df: DataFrame a ser persistido.
    - table_name: nome da tabela destino.
    - engine: instância de Engine (se não passada, será criada).
    - if_exists: 'fail', 'replace' ou 'append'.
    """
    engine = get_engine()
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method="multi",  
        )
        logger.info("Tabela '%s' escrita com sucesso (if_exists=%s).", table_name, if_exists)
    except Exception as e:
        logger.exception("Falha ao gravar tabela '%s': %s", table_name, e)
        raise

def drop_table(
    table_name: str
) -> None:
    """
    Remove uma tabela do banco de forma segura.
    
    Parâmetros:
    - table_name: nome da tabela a dropar.
    - engine: instância de Engine (se não passada, será criada).
    """
    engine = get_engine()
    metadata = MetaData()
    try:
        table = Table(table_name, metadata, autoload_with=engine)
        logger.info("Removendo tabela '%s'...", table_name)
        table.drop(engine, checkfirst=True)
        logger.info("Tabela '%s' removida.", table_name)
    except Exception as e:
        logger.exception("Erro ao remover tabela '%s': %s", table_name, e)
        raise

