import logging
import pandas as pd

from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, Table, inspect



logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_table(df: DataFrame, table_name: str, DATABASE_URL: str, if_exists: str = "fail") -> None:

    engine = create_engine(DATABASE_URL, echo=False, future=True)
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists, 
            index=False,
            method="multi",  
        )
        logger.info("Tabela '%s' %s com sucesso.", table_name, 
                   "atualizada" if if_exists in ["replace", "append"] else "criada")
    except Exception as e:
        logger.exception("Falha ao gravar tabela '%s': %s", table_name, e)
        raise


def check_table_exists(table_name: str, DATABASE_URL: str) -> bool:
    """Verifica se uma tabela existe no banco de dados."""
    engine = create_engine(DATABASE_URL, echo=False, future=True)
    inspector = inspect(engine)
    exists = inspector.has_table(table_name)
    logger.info("Tabela '%s' %s no banco de dados.", table_name, "existe" if exists else "não existe")
    return exists


def drop_table(table_name: str, DATABASE_URL: str) -> None:

    engine = create_engine(DATABASE_URL, echo=False, future=True)
    metadata = MetaData()
    try:
        table = Table(table_name, metadata, autoload_with=engine)
        logger.info("Removendo tabela '%s'...", table_name)
        table.drop(engine, checkfirst=True)
        logger.info("Tabela '%s' removida.", table_name)
    except Exception as e:
        logger.exception("Erro ao remover tabela '%s': %s", table_name, e)
        raise


def read_table(table_name: str, DATABASE_URL: str) -> DataFrame:

    engine = create_engine(DATABASE_URL, echo=False, future=True)
    try:
        df = pd.read_sql_table(
            table_name,
            con=engine,
            schema=None
        )
        logger.info("Tabela '%s' lida com sucesso: %d registros.", table_name,len(df))
        return df
    except Exception as e:
        logger.exception("Falha ao ler tabela '%s': %s", table_name, e)
        raise