# backend/db/executor.py
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import current_app

logger = logging.getLogger(__name__)

def execute_query(query, params=None):
    """
    Executa uma query SELECT e retorna todos os resultados como uma lista de dicionários.
    """
    conn = None
    try:
        # Acessa a pool através da extensão do app
        pool = current_app.extensions['db_pool']
        conn = pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            logger.debug(f"Executando query: {query}")
            cursor.execute(query, params)
            results = cursor.fetchall()
            return results
    except (KeyError, psycopg2.Error, Exception) as e:
        logger.error(f"Erro ao executar a query: {e}", exc_info=True)
        return []
    finally:
        if conn:
            pool.putconn(conn)

def execute_query_one(query, params=None):
    """
    Executa uma query SELECT e retorna o resultado da primeira linha como um dicionário.
    Retorna None se a query não encontrar resultados.
    """
    conn = None
    try:
        # Acessa a pool através da extensão do app
        pool = current_app.extensions['db_pool']
        conn = pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            logger.debug(f"Executando query one: {query}")
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result
    except (KeyError, psycopg2.Error, Exception) as e:
        logger.error(f"Erro ao executar a query one: {e}", exc_info=True)
        return None
    finally:
        if conn:
            pool.putconn(conn)