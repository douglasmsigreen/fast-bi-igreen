# backend/db/executor.py
import psycopg2
import logging
from flask import g
from typing import List, Tuple, Optional, Any
from .connection import get_db

logger = logging.getLogger(__name__)

def execute_query(query: str, params: Optional[tuple] = None, fetch_one=False) -> List[tuple] or Tuple or None:
    """Executa uma query SQL usando a conexão da requisição atual."""
    conn = get_db()
    result = None
    try:
        with conn.cursor() as cur:
            # LOG MAIS DETALHADO DOS PARÂMETROS
            params_to_execute = params or () # Garante que seja uma tupla
            logger.debug(f"EXECUTOR_PY - Query: [{query}]")
            logger.debug(f"EXECUTOR_PY - Params ANTES de execute: {params_to_execute}, Tipo: {type(params_to_execute)}")
            if isinstance(params_to_execute, tuple) and len(params_to_execute) > 0:
                for i, p_item in enumerate(params_to_execute):
                    logger.debug(f"EXECUTOR_PY - Param[{i}]: {p_item}, Tipo: {type(p_item)}")
            
            cur.execute(query, params_to_execute) # Usar params_to_execute
            
            if fetch_one:
                result = cur.fetchone()
            else:
                result = cur.fetchall()
        return result
    # ... (resto dos blocos except como antes, talvez adicionando o log de query e params neles também) ...
    except psycopg2.OperationalError as e:
        logger.error(f"Erro operacional/conexão: {e}. Query: [{query}], Params: [{params}]", exc_info=False)
        try: conn.close()
        except: pass
        raise RuntimeError(f"Erro de conexão ou operacional: {e}")
    except psycopg2.errors.UndefinedColumn as e:
        logger.error(f"Erro de coluna indefinida: {e}. Query: [{query}], Params: [{params}]", exc_info=False)
        try: conn.rollback()
        except Exception as rb_err: logger.error(f"Erro durante rollback: {rb_err}")
        raise RuntimeError(f"Erro de coluna indefinida: {e}")
    except psycopg2.Error as e:
        error_type_name = type(e).__name__
        logger.error(f"Erro de psycopg2 ({error_type_name}): {e}. Query: [{query}], Params: [{params}]", exc_info=False)
        try: conn.rollback()
        except Exception as rb_err: logger.error(f"Erro durante rollback: {rb_err}")
        raise RuntimeError(f"Erro ao executar a query: {e}")
    except IndexError as e:
        logger.error(f"IndexError em execute_query: {e}. Query: [{query}], Params: [{params}]", exc_info=True)
        raise RuntimeError(f"Erro de índice (IndexError): {e}")
    except Exception as e:
        logger.error(f"Erro inesperado em execute_query ({type(e).__name__}): {e}. Query: [{query}], Params: [{params}]", exc_info=True)
        try: conn.rollback()
        except Exception as rb_err: logger.error(f"Erro durante rollback: {rb_err}")
        raise RuntimeError(f"Erro inesperado: {e}")