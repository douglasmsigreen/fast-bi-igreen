# backend/db/executor.py
import psycopg2
import logging
from flask import g # Usado para acessar a conexão via g.db_conn
from typing import List, Tuple, Optional, Any
from .connection import get_db # Importa get_db do módulo connection local

logger = logging.getLogger(__name__)

# --- Função Principal para Executar Queries ---
def execute_query(query: str, params: Optional[tuple] = None, fetch_one=False) -> List[tuple] or Tuple or None:
    """Executa uma query SQL usando a conexão da requisição atual."""
    conn = get_db() # Obtém a conexão através da função do módulo connection
    result = None
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            if fetch_one:
                result = cur.fetchone()
            else:
                result = cur.fetchall()
        # Importante: Commit implícito se não houver erro, mas para SELECT não é necessário.
        # Se fossem INSERT/UPDATE/DELETE, um conn.commit() explícito seria mais seguro aqui fora do 'with'.
        # conn.commit() # Descomentar se fizer operações de escrita
        return result
    except psycopg2.OperationalError as e:
        logger.error(f"Erro operacional/conexão durante query: {e}", exc_info=False)
        # Tenta fechar a conexão "ruim" e removê-la do contexto g
        # (g.pop é feito no close_db, mas fechar a conexão aqui pode ser útil)
        # g.pop('db_conn', None) # Removido, close_db faz isso
        try: conn.close()
        except: pass
        # Relança o erro para que a rota Flask saiba que algo deu errado
        raise RuntimeError(f"Erro de conexão ou operacional: {e}")
    except psycopg2.Error as e:
        # Log específico para erro comum de coluna
        if isinstance(e, psycopg2.errors.UndefinedColumn):
             logger.error(f"Erro de coluna indefinida: Verifique nomes na query/tabela. Detalhe: {e}", exc_info=False)
        else:
             logger.error(f"Erro de banco de dados ({type(e).__name__}): {e}", exc_info=False)
        # Tenta fazer rollback em caso de erro na transação (útil para INSERT/UPDATE/DELETE)
        try: conn.rollback()
        except Exception as rb_err: logger.error(f"Erro durante rollback: {rb_err}")
        # Relança o erro
        raise RuntimeError(f"Erro ao executar a query: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado durante query ({type(e).__name__}): {e}", exc_info=True)
        try: conn.rollback()
        except Exception as rb_err: logger.error(f"Erro durante rollback: {rb_err}")
        raise RuntimeError(f"Erro inesperado: {e}")