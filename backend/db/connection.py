# backend/db/connection.py
import psycopg2
import psycopg2.pool
import logging
from flask import g
from ..config import Config # Ajuste no import relativo

logger = logging.getLogger(__name__)
db_pool = None

# --- Funções de Pool e Conexão ---
def init_pool():
    """Inicializa o pool de conexões."""
    global db_pool
    if db_pool:
        return
    try:
        logger.info("Inicializando pool de conexões com o banco de dados...")
        db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=15, **Config.DB_CONFIG
        )
        # Verifica a conexão inicial
        conn = db_pool.getconn()
        db_pool.putconn(conn)
        logger.info("Pool de conexões inicializado com sucesso.")
    except (psycopg2.Error, KeyError, Exception) as e:
        logger.critical(f"Falha CRÍTICA ao inicializar pool de conexões: {e}", exc_info=True)
        db_pool = None
        raise ConnectionError(f"Não foi possível inicializar o pool DB: {e}")

def get_db():
    """Obtém uma conexão do pool para a requisição Flask atual (g)."""
    global db_pool # Garante que estamos usando a variável global
    if not db_pool:
        try:
            logger.warning("Pool não inicializado. Tentando inicializar em get_db...")
            init_pool()
        except ConnectionError as e:
             logger.error(f"Tentativa de inicializar pool falhou em get_db: {e}")
             raise ConnectionError("Pool de conexões não está disponível.")

    if 'db_conn' not in g: # Renomeado de 'db' para evitar conflito com o nome do módulo
        try:
            g.db_conn = db_pool.getconn()
            # logger.debug("Conexão obtida do pool para a requisição.")
        except psycopg2.Error as e:
             logger.error(f"Falha ao obter conexão do pool: {e}", exc_info=True)
             raise ConnectionError(f"Não foi possível obter conexão do banco: {e}")
    return g.db_conn

def close_db(e=None):
    """Fecha a conexão (devolve ao pool) ao final da requisição Flask."""
    global db_pool # Garante que estamos usando a variável global
    db = g.pop('db_conn', None) # Renomeado de 'db'
    if db is not None and db_pool is not None:
        try:
            db_pool.putconn(db)
            # logger.debug("Conexão devolvida ao pool.")
        except psycopg2.Error as e:
             logger.error(f"Falha ao devolver conexão ao pool: {e}", exc_info=True)
             try: db.close()
             except: pass
    elif db is not None:
         try:
             db.close()
             # logger.debug("Conexão fechada (pool não disponível).")
         except: pass

def close_pool():
    """Fecha todas as conexões no pool (útil no desligamento da app)."""
    global db_pool
    if db_pool:
        try:
            logger.info("Fechando pool de conexões...")
            db_pool.closeall()
            db_pool = None
            logger.info("Pool de conexões fechado.")
        except Exception as e:
            logger.error(f"Erro ao fechar o pool de conexões: {e}", exc_info=True)