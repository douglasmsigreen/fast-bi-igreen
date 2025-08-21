# backend/db/connection.py
import psycopg2
import psycopg2.pool
import logging
from flask import g, current_app
from ..config import Config

logger = logging.getLogger(__name__)

# A variável db_pool agora é um objeto global que será preenchido
db_pool = None

def init_app(app):
    """
    Função de inicialização que anexa o pool de conexões à aplicação Flask.
    """
    if 'db_pool' not in app.extensions:
        app.extensions['db_pool'] = None
    
    app.teardown_appcontext(close_db)
    
    app.extensions['db_pool'] = create_pool(app)

def create_pool(app):
    """
    Cria e retorna um pool de conexões.
    """
    try:
        logger.info("Inicializando pool de conexões com o banco de dados...")
        pool = psycopg2.pool.SimpleConnectionPool(
            minconn=2,
            maxconn=15,
            **app.config['DB_CONFIG']
        )
        conn = pool.getconn()
        pool.putconn(conn)
        logger.info("Pool de conexões inicializado com sucesso.")
        return pool
    except (psycopg2.Error, KeyError, Exception) as e:
        logger.critical(f"Falha CRÍTICA ao inicializar pool de conexões: {e}", exc_info=True)
        return None

def get_db():
    """Obtém uma conexão do pool para a requisição Flask atual (g)."""
    if 'db_conn' not in g:
        pool = current_app.extensions.get('db_pool')
        if pool is None:
            raise ConnectionError('Database pool not available.')
        try:
            g.db_conn = pool.getconn()
        except psycopg2.Error as e:
             logger.error(f"Falha ao obter conexão do pool: {e}", exc_info=True)
             raise ConnectionError(f"Não foi possível obter conexão do banco: {e}")
    return g.db_conn

def close_db(e=None):
    """Fecha a conexão (devolve ao pool) ao final da requisição Flask."""
    db = g.pop('db_conn', None)
    pool = g.pop('db_pool', None)
    if db is not None and pool is not None:
        try:
            pool.putconn(db)
        except Exception as e:
            logger.error(f"Falha ao devolver conexão ao pool: {e}", exc_info=True)
            try: db.close()
            except: pass
    elif db is not None:
         try: db.close()
         except: pass

def close_pool(app):
    """Fecha todas as conexões no pool (útil no desligamento da app)."""
    pool = app.extensions.get('db_pool')
    if pool:
        try:
            logger.info("Fechando pool de conexões...")
            pool.closeall()
            logger.info("Pool de conexões fechado.")
        except Exception as e:
            logger.error(f"Erro ao fechar o pool de conexões: {e}", exc_info=True)