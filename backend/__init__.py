# backend/__init__.py
import os
import logging
from flask import Flask, g
from flask_login import LoginManager
from .config import Config # Importa a configuração local
from . import database      # Importa o módulo database local
from .models import User    # Importa o modelo User

# --- Configuração de Logging (similar ao app.py original) ---
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s")
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
logger = logging.getLogger(__name__) # Logger específico do backend
if not logger.handlers: # Evita adicionar handlers duplicados em recargas
    logger.addHandler(log_handler)
logger.setLevel(logging.INFO) # Ou DEBUG conforme necessário

# --- Inicialização do LoginManager fora da factory ---
# Extensões que precisam ser acessadas globalmente (como em models.py ou decorators)
# podem ser inicializadas aqui e configuradas dentro da factory usando init_app()
login_manager = LoginManager()
login_manager.login_view = 'auth_bp.login' # Aponta para o endpoint de login no Blueprint 'auth_bp'
login_manager.login_message = "Por favor, faça login para aceder a esta página."
login_manager.login_message_category = "info"

@login_manager.user_loader
def load_user(user_id):
    """Carrega o utilizador pelo ID (igual ao original)."""
    # É importante que User.get_by_id funcione sem depender do app context diretamente aqui
    # O acesso ao banco via `database.execute_query` usará `g` que é gerenciado pelo Flask
    try:
        return User.get_by_id(int(user_id))
    except ValueError:
        logger.warning(f"ID de utilizador inválido fornecido para user_loader: {user_id}")
        return None
    except Exception as e:
        logger.error(f"Erro no user_loader para ID {user_id}: {e}", exc_info=True)
        return None

def create_app(config_class=Config):
    """
    Função App Factory: Cria e configura a instância da aplicação Flask.
    """
    logger.info("Criando instância da aplicação Flask...")
    # Define as pastas de templates e estáticos para procurar na raiz do projeto
    app = Flask(__name__, instance_relative_config=False,
                template_folder='../templates', # Aponta para a pasta templates na raiz
                static_folder='../static')      # Aponta para a pasta static na raiz

    app.config.from_object(config_class)
    logger.info(f"Configuração carregada: SECRET_KEY={'*' * 8 if app.config.get('SECRET_KEY') else 'None'}, DB_HOST={app.config.get('DB_CONFIG', {}).get('host')}")

    # --- Inicializar Extensões ---
    login_manager.init_app(app)
    # Não precisamos inicializar o DB pool aqui explicitamente se database.py
    # já faz isso na primeira chamada a get_db, mas é bom garantir o contexto.
    with app.app_context():
        try:
            database.init_pool() # Garante que o pool seja inicializado com o contexto da app
        except ConnectionError as e:
            logger.critical(f"Falha CRÍTICA ao inicializar pool de conexões na factory: {e}")
            # Considerar lançar o erro ou lidar de outra forma
        except Exception as e:
             logger.critical(f"Erro inesperado ao inicializar pool DB na factory: {e}", exc_info=True)


    # --- Registrar Context Processors e Teardown ---
    @app.teardown_appcontext
    def close_db_connection(exception=None):
        database.close_db(exception)

    @app.context_processor
    def inject_now():
        from datetime import datetime
        return {'now': datetime.now}

    # --- Registrar Blueprints ---
    logger.info("Registrando Blueprints...")
    try:
        from .routes import auth, dashboard, reports, api # Importa os módulos dos blueprints
        app.register_blueprint(auth.auth_bp)
        app.register_blueprint(dashboard.dashboard_bp)
        app.register_blueprint(reports.reports_bp)
        app.register_blueprint(api.api_bp, url_prefix='/api') # Adiciona prefixo /api para todas as rotas da API

        logger.info("Blueprints registrados com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao registrar Blueprints: {e}", exc_info=True)


    logger.info("Instância da aplicação Flask criada e configurada.")
    return app