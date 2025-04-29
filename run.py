# run.py
import os
import logging
from backend import create_app, logger as backend_logger # Importa a factory e o logger do backend

# Pega configurações de ambiente (opcional, pode estar no __init__.py também)
app_host = os.environ.get('FLASK_RUN_HOST', '127.0.0.1')
app_port = int(os.environ.get('FLASK_RUN_PORT', 5000))
app_debug = os.environ.get('FLASK_DEBUG', 'False').lower() in ['true', '1', 't']

# Configura o nível de log baseado no modo debug
log_level = logging.DEBUG if app_debug else logging.INFO
backend_logger.setLevel(log_level)
logging.getLogger('werkzeug').setLevel(log_level) # Ajusta log do Werkzeug

# Cria a aplicação usando a factory
app = create_app()

if __name__ == '__main__':
    if app_debug:
        backend_logger.warning("*"*10 + " MODO DEBUG ATIVADO! " + "*"*10)
    backend_logger.info(f"Iniciando servidor Flask em http://{app_host}:{app_port}/ (Debug={app_debug}, LogLevel={logging.getLevelName(backend_logger.getEffectiveLevel())})")
    # Roda a aplicação
    # Use debug=app_debug para ativar o debugger interativo e o reloader
    # use_reloader=False pode ser útil se o reloader estiver causando problemas com o pool DB
    app.run(host=app_host, port=app_port, debug=app_debug, use_reloader=app_debug)