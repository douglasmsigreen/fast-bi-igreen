# run.py
import os
import logging
from backend import create_app, logger as backend_logger # Importa a factory e o logger do backend

# --- Escolha o modo de execução ---
# Descomente a linha desejada e comente a outra para alternar

# Modo 1: Execução ACESSÍVEL NA REDE (outras máquinas podem conectar)
# app_host = '0.0.0.0'
# Modo 2: Execução APENAS LOCAL (ninguém de fora acessa)
app_host = '127.0.0.1'

# ------------------------------------

# Define a porta desejada
app_port = 8088
# Verifica se o modo debug está ativado via variável de ambiente
app_debug = os.environ.get('FLASK_DEBUG', 'False').lower() in ['true', '1', 't']

# Configura o nível de log baseado no modo debug
log_level = logging.DEBUG if app_debug else logging.INFO
backend_logger.setLevel(log_level)
# Ajusta o log do Werkzeug (servidor Flask)
werkzeug_log_level = logging.DEBUG if app_debug else logging.WARNING
logging.getLogger('werkzeug').setLevel(werkzeug_log_level)

# Cria a aplicação usando a factory
app = create_app()

if __name__ == '__main__':
    if app_debug:
        backend_logger.warning("*"*10 + " MODO DEBUG ATIVADO! " + "*"*10)

    # Mensagem de log dinâmica baseada no host escolhido
    backend_logger.info(f"Iniciando servidor Flask em http://{app_host}:{app_port}/")
    if app_host == '0.0.0.0':
        backend_logger.info(f"Servidor configurado para ser acessível na rede local.")
        # Tenta obter o IP local para facilitar (opcional)
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80)) # Conecta a um IP externo para descobrir o IP local
            local_ip = s.getsockname()[0]
            s.close()
            backend_logger.info(f"   -> Acessível (por exemplo) em: http://{local_ip}:{app_port}/")
        except Exception:
            backend_logger.info(f"   -> Verifique o IP da sua máquina na rede para acessá-lo.")
    else:
        backend_logger.info(f"Servidor configurado para aceitar conexões APENAS da máquina local (localhost).")

    backend_logger.info(f"(Debug={app_debug}, LogLevel={logging.getLevelName(backend_logger.getEffectiveLevel())})")

    # Roda a aplicação com as configurações definidas
    # use_reloader=app_debug garante que o servidor reinicie com alterações no código apenas em modo debug
    app.run(host=app_host, port=app_port, debug=app_debug, use_reloader=app_debug)