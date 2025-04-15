# app.py
import os
import logging
# from logging.handlers import RotatingFileHandler
from flask import (Flask, render_template, request, Response, g,
                   flash, redirect, url_for, abort, session)
# Importa secure_filename que estava faltando
from werkzeug.utils import secure_filename
# Importações para Login
from flask_login import (LoginManager, login_user, logout_user, login_required,
                         current_user)
# Importa modelos e formulários
from config import Config
import database
from exporter import ExcelExporter
from models import User # Modelo User adaptado
from forms import LoginForm # Formulário de Login adaptado
# Necessário para validação do 'next_page' e data/hora
from urllib.parse import urlparse
import math
from datetime import datetime # Import para o inject_now

# --- Configuração de Logging ---
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(name)s] - %(message)s")
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers(): logger.handlers.clear()
logger.addHandler(log_handler)
logger.info("="*20 + " Aplicação Web Iniciada " + "="*20)

# --- Criação da App Flask ---
app = Flask(__name__)
app.config.from_object(Config)

# --- Inicialização do Pool DB ---
try:
    with app.app_context(): database.init_pool()
except ConnectionError as e: logger.critical(f"NÃO FOI POSSÍVEL CONECTAR AO BANCO DE DADOS NA INICIALIZAÇÃO: {e}")

# --- Configuração do Flask-Login ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = "Por favor, faça login para aceder a esta página."
login_manager.login_message_category = "info"

@login_manager.user_loader
def load_user(user_id):
    return User.get_by_id(int(user_id))

# --- Gerenciamento de Conexão DB ---
@app.teardown_appcontext
def close_db_connection(exception=None):
     database.close_db(exception)

# --- Funções de Contexto Jinja ---
@app.context_processor
def inject_now():
    return {'now': datetime.utcnow}

# --- Rotas da Aplicação ---

# --- Rota de Login (já ajustada) ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard')) # Redireciona para o dashboard se já logado

    form = LoginForm()
    if form.validate_on_submit():
        email_input = form.email.data
        password_input = form.password.data
        remember = form.remember_me.data
        user = User.get_by_email(email_input)
        if user and user.verify_password(password_input):
            login_user(user, remember=remember)
            logger.info(f"Utilizador '{email_input}' logado com sucesso.")
            next_page = request.args.get('next')
            if not next_page or urlparse(next_page).netloc != '':
                next_page = url_for('dashboard') # Redirecionamento padrão após login
            return redirect(next_page)
        else:
            logger.warning(f"Tentativa de login falhada para email: '{email_input}'")
            flash('Email ou senha inválidos.', 'danger')
    return render_template('login.html', title='Login', form=form)


# --- Rota de Logout (já ajustada) ---
@app.route('/logout')
@login_required
def logout():
    user_email = current_user.email if hasattr(current_user, 'email') else 'desconhecido'
    logger.info(f"Utilizador '{user_email}' a fazer logout.")
    logout_user()
    # Removido o flash message daqui para não aparecer na tela de login
    # flash('Logout realizado com sucesso.', 'success')
    return redirect(url_for('login'))

# --- Rota Principal NOVA (Dashboard) ---
@app.route('/')
@login_required
def dashboard():
    """Rota para a página inicial do dashboard."""
    user_nome = current_user.nome if hasattr(current_user, 'nome') else 'Utilizador'
    logger.info(f"Acesso ao dashboard por '{user_nome}'.")
    # Pode passar dados adicionais para o template dashboard.html se necessário
    return render_template('dashboard.html', title="Dashboard - Fast BI")


# --- Rota de Relatórios (Antiga Index) ---
@app.route('/relatorios')
@login_required
def relatorios():
    """Rota para exibir a tabela de dados (antiga página inicial)."""
    user_nome = current_user.nome if hasattr(current_user, 'nome') else 'Utilizador' # Usa nome para log
    logger.info(f"Requisição GET para /relatorios por '{user_nome}' - Args: {request.args}")
    try:
        # --- Lógica para buscar dados da tabela (igual à função index anterior) ---
        page = request.args.get('page', 1, type=int)
        selected_report_type = request.args.get('report_type', 'base_clientes')
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado')
        fornecedoras_db = database.get_fornecedoras()
        if selected_report_type == 'rateio':
            fornecedoras_list = fornecedoras_db
        else:
            fornecedoras_list = ['Consolidado'] + fornecedoras_db
        items_per_page = app.config.get('ITEMS_PER_PAGE', 50)
        offset = (page - 1) * items_per_page
        data_query, data_params = database.build_query(selected_report_type, selected_fornecedora, offset, items_per_page)
        dados = database.execute_query(data_query, data_params) or []
        count_q, count_p = database.count_query(selected_report_type, selected_fornecedora)
        total_items_result = database.execute_query(count_q, count_p, fetch_one=True)
        total_items = total_items_result[0] if total_items_result else 0
        total_pages = math.ceil(total_items / items_per_page) if items_per_page > 0 else 0
        headers = database.get_headers(selected_report_type)

        # Renderiza o template renomeado 'relatorios.html'
        return render_template(
            'relatorios.html', # <<< TEMPLATE CORRETO
            fornecedoras=fornecedoras_list,
            selected_fornecedora=selected_fornecedora,
            selected_report_type=selected_report_type,
            headers=headers,
            dados=dados,
            page=page,
            total_pages=total_pages,
            total_items=total_items,
            items_per_page=items_per_page,
            title="Relatórios - Fast BI" # Passa um título para a página
        )
    # --- Bloco except (ajustado para renderizar relatorios.html) ---
    except (ConnectionError, RuntimeError) as e:
        logger.error(f"Erro ao carregar dados para /relatorios por '{user_nome}': {e}", exc_info=False)
        flash(f"Erro ao conectar ou buscar dados no banco: {e}", "error")
        return render_template('relatorios.html', title="Erro - Relatórios", fornecedoras=['Consolidado'], error=str(e), dados=[], headers=database.get_headers('base_clientes'), page=1, total_pages=0, selected_report_type='base_clientes', selected_fornecedora='Consolidado')
    except Exception as e:
        logger.error(f"Erro inesperado em /relatorios por '{user_nome}': {e}", exc_info=True)
        flash(f"Ocorreu um erro inesperado ao carregar a página de relatórios.", "error")
        return render_template('relatorios.html', title="Erro - Relatórios", fornecedoras=['Consolidado'], error="Erro interno inesperado.", dados=[], headers=database.get_headers('base_clientes'), page=1, total_pages=0, selected_report_type='base_clientes', selected_fornecedora='Consolidado'), 500


# --- Rota de Exportação (protegida, redireciona para relatorios) ---
@app.route('/export', methods=['GET'])
@login_required
def exportar_excel_route():
    """Rota para gerar e baixar o ficheiro Excel (protegida)."""
    user_nome = current_user.nome if hasattr(current_user, 'nome') else 'Utilizador' # Usa nome para log
    logger.info(f"Requisição GET para /export por '{user_nome}' - Args: {request.args}")
    try:
        # --- Lógica de exportação (igual à versão anterior) ---
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado')
        selected_report_type = request.args.get('report_type', 'base_clientes')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_exp = ExcelExporter()
        forn_fn = 'Consolidado' if selected_fornecedora.lower() == "consolidado" else secure_filename(selected_fornecedora) # Usa secure_filename importado
        filename_base = f"Clientes_{selected_report_type}_{forn_fn}_{timestamp}.xlsx"

        if selected_report_type == 'rateio':
            filename = f"Clientes_Rateio_{forn_fn}_{timestamp}.xlsx"
            logger.info(f"Iniciando exportação Excel MULTI-ABAS para: tipo='rateio', fornecedora='{selected_fornecedora}'")
            nova_ids = database.get_base_nova_ids(fornecedora=selected_fornecedora); enviada_ids = database.get_base_enviada_ids(fornecedora=selected_fornecedora)
            logger.info(f"IDs encontrados (Forn: {selected_fornecedora}) - Base Nova: {len(nova_ids)}, Base Enviada: {len(enviada_ids)}")
            if not nova_ids and not enviada_ids:
                logger.warning(f"Nenhum ID encontrado para 'Base Nova' ou 'Base Enviada' (Forn: {selected_fornecedora}). Exportação cancelada.")
                flash(f"Nenhum dado encontrado para as bases 'Nova' ou 'Enviada' com a fornecedora '{selected_fornecedora}'.", "warning")
                return redirect(url_for('relatorios', fornecedora=selected_fornecedora, report_type=selected_report_type)) # Redireciona para relatorios
            rateio_headers = database.get_headers('rateio'); nova_data = database.get_client_details_by_ids('rateio', nova_ids) if nova_ids else []; enviada_data = database.get_client_details_by_ids('rateio', enviada_ids) if enviada_ids else []
            logger.info(f"Dados detalhados buscados (Forn: {selected_fornecedora}) - Base Nova: {len(nova_data)}, Base Enviada: {len(enviada_data)}")
            sheets_to_export = [{'name': 'Base Nova', 'headers': rateio_headers, 'data': nova_data}, {'name': 'Base Enviada', 'headers': rateio_headers, 'data': enviada_data}]
            excel_bytes = excel_exp.export_multi_sheet_excel_bytes(sheets_to_export)
        else:
            filename = filename_base
            logger.info(f"Iniciando exportação Excel ABA ÚNICA para: tipo='{selected_report_type}', fornecedora='{selected_fornecedora}'")
            data_query, data_params = database.build_query(selected_report_type, selected_fornecedora, 0, None)
            dados_completos = database.execute_query(data_query, data_params) or []
            if not dados_completos:
                 logger.warning(f"Nenhum dado encontrado para exportar (tipo: {selected_report_type}, forn: {selected_fornecedora}).")
                 flash(f"Nenhum dado encontrado para exportar (tipo: {selected_report_type}, forn: {selected_fornecedora}).", "warning")
                 return redirect(url_for('relatorios', fornecedora=selected_fornecedora, report_type=selected_report_type)) # Redireciona para relatorios
            headers = database.get_headers(selected_report_type); sheet_title = "Base Clientes" if selected_report_type == "base_clientes" else "Dados"
            excel_bytes = excel_exp.export_to_excel_bytes(dados_completos, headers, sheet_name=sheet_title)

        # --- Retornar Resposta ---
        logger.info(f"Exportação concluída. Enviando ficheiro: {filename}")
        return Response(excel_bytes, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={'Content-Disposition': f'attachment;filename="{filename}"'})

    # --- Tratamento de Erros da Rota Export (redireciona para relatorios) ---
    except (ConnectionError, RuntimeError) as e:
        logger.error(f"Erro de banco de dados ou runtime ao gerar exportação Excel: {e}", exc_info=False)
        flash(f"Erro ao gerar o ficheiro Excel: {e}", "error")
        return redirect(url_for('relatorios', fornecedora=request.args.get('fornecedora'), report_type=request.args.get('report_type'))) # Redireciona para relatorios
    except Exception as e:
        logger.error(f"Erro inesperado em /export: {e}", exc_info=True)
        flash(f"Ocorreu um erro inesperado durante a exportação.", "error")
        return redirect(url_for('relatorios', fornecedora=request.args.get('fornecedora'), report_type=request.args.get('report_type'))) # Redireciona para relatorios


# --- Execução da Aplicação ---
if __name__ == '__main__':
    app_host = os.environ.get('FLASK_RUN_HOST', '0.0.0.0')
    app_port = int(os.environ.get('FLASK_RUN_PORT', 5000))
    app_debug = os.environ.get('FLASK_DEBUG', 'True').lower() in ['true', '1', 't']
    app.run(host=app_host, port=app_port, debug=app_debug)