# app.py (Versão Final com Exportação Rateio RZK Multi-Abas e Card Fornecedora no Dashboard)

import os
import logging
# from logging.handlers import RotatingFileHandler
from flask import (Flask, render_template, request, Response, g,
                   flash, redirect, url_for, abort, session, jsonify)
from werkzeug.utils import secure_filename
# Importações para Login
from flask_login import (LoginManager, login_user, logout_user, login_required,
                         current_user)
# Importa modelos e formulários
from config import Config
import database # Importa nosso módulo database [cite: 1]
from exporter import ExcelExporter
from models import User
from forms import LoginForm
# Necessário para validação do 'next_page' e data/hora
from urllib.parse import urlparse, urljoin
import math
from datetime import datetime

# --- Configuração de Logging ---
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s")
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
# Configurar logger raiz (nível será definido abaixo)
logger = logging.getLogger()
if logger.hasHandlers(): logger.handlers.clear()
logger.addHandler(log_handler)
logging.getLogger('werkzeug').setLevel(logging.INFO)

# --- Criação da App Flask ---
app = Flask(__name__)
app.config.from_object(Config)

# --- Inicialização do Pool DB ---
try:
    with app.app_context():
        database.init_pool() # [cite: 1]
except ConnectionError as e:
    logger.critical(f"NÃO FOI POSSÍVEL CONECTAR AO BANCO DE DADOS NA INICIALIZAÇÃO: {e}")

# --- Configuração do Flask-Login ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' # [cite: 1]
login_manager.login_message = "Por favor, faça login para aceder a esta página."
login_manager.login_message_category = "info"

@login_manager.user_loader
def load_user(user_id):
    """Carrega o utilizador pelo ID."""
    try: return User.get_by_id(int(user_id)) # [cite: 1]
    except ValueError: logger.warning(f"ID inválido: {user_id}"); return None
    except Exception as e: logger.error(f"Erro load_user ID {user_id}: {e}", exc_info=True); return None

# --- Gerenciamento de Conexão DB ---
@app.teardown_appcontext
def close_db_connection(exception=None): database.close_db(exception) # [cite: 1]

# --- Funções de Contexto Jinja ---
@app.context_processor
def inject_now(): return {'now': datetime.now} # [cite: 1]

# --- Validador de URL 'next' ---
def is_safe_url(target):
    if not target: return True
    ref_url = urlparse(request.host_url); test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and ref_url.netloc == test_url.netloc # [cite: 1]

# --- Rotas da Aplicação ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Rota para autenticação do utilizador."""
    if current_user.is_authenticated: return redirect(url_for('dashboard')) # [cite: 1]
    form = LoginForm() # [cite: 1]
    if form.validate_on_submit():
        user = User.get_by_email(form.email.data) # [cite: 1]
        if user and user.verify_password(form.password.data): # [cite: 1]
            login_user(user, remember=form.remember_me.data) # [cite: 1]
            logger.info(f"Login OK: '{form.email.data}'"); next_page = request.args.get('next')
            if not is_safe_url(next_page): logger.warning(f"Redirect inseguro: '{next_page}'"); next_page = url_for('dashboard') # [cite: 1]
            return redirect(next_page or url_for('dashboard')) # [cite: 1]
        else: logger.warning(f"Login falhou: '{form.email.data}'"); flash('Email ou senha inválidos.', 'danger') # [cite: 1]
    return render_template('login.html', title='Login', form=form) # [cite: 1]

@app.route('/logout')
@login_required
def logout():
    """Rota para fazer logout do utilizador."""
    logger.info(f"Logout: '{current_user.email if hasattr(current_user, 'email') else '?'}'") # [cite: 1]
    logout_user(); flash('Logout efetuado.', 'success'); return redirect(url_for('login')) # [cite: 1]

@app.route('/')
@login_required
def dashboard():
    """Rota para a página inicial do dashboard."""
    # <<< INÍCIO DAS MODIFICAÇÕES PARA O NOVO CARD >>>
    fornecedora_summary_data = None # Inicializa como None
    error_summary = None

    try:
        # Busca os dados do resumo por fornecedora usando a nova função em database.py
        fornecedora_summary_data = database.get_fornecedora_summary() # Chama a nova função
        if fornecedora_summary_data is None:
             # Se a função retornou None, houve um erro no DB
             error_summary = "Erro ao buscar dados de resumo por fornecedora."
             fornecedora_summary_data = [] # Garante que seja uma lista vazia para o template

    except Exception as e:
        logger.error(f"Erro geral na rota dashboard ao buscar summary: {e}", exc_info=True)
        error_summary = "Erro inesperado ao carregar dados do dashboard."
        fornecedora_summary_data = [] # Garante lista vazia

    # Exibe mensagem de erro se houver
    if error_summary:
        flash(error_summary, "warning")

    # Renderiza o template passando os dados (mesmo que vazios)
    return render_template(
        'dashboard.html',
        title="Dashboard - Fast BI",
        fornecedora_summary=fornecedora_summary_data # Passa a lista de tuplas para o template
        # Mantenha outras variáveis que você já passa para o dashboard, se houver
    )
    # <<< FIM DAS MODIFICAÇÕES PARA O NOVO CARD >>>


# --- Rota de Relatórios (ATUALIZADA para novo tipo 'rateio_rzk' e filtro boletos) ---
@app.route('/relatorios')
@login_required
def relatorios():
    """Rota para exibir tabelas de dados paginadas e filtradas."""
    user_nome = current_user.nome if hasattr(current_user, 'nome') else '?' # [cite: 1]
    # logger.info(f"[RELATORIOS] Req GET por '{user_nome}' - Args: {request.args}")
    try:
        page = request.args.get('page', 1, type=int); page = max(1, page) # [cite: 1]
        selected_report_type = request.args.get('report_type', 'base_clientes') # [cite: 1]
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado') # [cite: 1]

        try: fornecedoras_db = database.get_fornecedoras() # [cite: 1]
        except Exception as db_err: logger.error(f"[RELATORIOS] Erro fornecedoras: {db_err}", exc_info=True); flash("Erro lista fornecedoras.", "warning"); fornecedoras_db = []
        fornecedoras_list = ['Consolidado'] + fornecedoras_db # [cite: 1]

        items_per_page = app.config.get('ITEMS_PER_PAGE', 50) # [cite: 1]
        offset = (page - 1) * items_per_page # [cite: 1]
        dados = []; headers = []; total_items = 0; total_pages = 0; error_message = None # [cite: 1]

        logger.info(f"[RELATORIOS] Processando: Tipo='{selected_report_type}', Forn='{selected_fornecedora}', Pag={page}")

        # --- Lógica de Carregamento por Tipo de Relatório ---
        if selected_report_type == 'base_clientes': # [cite: 1]
             data_query, data_params = database.build_query(selected_report_type, selected_fornecedora, offset, items_per_page) # [cite: 1]
             dados = database.execute_query(data_query, data_params) or [] # [cite: 1]
             count_q, count_p = database.count_query(selected_report_type, selected_fornecedora) # [cite: 1]
             total_items_result = database.execute_query(count_q, count_p, fetch_one=True) # [cite: 1]
             total_items = total_items_result[0] if total_items_result else 0 # [cite: 1]
             headers = database.get_headers(selected_report_type) # Passa só o tipo # [cite: 1]

        elif selected_report_type == 'rateio': # Rateio Geral # [cite: 1]
             data_query, data_params = database.build_query(selected_report_type, selected_fornecedora, offset, items_per_page) # [cite: 1]
             dados = database.execute_query(data_query, data_params) or [] # [cite: 1]
             count_q, count_p = database.count_query(selected_report_type, selected_fornecedora) # [cite: 1]
             total_items_result = database.execute_query(count_q, count_p, fetch_one=True) # [cite: 1]
             total_items = total_items_result[0] if total_items_result else 0 # [cite: 1]
             headers = database.get_headers(selected_report_type) # Passa só o tipo # [cite: 1]

        elif selected_report_type == 'rateio_rzk': # Novo tipo Rateio RZK # [cite: 1]
             total_items = database.count_rateio_rzk() # Função de contagem específica # [cite: 1]
             dados = database.get_rateio_rzk_data(offset=offset, limit=items_per_page) # Função de dados específica # [cite: 1]
             headers = database.get_headers(selected_report_type) # Pega cabeçalhos RZK # [cite: 1]
             selected_fornecedora = 'RZK' # Garante RZK para o template # [cite: 1]

        elif selected_report_type == 'clientes_por_licenciado': # [cite: 1]
             total_items = database.count_clientes_por_licenciado() # [cite: 1]
             dados = database.get_clientes_por_licenciado_data(offset=offset, limit=items_per_page) # [cite: 1]
             headers = database.get_headers(selected_report_type) # [cite: 1]

        elif selected_report_type == 'boletos_por_cliente': # <<< BLOCO JÁ CORRIGIDO ANTERIORMENTE >>> # [cite: 1]
             # Passa selected_fornecedora para as funções de contagem e dados
             total_items = database.count_boletos_por_cliente(fornecedora=selected_fornecedora) # [cite: 1]
             dados = database.get_boletos_por_cliente_data(offset=offset, limit=items_per_page, fornecedora=selected_fornecedora) # [cite: 1]
             headers = database.get_headers(selected_report_type) # [cite: 1]

        else: # Tipo desconhecido
            logger.warning(f"[RELATORIOS] Tipo desconhecido: '{selected_report_type}'.") # [cite: 1]
            error_message = f"Tipo de relatório desconhecido: '{selected_report_type}'."; flash(error_message, "warning") # [cite: 1]
            headers = [] # [cite: 1]

        # --- Paginação Final ---
        if not error_message and total_items > 0 and items_per_page > 0: # [cite: 1]
            total_pages = math.ceil(total_items / items_per_page) # [cite: 1]
            if page > total_pages: page = total_pages # [cite: 1]
        elif not error_message: total_pages = 0 # [cite: 1]

        # --- Renderização ---
        # logger.debug(f"[RELATORIOS] Renderizando: Headers={len(headers)}, Dados={len(dados)}")
        return render_template('relatorios.html', # [cite: 1]
            fornecedoras=fornecedoras_list, selected_fornecedora=selected_fornecedora,
            selected_report_type=selected_report_type, headers=headers, dados=dados,
            page=page, total_pages=total_pages, total_items=total_items,
            items_per_page=items_per_page, error=error_message,
            title=f"{selected_report_type.replace('_', ' ').title()} - Relatórios"
        )

    except Exception as e: # Captura geral
        logger.error(f"[RELATORIOS] Erro GERAL na rota: {e}", exc_info=True) # [cite: 1]
        flash("Ocorreu um erro inesperado ao processar sua solicitação.", "error") # [cite: 1]
        return render_template('relatorios.html', title="Erro Crítico", fornecedoras=['Consolidado'], error="Erro interno grave.", dados=[], headers=[], page=1, total_pages=0, total_items=0, selected_report_type='base_clientes', selected_fornecedora='Consolidado'), 500 # [cite: 1]


# --- Rota de Exportação para Excel (ATUALIZADA para Rateio RZK multi-abas) ---
@app.route('/export', methods=['GET'])
@login_required
def exportar_excel_route():
    """Rota para gerar e baixar o ficheiro Excel."""
    user_nome = current_user.nome if hasattr(current_user, 'nome') else '?' # [cite: 1]
    try:
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado') # [cite: 1]
        selected_report_type = request.args.get('report_type', 'base_clientes') # [cite: 1]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") # [cite: 1]
        excel_exp = ExcelExporter() # [cite: 1]
        # Nome do arquivo será ajustado dentro dos blocos if/elif
        excel_bytes = None # [cite: 1]
        filename = f"Relatorio_{secure_filename(selected_report_type)}_{timestamp}.xlsx" # Default # [cite: 1]

        logger.info(f"[EXPORT] Iniciando: tipo='{selected_report_type}', fornecedora='{selected_fornecedora}'") # [cite: 1]

        if selected_report_type == 'rateio': # Rateio Geral (Multi-abas) # [cite: 1]
            forn_fn = 'Consolidado' if selected_fornecedora.lower() == "consolidado" else secure_filename(selected_fornecedora).replace('_', '') # [cite: 1]
            filename = f"Clientes_Rateio_{forn_fn}_{timestamp}.xlsx" # [cite: 1]
            nova_ids = database.get_base_nova_ids(fornecedora=selected_fornecedora) # [cite: 1]
            enviada_ids = database.get_base_enviada_ids(fornecedora=selected_fornecedora) # [cite: 1]
            if not nova_ids and not enviada_ids: flash(f"Nenhum dado para Rateio Geral (Forn: {selected_fornecedora}).", "warning"); return redirect(url_for('relatorios', **request.args)) # [cite: 1]

            rateio_headers = database.get_headers('rateio') # Cabeçalhos base # [cite: 1]
            nova_data = database.get_client_details_by_ids('rateio', nova_ids) if nova_ids else [] # [cite: 1]
            enviada_data = database.get_client_details_by_ids('rateio', enviada_ids) if enviada_ids else [] # [cite: 1]
            sheets_to_export = [{'name': 'Base Nova', 'headers': rateio_headers, 'data': nova_data}, {'name': 'Base Enviada', 'headers': rateio_headers, 'data': enviada_data}] # [cite: 1]
            excel_bytes = excel_exp.export_multi_sheet_excel_bytes(sheets_to_export) # [cite: 1]

        elif selected_report_type == 'rateio_rzk': # Rateio RZK (Multi-abas) # [cite: 1]
             filename = f"Clientes_Rateio_RZK_MultiBase_{timestamp}.xlsx" # [cite: 1]
             nova_ids_rzk = database.get_rateio_rzk_base_nova_ids() # [cite: 1]
             enviada_ids_rzk = database.get_rateio_rzk_base_enviada_ids() # [cite: 1]
             if not nova_ids_rzk and not enviada_ids_rzk: flash("Nenhum dado para Rateio RZK.", "warning"); return redirect(url_for('relatorios', report_type='rateio_rzk')) # [cite: 1]
             rzk_headers = database.get_headers('rateio_rzk') # [cite: 1]
             nova_data_rzk = database.get_rateio_rzk_client_details_by_ids(nova_ids_rzk) if nova_ids_rzk else [] # [cite: 1]
             enviada_data_rzk = database.get_rateio_rzk_client_details_by_ids(enviada_ids_rzk) if enviada_ids_rzk else [] # [cite: 1]
             sheets_to_export = [{'name': 'Base Nova RZK', 'headers': rzk_headers, 'data': nova_data_rzk}, {'name': 'Base Enviada RZK', 'headers': rzk_headers, 'data': enviada_data_rzk}] # [cite: 1]
             excel_bytes = excel_exp.export_multi_sheet_excel_bytes(sheets_to_export) # [cite: 1]

        elif selected_report_type in ['clientes_por_licenciado', 'boletos_por_cliente', 'base_clientes']: # [cite: 1]
            # Exportação de aba única para outros tipos
            forn_fn = secure_filename(selected_fornecedora).replace('_', '') if selected_fornecedora and selected_fornecedora.lower() != 'consolidado' else 'Consolidado' # [cite: 1]
            dados_completos = []; sheet_title = selected_report_type.replace('_', ' ').title(); headers = [] # [cite: 1]
            if selected_report_type == 'clientes_por_licenciado': # [cite: 1]
                 filename=f"Qtd_Clientes_Licenciado_{timestamp}.xlsx"; dados_completos = database.get_clientes_por_licenciado_data(limit=None); sheet_title="Clientes por Licenciado"; headers = database.get_headers(selected_report_type) # [cite: 1]
            elif selected_report_type == 'boletos_por_cliente': # <<< BLOCO CORRIGIDO ANTERIORMENTE >>> # [cite: 1]
                 filename=f"Qtd_Boletos_Cliente_{forn_fn}_{timestamp}.xlsx" # Adiciona Forn ao nome # [cite: 1]
                 # Chama função de dados com filtro de fornecedora
                 dados_completos = database.get_boletos_por_cliente_data(limit=None, fornecedora=selected_fornecedora) # [cite: 1]
                 sheet_title=f"Boletos por Cliente ({selected_fornecedora})" # [cite: 1]
                 headers = database.get_headers(selected_report_type) # [cite: 1]
            elif selected_report_type == 'base_clientes': # [cite: 1]
                 filename=f"Clientes_Base_{forn_fn}_{timestamp}.xlsx"; data_query, data_params = database.build_query(selected_report_type, selected_fornecedora, 0, None); dados_completos = database.execute_query(data_query, data_params) or []; sheet_title=f"Base Clientes ({selected_fornecedora})"; headers = database.get_headers(selected_report_type) # [cite: 1]

            if not dados_completos: flash(f"Nenhum dado para exportar ({sheet_title}).", "warning"); return redirect(url_for('relatorios', **request.args)) # [cite: 1]
            excel_bytes = excel_exp.export_to_excel_bytes(dados_completos, headers, sheet_name=sheet_title) # [cite: 1]
        else:
             flash(f"Tipo de relatório inválido para exportação: '{selected_report_type}'.", "error"); return redirect(url_for('relatorios')) # [cite: 1]

        # --- Retorno da Resposta ---
        if excel_bytes: # [cite: 1]
            logger.info(f"[EXPORT] Concluído. Enviando: {filename} ({len(excel_bytes)} bytes)") # [cite: 1]
            return Response(excel_bytes, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={'Content-Disposition': f'attachment;filename="{filename}"'}) # [cite: 1]
        else:
            logger.error(f"[EXPORT] Falha ao gerar bytes Excel para '{selected_report_type}' (Forn: {selected_fornecedora})."); flash("Falha interna ao gerar o arquivo Excel.", "error"); return redirect(url_for('relatorios', **request.args)) # [cite: 1]

    except Exception as exp_err: # Captura geral para exportação
        logger.error(f"[EXPORT] Erro Inesperado: {exp_err}", exc_info=True) # [cite: 1]
        flash("Erro inesperado durante exportação.", "error") # [cite: 1]
        return redirect(url_for('relatorios', **request.args)) # [cite: 1]


# --- Rota para a Página do Mapa de Clientes ---
@app.route('/mapa-clientes')
@login_required
def mapa_clientes(): return render_template('mapa_clientes.html', title="Mapa de Clientes - Fast BI") # [cite: 1]

# --- Rota da API para Dados do Mapa ---
@app.route('/api/map-data/client-count-by-state')
@login_required
def api_client_count_by_state():
    try:
        data = database.get_client_count_by_state() # [cite: 1]
        formatted_data = {'locations': [r[0] for r in data if r and len(r)>0], 'z': [r[1] for r in data if r and len(r)>1]} # [cite: 1]
        if len(formatted_data['locations']) != len(formatted_data['z']): logger.error("API Mapa: Tamanhos diferentes."); return jsonify({"error": "Erro interno"}), 500 # [cite: 1]
        return jsonify(formatted_data) # [cite: 1]
    except Exception as e: logger.error(f"API Mapa Erro: {e}", exc_info=True); return jsonify({"error": "Erro interno"}), 500 # [cite: 1]

# --- Execução da Aplicação ---
if __name__ == '__main__':
    app_host = os.environ.get('FLASK_RUN_HOST', '127.0.0.1') # [cite: 1]
    app_port = int(os.environ.get('FLASK_RUN_PORT', 5000)) # [cite: 1]
    app_debug = os.environ.get('FLASK_DEBUG', 'False').lower() in ['true', '1', 't'] # [cite: 1]

    log_level = logging.DEBUG if app_debug else logging.INFO # [cite: 1]
    logger.setLevel(log_level) # [cite: 1]
    if not logger.handlers: logger.addHandler(log_handler) # [cite: 1]
    if app_debug: logger.warning("*"*10 + " MODO DEBUG ATIVADO! " + "*"*10) # [cite: 1]
    logging.getLogger('werkzeug').setLevel(logging.DEBUG if app_debug else logging.INFO) # [cite: 1]

    logger.info(f"Iniciando servidor Flask em http://{app_host}:{app_port}/ (Debug={app_debug}, LogLevel={logging.getLevelName(logger.getEffectiveLevel())})") # [cite: 1]
    app.run(host=app_host, port=app_port, debug=app_debug) # [cite: 1]