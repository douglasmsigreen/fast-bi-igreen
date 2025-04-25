# app.py

import os
import logging
# from logging.handlers import RotatingFileHandler
from flask import (Flask, render_template, request, Response, g,
                   flash, redirect, url_for, abort, session, jsonify) # Adicionar jsonify
from werkzeug.utils import secure_filename
# Importações para Login
from flask_login import (LoginManager, login_user, logout_user, login_required,
                         current_user)
# Importa modelos e formulários
from config import Config
import database # Importa nosso módulo database
from exporter import ExcelExporter
from models import User
from forms import LoginForm
# Necessário para validação do 'next_page' e data/hora
from urllib.parse import urlparse, urljoin
import math
from datetime import datetime, timedelta # Adicionar timedelta
import re # Para validar o formato do mês


# --- Configuração de Logging ---
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s")
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
logger = logging.getLogger()
if logger.hasHandlers(): logger.handlers.clear()
logger.addHandler(log_handler)
logger.setLevel(logging.INFO) # Nível padrão INFO
logging.getLogger('werkzeug').setLevel(logging.INFO)

# --- Criação da App Flask ---
app = Flask(__name__)
app.config.from_object(Config)

# --- Inicialização do Pool DB ---
try:
    with app.app_context():
        database.init_pool()
except ConnectionError as e:
    logger.critical(f"NÃO FOI POSSÍVEL CONECTAR AO BANCO DE DADOS NA INICIALIZAÇÃO: {e}")

# --- Configuração do Flask-Login ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = "Por favor, faça login para aceder a esta página."
login_manager.login_message_category = "info"

@login_manager.user_loader
def load_user(user_id):
    """Carrega o utilizador pelo ID."""
    try: return User.get_by_id(int(user_id))
    except ValueError: logger.warning(f"ID inválido: {user_id}"); return None
    except Exception as e: logger.error(f"Erro load_user ID {user_id}: {e}", exc_info=True); return None

# --- Gerenciamento de Conexão DB ---
@app.teardown_appcontext
def close_db_connection(exception=None): database.close_db(exception)

# --- Funções de Contexto Jinja ---
@app.context_processor
def inject_now(): return {'now': datetime.now}

# --- Validador de URL 'next' ---
def is_safe_url(target):
    if not target: return True
    ref_url = urlparse(request.host_url); test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and ref_url.netloc == test_url.netloc

# --- Rotas da Aplicação ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Rota para autenticação do utilizador."""
    if current_user.is_authenticated: return redirect(url_for('dashboard'))
    form = LoginForm()
    if form.validate_on_submit():
        user = User.get_by_email(form.email.data)
        if user and user.verify_password(form.password.data):
            login_user(user, remember=form.remember_me.data)
            logger.info(f"Login OK: '{form.email.data}'"); next_page = request.args.get('next')
            if not is_safe_url(next_page): logger.warning(f"Redirect inseguro: '{next_page}'"); next_page = url_for('dashboard')
            return redirect(next_page or url_for('dashboard'))
        else: logger.warning(f"Login falhou: '{form.email.data}'"); flash('Email ou senha inválidos.', 'danger')
    return render_template('login.html', title='Login', form=form)

@app.route('/logout')
@login_required
def logout():
    """Rota para fazer logout do utilizador."""
    logger.info(f"Logout: '{current_user.email if hasattr(current_user, 'email') else '?'}'")
    logout_user(); flash('Logout efetuado.', 'success'); return redirect(url_for('login'))

# --- Rota Dashboard ---
@app.route('/')
@login_required
def dashboard():
    """Rota para a página inicial do dashboard."""
    # Dados iniciais para placeholders (a maioria é carregada via AJAX)
    total_kwh_mes = 0.0
    clientes_ativos_count = 0
    clientes_registrados_count = 0
    error_dashboard = None

    # Valida e define o mês selecionado (padrão: mês atual)
    selected_month_str = request.args.get('month')
    if not selected_month_str or not re.match(r'^\d{4}-\d{2}$', selected_month_str):
        selected_month_str = datetime.now().strftime('%Y-%m')
        logger.debug(f"Mês não fornecido ou inválido, usando padrão: {selected_month_str}")

    # Busca inicial de placeholders (opcional, mas pode melhorar UX)
    try:
        # Não vamos mais buscar o resumo aqui, será via AJAX
        # fornecedora_summary_data = database.get_fornecedora_summary(month_str=selected_month_str) # REMOVIDO
        # concessionaria_summary_data = database.get_concessionaria_summary(month_str=selected_month_str) # REMOVIDO
        total_kwh_mes = database.get_total_consumo_medio_by_month(month_str=selected_month_str)
        clientes_ativos_count = database.count_clientes_ativos_by_month(month_str=selected_month_str)
        clientes_registrados_count = database.count_clientes_registrados_by_month(month_str=selected_month_str)
        # if fornecedora_summary_data is None or concessionaria_summary_data is None: # Verificação REMOVIDA
        #     error_dashboard = f"Erro ao buscar resumos iniciais para {selected_month_str}."
    except Exception as e:
        logger.error(f"Erro dashboard (carga inicial KPIs) para {selected_month_str}: {e}", exc_info=True)
        error_dashboard = "Erro ao carregar KPIs iniciais do dashboard."
        total_kwh_mes = 0.0; clientes_ativos_count = 0; clientes_registrados_count = 0
    if error_dashboard: flash(error_dashboard, "warning")

    # Gera opções para o dropdown de mês
    month_options = []
    current_date = datetime.now()
    for i in range(12): # Gera opções para os últimos 12 meses
        dt = current_date - timedelta(days=i * 30) # Aproximação
        month_val = dt.strftime('%Y-%m')
        # Formato de texto mais amigável
        month_text = dt.strftime('%b/%Y').upper().replace('JAN','JAN').replace('FEV','FEV').replace('MAR','MAR').replace('ABR','ABR').replace('MAI','MAI').replace('JUN','JUN').replace('JUL','JUL').replace('AGO','AGO').replace('SET','SET').replace('OUT','OUT').replace('NOV','NOV').replace('DEZ','DEZ')
        month_options.append({'value': month_val, 'text': month_text})
    # Garante que o mês selecionado esteja na lista, mesmo se for antigo
    if selected_month_str not in [m['value'] for m in month_options]:
         try:
            sel_dt = datetime.strptime(selected_month_str + '-01', '%Y-%m-%d')
            sel_text = sel_dt.strftime('%b/%Y').upper().replace('JAN','JAN').replace('FEV','FEV').replace('MAR','MAR').replace('ABR','ABR').replace('MAI','MAI').replace('JUN','JUN').replace('JUL','JUL').replace('AGO','AGO').replace('SET','SET').replace('OUT','OUT').replace('NOV','NOV').replace('DEZ','DEZ')
            month_options.insert(0, {'value': selected_month_str, 'text': sel_text}) # Adiciona no início
         except ValueError:
             logger.warning(f"Não foi possível formatar o mês selecionado fora do padrão: {selected_month_str}")
             # Adiciona com o formato YYYY-MM se a formatação falhar
             month_options.insert(0, {'value': selected_month_str, 'text': selected_month_str})


    return render_template(
        'dashboard.html',
        title="Dashboard - Fast BI",
        total_kwh=total_kwh_mes, # Placeholder
        clientes_ativos_count=clientes_ativos_count, # Placeholder
        clientes_registrados_count=clientes_registrados_count, # Placeholder
        # Não passamos mais os resumos aqui, serão carregados via AJAX
        # fornecedora_summary=None, # REMOVIDO
        # concessionaria_summary=None, # REMOVIDO
        month_options=month_options,
        selected_month=selected_month_str,
        error_summary=error_dashboard # Apenas para erros de KPIs iniciais
    )
# --- FIM Rota Dashboard ---


# --- Rota de Relatórios ---
@app.route('/relatorios')
@login_required
def relatorios():
    # Código da rota relatorios (sem alterações relevantes para o mapa)
    user_nome = current_user.nome if hasattr(current_user, 'nome') else '?'
    try:
        page = request.args.get('page', 1, type=int); page = max(1, page)
        selected_report_type = request.args.get('report_type', 'base_clientes')
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado')
        try: fornecedoras_db = database.get_fornecedoras()
        except Exception as db_err: logger.error(f"[RELATORIOS] Erro fornecedoras: {db_err}", exc_info=True); flash("Erro lista fornecedoras.", "warning"); fornecedoras_db = []
        fornecedoras_list = ['Consolidado'] + fornecedoras_db
        items_per_page = app.config.get('ITEMS_PER_PAGE', 50)
        offset = (page - 1) * items_per_page
        dados = []; headers = []; total_items = 0; total_pages = 0; error_message = None
        logger.info(f"[RELATORIOS] Processando: Tipo='{selected_report_type}', Forn='{selected_fornecedora}', Pag={page}")
        if selected_report_type == 'base_clientes':
             data_query, data_params = database.build_query(selected_report_type, selected_fornecedora, offset, items_per_page)
             dados = database.execute_query(data_query, data_params) or []
             count_q, count_p = database.count_query(selected_report_type, selected_fornecedora)
             total_items_result = database.execute_query(count_q, count_p, fetch_one=True)
             total_items = total_items_result[0] if total_items_result else 0
             headers = database.get_headers(selected_report_type)
        elif selected_report_type == 'rateio':
             data_query, data_params = database.build_query(selected_report_type, selected_fornecedora, offset, items_per_page)
             dados = database.execute_query(data_query, data_params) or []
             count_q, count_p = database.count_query(selected_report_type, selected_fornecedora)
             total_items_result = database.execute_query(count_q, count_p, fetch_one=True)
             total_items = total_items_result[0] if total_items_result else 0
             headers = database.get_headers(selected_report_type)
        elif selected_report_type == 'rateio_rzk':
             total_items = database.count_rateio_rzk()
             dados = database.get_rateio_rzk_data(offset=offset, limit=items_per_page)
             headers = database.get_headers(selected_report_type)
             selected_fornecedora = 'RZK'
        elif selected_report_type == 'clientes_por_licenciado':
             total_items = database.count_clientes_por_licenciado()
             dados = database.get_clientes_por_licenciado_data(offset=offset, limit=items_per_page)
             headers = database.get_headers(selected_report_type)
        elif selected_report_type == 'boletos_por_cliente':
             total_items = database.count_boletos_por_cliente(fornecedora=selected_fornecedora)
             dados = database.get_boletos_por_cliente_data(offset=offset, limit=items_per_page, fornecedora=selected_fornecedora)
             headers = database.get_headers(selected_report_type)
        else:
            logger.warning(f"[RELATORIOS] Tipo desconhecido: '{selected_report_type}'.")
            error_message = f"Tipo de relatório desconhecido: '{selected_report_type}'."; flash(error_message, "warning")
            headers = []
        if not error_message and total_items > 0 and items_per_page > 0:
            total_pages = math.ceil(total_items / items_per_page)
            if page > total_pages: page = total_pages
        elif not error_message: total_pages = 0
        return render_template('relatorios.html', fornecedoras=fornecedoras_list, selected_fornecedora=selected_fornecedora, selected_report_type=selected_report_type, headers=headers, dados=dados, page=page, total_pages=total_pages, total_items=total_items, items_per_page=items_per_page, error=error_message, title=f"{selected_report_type.replace('_', ' ').title()} - Relatórios" )
    except Exception as e:
        logger.error(f"[RELATORIOS] Erro GERAL na rota: {e}", exc_info=True)
        flash("Ocorreu um erro inesperado ao processar sua solicitação.", "error")
        return render_template('relatorios.html', title="Erro Crítico", fornecedoras=['Consolidado'], error="Erro interno grave.", dados=[], headers=[], page=1, total_pages=0, total_items=0, selected_report_type='base_clientes', selected_fornecedora='Consolidado'), 500


# --- Rota de Exportação para Excel ---
@app.route('/export', methods=['GET'])
@login_required
def exportar_excel_route():
    # Código da rota exportar_excel_route (sem alterações relevantes para o mapa)
    user_nome = current_user.nome if hasattr(current_user, 'nome') else '?'
    try:
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado')
        selected_report_type = request.args.get('report_type', 'base_clientes')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_exp = ExcelExporter()
        excel_bytes = None
        filename = f"Relatorio_{secure_filename(selected_report_type)}_{timestamp}.xlsx"
        logger.info(f"[EXPORT] Iniciando: tipo='{selected_report_type}', fornecedora='{selected_fornecedora}'")
        if selected_report_type == 'rateio':
            forn_fn = 'Consolidado' if selected_fornecedora.lower() == "consolidado" else secure_filename(selected_fornecedora).replace('_', '')
            filename = f"Clientes_Rateio_{forn_fn}_{timestamp}.xlsx"
            nova_ids = database.get_base_nova_ids(fornecedora=selected_fornecedora)
            enviada_ids = database.get_base_enviada_ids(fornecedora=selected_fornecedora)
            if not nova_ids and not enviada_ids: flash(f"Nenhum dado para Rateio Geral (Forn: {selected_fornecedora}).", "warning"); return redirect(url_for('relatorios', **request.args))
            rateio_headers = database.get_headers('rateio')
            nova_data = database.get_client_details_by_ids('rateio', nova_ids) if nova_ids else []
            enviada_data = database.get_client_details_by_ids('rateio', enviada_ids) if enviada_ids else []
            sheets_to_export = [{'name': 'Base Nova', 'headers': rateio_headers, 'data': nova_data}, {'name': 'Base Enviada', 'headers': rateio_headers, 'data': enviada_data}]
            excel_bytes = excel_exp.export_multi_sheet_excel_bytes(sheets_to_export)
        elif selected_report_type == 'rateio_rzk':
             filename = f"Clientes_Rateio_RZK_MultiBase_{timestamp}.xlsx"
             nova_ids_rzk = database.get_rateio_rzk_base_nova_ids()
             enviada_ids_rzk = database.get_rateio_rzk_base_enviada_ids()
             if not nova_ids_rzk and not enviada_ids_rzk: flash("Nenhum dado para Rateio RZK.", "warning"); return redirect(url_for('relatorios', report_type='rateio_rzk'))
             rzk_headers = database.get_headers('rateio_rzk')
             nova_data_rzk = database.get_rateio_rzk_client_details_by_ids(nova_ids_rzk) if nova_ids_rzk else []
             enviada_data_rzk = database.get_rateio_rzk_client_details_by_ids(enviada_ids_rzk) if enviada_ids_rzk else []
             sheets_to_export = [{'name': 'Base Nova RZK', 'headers': rzk_headers, 'data': nova_data_rzk}, {'name': 'Base Enviada RZK', 'headers': rzk_headers, 'data': enviada_data_rzk}]
             excel_bytes = excel_exp.export_multi_sheet_excel_bytes(sheets_to_export)
        elif selected_report_type in ['clientes_por_licenciado', 'boletos_por_cliente', 'base_clientes']:
            forn_fn = secure_filename(selected_fornecedora).replace('_', '') if selected_fornecedora and selected_fornecedora.lower() != 'consolidado' else 'Consolidado'
            dados_completos = []; sheet_title = selected_report_type.replace('_', ' ').title(); headers = []
            if selected_report_type == 'clientes_por_licenciado':
                 filename=f"Qtd_Clientes_Licenciado_{timestamp}.xlsx"; dados_completos = database.get_clientes_por_licenciado_data(limit=None); sheet_title="Clientes por Licenciado"; headers = database.get_headers(selected_report_type)
            elif selected_report_type == 'boletos_por_cliente':
                 filename=f"Qtd_Boletos_Cliente_{forn_fn}_{timestamp}.xlsx"
                 dados_completos = database.get_boletos_por_cliente_data(limit=None, fornecedora=selected_fornecedora)
                 sheet_title=f"Boletos por Cliente ({selected_fornecedora})"
                 headers = database.get_headers(selected_report_type)
            elif selected_report_type == 'base_clientes':
                 filename=f"Clientes_Base_{forn_fn}_{timestamp}.xlsx"; data_query, data_params = database.build_query(selected_report_type, selected_fornecedora, 0, None); dados_completos = database.execute_query(data_query, data_params) or []; sheet_title=f"Base Clientes ({selected_fornecedora})"; headers = database.get_headers(selected_report_type)
            if not dados_completos: flash(f"Nenhum dado para exportar ({sheet_title}).", "warning"); return redirect(url_for('relatorios', **request.args))
            excel_bytes = excel_exp.export_to_excel_bytes(dados_completos, headers, sheet_name=sheet_title)
        else:
             flash(f"Tipo de relatório inválido para exportação: '{selected_report_type}'.", "error"); return redirect(url_for('relatorios'))
        if excel_bytes:
            logger.info(f"[EXPORT] Concluído. Enviando: {filename} ({len(excel_bytes)} bytes)")
            return Response(excel_bytes, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={'Content-Disposition': f'attachment;filename="{filename}"'})
        else:
            logger.error(f"[EXPORT] Falha ao gerar bytes Excel para '{selected_report_type}' (Forn: {selected_fornecedora})."); flash("Falha interna ao gerar o arquivo Excel.", "error"); return redirect(url_for('relatorios', **request.args))
    except Exception as exp_err:
        logger.error(f"[EXPORT] Erro Inesperado: {exp_err}", exc_info=True)
        flash("Erro inesperado durante exportação.", "error")
        return redirect(url_for('relatorios', **request.args))


# --- Rota para a Página do Mapa de Clientes ---
@app.route('/mapa-clientes')
@login_required
def mapa_clientes():
    # A página HTML em si não precisa de dados extras do Flask aqui
    return render_template('mapa_clientes.html', title="Mapa de Clientes - Fast BI")

# --- >>> ROTA API MODIFICADA PARA O MAPA <<< ---
# Renomeada para maior clareza e retorna dados mais completos
@app.route('/api/map-data/state-summary')
@login_required
def api_state_summary_for_map():
    """Retorna dados agregados (UF, contagem, soma consumo) por estado para o mapa."""
    try:
        # Chama a função MODIFICADA do banco que retorna UF, Contagem e Soma
        data_from_db = database.get_state_map_data() # Ex: [('SP', 100, 50000.0), ('MG', 80, 42000.0)]

        # Transforma a lista de tuplas em uma lista de dicionários para facilitar o JS
        structured_data = [
            {'uf': row[0], 'count': row[1], 'sum_consumo': row[2]}
            for row in data_from_db
        ]
        logger.debug(f"API Mapa: Enviando {len(structured_data)} registros de estado.")
        # Retorna a lista de dicionários como JSON
        return jsonify(structured_data)

    except Exception as e:
        logger.error(f"API Mapa (/api/map-data/state-summary) Erro: {e}", exc_info=True)
        return jsonify({"error": "Erro interno ao buscar dados do mapa."}), 500
# --- >>> FIM ROTA API MODIFICADA <<< ---

# --- Rota API para Resumo por Fornecedora ---
@app.route('/api/summary/fornecedora')
@login_required
def api_fornecedora_summary():
     month_str = request.args.get('month')
     if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str): return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400
     try:
        summary_data = database.get_fornecedora_summary(month_str=month_str)
        if summary_data is None: return jsonify({"error": "Erro interno ao buscar dados."}), 500
        elif not summary_data: return jsonify([])
        else: data_as_dicts = [{"fornecedora": r[0], "qtd_clientes": r[1], "soma_consumo": r[2]} for r in summary_data]; return jsonify(data_as_dicts)
     except Exception as e: logger.error(f"API Fornecedora Summary: Erro inesperado para o mês {month_str}: {e}", exc_info=True); return jsonify({"error": "Erro inesperado no servidor."}), 500

# --- NOVA Rota API para Resumo por Concessionária ---
@app.route('/api/summary/concessionaria')
@login_required
def api_concessionaria_summary():
     """Retorna dados de resumo (qtd, kwh) agrupados por Concessionária."""
     month_str = request.args.get('month')
     if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str):
         logger.warning(f"API Concessionaria Summary: Formato de mês inválido recebido '{month_str}'")
         return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400
     logger.debug(f"API Concessionaria Summary: Recebida requisição para mês '{month_str}'.")
     try:
        summary_data = database.get_concessionaria_summary(month_str=month_str) # Chama a nova função
        if summary_data is None:
            # Erro interno durante a busca no DB
            logger.error(f"API Concessionaria Summary: Erro interno (DB retornou None) para o mês {month_str}.")
            return jsonify({"error": "Erro interno ao buscar dados por concessionária."}), 500
        elif not summary_data:
             # Nenhum dado encontrado para o mês
             logger.debug(f"API Concessionaria Summary: Nenhum dado encontrado para o mês {month_str}.")
             return jsonify([]) # Retorna lista vazia
        else:
             # Formata como lista de dicionários para o JS
             data_as_dicts = [{"concessionaria": r[0], "qtd_clientes": r[1], "soma_consumo": r[2]} for r in summary_data]
             logger.debug(f"API Concessionaria Summary: Enviando {len(data_as_dicts)} registros para o mês {month_str}.")
             return jsonify(data_as_dicts)
     except Exception as e:
         logger.error(f"API Concessionaria Summary: Erro inesperado para o mês {month_str}: {e}", exc_info=True)
         return jsonify({"error": "Erro inesperado no servidor."}), 500
# --- FIM DA NOVA Rota API ---


# --- Rota API para KPI Total kWh ---
@app.route('/api/kpi/total-kwh')
@login_required
def api_kpi_total_kwh():
    month_str = request.args.get('month')
    if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str): return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400
    # logger.info(f"API KPI Total kWh: Recebida requisição para mês '{month_str}'.") # Log menos verboso
    try: total_kwh = database.get_total_consumo_medio_by_month(month_str=month_str); return jsonify({"total_kwh": total_kwh})
    except Exception as e: logger.error(f"API KPI Total kWh: Erro inesperado para o mês {month_str}: {e}", exc_info=True); return jsonify({"error": "Erro inesperado no servidor."}), 500


# --- Rota API para KPI Clientes Ativos (data_ativo) ---
@app.route('/api/kpi/clientes-ativos')
@login_required
def api_kpi_clientes_ativos():
    month_str = request.args.get('month')
    if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str): return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400
    # logger.info(f"API KPI Clientes Ativos: Recebida requisição para mês '{month_str}'.") # Log menos verboso
    try: count = database.count_clientes_ativos_by_month(month_str=month_str); return jsonify({"clientes_ativos_count": count})
    except Exception as e: logger.error(f"API KPI Clientes Ativos: Erro inesperado para o mês {month_str}: {e}", exc_info=True); return jsonify({"error": "Erro inesperado no servidor."}), 500

# --- Rota API para KPI Clientes REGISTRADOS (dtcad) ---
@app.route('/api/kpi/clientes-registrados')
@login_required
def api_kpi_clientes_registrados():
    """Retorna a contagem de clientes REGISTRADOS no mês (baseado em dtcad)."""
    month_str = request.args.get('month')
    if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str): return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400
    # logger.info(f"API KPI Clientes Registrados: Recebida requisição para mês '{month_str}'.") # Log menos verboso
    try: count = database.count_clientes_registrados_by_month(month_str=month_str); return jsonify({"clientes_registrados_count": count})
    except Exception as e: logger.error(f"API KPI Clientes Registrados: Erro inesperado para o mês {month_str}: {e}", exc_info=True); return jsonify({"error": "Erro inesperado no servidor."}), 500


# --- Rota API para Dados do Gráfico Mensal ---
@app.route('/api/chart/monthly-active-clients')
@login_required
def api_chart_monthly_active_clients():
    """Retorna a contagem de clientes ativados por mês para um dado ano."""
    year_str = request.args.get('year')
    if not year_str or not re.match(r'^\d{4}$', year_str): return jsonify({"error": "Formato de ano inválido. Use YYYY."}), 400
    try:
        year = int(year_str)
        # logger.info(f"API Chart Mensal: Recebida requisição para ano '{year}'.") # Log menos verboso
        monthly_data = database.get_monthly_active_clients_by_year(year=year)
        return jsonify({"monthly_counts": monthly_data})
    except ValueError: logger.warning(f"API Chart Mensal: Ano inválido (não inteiro) '{year_str}'."); return jsonify({"error": "Ano inválido."}), 400
    except Exception as e: logger.error(f"API Chart Mensal: Erro inesperado para o ano {year_str}: {e}", exc_info=True); return jsonify({"error": "Erro inesperado no servidor."}), 500
# --- FIM Rota API ---


# --- Execução da Aplicação ---
if __name__ == '__main__':
    app_host = os.environ.get('FLASK_RUN_HOST', '127.0.0.1')
    app_port = int(os.environ.get('FLASK_RUN_PORT', 5000))
    app_debug = os.environ.get('FLASK_DEBUG', 'False').lower() in ['true', '1', 't']

    log_level = logging.DEBUG if app_debug else logging.INFO
    logger.setLevel(log_level)
    if not logger.handlers: logger.addHandler(log_handler)
    if app_debug: logger.warning("*"*10 + " MODO DEBUG ATIVADO! " + "*"*10)
    logging.getLogger('werkzeug').setLevel(logging.DEBUG if app_debug else logging.INFO)

    logger.info(f"Iniciando servidor Flask em http://{app_host}:{app_port}/ (Debug={app_debug}, LogLevel={logging.getLevelName(logger.getEffectiveLevel())})")
    app.run(host=app_host, port=app_port, debug=app_debug)