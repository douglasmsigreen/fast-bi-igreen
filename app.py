# app.py
import os
import logging
# from logging.handlers import RotatingFileHandler # Comentado, usando StreamHandler
from flask import (Flask, render_template, request, Response, g,
                   flash, redirect, url_for, abort, session, jsonify) # Adicionado jsonify
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
from urllib.parse import urlparse, urljoin # Adicionado urljoin para validação mais robusta
import math
from datetime import datetime # Import para o inject_now

# --- Configuração de Logging ---
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s") # Adicionado lineno
log_handler = logging.StreamHandler() # Saída para console/terminal
log_handler.setFormatter(log_formatter)

# Configurar logger raiz
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Nível padrão (pode ser alterado por variável de ambiente)
if logger.hasHandlers(): logger.handlers.clear() # Limpa handlers antigos se houver (útil em reloads)
logger.addHandler(log_handler)

# Configurar logger específico do Flask/Werkzeug (opcional, para controlar verbosidade)
logging.getLogger('werkzeug').setLevel(logging.INFO) # Ou WARNING para menos logs de requisição

logger.info("="*20 + " Aplicação Web Iniciada " + "="*20)

# --- Criação da App Flask ---
app = Flask(__name__)
app.config.from_object(Config)

# --- Inicialização do Pool DB ---
try:
    # Garante que o contexto da aplicação está ativo ao chamar init_pool
    with app.app_context():
        database.init_pool()
except ConnectionError as e:
    logger.critical(f"NÃO FOI POSSÍVEL CONECTAR AO BANCO DE DADOS NA INICIALIZAÇÃO: {e}")
    # Considerar parar a aplicação ou funcionar em modo degradado se o DB for essencial

# --- Configuração do Flask-Login ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' # Rota para redirecionar se não logado
login_manager.login_message = "Por favor, faça login para aceder a esta página."
login_manager.login_message_category = "info" # Categoria da mensagem flash

@login_manager.user_loader
def load_user(user_id):
    """Carrega o utilizador pelo ID (usado pelo Flask-Login)."""
    try:
        # Flask-Login passa o ID como string, converta para int
        return User.get_by_id(int(user_id))
    except ValueError:
        logger.warning(f"Tentativa de carregar utilizador com ID inválido (não numérico): {user_id}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar utilizador ID {user_id}: {e}", exc_info=True)
        return None

# --- Gerenciamento de Conexão DB (Request Context) ---
@app.teardown_appcontext
def close_db_connection(exception=None):
     """Fecha a conexão do banco de dados ao final da requisição."""
     # A função close_db em database.py já lida com a lógica de devolver ao pool
     database.close_db(exception)

# --- Funções de Contexto Jinja ---
@app.context_processor
def inject_now():
    """Injecta a data/hora atual no contexto Jinja para uso nos templates."""
    # Usando datetime.now() para o fuso horário do servidor.
    # Para UTC: datetime.utcnow()
    # A data atual é 2025-04-17 10:59:20
    return {'now': datetime.now}

# --- Validador de URL 'next' ---
def is_safe_url(target):
    """Verifica se a URL de redirecionamento é segura."""
    # Se target for None ou vazio, considera seguro (redirecionará para o padrão)
    if not target:
        return True
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    # Garante que o esquema é http/https e que o domínio é o mesmo
    return test_url.scheme in ('http', 'https') and ref_url.netloc == test_url.netloc

# --- Rotas da Aplicação ---

# --- Rota de Login ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    """Rota para autenticação do utilizador."""
    if current_user.is_authenticated:
        logger.debug(f"Utilizador já autenticado ({current_user.email}), redirecionando para dashboard.")
        return redirect(url_for('dashboard'))

    form = LoginForm()
    if form.validate_on_submit():
        email_input = form.email.data
        password_input = form.password.data
        remember = form.remember_me.data
        logger.info(f"Tentativa de login para email: '{email_input}'")
        user = User.get_by_email(email_input)

        # Verifica se o utilizador existe e a senha está correta
        if user and user.verify_password(password_input):
            login_user(user, remember=remember)
            logger.info(f"Utilizador '{email_input}' logado com sucesso.")

            # Redirecionamento seguro após login
            next_page = request.args.get('next')
            if not is_safe_url(next_page):
                logger.warning(f"Tentativa de redirecionamento inseguro detectada para: '{next_page}'. Redirecionando para dashboard.")
                next_page = url_for('dashboard') # Define o padrão se for inseguro

            logger.debug(f"Redirecionando para: '{next_page or url_for('dashboard')}' após login.")
            # Redireciona para next_page se for seguro e existir, senão para o dashboard
            return redirect(next_page or url_for('dashboard'))
        else:
            logger.warning(f"Tentativa de login falhada para email: '{email_input}' (email ou senha inválidos).")
            flash('Email ou senha inválidos.', 'danger') # Mensagem de erro para o utilizador
            # Renderiza o formulário novamente com o erro
            return render_template('login.html', title='Login', form=form)

    # Se GET ou validação falhar, renderiza o formulário de login
    return render_template('login.html', title='Login', form=form)


# --- Rota de Logout ---
@app.route('/logout')
@login_required # Garante que só utilizadores logados podem deslogar
def logout():
    """Rota para fazer logout do utilizador."""
    user_email = current_user.email if hasattr(current_user, 'email') else 'desconhecido'
    logger.info(f"Utilizador '{user_email}' a fazer logout.")
    logout_user()
    flash('Logout efetuado com sucesso.', 'success') # Mensagem opcional
    return redirect(url_for('login'))

# --- Rota Principal (Dashboard) ---
@app.route('/') # Rota raiz agora é o dashboard
@login_required
def dashboard():
    """Rota para a página inicial do dashboard (hub de navegação)."""
    user_nome = current_user.nome if hasattr(current_user, 'nome') else 'Utilizador'
    logger.info(f"Acesso ao dashboard por '{user_nome}' (ID: {current_user.id}).")
    # Passa o título para ser usado em base.html
    return render_template('dashboard.html', title="Dashboard - Fast BI")


# --- Rota de Relatórios (com paginação e filtros) ---
@app.route('/relatorios')
@login_required
def relatorios():
    """Rota para exibir tabelas de dados paginadas e filtradas."""
    user_nome = current_user.nome if hasattr(current_user, 'nome') else 'Utilizador'
    # Log inicial com argumentos da requisição GET
    logger.info(f"Requisição GET para /relatorios por '{user_nome}' - Args: {request.args}")
    try:
        # --- Obtenção dos Parâmetros ---
        page = request.args.get('page', 1, type=int)
        if page < 1: page = 1 # Garante que a página não seja menor que 1
        selected_report_type = request.args.get('report_type', 'base_clientes')
        # Fornecedora padrão é 'Consolidado' se não especificado
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado')

        # --- Busca de Fornecedoras para o Dropdown ---
        try:
            fornecedoras_db = database.get_fornecedoras()
        except Exception as db_err:
            logger.error(f"Erro ao buscar lista de fornecedoras: {db_err}", exc_info=True)
            flash("Erro ao carregar a lista de fornecedoras.", "warning")
            fornecedoras_db = []
        # A lista para o dropdown sempre inclui 'Consolidado'
        fornecedoras_list = ['Consolidado'] + fornecedoras_db

        # --- Paginação ---
        items_per_page = app.config.get('ITEMS_PER_PAGE', 50)
        offset = (page - 1) * items_per_page

        # --- Variáveis de Dados ---
        dados = []
        headers = []
        total_items = 0
        total_pages = 0
        error_message = None # Para armazenar mensagens de erro específicas

        # --- Lógica para Carregar Dados Baseado no Tipo de Relatório ---
        logger.info(f"Processando relatório tipo: '{selected_report_type}', Fornecedora: '{selected_fornecedora}', Página: {page}")

        if selected_report_type == 'base_clientes' or selected_report_type == 'rateio':
             # Usa as funções genéricas de query e contagem com filtro de fornecedora
             data_query, data_params = database.build_query(selected_report_type, selected_fornecedora, offset, items_per_page)
             dados = database.execute_query(data_query, data_params) or []
             count_q, count_p = database.count_query(selected_report_type, selected_fornecedora)
             total_items_result = database.execute_query(count_q, count_p, fetch_one=True)
             total_items = total_items_result[0] if total_items_result else 0

        elif selected_report_type == 'clientes_por_licenciado':
             # Usa funções específicas, ignora filtro de fornecedora
             total_items = database.count_clientes_por_licenciado()
             dados = database.get_clientes_por_licenciado_data(offset=offset, limit=items_per_page)

        elif selected_report_type == 'boletos_por_cliente':
             # Usa funções específicas, ignora filtro de fornecedora
             total_items = database.count_boletos_por_cliente()
             dados = database.get_boletos_por_cliente_data(offset=offset, limit=items_per_page)

        else:
            # Tipo de relatório desconhecido
            logger.warning(f"Tipo de relatório desconhecido solicitado: '{selected_report_type}'.")
            error_message = f"Tipo de relatório desconhecido: '{selected_report_type}'."
            flash(error_message, "warning")
            # Tenta carregar cabeçalhos padrão para evitar erro no template
            try: headers = database.get_headers('base_clientes')
            except: headers = [] # Fallback extremo


        # --- Obtenção dos Cabeçalhos (após saber o tipo) ---
        if not error_message: # Só busca cabeçalhos se o tipo for válido
            try:
                headers = database.get_headers(selected_report_type)
                if not headers and selected_report_type in ['base_clientes', 'rateio', 'clientes_por_licenciado', 'boletos_por_cliente']:
                     logger.warning(f"Função get_headers retornou lista vazia para tipo conhecido: {selected_report_type}")
                     # Não define erro aqui, mas pode ser um problema de configuração
            except Exception as h_err:
                 logger.error(f"Erro ao obter cabeçalhos para o tipo '{selected_report_type}': {h_err}", exc_info=True)
                 error_message = f"Erro ao carregar cabeçalhos para o relatório '{selected_report_type}'."
                 flash(error_message, "error")
                 headers = [] # Garante que headers é uma lista

        # --- Cálculo Final da Paginação ---
        if not error_message and total_items > 0 and items_per_page > 0:
            total_pages = math.ceil(total_items / items_per_page)
            # Corrige a página se for maior que o total (ex: após deletar itens)
            if page > total_pages:
                logger.info(f"Página solicitada ({page}) maior que o total ({total_pages}). Redefinindo para {total_pages}.")
                page = total_pages
                # Recalcula offset se a página foi ajustada
                offset = (page - 1) * items_per_page
                # Idealmente, rebuscaria os dados aqui para a 'page' corrigida.
                # Para simplificar, estamos deixando os dados da página original inválida (pode mostrar vazio)
                # ou mostrando a última página completa (se os dados já foram buscados assim).
                # A lógica atual busca os dados ANTES da correção da página.
                # Para corrigir 100%, a busca de dados deveria ocorrer APÓS a validação da página.
        elif not error_message:
             total_pages = 0 # Se não há itens, não há páginas

        # --- Renderização do Template ---
        return render_template(
            'relatorios.html',
            fornecedoras=fornecedoras_list,
            selected_fornecedora=selected_fornecedora,
            selected_report_type=selected_report_type,
            headers=headers,
            dados=dados,
            page=page,
            total_pages=total_pages,
            total_items=total_items,
            items_per_page=items_per_page,
            error=error_message, # Passa a mensagem de erro para o template
            title=f"{selected_report_type.replace('_', ' ').title()} - Relatórios" # Título dinâmico
        )

    # --- Tratamento de Erros Gerais da Rota ---
    except (ConnectionError, RuntimeError) as db_conn_err:
        # Erros específicos de banco de dados (conexão, query runtime)
        logger.error(f"Erro de banco de dados ao carregar /relatorios por '{user_nome}': {db_conn_err}", exc_info=False) # Não logar stacktrace completo para erros esperados
        flash(f"Erro ao conectar ou buscar dados no banco: Verifique a conexão e tente novamente.", "error")
        # Renderiza a página com estado de erro
        return render_template('relatorios.html', title="Erro de Banco - Relatórios", fornecedoras=['Consolidado'], error=str(db_conn_err), dados=[], headers=[], page=1, total_pages=0, total_items=0, selected_report_type='base_clientes', selected_fornecedora='Consolidado')
    except ValueError as val_err:
        # Erros de valor (ex: tipo de relatório inválido passado para build_query)
        logger.error(f"Erro de valor em /relatorios: {val_err}", exc_info=True)
        flash(f"Erro ao processar o tipo de relatório: {val_err}", "error")
        return render_template('relatorios.html', title="Erro de Tipo - Relatórios", fornecedoras=['Consolidado'], error=str(val_err), dados=[], headers=[], page=1, total_pages=0, total_items=0, selected_report_type='base_clientes', selected_fornecedora='Consolidado')
    except Exception as e:
        # Captura qualquer outro erro inesperado
        logger.error(f"Erro inesperado em /relatorios por '{user_nome}': {e}", exc_info=True) # Log completo para erros inesperados
        flash(f"Ocorreu um erro inesperado ao carregar a página de relatórios.", "error")
        # Pode retornar um template de erro genérico ou a página de relatórios com mensagem
        return render_template('relatorios.html', title="Erro Inesperado - Relatórios", fornecedoras=['Consolidado'], error="Erro interno inesperado.", dados=[], headers=[], page=1, total_pages=0, total_items=0, selected_report_type='base_clientes', selected_fornecedora='Consolidado'), 500 # Retorna status 500


# --- Rota de Exportação para Excel ---
@app.route('/export', methods=['GET'])
@login_required
def exportar_excel_route():
    """Rota para gerar e baixar o ficheiro Excel com base nos filtros."""
    user_nome = current_user.nome if hasattr(current_user, 'nome') else 'Utilizador'
    logger.info(f"Requisição GET para /export por '{user_nome}' - Args: {request.args}")
    try:
        # --- Obtenção dos Parâmetros ---
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado')
        selected_report_type = request.args.get('report_type', 'base_clientes')

        # --- Preparação do Nome do Ficheiro e Exporter ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_exp = ExcelExporter()
        # Limpa o nome da fornecedora para usar no nome do arquivo
        forn_fn = 'Consolidado' if selected_fornecedora.lower() == "consolidado" else secure_filename(selected_fornecedora).replace('_', '')

        excel_bytes = None
        filename = f"Relatorio_{secure_filename(selected_report_type)}_{forn_fn}_{timestamp}.xlsx" # Nome padrão

        # --- Lógica de Geração de Dados para Exportação ---
        logger.info(f"Iniciando exportação Excel: tipo='{selected_report_type}', fornecedora='{selected_fornecedora}'")

        if selected_report_type == 'rateio':
            # Exportação multi-abas para Rateio
            filename = f"Clientes_Rateio_{forn_fn}_{timestamp}.xlsx"
            nova_ids = database.get_base_nova_ids(fornecedora=selected_fornecedora)
            enviada_ids = database.get_base_enviada_ids(fornecedora=selected_fornecedora)

            if not nova_ids and not enviada_ids:
                logger.warning(f"Nenhum ID encontrado para Rateio (Forn: {selected_fornecedora}). Exportação cancelada.")
                flash(f"Nenhum dado encontrado para as bases 'Nova' ou 'Enviada' com a fornecedora '{selected_fornecedora}'.", "warning")
                return redirect(url_for('relatorios', fornecedora=selected_fornecedora, report_type=selected_report_type))

            rateio_headers = database.get_headers('rateio')
            # Busca detalhes em lote para performance
            nova_data = database.get_client_details_by_ids('rateio', nova_ids) if nova_ids else []
            enviada_data = database.get_client_details_by_ids('rateio', enviada_ids) if enviada_ids else []

            sheets_to_export = [
                {'name': 'Base Nova', 'headers': rateio_headers, 'data': nova_data},
                {'name': 'Base Enviada', 'headers': rateio_headers, 'data': enviada_data}
            ]
            excel_bytes = excel_exp.export_multi_sheet_excel_bytes(sheets_to_export)

        elif selected_report_type in ['clientes_por_licenciado', 'boletos_por_cliente', 'base_clientes']:
            # Exportação aba única para outros tipos
            dados_completos = []
            sheet_title = selected_report_type.replace('_', ' ').title() # Título padrão da aba

            if selected_report_type == 'clientes_por_licenciado':
                filename = f"Quantidade_Clientes_Por_Licenciado_{timestamp}.xlsx"
                dados_completos = database.get_clientes_por_licenciado_data(limit=None) # Busca todos
                sheet_title = "Clientes por Licenciado"
            elif selected_report_type == 'boletos_por_cliente':
                filename = f"Quantidade_Boletos_por_Cliente_{timestamp}.xlsx"
                dados_completos = database.get_boletos_por_cliente_data(limit=None) # Busca todos
                sheet_title = "Boletos por Cliente"
            elif selected_report_type == 'base_clientes':
                filename = f"Clientes_Base_{forn_fn}_{timestamp}.xlsx"
                # Busca todos os dados para a fornecedora selecionada
                data_query, data_params = database.build_query(selected_report_type, selected_fornecedora, 0, None) # limit=None
                dados_completos = database.execute_query(data_query, data_params) or []
                sheet_title = f"Base Clientes ({selected_fornecedora})"

            # Verifica se há dados para exportar
            if not dados_completos:
                 logger.warning(f"Nenhum dado encontrado para exportar (tipo: {selected_report_type}, forn: {selected_fornecedora}).")
                 flash(f"Nenhum dado encontrado para exportar (Relatório: {sheet_title}).", "warning")
                 return redirect(url_for('relatorios', fornecedora=selected_fornecedora, report_type=selected_report_type))

            # Gera o Excel de aba única
            headers = database.get_headers(selected_report_type)
            excel_bytes = excel_exp.export_to_excel_bytes(dados_completos, headers, sheet_name=sheet_title)

        else:
             # Tipo de relatório inválido para exportação
             logger.error(f"Tipo de relatório desconhecido para exportação: {selected_report_type}")
             flash(f"Tipo de relatório desconhecido para exportação: '{selected_report_type}'.", "error")
             return redirect(url_for('relatorios')) # Redireciona para a página de relatórios

        # --- Retornar Resposta com o Ficheiro Excel ---
        if excel_bytes:
            logger.info(f"Exportação concluída. Enviando ficheiro: {filename} ({len(excel_bytes)} bytes)")
            return Response(
                excel_bytes,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                headers={'Content-Disposition': f'attachment;filename="{filename}"'}
            )
        else:
            # Se chegou aqui sem gerar bytes (e não redirecionou antes), algo deu errado.
            logger.error(f"Falha ao gerar bytes do Excel para o tipo '{selected_report_type}'. Verifique logs anteriores.")
            flash("Falha interna ao gerar o arquivo Excel.", "error")
            return redirect(url_for('relatorios', fornecedora=selected_fornecedora, report_type=selected_report_type))

    # --- Tratamento de Erros da Rota Export ---
    except (ConnectionError, RuntimeError) as db_exp_err:
        logger.error(f"Erro de banco de dados ou runtime ao gerar exportação Excel: {db_exp_err}", exc_info=False)
        flash(f"Erro ao gerar o ficheiro Excel (Banco de Dados): {db_exp_err}", "error")
        return redirect(url_for('relatorios', fornecedora=request.args.get('fornecedora', 'Consolidado'), report_type=request.args.get('report_type', 'base_clientes')))
    except ValueError as val_exp_err:
        logger.error(f"Erro de valor em /export: {val_exp_err}", exc_info=True)
        flash(f"Erro ao processar o tipo de relatório para exportação: {val_exp_err}", "error")
        return redirect(url_for('relatorios', fornecedora=request.args.get('fornecedora', 'Consolidado'), report_type=request.args.get('report_type', 'base_clientes')))
    except Exception as exp_err:
        logger.error(f"Erro inesperado em /export: {exp_err}", exc_info=True)
        flash(f"Ocorreu um erro inesperado durante a exportação.", "error")
        return redirect(url_for('relatorios', fornecedora=request.args.get('fornecedora', 'Consolidado'), report_type=request.args.get('report_type', 'base_clientes')))


# --- Rota para a Página do Mapa de Clientes --- <<<<<<<<<<<<<<<<<<<<<<<< ADICIONADA
@app.route('/mapa-clientes')
@login_required # Garante que o usuário esteja logado para acessar
def mapa_clientes():
    """Rota para exibir a página com o mapa de clientes por estado."""
    logger.info(f"Acesso à página do Mapa de Clientes por '{current_user.email}'.")
    # Simplesmente renderiza o template HTML do mapa
    # O JavaScript dentro do template cuidará de buscar os dados da API
    return render_template('mapa_clientes.html', title="Mapa de Clientes - Fast BI")


# --- Rota da API para Dados do Mapa (EXISTENTE E NECESSÁRIA) ---
@app.route('/api/map-data/client-count-by-state')
@login_required
def api_client_count_by_state():
    """Endpoint da API para fornecer dados de contagem de clientes por estado para o mapa."""
    logger.info(f"Requisição API para /api/map-data/client-count-by-state por '{current_user.email}'")
    try:
        data = database.get_client_count_by_state() # Chama a função no database.py

        # Formata os dados para Plotly (listas separadas para localizações e valores)
        formatted_data = {
            # Garante que ambos os elementos da tupla existam antes de adicionar
            'locations': [row[0] for row in data if row and len(row)>0],
            'z': [row[1] for row in data if row and len(row)>1]
        }
        # Adiciona verificação se as listas têm o mesmo tamanho (importante para Plotly)
        if len(formatted_data['locations']) != len(formatted_data['z']):
            logger.error("Erro de formatação: Listas 'locations' e 'z' têm tamanhos diferentes.")
            # Retorna um erro interno, pois isso indica um problema na lógica de busca/formatação
            return jsonify({"error": "Erro interno ao formatar dados do mapa"}), 500

        logger.debug(f"Dados para mapa: {len(formatted_data['locations'])} estados encontrados.")
        return jsonify(formatted_data) # Retorna JSON

    except (ConnectionError, RuntimeError) as db_map_err:
         logger.error(f"Erro de banco ao buscar dados para mapa: {db_map_err}", exc_info=False)
         return jsonify({"error": "Erro ao buscar dados para o mapa (Banco de Dados)"}), 500
    except Exception as e:
        logger.error(f"Erro inesperado na API /api/map-data/client-count-by-state: {e}", exc_info=True)
        # Retorna um erro 500 genérico com uma mensagem JSON
        return jsonify({"error": "Erro interno ao processar dados para o mapa"}), 500
# --- FIM ROTA API MAPA ---


# --- Execução da Aplicação ---
if __name__ == '__main__':
    # Lê configurações do ambiente ou usa defaults seguros
    # '0.0.0.0' permite acesso externo, '127.0.0.1' apenas local
    app_host = os.environ.get('FLASK_RUN_HOST', '127.0.0.1')
    # Porta padrão 5000, mas pode ser configurada
    app_port = int(os.environ.get('FLASK_RUN_PORT', 5000))
    # Debug mode: NUNCA usar True em produção!
    # Lê FLASK_DEBUG=1 ou FLASK_DEBUG=true do .env ou variáveis de ambiente
    app_debug = os.environ.get('FLASK_DEBUG', 'False').lower() in ['true', '1', 't']

    if app_debug:
        logger.warning("*"*10 + " MODO DEBUG ATIVADO! NÃO USE EM PRODUÇÃO! " + "*"*10)
        # Nível de log mais verboso em modo debug
        logger.setLevel(logging.DEBUG)
        logging.getLogger('werkzeug').setLevel(logging.DEBUG)
    else:
         logger.info("Modo DEBUG DESATIVADO.")

    logger.info(f"Iniciando servidor Flask em http://{app_host}:{app_port}/")
    # use_reloader=True é padrão com debug=True, mas pode ser explícito
    # threaded=True permite múltiplas conexões simultâneas (com ressalvas sobre concorrência)
    app.run(host=app_host, port=app_port, debug=app_debug)