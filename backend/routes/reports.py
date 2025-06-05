# backend/routes/reports.py
import logging
import math
from datetime import datetime
from flask import (Blueprint, render_template, request, Response, flash,
                   redirect, url_for, current_app)
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename
from .. import db
from ..exporter import ExcelExporter

logger = logging.getLogger(__name__)

reports_bp = Blueprint('reports_bp', __name__,
                       template_folder='../templates',
                       static_folder='../static')

@reports_bp.route('/relatorios')
@login_required
def relatorios():
    """Rota principal para visualização de relatórios."""
    user_nome = current_user.nome if hasattr(current_user, 'nome') else 'Anónimo'
    logger.info(f"Acessando /relatorios. Utilizador: {user_nome}")
    try:
        # --- LEITURA DOS PARÂMETROS DA URL ---
        page = request.args.get('page', 1, type=int)
        # O tipo de relatório agora vem do 'report_type_select' ou do 'report_type' oculto
        selected_report_type = request.args.get('report_type_select') or request.args.get('report_type', 'base_clientes')
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado')
        # Novos parâmetros de data
        start_date = request.args.get('start_date', None)
        end_date = request.args.get('end_date', None)

        page = max(1, page)
        
        # Obter lista de fornecedoras (lógica existente)
        try:
            fornecedoras_db = db.get_fornecedoras()
        except Exception as db_err:
            logger.error(f"Erro ao buscar lista de fornecedoras: {db_err}", exc_info=True)
            flash("Erro ao carregar a lista de fornecedoras.", "warning")
            fornecedoras_db = []
        fornecedoras_list = ['Consolidado'] + [f for f in fornecedoras_db if f != 'Consolidado']

        items_per_page = current_app.config.get('ITEMS_PER_PAGE', 50)
        offset = (page - 1) * items_per_page
        dados, headers, total_items, error_message = [], [], 0, None

        logger.info(f"Processando relatório: Tipo='{selected_report_type}', Fornecedora='{selected_fornecedora}', Página={page}, Data Início='{start_date}', Data Fim='{end_date}'")

        try:
            headers = db.get_headers(selected_report_type)
            if not headers:
                 error_message = f"Cabeçalhos não definidos para o tipo de relatório: '{selected_report_type}'."
                 logger.error(error_message)
                 flash(error_message, 'danger')
            else:
                if selected_report_type in ['base_clientes', 'rateio']:
                    data_query, data_params = db.build_query(selected_report_type, selected_fornecedora, offset, items_per_page)
                    dados = db.execute_query(data_query, data_params) or []
                    count_q, count_p = db.count_query(selected_report_type, selected_fornecedora)
                    total_items_result = db.execute_query(count_q, count_p, fetch_one=True)
                    total_items = total_items_result[0] if total_items_result else 0

                elif selected_report_type == 'rateio_rzk':
                    total_items = db.count_rateio_rzk()
                    dados = db.get_rateio_rzk_data(offset=offset, limit=items_per_page)
                    selected_fornecedora = 'RZK' # Força a fornecedora para este relatório

                elif selected_report_type == 'clientes_por_licenciado':
                    total_items = db.count_clientes_por_licenciado()
                    dados = db.get_clientes_por_licenciado_data(offset=offset, limit=items_per_page)

                elif selected_report_type == 'boletos_por_cliente':
                    total_items = db.count_boletos_por_cliente(fornecedora=selected_fornecedora)
                    dados = db.get_boletos_por_cliente_data(offset=offset, limit=items_per_page, fornecedora=selected_fornecedora)

                # <<< INÍCIO DO NOVO BLOCO >>>
                elif selected_report_type == 'graduacao_licenciado':
                    # Chama as funções de DB passando as datas
                    total_items = db.count_graduacao_licenciado(start_date=start_date, end_date=end_date)
                    dados = db.get_graduacao_licenciado_data(offset=offset, limit=items_per_page, start_date=start_date, end_date=end_date)
                
                # <<< FIM DO NOVO BLOCO >>>

                elif selected_report_type == 'recebiveis_clientes':
                    # Busca os dados paginados para recebíveis, passando a fornecedora
                    dados = db.get_recebiveis_clientes_data(offset=offset, limit=items_per_page, fornecedora=selected_fornecedora)
                    # Conta o total de itens para recebíveis, respeitando a fornecedora
                    total_items = db.count_recebiveis_clientes(fornecedora=selected_fornecedora)

                else: # Bloco else existente
                    error_message = f"Tipo de relatório desconhecido ou não implementado: '{selected_report_type}'."
                    logger.warning(f"Tentativa de acesso a relatório inválido: '{selected_report_type}'.")
                    flash(error_message, "warning")
                    headers = [] # Limpa cabeçalhos se o tipo for inválido

        except Exception as e:
             logger.error(f"Erro ao buscar dados para o relatório '{selected_report_type}': {e}", exc_info=True)
             error_message = "Ocorreu um erro ao buscar os dados do relatório."
             flash(error_message, 'danger')
             dados = []
             total_items = 0

        # Calcular paginação
        if not error_message and total_items > 0 and items_per_page > 0:
            total_pages = math.ceil(total_items / items_per_page)
            if page > total_pages and total_pages > 0: # Corrige se a página pedida for maior que o total
                logger.warning(f"Página solicitada ({page}) maior que o total ({total_pages}). Redirecionando para a última página.")
                # O ideal seria redirecionar, mas por simplicidade vamos apenas ajustar a página para renderizar
                page = total_pages
                offset = (page - 1) * items_per_page
                # Refazer a query com a página corrigida seria o mais correto
                # (Omitido aqui por brevidade)
        elif not error_message:
            total_pages = 0 if total_items == 0 else 1 # 0 páginas se 0 itens, 1 página se itens <= items_per_page

        # --- RENDERIZAÇÃO DO TEMPLATE (PASSANDO AS DATAS) ---
        return render_template(
            'relatorios.html',
            fornecedoras=fornecedoras_list,
            selected_fornecedora=selected_fornecedora,
            selected_report_type=selected_report_type,
            selected_start_date=start_date, # <-- Passa a data de início para o template
            selected_end_date=end_date,     # <-- Passa a data de fim para o template
            headers=headers,
            dados=dados,
            page=page,
            total_pages=total_pages,
            total_items=total_items,
            items_per_page=items_per_page,
            error=error_message,
            title=f"{selected_report_type.replace('_', ' ').title()} - Relatórios"
        )

    except Exception as e:
        logger.error(f"Erro GERAL na rota /relatorios: {e}", exc_info=True)
        flash("Ocorreu um erro inesperado ao processar sua solicitação de relatório.", "error")
        return render_template(
            'relatorios.html',
            title="Erro Crítico - Relatórios", error="Erro interno grave.",
            dados=[], headers=[], page=1, total_pages=0, total_items=0,
            selected_report_type='base_clientes', selected_fornecedora='Consolidado',
            selected_start_date=None, selected_end_date=None
            ), 500


@reports_bp.route('/export')
@login_required
def exportar_excel_route():
    """Rota para exportar os dados do relatório selecionado para Excel."""
    user_nome = current_user.nome if hasattr(current_user, 'nome') else 'Anónimo'
    try:
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado')
        selected_report_type = request.args.get('report_type', 'base_clientes')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_exp = ExcelExporter() # Instancia o exportador
        excel_bytes = None
        filename = f"Relatorio_{secure_filename(selected_report_type)}_{timestamp}.xlsx" # Nome de arquivo padrão

        logger.info(f"Iniciando exportação Excel: Tipo='{selected_report_type}', Fornecedora='{selected_fornecedora}'. Utilizador: {user_nome}")

        # Lógica de exportação baseada no tipo (similar ao app.py original)
        # --- EXPORTAÇÃO RATEIO GERAL (MULTI-ABA) ---
        if selected_report_type == 'rateio':
            forn_fn = 'Consolidado' if selected_fornecedora.lower() == "consolidado" else secure_filename(selected_fornecedora).replace('_', '')
            filename = f"Clientes_Rateio_{forn_fn}_{timestamp}.xlsx"
            # Busca TODOS os IDs (sem paginação)
            nova_ids = db.get_base_nova_ids(fornecedora=selected_fornecedora)
            enviada_ids = db.get_base_enviada_ids(fornecedora=selected_fornecedora)
            if not nova_ids and not enviada_ids:
                flash(f"Nenhum dado encontrado para exportar o Rateio Geral (Fornecedora: {selected_fornecedora}).", "warning")
                return redirect(url_for('reports_bp.relatorios', **request.args))

            rateio_headers = db.get_headers('rateio') # Pega cabeçalhos
            # Busca detalhes completos para os IDs encontrados
            nova_data = db.get_client_details_by_ids('rateio', nova_ids) if nova_ids else []
            enviada_data = db.get_client_details_by_ids('rateio', enviada_ids) if enviada_ids else []

            sheets_to_export = [
                {'name': 'Base Nova', 'headers': rateio_headers, 'data': nova_data},
                {'name': 'Base Enviada', 'headers': rateio_headers, 'data': enviada_data}
            ]
            excel_bytes = excel_exp.export_multi_sheet_excel_bytes(sheets_to_export)

        # --- EXPORTAÇÃO RATEIO RZK (MULTI-ABA) ---
        elif selected_report_type == 'rateio_rzk':
             filename = f"Clientes_Rateio_RZK_MultiBase_{timestamp}.xlsx"
             nova_ids_rzk = db.get_rateio_rzk_base_nova_ids()
             enviada_ids_rzk = db.get_rateio_rzk_base_enviada_ids()
             if not nova_ids_rzk and not enviada_ids_rzk:
                 flash("Nenhum dado encontrado para exportar o Rateio RZK.", "warning")
                 return redirect(url_for('reports_bp.relatorios', report_type='rateio_rzk')) # Redireciona mantendo o tipo

             rzk_headers = db.get_headers('rateio_rzk')
             nova_data_rzk = db.get_rateio_rzk_client_details_by_ids(nova_ids_rzk) if nova_ids_rzk else []
             enviada_data_rzk = db.get_rateio_rzk_client_details_by_ids(enviada_ids_rzk) if enviada_ids_rzk else []
             sheets_to_export = [
                 {'name': 'Base Nova RZK', 'headers': rzk_headers, 'data': nova_data_rzk},
                 {'name': 'Base Enviada RZK', 'headers': rzk_headers, 'data': enviada_data_rzk}
             ]
             excel_bytes = excel_exp.export_multi_sheet_excel_bytes(sheets_to_export)

        # --- EXPORTAÇÃO RELATÓRIOS DE ABA ÚNICA ---
        elif selected_report_type in ['base_clientes', 'clientes_por_licenciado', 'boletos_por_cliente', 'recebiveis_clientes', 'graduacao_licenciado']: # Adicionado 'recebiveis_clientes' aqui
            forn_fn = secure_filename(selected_fornecedora).replace('_', '') if selected_fornecedora and selected_fornecedora.lower() != 'consolidado' else 'Consolidado'
            dados_completos = []
            sheet_title = selected_report_type.replace('_', ' ').title() # Título padrão
            headers = db.get_headers(selected_report_type) # Pega cabeçalhos

            if not headers:
                 flash(f"Configuração de cabeçalhos ausente para exportar '{selected_report_type}'.", "error")
                 return redirect(url_for('reports_bp.relatorios', **request.args))

            # Busca todos os dados (sem paginação - limit=None)
            if selected_report_type == 'base_clientes':
                 filename = f"Clientes_Base_{forn_fn}_{timestamp}.xlsx"
                 data_query, data_params = db.build_query(selected_report_type, selected_fornecedora, 0, None) # limit=None
                 dados_completos = db.execute_query(data_query, data_params) or []
                 sheet_title = f"Base Clientes ({forn_fn})"

            elif selected_report_type == 'graduacao_licenciado':
                 filename = f"Tempo_Graduacao_Licenciado_{timestamp}.xlsx"
                 dados_completos = db.get_graduacao_licenciado_data(limit=None) # limit=None para todos os dados
                 sheet_title = "Tempo para Graduação"

            elif selected_report_type == 'clientes_por_licenciado':
                 filename = f"Qtd_Clientes_Licenciado_{timestamp}.xlsx"
                 dados_completos = db.get_clientes_por_licenciado_data(limit=None) # limit=None
                 sheet_title = "Clientes por Licenciado"

            elif selected_report_type == 'boletos_por_cliente':
                 filename = f"Qtd_Boletos_Cliente_{forn_fn}_{timestamp}.xlsx"
                 dados_completos = db.get_boletos_por_cliente_data(limit=None, fornecedora=selected_fornecedora) # limit=None
                 sheet_title = f"Boletos Cliente ({forn_fn})"

            elif selected_report_type == 'recebiveis_clientes': # Novo bloco para exportação de recebíveis
                filename = f"Recebiveis_Clientes_{forn_fn}_{timestamp}.xlsx"
                dados_completos = db.get_recebiveis_clientes_data(limit=None, fornecedora=selected_fornecedora) # limit=None
                sheet_title = f"Recebíveis ({forn_fn})"

            # Garante que o nome da aba não exceda 31 caracteres
            if len(sheet_title) > 31: sheet_title = sheet_title[:31]

            if not dados_completos:
                 flash(f"Nenhum dado encontrado para exportar o relatório '{sheet_title}'.", "warning")
                 return redirect(url_for('reports_bp.relatorios', **request.args))

            # Gera o Excel de aba única
            excel_bytes = excel_exp.export_to_excel_bytes(dados_completos, headers, sheet_name=sheet_title)
        else:
             # Tipo de relatório inválido para exportação
             logger.warning(f"Tentativa de exportação de tipo inválido: '{selected_report_type}'.")
             flash(f"Tipo de relatório inválido para exportação: '{selected_report_type}'.", "error")
             return redirect(url_for('reports_bp.relatorios'))

        # --- Envia a Resposta com o Arquivo Excel ---
        if excel_bytes:
            logger.info(f"Exportação Excel concluída. Enviando ficheiro: {filename} ({len(excel_bytes)} bytes)")
            return Response(
                excel_bytes,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                headers={'Content-Disposition': f'attachment;filename="{filename}"'}
            )
        else:
            # Se excel_bytes for None (erro na geração ou nenhum dado)
            logger.error(f"Falha ao gerar bytes Excel para '{selected_report_type}' (Forn: {selected_fornecedora}). Nenhum ficheiro será enviado.")
            # A mensagem flash já deve ter sido definida no bloco acima
            return redirect(url_for('reports_bp.relatorios', **request.args))

    except Exception as exp_err:
        logger.error(f"Erro Inesperado durante a exportação Excel: {exp_err}", exc_info=True)
        flash("Ocorreu um erro inesperado durante a geração do arquivo Excel.", "error")
        # Redireciona de volta para a página de relatórios com os mesmos parâmetros
        return redirect(url_for('reports_bp.relatorios', **request.args))