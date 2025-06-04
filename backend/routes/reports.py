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
    """Rota principal para visualização de relatórios.
       Agora carrega apenas a 'casca' da página. Os dados são carregados via API.
    """
    user_nome = current_user.nome if hasattr(current_user, 'nome') else 'Anónimo'
    logger.info(f"Acessando /relatorios (carregamento assíncrono). Utilizador: {user_nome}")
    try:
        # Apenas obtemos os parâmetros para pré-selecionar os filtros no HTML
        selected_report_type = request.args.get('report_type', 'base_clientes')
        selected_fornecedora = request.args.get('fornecedora', 'Consolidado')

        # Busca a lista de fornecedoras para o dropdown
        try:
            fornecedoras_db = db.get_fornecedoras()
        except Exception as db_err:
            logger.error(f"Erro ao buscar lista de fornecedoras: {db_err}", exc_info=True)
            flash("Erro ao carregar a lista de fornecedoras.", "warning")
            fornecedoras_db = []
        
        fornecedoras_list = ['Consolidado'] + [f for f in fornecedoras_db if f != 'Consolidado']

        # Não buscamos mais dados aqui. Apenas renderizamos o template.
        # Os valores de dados, headers, paginação, etc., são deixados vazios ou nulos.
        return render_template(
            'relatorios.html',
            fornecedoras=fornecedoras_list,
            selected_fornecedora=selected_fornecedora,
            selected_report_type=selected_report_type,
            headers=[], # Vazio
            dados=None, # Nulo ou vazio
            page=request.args.get('page', 1, type=int), # Passamos a página para o JS saber qual buscar
            total_pages=0, # Vazio
            total_items=0, # Vazio
            error=None,
            title="Relatórios - Fast BI"
        )

    except Exception as e:
        logger.error(f"Erro GERAL na rota /relatorios (casca): {e}", exc_info=True)
        flash("Ocorreu um erro inesperado ao carregar a página de relatórios.", "error")
        return render_template('relatorios.html', error="Erro interno grave.", fornecedoras=['Consolidado'])

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

        if selected_report_type == 'rateio':
            forn_fn = 'Consolidado' if selected_fornecedora.lower() == "consolidado" else secure_filename(selected_fornecedora).replace('_', '')
            filename = f"Clientes_Rateio_{forn_fn}_{timestamp}.xlsx"
            nova_ids = db.get_base_nova_ids(fornecedora=selected_fornecedora)
            enviada_ids = db.get_base_enviada_ids(fornecedora=selected_fornecedora)
            if not nova_ids and not enviada_ids:
                flash(f"Nenhum dado encontrado para exportar o Rateio Geral (Fornecedora: {selected_fornecedora}).", "warning")
                return redirect(url_for('reports_bp.relatorios', **request.args))

            rateio_headers = db.get_headers('rateio') # Pega cabeçalhos
            nova_data = db.get_client_details_by_ids('rateio', nova_ids) if nova_ids else []
            enviada_data = db.get_client_details_by_ids('rateio', enviada_ids) if enviada_ids else []

            sheets_to_export = [
                {'name': 'Base Nova', 'headers': rateio_headers, 'data': nova_data},
                {'name': 'Base Enviada', 'headers': rateio_headers, 'data': enviada_data}
            ]
            excel_bytes = excel_exp.export_multi_sheet_excel_bytes(sheets_to_export)

        elif selected_report_type == 'rateio_rzk':
             filename = f"Clientes_Rateio_RZK_MultiBase_{timestamp}.xlsx"
             nova_ids_rzk = db.get_rateio_rzk_base_nova_ids()
             enviada_ids_rzk = db.get_rateio_rzk_base_enviada_ids()
             if not nova_ids_rzk and not enviada_ids_rzk:
                 flash("Nenhum dado encontrado para exportar o Rateio RZK.", "warning")
                 return redirect(url_for('reports_bp.relatorios', report_type='rateio_rzk'))

             rzk_headers = db.get_headers('rateio_rzk')
             nova_data_rzk = db.get_rateio_rzk_client_details_by_ids(nova_ids_rzk) if nova_ids_rzk else []
             enviada_data_rzk = db.get_rateio_rzk_client_details_by_ids(enviada_ids_rzk) if enviada_ids_rzk else []
             sheets_to_export = [
                 {'name': 'Base Nova RZK', 'headers': rzk_headers, 'data': nova_data_rzk},
                 {'name': 'Base Enviada RZK', 'headers': rzk_headers, 'data': enviada_data_rzk}
             ]
             excel_bytes = excel_exp.export_multi_sheet_excel_bytes(sheets_to_export)

        elif selected_report_type in ['base_clientes', 'clientes_por_licenciado', 'recebiveis_clientes']:
            forn_fn = secure_filename(selected_fornecedora).replace('_', '') if selected_fornecedora and selected_fornecedora.lower() != 'consolidado' else 'Consolidado'
            dados_completos = []
            sheet_title = selected_report_type.replace('_', ' ').title() # Título padrão
            headers = db.get_headers(selected_report_type) # Pega cabeçalhos

            if not headers:
                 flash(f"Configuração de cabeçalhos ausente para exportar '{selected_report_type}'.", "error")
                 return redirect(url_for('reports_bp.relatorios', **request.args))

            if selected_report_type == 'base_clientes':
                 filename = f"Clientes_Base_{forn_fn}_{timestamp}.xlsx"
                 data_query, data_params = db.build_query(selected_report_type, selected_fornecedora, 0, None)
                 dados_completos = db.execute_query(data_query, data_params) or []
                 sheet_title = f"Base Clientes ({forn_fn})"

            elif selected_report_type == 'clientes_por_licenciado':
                 filename = f"Qtd_Clientes_Licenciado_{timestamp}.xlsx"
                 dados_completos = db.get_clientes_por_licenciado_data(limit=None)
                 sheet_title = "Clientes por Licenciado"

            elif selected_report_type == 'recebiveis_clientes':
                filename = f"Recebiveis_Clientes_{forn_fn}_{timestamp}.xlsx"
                dados_completos = db.get_recebiveis_clientes_data(limit=None, fornecedora=selected_fornecedora)
                sheet_title = f"Recebíveis ({forn_fn})"

            if len(sheet_title) > 31: sheet_title = sheet_title[:31]

            if not dados_completos:
                 flash(f"Nenhum dado encontrado para exportar o relatório '{sheet_title}'.", "warning")
                 return redirect(url_for('reports_bp.relatorios', **request.args))

            current_app.logger.debug(f"Headers para exportação: {headers}")
            current_app.logger.debug(f"Dados para exportação (primeiras linhas): {dados_completos[:5]}")

            def sanitize_excel_data_extreme(data):
                sanitized_data = []
                for row in data:
                    sanitized_row = []
                    for cell in row:
                        if isinstance(cell, str):
                            if "CS" in cell and "FUN" in cell:
                                sanitized_row.append("[DADOS_SANITIZADOS]")
                            elif cell.startswith(('=', '+', '-', '@')) and len(cell) > 1:
                                sanitized_row.append(f"'{cell}")
                            else:
                                sanitized_row.append(cell)
                        else:
                            sanitized_row.append(cell)
                    sanitized_data.append(sanitized_row)
                return sanitized_data

            def sanitize_headers_extreme(header_list):
                sanitized_headers = []
                for header in header_list:
                    if isinstance(header, str):
                        if "CS" in header and "FUN" in header:
                            sanitized_headers.append("COLUNA_SANITIZADA")
                        elif header.startswith(('=', '+', '-', '@')) and len(header) > 1:
                            sanitized_headers.append(f"'{header}")
                        else:
                            sanitized_headers.append(header)
                    else:
                        sanitized_headers.append(header)
                return sanitized_headers

            dados_sanitizados = sanitize_excel_data_extreme(dados_completos)
            headers_sanitizados = sanitize_headers_extreme(headers)
            
            excel_bytes = excel_exp.export_to_excel_bytes(dados_sanitizados, headers_sanitizados, sheet_name=sheet_title)
        else:
             logger.warning(f"Tentativa de exportação de tipo inválido: '{selected_report_type}'.")
             flash(f"Tipo de relatório inválido para exportação: '{selected_report_type}'.", "error")
             return redirect(url_for('reports_bp.relatorios'))

        if excel_bytes:
            logger.info(f"Exportação Excel concluída. Enviando ficheiro: {filename} ({len(excel_bytes)} bytes)")
            return Response(
                excel_bytes,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                headers={'Content-Disposition': f'attachment;filename="{filename}"'}
            )
        else:
            logger.error(f"Falha ao gerar bytes Excel para '{selected_report_type}' (Forn: {selected_fornecedora}). Nenhum ficheiro será enviado.")
            return redirect(url_for('reports_bp.relatorios', **request.args))

    except Exception as exp_err:
        logger.error(f"Erro Inesperado durante a exportação Excel: {exp_err}", exc_info=True)
        flash("Ocorreu um erro inesperado durante a geração do arquivo Excel.", "error")
        return redirect(url_for('reports_bp.relatorios', **request.args))