# backend/routes/api.py
import logging
import re
from flask import Blueprint, request, jsonify
from flask_login import login_required
from .. import db

logger = logging.getLogger(__name__)

# Blueprint para as rotas da API, com prefixo /api definido no __init__.py
api_bp = Blueprint('api_bp', __name__)

# --- Rota API para Dados do Mapa (UF, Contagem, Soma Consumo) ---
@api_bp.route('/map-data/state-summary')
@login_required
def api_state_summary_for_map():
    """Retorna dados agregados (UF, contagem, soma consumo) por estado para o mapa."""
    logger.info("Requisição recebida em /api/map-data/state-summary")
    try:
        # Chama a função do banco que retorna UF, Contagem e Soma
        data_from_db = db.get_state_map_data() # Ex: [('SP', 100, 50000.0), ('MG', 80, 42000.0)]

        if data_from_db is None: # Verifica se o DB retornou None (indicando erro interno)
             logger.error("API Mapa: db.get_state_map_data() retornou None.")
             return jsonify({"error": "Erro interno ao buscar dados do mapa do banco de dados."}), 500

        # Transforma a lista de tuplas em uma lista de dicionários para facilitar o JS
        structured_data = [
            {'uf': row[0], 'count': row[1], 'sum_consumo': row[2]}
            for row in data_from_db
        ]
        logger.info(f"API Mapa: Enviando {len(structured_data)} registros de estado.")
        # Retorna a lista de dicionários como JSON
        return jsonify(structured_data)

    except Exception as e:
        logger.error(f"API Mapa (/api/map-data/state-summary) Erro inesperado: {e}", exc_info=True)
        return jsonify({"error": "Erro interno inesperado ao processar dados do mapa."}), 500

# --- Rota API para Resumo por Fornecedora ---
@api_bp.route('/summary/fornecedora')
@login_required
def api_fornecedora_summary():
    """Retorna dados de resumo (qtd, kwh) agrupados por Fornecedora."""
    month_str = request.args.get('month')
    if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str):
        logger.warning(f"API Fornecedora Summary: Formato de mês inválido '{month_str}'")
        return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400

    logger.debug(f"API Fornecedora Summary: Buscando dados para o mês '{month_str}'.")
    try:
        summary_data = db.get_fornecedora_summary(month_str=month_str)
        if summary_data is None:
            logger.error(f"API Fornecedora Summary: Erro interno (DB retornou None) para o mês {month_str}.")
            return jsonify({"error": "Erro interno ao buscar dados por fornecedora."}), 500
        elif not summary_data:
            logger.info(f"API Fornecedora Summary: Nenhum dado encontrado para o mês {month_str}.")
            return jsonify([]) # Retorna lista vazia se não houver dados
        else:
            # Formata como lista de dicionários
            data_as_dicts = [{"fornecedora": r[0], "qtd_clientes": r[1], "soma_consumo": r[2]} for r in summary_data]
            logger.debug(f"API Fornecedora Summary: Enviando {len(data_as_dicts)} registros para {month_str}.")
            return jsonify(data_as_dicts)
    except Exception as e:
        logger.error(f"API Fornecedora Summary: Erro inesperado para o mês {month_str}: {e}", exc_info=True)
        return jsonify({"error": "Erro inesperado no servidor ao buscar resumo por fornecedora."}), 500


# --- Rota API para Resumo por Concessionária ---
@api_bp.route('/summary/concessionaria')
@login_required
def api_concessionaria_summary():
    """Retorna dados de resumo (qtd, kwh) agrupados por Concessionária."""
    month_str = request.args.get('month')
    if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str):
        logger.warning(f"API Concessionaria Summary: Formato de mês inválido '{month_str}'")
        return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400

    logger.debug(f"API Concessionaria Summary: Buscando dados para o mês '{month_str}'.")
    try:
        summary_data = db.get_concessionaria_summary(month_str=month_str)
        if summary_data is None:
            logger.error(f"API Concessionaria Summary: Erro interno (DB retornou None) para o mês {month_str}.")
            return jsonify({"error": "Erro interno ao buscar dados por concessionária."}), 500
        elif not summary_data:
            logger.info(f"API Concessionaria Summary: Nenhum dado encontrado para o mês {month_str}.")
            return jsonify([])
        else:
            data_as_dicts = [{"concessionaria": r[0], "qtd_clientes": r[1], "soma_consumo": r[2]} for r in summary_data]
            logger.debug(f"API Concessionaria Summary: Enviando {len(data_as_dicts)} registros para {month_str}.")
            return jsonify(data_as_dicts)
    except Exception as e:
        logger.error(f"API Concessionaria Summary: Erro inesperado para o mês {month_str}: {e}", exc_info=True)
        return jsonify({"error": "Erro inesperado no servidor ao buscar resumo por concessionária."}), 500

# --- Rota API para KPI Total kWh ---
@api_bp.route('/kpi/total-kwh')
@login_required
def api_kpi_total_kwh():
    """Retorna o KPI de consumo total de kWh para o mês."""
    month_str = request.args.get('month')
    if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str):
        return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400
    try:
        total_kwh = db.get_total_consumo_medio_by_month(month_str=month_str)
        logger.debug(f"API KPI Total kWh: Mês={month_str}, Resultado={total_kwh}")
        return jsonify({"total_kwh": total_kwh})
    except Exception as e:
        logger.error(f"API KPI Total kWh: Erro para o mês {month_str}: {e}", exc_info=True)
        return jsonify({"error": "Erro inesperado ao buscar KPI Total kWh."}), 500

# --- Rota API para KPI Clientes Ativos (data_ativo) ---
@api_bp.route('/kpi/clientes-ativos')
@login_required
def api_kpi_clientes_ativos():
    """Retorna o KPI de contagem de clientes ativos no mês (por data_ativo)."""
    month_str = request.args.get('month')
    if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str):
        return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400
    try:
        count = db.count_clientes_ativos_by_month(month_str=month_str)
        logger.debug(f"API KPI Clientes Ativos: Mês={month_str}, Resultado={count}")
        return jsonify({"clientes_ativos_count": count})
    except Exception as e:
        logger.error(f"API KPI Clientes Ativos: Erro para o mês {month_str}: {e}", exc_info=True)
        return jsonify({"error": "Erro inesperado ao buscar KPI Clientes Ativos."}), 500

# --- Rota API para KPI Clientes REGISTRADOS (dtcad) ---
@api_bp.route('/kpi/clientes-registrados')
@login_required
def api_kpi_clientes_registrados():
    """Retorna o KPI de contagem de clientes registrados no mês (por dtcad)."""
    month_str = request.args.get('month')
    if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str):
        return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400
    try:
        count = db.count_clientes_registrados_by_month(month_str=month_str)
        logger.debug(f"API KPI Clientes Registrados: Mês={month_str}, Resultado={count}")
        return jsonify({"clientes_registrados_count": count})
    except Exception as e:
        logger.error(f"API KPI Clientes Registrados: Erro para o mês {month_str}: {e}", exc_info=True)
        return jsonify({"error": "Erro inesperado ao buscar KPI Clientes Registrados."}), 500

# --- Rota API para Dados do Gráfico Mensal (Evolução Ativações) ---
@api_bp.route('/chart/monthly-active-clients')
@login_required
def api_chart_monthly_active_clients():
    """Retorna a contagem de clientes ativados por mês para um dado ano (gráfico linha)."""
    year_str = request.args.get('year')
    if not year_str or not re.match(r'^\d{4}$', year_str):
        logger.warning(f"API Chart Mensal: Ano inválido '{year_str}'")
        return jsonify({"error": "Formato de ano inválido. Use YYYY."}), 400
    try:
        year = int(year_str)
        logger.debug(f"API Chart Mensal: Buscando dados para o ano '{year}'.")
        monthly_data = db.get_monthly_active_clients_by_year(year=year)
        if monthly_data is None: # Verifica erro no DB
             logger.error(f"API Chart Mensal: Erro interno (DB retornou None) para o ano {year}.")
             return jsonify({"error": f"Erro interno ao buscar dados mensais para {year}."}), 500

        logger.debug(f"API Chart Mensal: Enviando dados para {year}: {monthly_data}")
        return jsonify({"monthly_counts": monthly_data})
    except ValueError:
        logger.warning(f"API Chart Mensal: Ano inválido (não inteiro) '{year_str}'.")
        return jsonify({"error": "Ano fornecido é inválido."}), 400
    except Exception as e:
        logger.error(f"API Chart Mensal: Erro inesperado para o ano {year_str}: {e}", exc_info=True)
        return jsonify({"error": "Erro inesperado no servidor ao buscar dados do gráfico mensal."}), 500

# --- Rota API para Gráfico Pizza Fornecedora ---
@api_bp.route('/pie/clientes-fornecedora')
@login_required
def api_clientes_fornecedora_pie():
    """Retorna dados para o gráfico de pizza de clientes ativos por fornecedora."""
    month_str = request.args.get('month')
    if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str):
        logger.warning(f"API Pizza Fornecedora: Formato de mês inválido '{month_str}'")
        return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400

    logger.debug(f"API Pizza Fornecedora: Buscando dados para o mês '{month_str}'.")
    try:
        # Reutiliza a função de resumo, pegando apenas fornecedora (índice 0) e contagem (índice 1)
        data_from_db = db.get_fornecedora_summary(month_str=month_str)

        if data_from_db is None:
            logger.error(f"API Pizza Fornecedora: Erro interno (DB retornou None) para o mês {month_str}.")
            return jsonify({"error": "Erro interno ao buscar dados por fornecedora para o gráfico."}), 500
        elif not data_from_db:
            logger.info(f"API Pizza Fornecedora: Nenhum dado encontrado para o mês {month_str}.")
            return jsonify({"labels": [], "data": []}) # Estrutura vazia para Chart.js

        # Separa labels (nomes) e data (valores)
        labels = [item[0] for item in data_from_db]
        data_values = [item[1] for item in data_from_db]

        logger.debug(f"API Pizza Fornecedora: Enviando {len(labels)} fornecedoras para {month_str}.")
        return jsonify({"labels": labels, "data": data_values})

    except Exception as e:
        logger.error(f"API Pizza Fornecedora: Erro inesperado para o mês {month_str}: {e}", exc_info=True)
        return jsonify({"error": "Erro inesperado no servidor ao buscar dados do gráfico de pizza."}), 500

# --- API para Gráfico Barras Concessionária ---
@api_bp.route('/bar/clientes-concessionaria')
@login_required
def api_clientes_concessionaria_bar():
    """Retorna dados para o gráfico de barras de clientes ativos por Região/Concessionária."""
    month_str = request.args.get('month')
    limit_str = request.args.get('limit', '15') # Limite padrão de 15, pode ser ajustado via query param

    if not month_str or not re.match(r'^\d{4}-\d{2}$', month_str):
        logger.warning(f"API Barra Concessionária: Formato de mês inválido '{month_str}'")
        return jsonify({"error": "Formato de mês inválido. Use YYYY-MM."}), 400
    try:
        limit = int(limit_str)
        if limit <= 0: limit = None # Sem limite se for 0 ou negativo
    except ValueError:
        logger.warning(f"API Barra Concessionária: Limite inválido '{limit_str}', usando padrão (15).")
        limit = 15

    logger.debug(f"API Barra Concessionária: Buscando dados para mês '{month_str}', limite={limit or 'Nenhum'}.")
    try:
        # Chama a função que retorna (regiao, contagem), já ordenada por contagem DESC
        data_from_db = db.get_active_clients_count_by_concessionaria_month(month_str=month_str)

        if data_from_db is None:
            logger.error(f"API Barra Concessionária: Erro interno (DB retornou None) para o mês {month_str}.")
            return jsonify({"error": "Erro interno ao buscar dados por concessionária para o gráfico."}), 500
        elif not data_from_db:
            logger.info(f"API Barra Concessionária: Nenhum dado encontrado para o mês {month_str}.")
            return jsonify({"labels": [], "data": []})

        # Aplica o limite, se houver
        if limit and len(data_from_db) > limit:
             logger.debug(f"API Barra Concessionária: Limitando resultados de {len(data_from_db)} para {limit}.")
             data_to_send = data_from_db[:limit]
        else:
             data_to_send = data_from_db

        # Separa labels e data
        labels = [item[0] for item in data_to_send]
        data_values = [item[1] for item in data_to_send]

        logger.debug(f"API Barra Concessionária: Enviando {len(labels)} concessionárias para {month_str}.")
        return jsonify({"labels": labels, "data": data_values})

    except Exception as e:
        logger.error(f"API Barra Concessionária: Erro inesperado para o mês {month_str}: {e}", exc_info=True)
        return jsonify({"error": "Erro inesperado no servidor ao buscar dados do gráfico de barras."}), 500