# backend/db/dashboard.py
import logging
from typing import List, Tuple, Optional
from datetime import datetime
from .executor import execute_query # Import local

logger = logging.getLogger(__name__)

# --- FUNÇÕES PARA O DASHBOARD (KPIs, Resumos, Gráficos) ---
def get_total_consumo_medio_by_month(month_str: Optional[str] = None) -> float:
    """Calcula a soma total de 'consumomedio' para clientes ativos no mês (data_ativo)."""
    base_query = """
        SELECT SUM(COALESCE(c.consumomedio, 0)) FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) AND {date_filter};
    """
    params = []; date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError: logger.warning(f"Formato de mês inválido para consumo: '{month_str}'."); date_filter_sql = "c.data_ativo IS NOT NULL"; params = []
    final_query = base_query.format(date_filter=date_filter_sql)
    try: result = execute_query(final_query, tuple(params), fetch_one=True); return float(result[0]) if result and result[0] is not None else 0.0
    except Exception as e: logger.error(f"Erro get_total_consumo_medio_by_month ({month_str}): {e}", exc_info=True); return 0.0

def count_clientes_ativos_by_month(month_str: Optional[str] = None) -> int:
    """Conta clientes ativos no mês (data_ativo)."""
    base_query = """
        SELECT COUNT(c.idcliente) FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) AND {date_filter};
    """
    params = []; date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError: logger.warning(f"Formato de mês inválido para contagem ativos: '{month_str}'."); date_filter_sql = "c.data_ativo IS NOT NULL"; params = []
    final_query = base_query.format(date_filter=date_filter_sql)
    try: result = execute_query(final_query, tuple(params), fetch_one=True); return int(result[0]) if result and result[0] is not None else 0
    except Exception as e: logger.error(f"Erro count_clientes_ativos_by_month ({month_str}): {e}", exc_info=True); return 0

def count_clientes_registrados_by_month(month_str: Optional[str] = None) -> int:
    """Conta clientes REGISTRADOS no mês (dtcad)."""
    base_query = """
        SELECT COUNT(c.idcliente) FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) AND {date_filter};
    """
    params = []; date_filter_sql = "c.dtcad IS NOT NULL" # Filtra por dtcad
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.dtcad >= %s AND c.dtcad < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError: logger.warning(f"Formato de mês inválido para contagem registrados: '{month_str}'."); date_filter_sql = "c.dtcad IS NOT NULL"; params = []
    final_query = base_query.format(date_filter=date_filter_sql)
    try: result = execute_query(final_query, tuple(params), fetch_one=True); return int(result[0]) if result and result[0] is not None else 0
    except Exception as e: logger.error(f"Erro count_clientes_registrados_by_month ({month_str}): {e}", exc_info=True); return 0

def get_fornecedora_summary(month_str: Optional[str] = None) -> List[Tuple[str, int, float]] or None:
    """Busca resumo (qtd, consumo) por fornecedora para clientes ativos no mês (data_ativo)."""
    base_query = """
        SELECT
            COALESCE(NULLIF(TRIM(c.fornecedora), ''), 'NÃO ESPECIFICADA') AS fornecedora_tratada,
            COUNT(c.idcliente) AS qtd_clientes,
            SUM(COALESCE(c.consumomedio, 0)) AS soma_consumo_medio_por_fornecedora
        FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) AND {date_filter}
        GROUP BY fornecedora_tratada ORDER BY fornecedora_tratada;
    """
    params = []; date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError: logger.warning(f"Formato de mês inválido para resumo fornecedora: '{month_str}'."); date_filter_sql = "c.data_ativo IS NOT NULL"; params = []
    final_query = base_query.format(date_filter=date_filter_sql)
    try:
        results = execute_query(final_query, tuple(params))
        if results: return [(str(row[0]), int(row[1]), float(row[2]) if row[2] is not None else 0.0) for row in results]
        else: return []
    except Exception as e: logger.error(f"Erro get_fornecedora_summary ({month_str}): {e}", exc_info=True); return None

def get_concessionaria_summary(month_str: Optional[str] = None) -> List[Tuple[str, int, float]] or None:
    """Busca resumo (qtd, consumo) por CONCESSIONÁRIA para clientes ativos no mês (data_ativo)."""
    base_query = """
        SELECT
            CASE
                WHEN c.concessionaria IS NULL OR TRIM(c.concessionaria) = '' THEN COALESCE(UPPER(TRIM(c.ufconsumo)), 'NÃO ESPECIFICADA')
                WHEN c.ufconsumo IS NULL OR TRIM(c.ufconsumo) = '' THEN UPPER(TRIM(c.concessionaria))
                ELSE (UPPER(TRIM(c.ufconsumo)) || '-' || UPPER(TRIM(c.concessionaria)))
            END AS regiao_concessionaria,
            COUNT(c.idcliente) AS qtd_clientes,
            SUM(COALESCE(c.consumomedio, 0)) AS soma_consumo_medio
        FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) AND {date_filter}
        GROUP BY regiao_concessionaria ORDER BY regiao_concessionaria;
    """
    params = []; date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError: logger.warning(f"Formato de mês inválido para resumo concessionaria: '{month_str}'."); date_filter_sql = "c.data_ativo IS NOT NULL"; params = []
    final_query = base_query.format(date_filter=date_filter_sql)
    try:
        results = execute_query(final_query, tuple(params))
        if results: return [(str(row[0]), int(row[1]), float(row[2]) if row[2] is not None else 0.0) for row in results]
        else: return []
    except Exception as e: logger.error(f"Erro get_concessionaria_summary ({month_str}): {e}", exc_info=True); return None

def get_monthly_active_clients_by_year(year: int) -> List[int]:
    """Busca contagem mensal de clientes ativados por ano (data_ativo) para gráfico."""
    query = """
        SELECT EXTRACT(MONTH FROM c.data_ativo)::INTEGER AS mes, COUNT(c.idcliente) AS contagem
        FROM public."CLIENTES" c
        WHERE EXTRACT(YEAR FROM c.data_ativo) = %s
            AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        GROUP BY mes ORDER BY mes;
    """
    params = (year,); monthly_counts = [0] * 12
    try:
        results = execute_query(query, params)
        if results:
            for row in results:
                month_index = row[0] - 1
                if 0 <= month_index < 12: monthly_counts[month_index] = int(row[1])
        return monthly_counts
    except Exception as e: logger.error(f"Erro get_monthly_active_clients_by_year ({year}): {e}", exc_info=True); return [0] * 12

# --- FUNÇÕES PARA GRÁFICOS PIZZA/BARRAS DO DASHBOARD ---
def get_active_clients_count_by_fornecedora_month(month_str: Optional[str] = None) -> List[Tuple[str, int]] or None:
    """
    Busca a contagem de clientes ativos (por data_ativo) agrupados por fornecedora
    para um mês específico. Usado no gráfico de pizza do dashboard.
    """
    base_query = """
        SELECT
            COALESCE(NULLIF(TRIM(c.fornecedora), ''), 'NÃO ESPECIFICADA') AS fornecedora_tratada,
            COUNT(c.idcliente) AS qtd_clientes
        FROM public."CLIENTES" c
        WHERE
            (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
            AND {date_filter}
        GROUP BY fornecedora_tratada
        HAVING COUNT(c.idcliente) > 0
        ORDER BY qtd_clientes DESC, fornecedora_tratada;
    """
    params = []
    date_filter_sql = "c.data_ativo IS NOT NULL"

    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12:
                end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else:
                end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"
            params.extend([start_date, end_date_exclusive])
        except ValueError:
            logger.warning(f"[PIE CHART] Formato de mês inválido: '{month_str}'. Usando filtro padrão.")
            date_filter_sql = "c.data_ativo IS NOT NULL"
            params = []

    final_query = base_query.format(date_filter=date_filter_sql)
    logger.debug(f"Executando query para gráfico pizza fornecedora (Mês: {month_str or 'Todos'}): {final_query} com params: {params}")

    try:
        results = execute_query(final_query, tuple(params))
        if results:
            formatted_results = [(str(row[0]), int(row[1])) for row in results]
            logger.info(f"[PIE CHART] Dados por fornecedora (Mês: {month_str or 'Todos'}) encontrados: {len(formatted_results)} registros.")
            return formatted_results
        else:
            logger.info(f"[PIE CHART] Nenhum dado encontrado para gráfico pizza fornecedora (Mês: {month_str or 'Todos'}).")
            return []
    except Exception as e:
        logger.error(f"[PIE CHART] Erro ao buscar dados para gráfico pizza fornecedora (Mês: {month_str or 'Todos'}): {e}", exc_info=True)
        return None

def get_active_clients_count_by_concessionaria_month(month_str: Optional[str] = None) -> List[Tuple[str, int]] or None:
    """
    Busca a CONTAGEM de clientes ativos agrupados por Região/Concessionária,
    filtrando por clientes cuja data_ativo cai dentro do mês especificado.
    """
    base_query = """
        SELECT
            CASE
                WHEN c.concessionaria IS NULL OR TRIM(c.concessionaria) = '' THEN COALESCE(UPPER(TRIM(c.ufconsumo)), 'NÃO ESPECIFICADA')
                WHEN c.ufconsumo IS NULL OR TRIM(c.ufconsumo) = '' THEN UPPER(TRIM(c.concessionaria))
                ELSE (UPPER(TRIM(c.ufconsumo)) || '-' || UPPER(TRIM(c.concessionaria)))
            END AS regiao_concessionaria,
            COUNT(c.idcliente) AS qtd_clientes
        FROM public."CLIENTES" c
        WHERE
            (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
            AND {date_filter}
        GROUP BY regiao_concessionaria
        ORDER BY qtd_clientes DESC;
    """
    params = []
    date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12:
                end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else:
                end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"
            params.extend([start_date, end_date_exclusive])
        except ValueError:
            logger.warning(f"[CONCESSIONARIA COUNT] Formato de mês inválido: '{month_str}'. Usando data_ativo IS NOT NULL.")
            date_filter_sql = "c.data_ativo IS NOT NULL"
            params = []

    final_query = base_query.format(date_filter=date_filter_sql)
    logger.debug(f"Buscando contagem de clientes por concessionária (Mês: {month_str or 'Todos'})...")
    try:
        results = execute_query(final_query, tuple(params))
        if results:
            formatted_results = [(str(row[0]), int(row[1])) for row in results]
            logger.debug(f"Contagem por concessionária (Mês: {month_str or 'Todos'}) encontrada: {len(formatted_results)} registros.")
            return formatted_results
        else:
            logger.debug(f"Nenhum dado encontrado para contagem por concessionária (Mês: {month_str or 'Todos'}).")
            return []
    except Exception as e:
        logger.error(f"Erro ao buscar contagem por concessionária (Mês: {month_str or 'Todos'}): {e}", exc_info=True)
        return None
# --- FIM DA FUNÇÃO ---

# --- Função para buscar DADOS AGREGADOS POR ESTADO para o MAPA ---
def get_state_map_data() -> List[Tuple[str, int, float]]: # Retorna UF, CONTAGEM, SOMA
    """
    Busca a CONTAGEM de clientes ativos e a SOMA de 'consumomedio' desses clientes,
    agrupado por estado (UF).
    """
    query = """
        SELECT
            UPPER(c.ufconsumo) as estado_uf,
            COUNT(c.idcliente) as total_clientes,
            SUM(COALESCE(c.consumomedio, 0)) as total_consumo_medio
        FROM public."CLIENTES" c
        WHERE
            c.data_ativo IS NOT NULL
            AND c.ufconsumo IS NOT NULL AND c.ufconsumo <> ''
            AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        GROUP BY
            UPPER(c.ufconsumo)
        ORDER BY
            estado_uf;
    """
    logger.info("Buscando CONTAGEM e SOMA de consumo médio por estado para o mapa...")
    try:
        results = execute_query(query)
        formatted_results = [
            (
                str(row[0]),
                int(row[1]) if row[1] is not None else 0,
                float(row[2]) if row[2] is not None else 0.0
            )
            for row in results if row and len(row) > 2
        ]
        logger.info(f"Dados de contagem e soma por estado encontrados: {len(formatted_results)} estados.")
        return formatted_results or []
    except Exception as e:
        logger.error(f"Erro ao buscar dados agregados por estado para o mapa: {e}", exc_info=True)
        return []
# --- FIM FUNÇÃO MAPA ---