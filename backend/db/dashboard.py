# backend/db/dashboard.py
import logging
from typing import List, Tuple, Optional, Union
from datetime import datetime
from .executor import execute_query
from collections import defaultdict
from . import reports_boletos
import pandas as pd
from .reports_boletos import final_columns_order as reports_boletos_columns_order

logger = logging.getLogger(__name__)

# --- FUNÇÕES PARA O DASHBOARD (KPIs, Resumos, Gráficos) ---
def get_total_consumo_medio_by_month(month_str: Optional[str] = None, fornecedora: Optional[str] = None) -> float:
    """Calcula a soma total de 'consumomedio' para clientes ativos no mês (data_ativo), opcionalmente filtrado por fornecedora."""
    base_query = """
        SELECT SUM(COALESCE(c.consumomedio, 0)) FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        AND {date_filter}
        {fornecedora_filter};
    """
    params = []
    date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError: logger.warning(f"Formato de mês inválido para consumo: '{month_str}'."); date_filter_sql = "c.data_ativo IS NOT NULL"; params = []

    fornecedora_filter_sql = ""
    if fornecedora and fornecedora.lower() != 'consolidado':
        fornecedora_filter_sql = "AND c.fornecedora = %s"
        params.append(fornecedora)

    final_query = base_query.format(date_filter=date_filter_sql, fornecedora_filter=fornecedora_filter_sql)
    try:
        result = execute_query(final_query, tuple(params), fetch_one=True)
        return float(result[0]) if result and result[0] is not None else 0.0
    except Exception as e: logger.error(f"Erro get_total_consumo_medio_by_month ({month_str}, {fornecedora}): {e}", exc_info=True); return 0.0

def count_clientes_ativos_by_month(month_str: Optional[str] = None, fornecedora: Optional[str] = None) -> int:
    """Conta clientes ativos no mês (data_ativo)."""
    base_query = """
        SELECT COUNT(DISTINCT c.idcliente)
        FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        AND {date_filter} {fornecedora_filter};
    """
    params = []
    date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError: logger.warning(f"Formato de mês inválido para contagem ativos: '{month_str}'."); date_filter_sql = "c.data_ativo IS NOT NULL"; params = []

    fornecedora_filter_sql = ""
    if fornecedora and fornecedora.lower() != 'consolidado':
        fornecedora_filter_sql = "AND c.fornecedora = %s"
        params.append(fornecedora)

    final_query = base_query.format(date_filter=date_filter_sql, fornecedora_filter=fornecedora_filter_sql)
    try:
        result = execute_query(final_query, tuple(params), fetch_one=True)
        return int(result[0]) if result and result[0] is not None else 0
    except Exception as e: logger.error(f"Erro count_clientes_ativos_by_month ({month_str}, {fornecedora}): {e}", exc_info=True); return 0

def count_clientes_registrados_by_month(month_str: Optional[str] = None, fornecedora: Optional[str] = None) -> int:
    """Conta clientes registrados no mês (dtcad)."""
    base_query = """
        SELECT COUNT(DISTINCT c.idcliente)
        FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        AND {date_filter} {fornecedora_filter};
    """
    params = []
    date_filter_sql = "c.dtcad IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.dtcad >= %s AND c.dtcad < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError: logger.warning(f"Formato de mês inválido para contagem registrados: '{month_str}'."); date_filter_sql = "c.dtcad IS NOT NULL"; params = []

    fornecedora_filter_sql = ""
    if fornecedora and fornecedora.lower() != 'consolidado':
        fornecedora_filter_sql = "AND c.fornecedora = %s"
        params.append(fornecedora)

    final_query = base_query.format(date_filter=date_filter_sql, fornecedora_filter=fornecedora_filter_sql)
    try:
        result = execute_query(final_query, tuple(params), fetch_one=True)
        return int(result[0]) if result and result[0] is not None else 0
    except Exception as e: logger.error(f"Erro count_clientes_registrados_by_month ({month_str}, {fornecedora}): {e}", exc_info=True); return 0

def get_fornecedora_summary(month_str: Optional[str] = None) -> Union[List[Tuple[str, int, float]], None]:
    """Busca resumo (qtd, consumo) por fornecedora para clientes ativos no mês (data_ativo)."""
    base_query = """
        SELECT
            COALESCE(NULLIF(TRIM(c.fornecedora), ''), 'NÃO ESPECIFICADA') AS fornecedora_tratada,
            COUNT(DISTINCT c.idcliente) AS qtd_clientes,
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

def get_concessionaria_summary(month_str: Optional[str] = None) -> Union[List[Tuple[str, int, float]], None]:
    """Busca resumo (qtd, consumo) por CONCESSIONÁRIA para clientes ativos no mês (data_ativo)."""
    base_query = """
        SELECT
            CASE
                WHEN c.concessionaria IS NULL OR TRIM(c.concessionaria) = '' THEN COALESCE(UPPER(TRIM(c.ufconsumo)), 'NÃO ESPECIFICADA')
                WHEN c.ufconsumo IS NULL OR TRIM(c.ufconsumo) = '' THEN UPPER(TRIM(c.concessionaria))
                ELSE (UPPER(TRIM(c.ufconsumo)) || '-' || UPPER(TRIM(c.concessionaria)))
            END AS regiao_concessionaria,
            COUNT(DISTINCT c.idcliente) AS qtd_clientes,
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

def get_monthly_active_clients_by_year(year: int, fornecedora: Optional[str] = None) -> List[int]:
    """Busca contagem mensal de clientes ativados por ano (data_ativo) para gráfico."""
    query = """
        SELECT EXTRACT(MONTH FROM c.data_ativo)::INTEGER AS mes, COUNT(DISTINCT c.idcliente) AS contagem
        FROM public."CLIENTES" c
        WHERE EXTRACT(YEAR FROM c.data_ativo) = %s
            AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
            {fornecedora_filter}
        GROUP BY mes ORDER BY mes;
    """
    params = [year]
    fornecedora_filter_sql = ""
    if fornecedora and fornecedora.lower() != 'consolidado':
        fornecedora_filter_sql = "AND c.fornecedora = %s"
        params.append(fornecedora)

    final_query = query.format(fornecedora_filter=fornecedora_filter_sql)
    
    monthly_counts = [0] * 12
    try:
        results = execute_query(final_query, tuple(params))
        if results:
            for row in results:
                month_index = row[0] - 1
                if 0 <= month_index < 12: monthly_counts[month_index] = int(row[1])
        return monthly_counts
    except Exception as e: logger.error(f"Erro get_monthly_active_clients_by_year ({year}): {e}", exc_info=True); return [0] * 12

# --- FUNÇÕES PARA GRÁFICOS PIZZA/BARRAS DO DASHBOARD ---
def get_active_clients_count_by_fornecedora_month(month_str: Optional[str] = None) -> Union[List[Tuple[str, int]], None]:
    """
    Busca a contagem de clientes ativos (por data_ativo) agrupados por fornecedora
    para um mês específico. Usado no gráfico de pizza do dashboard.
    """
    base_query = """
        SELECT
            COALESCE(NULLIF(TRIM(c.fornecedora), ''), 'NÃO ESPECIFICADA') AS fornecedora_tratada,
            COUNT(DISTINCT c.idcliente) AS qtd_clientes
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

def get_active_clients_count_by_concessionaria_month(month_str: Optional[str] = None) -> Union[List[Tuple[str, int]], None]:
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
            COUNT(DISTINCT c.idcliente) AS qtd_clientes
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
            COUNT(DISTINCT c.idcliente) as total_clientes,
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

# --- FUNÇÃO MODIFICADA (2ª Vez) PARA O CARD ---
def get_fornecedora_summary_no_rcb() -> List[Tuple[str, int, float]] or None:
    """
    Busca resumo (qtd clientes, soma consumo) por fornecedora para clientes
    cujo 'numinstalacao' não aparece na tabela RCB_CLIENTES E cujo 'data_ativo'
    é anterior a 100 dias atrás.
    """
    # <<< INÍCIO DA QUERY SUBSTITUÍDA (NOVA VERSÃO) >>>
    query = """
        -- CTE para calcular a contagem de cada numinstalacao em RCB_CLIENTES
        WITH ContagemInstalacoes AS (
            SELECT
                numinstalacao,
                COUNT(*) AS quantidade_ocorrencias
            FROM
                public."RCB_CLIENTES"
            GROUP BY
                numinstalacao
        ),
        -- CTE para juntar CLIENTES com as contagens, incluindo consumomedio e data_ativo
        ClientesComContagem AS (
            SELECT
                c.idcliente,
                c.fornecedora,
                c.numinstalacao,
                c.consumomedio,
                c.data_ativo, -- Incluindo data_ativo
                COALESCE(ci.quantidade_ocorrencias, 0) AS contagem_numinstalacao_rcb
            FROM
                public."CLIENTES" c
            LEFT JOIN
                ContagemInstalacoes ci ON c.numinstalacao = ci.numinstalacao
            WHERE
                 -- Filtro de origem padrão da aplicação
                (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        )
        -- Query final: Agrupa, filtra (contagem zero E data < 100 dias atrás), conta e soma
        SELECT
            ccc.fornecedora,
            -- Mantendo aliases antigos para compatibilidade com API/JS
            COUNT(ccc.idcliente) AS numero_clientes,
            SUM(COALESCE(ccc.consumomedio, 0)) AS soma_consumomedio
        FROM
            ClientesComContagem ccc
        WHERE
            ccc.contagem_numinstalacao_rcb = 0 -- Critério 1: contagem zero
            -- Critério 2: data_ativo anterior a 100 dias atrás (usando INTERVAL)
            AND ccc.data_ativo < (CURRENT_DATE - INTERVAL '100 day')
            -- Se 'data_ativo' for garantidamente DATE, pode usar:
            -- AND ccc.data_ativo < (CURRENT_DATE - 100)
        GROUP BY
            ccc.fornecedora
        HAVING
            COUNT(ccc.idcliente) > 0 -- Garante que só listamos fornecedoras com resultados > 0 após filtros
        ORDER BY
            ccc.fornecedora;
    """
    # <<< FIM DA QUERY SUBSTITUÍDA (NOVA VERSÃO) >>>

    # Log message atualizado para refletir ambos critérios
    logger.info("Executando query para card 'Fornecedoras s/ RCB (Clientes > 100d)'...")
    try:
        results = execute_query(query)
        if results:
            # Formatação (continua funcionando)
            formatted_results = [
                (
                    str(row[0]) if row[0] else 'N/A', # Fornecedora
                    int(row[1]) if row[1] is not None else 0, # numero_clientes
                    float(row[2]) if row[2] is not None else 0.0 # soma_consumomedio
                )
                for row in results
            ]
            logger.info(f"Dados 'Fornecedoras s/ RCB (Clientes > 100d)' encontrados: {len(formatted_results)} registros.")
            return formatted_results
        else:
            logger.info("Nenhum dado encontrado para 'Fornecedoras s/ RCB (Clientes > 100d)'.")
            return []
    except Exception as e:
        # Mensagem de erro atualizada
        logger.error(f"Erro ao buscar dados para 'Fornecedoras s/ RCB (Clientes > 100d)': {e}", exc_info=True)
        return None

# --- FIM DA FUNÇÃO MODIFICADA (2ª Vez) ---


# --- INÍCIO DA NOVA FUNÇÃO (COM AJUSTE PARA 120 DIAS) ---
def get_overdue_payments_by_fornecedora(days_overdue: int = 30) -> List[Tuple[str, int]] or None:
    """
    Busca a contagem de instalações com pagamentos vencidos há X dias (sem pagamento),
    agrupados por fornecedora.

    Args:
        days_overdue: O número de dias de atraso (ex: 30, 60, 90, 120).

    Returns:
        Lista de tuplas (fornecedora, quantidade) ou None em caso de erro.
    """
    # Modifique a lista para incluir 120
    if days_overdue not in [30, 60, 90, 120]:
        logger.warning(f"Valor inválido para days_overdue: {days_overdue}. Usando 30 por padrão.")
        days_overdue = 30

    # Usa INTERVAL para subtrair dias da data atual
    # Placeholders (%s) são usados para segurança (SQL Injection)
    query = """
        SELECT
            COALESCE(NULLIF(TRIM(c.fornecedora), ''), 'NÃO ESPECIFICADA') AS fornecedora_tratada,
            COUNT(rc.numinstalacao) AS quantidade_vencido_sem_pgto
        FROM
            public."CLIENTES" c
        INNER JOIN
            public."RCB_CLIENTES" rc ON c.numinstalacao = rc.numinstalacao
        WHERE
            rc.dtpagamento IS NULL
            AND rc.dtvencimento < (CURRENT_DATE - INTERVAL '%s day') -- Usa placeholder para o intervalo
            AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) -- Filtro padrão
        GROUP BY
            fornecedora_tratada
        ORDER BY
            quantidade_vencido_sem_pgto DESC, -- Ordena pela quantidade (maior primeiro)
            fornecedora_tratada;
    """
    params = (days_overdue,) # Passa o valor como parâmetro

    logger.info(f"Buscando pagamentos vencidos há {days_overdue} dias por fornecedora...")
    try:
        results = execute_query(query, params)
        if results:
            formatted_results = [(str(row[0]), int(row[1])) for row in results]
            logger.info(f"Dados de vencidos ({days_overdue} dias) encontrados: {len(formatted_results)} fornecedoras.")
            return formatted_results
        else:
            logger.info(f"Nenhum dado de vencidos ({days_overdue} dias) encontrado.")
            return []
    except Exception as e:
        logger.error(f"Erro ao buscar pagamentos vencidos ({days_overdue} dias) por fornecedora: {e}", exc_info=True)
        return None
# --- FIM DA NOVA FUNÇÃO ---


# --- INÍCIO DA NOVA FUNÇÃO PARA GREEN SCORE ---
def get_green_score_by_fornecedora(fornecedora_filter: Optional[str] = None) -> List[Tuple[str, float]] or None:
    """
    Calcula o "Green Score" para cada fornecedora baseado na pontualidade de injeção.
    O score é a porcentagem de clientes ativos que NÃO estão com a injeção atrasada.
    Lógica derivada do relatório 'Boletos por Cliente'.
    
    Args:
        fornecedora_filter: Se None, retorna scores para todas as fornecedoras.
                            Se uma string, filtra por essa fornecedora.
    Returns:
        Uma lista de tuplas (fornecedora, score) ou None em caso de erro.
    """
    log_msg = "Calculando Green Score"
    if fornecedora_filter:
        log_msg += f" para a fornecedora: {fornecedora_filter}"
    else:
        log_msg += " para TODAS as fornecedoras (Consolidado)"
    logger.info(log_msg)

    try:
        # 1. Obter todos os dados do relatório de boletos, sem paginação.
        # Passa o fornecedora_filter diretamente. Se for None, reports_boletos trará todos.
        all_clients_data = reports_boletos.get_boletos_por_cliente_data(
            limit=None, 
            export_mode=True, 
            fornecedora=fornecedora_filter
        )

        if not all_clients_data:
            logger.warning("Nenhum dado retornado do relatório de boletos para calcular o Green Score.")
            return []

        # 2. ATENÇÃO: A lógica abaixo depende da ordem das colunas definida em `reports_boletos.py`.
        # Se a ordem lá mudar, aqui também precisará ser ajustado.
        # Esta é a ordem esperada, conforme o arquivo `reports_boletos.py`:
        final_columns_order = [
             "codigo", "nome", "instalacao", "numero_cliente", "cpf_cnpj", "cidade",
             "ufconsumo", "concessionaria", "fornecedora", 
             "consumomedio", "data_ativo", "dias_desde_ativacao",
             "injecao", "atraso_na_injecao", "dias_em_atraso", # <-- 'atraso_na_injecao' é a 14ª coluna (índice 13)
             "validado_sucesso", "devolutiva", "retorno_fornecedora",
             "id_licenciado", "licenciado", "status_pro",
             "data_graduacao_pro", "quantidade_boletos"
        ]
        
        try:
            fornecedora_idx = final_columns_order.index('fornecedora')
            atraso_idx = final_columns_order.index('atraso_na_injecao')
        except ValueError as e:
            logger.error(f"Erro Crítico: Coluna '{e}' não encontrada na ordem definida. O cálculo do score falhará.")
            return None

        # 3. Processar os dados em Python para calcular o score por fornecedora
        totals_by_supplier = defaultdict(int)
        on_time_by_supplier = defaultdict(int)

        for row in all_clients_data:
            fornecedora = row[fornecedora_idx]
            atraso_status = row[atraso_idx]

            # >>> MODIFICAÇÃO AQUI: Ignora o registro se a fornecedora for nula, vazia ou 'NÃO ESPECIFICADA' <<<
            if not fornecedora or (isinstance(fornecedora, str) and fornecedora.strip() == '') or pd.isna(fornecedora):
                continue # Pula para o próximo registro no loop
            
            # Limpa espaços em branco e padroniza para maiúsculas (importante manter)
            if isinstance(fornecedora, str):
                fornecedora = fornecedora.strip().upper()

            totals_by_supplier[fornecedora] += 1
            if atraso_status == 'NÃO':
                on_time_by_supplier[fornecedora] += 1
        
        # 4. Calcular o score final para cada fornecedora
        scores = []
        for fornecedora, total_count in totals_by_supplier.items():
            on_time_count = on_time_by_supplier.get(fornecedora, 0)
            score = (on_time_count / total_count) * 100 if total_count > 0 else 0
            scores.append((fornecedora, round(score, 2)))

        # 5. Ordenar pelo score, do maior para o menor
        scores.sort(key=lambda x: x[1], reverse=True)
        
        logger.info(f"Green Score (Atraso Injeção) calculado para {len(scores)} fornecedoras.")
        return scores

    except Exception as e:
        logger.error(f"Erro inesperado ao calcular o Green Score (Atraso Injeção): {e}", exc_info=True)
        return None
# --- FIM DA NOVA FUNÇÃO ---


def get_overdue_clients_by_state_for_map() -> List[Tuple[str, int]]:
    """
    Busca a contagem de clientes com 'Atraso na Injeção' = 'SIM',
    agrupados por estado (UF).
    """
    logger.info("Buscando clientes com atraso na injeção por estado para o mapa...")

    try:
        all_clients_data = reports_boletos.get_boletos_por_cliente_data(
            limit=None,
            export_mode=True,
            fornecedora=None
        )

        if not all_clients_data:
            logger.warning("Nenhum dado retornado do relatório de boletos para calcular atraso por estado.")
            return []

        try:
            codigo_idx = reports_boletos_columns_order.index('codigo')
            uf_idx = reports_boletos_columns_order.index('ufconsumo')
            atraso_idx = reports_boletos_columns_order.index('atraso_na_injecao')
        except ValueError as e:
            logger.error(f"Erro Crítico: Coluna '{e}' não encontrada na ordem esperada do relatório de boletos. Não é possível calcular o atraso por estado.")
            return []

        overdue_counts_by_uf = defaultdict(int)
        seen_codigos = set()

        for row in all_clients_data:
            codigo_cliente = row[codigo_idx]
            if codigo_cliente not in seen_codigos:
                seen_codigos.add(codigo_cliente)
                
                if row[atraso_idx] == 'SIM':
                    uf = row[uf_idx]
                    if uf and isinstance(uf, str) and uf.strip() != '':
                        uf_cleaned = uf.strip().upper()
                        overdue_counts_by_uf[uf_cleaned] += 1
                    else:
                        logger.debug(f"UF inválida ou vazia encontrada no registro: {uf}")

        formatted_results = sorted(overdue_counts_by_uf.items(), key=lambda item: item[0])

        logger.info(f"Dados de clientes com atraso por estado para o mapa encontrados: {len(formatted_results)} estados.")
        return formatted_results

    except Exception as e:
        logger.error(f"Erro ao buscar clientes com atraso por estado para o mapa: {e}", exc_info=True)
        return []
# --- FIM DA FUNÇÃO ---

# --- NOVA FUNÇÃO: get_total_consumo_medio_consolidado ---
def get_total_consumo_medio_consolidado(fornecedora: Optional[str] = None) -> float:
    """
    Calcula a soma total de 'consumomedio' para clientes ativos,
    opcionalmente filtrado por fornecedora, SEM filtro de mês.
    """
    base_query = """
        SELECT SUM(COALESCE(c.consumomedio, 0)) FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        AND c.data_ativo IS NOT NULL
        {fornecedora_filter};
    """
    params = []
    fornecedora_filter_sql = ""
    if fornecedora and fornecedora.lower() != 'consolidado':
        fornecedora_filter_sql = "AND c.fornecedora = %s"
        params.append(fornecedora)

    final_query = base_query.format(fornecedora_filter=fornecedora_filter_sql)
    logger.info(f"Buscando consumo médio consolidado (Forn: {fornecedora or 'Todos'}): {final_query}")
    try:
        result = execute_query(final_query, tuple(params), fetch_one=True)
        return float(result[0]) if result and result[0] is not None else 0.0
    except Exception as e:
        logger.error(f"Erro get_total_consumo_medio_consolidado (Forn: {fornecedora}): {e}", exc_info=True)
        return 0.0

# --- NOVA FUNÇÃO: count_clientes_ativos_consolidado ---
def count_clientes_ativos_consolidado(fornecedora: Optional[str] = None) -> int:
    """
    Conta o total de clientes ativos (data_ativo)
    opcionalmente filtrado por fornecedora, SEM filtro de mês.
    """
    base_query = """
        SELECT COUNT(DISTINCT c.idcliente) FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        AND c.data_ativo IS NOT NULL
        {fornecedora_filter};
    """
    params = []
    fornecedora_filter_sql = ""
    if fornecedora and fornecedora.lower() != 'consolidado':
        fornecedora_filter_sql = "AND c.fornecedora = %s"
        params.append(fornecedora)

    final_query = base_query.format(fornecedora_filter=fornecedora_filter_sql)
    logger.info(f"Contando clientes ativos consolidados (Forn: {fornecedora or 'Todos'}): {final_query}")
    try:
        result = execute_query(final_query, tuple(params), fetch_one=True)
        return int(result[0]) if result and result[0] is not None else 0
    except Exception as e:
        logger.error(f"Erro count_clientes_ativos_consolidado (Forn: {fornecedora}): {e}", exc_info=True)
        return 0

# --- NOVA FUNÇÃO: count_clientes_registrados_consolidado ---
def count_clientes_registrados_consolidado(fornecedora: Optional[str] = None) -> int:
    """
    Conta o total de clientes registrados (dtcad)
    opcionalmente filtrado por fornecedora, SEM filtro de mês.
    """
    base_query = """
        SELECT COUNT(DISTINCT c.idcliente) FROM public."CLIENTES" c
        WHERE (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        AND c.dtcad IS NOT NULL
        {fornecedora_filter};
    """
    params = []
    fornecedora_filter_sql = ""
    if fornecedora and fornecedora.lower() != 'consolidado':
        fornecedora_filter_sql = "AND c.fornecedora = %s"
        params.append(fornecedora)

    final_query = base_query.format(fornecedora_filter=fornecedora_filter_sql)
    logger.info(f"Contando clientes registrados consolidados (Forn: {fornecedora or 'Todos'}): {final_query}")
    try:
        result = execute_query(final_query, tuple(params), fetch_one=True)
        return int(result[0]) if result and result[0] is not None else 0
    except Exception as e:
        logger.error(f"Erro count_clientes_registrados_consolidado (Forn: {fornecedora}): {e}", exc_info=True)
        return 0

def count_overdue_injection_clients(fornecedora: Optional[str] = None) -> dict:
    """
    Conta o número total de clientes que possuem "Atraso na Injeção" = 'SIM',
    calcula a média de dias de atraso e soma o consumo médio (kWh pendentes),
    opcionalmente filtrado por fornecedora.
    
    Returns:
        dict: {
            'count': int,
            'average_delay_days': int,
            'pending_kwh': float
        }
    """
    log_msg = "Contando clientes com Atraso na Injeção"
    if fornecedora:
        log_msg += f" para a fornecedora: {fornecedora}"
    else:
        log_msg += " para TODAS as fornecedoras (Consolidado)"
    logger.info(log_msg)

    try:
        # Reutiliza a função get_boletos_por_cliente_data para obter os dados já processados.
        all_clients_data = reports_boletos.get_boletos_por_cliente_data(
            limit=None,
            export_mode=True,
            fornecedora=fornecedora
        )

        if not all_clients_data:
            logger.warning("Nenhum dado retornado do relatório de boletos para contar clientes com atraso na injeção.")
            return {'count': 0, 'average_delay_days': 0, 'pending_kwh': 0}

        # Buscar índices das colunas necessárias
        try:
            codigo_idx = reports_boletos_columns_order.index('codigo')
            atraso_idx = reports_boletos_columns_order.index('atraso_na_injecao')
            dias_atraso_idx = reports_boletos_columns_order.index('dias_em_atraso')
            consumo_medio_idx = reports_boletos_columns_order.index('consumomedio')
        except ValueError as e:
            logger.error(f"Erro Crítico: Coluna '{e}' não encontrada na ordem esperada. Não é possível processar clientes com atraso.")
            return {'count': 0, 'average_delay_days': 0, 'pending_kwh': 0}

        # Processar dados únicos por cliente
        seen_codigos = set()
        unique_overdue_count = 0
        total_dias_atraso = 0
        total_consumo_medio = 0
        clientes_com_dias_validos = 0

        for row in all_clients_data:
            codigo_cliente = row[codigo_idx]
            if codigo_cliente not in seen_codigos:
                seen_codigos.add(codigo_cliente)
                if row[atraso_idx] == 'SIM':
                    unique_overdue_count += 1
                    
                    # Processar dias em atraso
                    try:
                        dias_atraso = float(row[dias_atraso_idx]) if row[dias_atraso_idx] not in [None, '', 'N/A'] else 0
                        if dias_atraso > 0:
                            total_dias_atraso += dias_atraso
                            clientes_com_dias_validos += 1
                    except (ValueError, TypeError):
                        pass  # Ignora valores inválidos
                    
                    # Processar consumo médio
                    try:
                        consumo_medio = float(row[consumo_medio_idx]) if row[consumo_medio_idx] not in [None, '', 'N/A'] else 0
                        total_consumo_medio += consumo_medio
                    except (ValueError, TypeError):
                        pass  # Ignora valores inválidos

        # Calcular média de dias de atraso
        average_delay_days = int(total_dias_atraso / clientes_com_dias_validos) if clientes_com_dias_validos > 0 else 0

        # Log detalhado da contagem e cálculos
        logger.info(f"Contagem de clientes com atraso na injeção: {unique_overdue_count}")
        logger.info(f"Soma total de dias de atraso: {total_dias_atraso} (Clientes com dias válidos: {clientes_com_dias_validos})")
        logger.info(f"Soma total de consumo médio (kWh pendentes): {total_consumo_medio}")

        return {
            'count': unique_overdue_count,
            'average_delay_days': average_delay_days,
            'pending_kwh': total_consumo_medio
        }

    except Exception as e:
        logger.error(f"Erro inesperado ao contar clientes com atraso na injeção: {e}", exc_info=True)
        return {'count': 0, 'average_delay_days': 0, 'pending_kwh': 0}


# --- NOVA FUNÇÃO: count_overdue_injection_clients_up_to_30_days ---
def count_overdue_injection_clients_up_to_30_days(fornecedora: Optional[str] = None) -> dict:
    """
    Conta o número total de clientes com "Atraso na Injeção" = 'SIM'
    E cujo "Dias em Atraso" seja MENOR OU IGUAL a 30 dias.
    Calcula também a média de dias de atraso e soma o consumo médio (kWh pendentes)
    para esses clientes específicos.

    Args:
        fornecedora: Opcional. Filtra os resultados por uma fornecedora específica.

    Returns:
        dict: {
            'count': int,
            'average_delay_days': int,
            'pending_kwh': float
        }
    """
    log_msg = "Contando clientes com Atraso na Injeção <= 30 dias"
    if fornecedora:
        log_msg += f" para a fornecedora: {fornecedora}"
    else:
        log_msg += " para TODAS as fornecedoras (Consolidado)"
    logger.info(log_msg)

    try:
        # Reutiliza a função get_boletos_por_cliente_data para obter os dados já processados.
        # Isso garante que a lógica de "Atraso na Injeção" e "Dias em Atraso" já esteja aplicada.
        all_clients_data = reports_boletos.get_boletos_por_cliente_data(
            limit=None,
            export_mode=True,
            fornecedora=fornecedora
        )

        if not all_clients_data:
            logger.warning("Nenhum dado retornado do relatório de boletos para contar clientes com atraso na injeção <= 30 dias.")
            return {'count': 0, 'average_delay_days': 0, 'pending_kwh': 0}

        # Buscar índices das colunas necessárias usando reports_boletos_columns_order
        try:
            codigo_idx = reports_boletos_columns_order.index('codigo')
            atraso_idx = reports_boletos_columns_order.index('atraso_na_injecao')
            dias_atraso_idx = reports_boletos_columns_order.index('dias_em_atraso')
            consumo_medio_idx = reports_boletos_columns_order.index('consumomedio')
        except ValueError as e:
            logger.error(f"Erro Crítico: Coluna '{e}' não encontrada na ordem esperada. Não é possível processar clientes com atraso <= 30 dias.")
            return {'count': 0, 'average_delay_days': 0, 'pending_kwh': 0}

        # Processar dados únicos por cliente com a nova condição de 30 dias
        seen_codigos = set()
        unique_overdue_count = 0
        total_dias_atraso = 0
        total_consumo_medio = 0
        clientes_com_dias_validos = 0

        for row in all_clients_data:
            codigo_cliente = row[codigo_idx]
            if codigo_cliente not in seen_codigos:
                seen_codigos.add(codigo_cliente)
                
                # Verifica a condição principal de atraso na injeção
                if row[atraso_idx] == 'SIM':
                    # Nova condição: Dias em Atraso <= 30
                    try:
                        dias_atraso = float(row[dias_atraso_idx]) if row[dias_atraso_idx] not in [None, '', 'N/A'] else 0
                        
                        if dias_atraso > 0 and dias_atraso <= 30: # Aplica o filtro de 30 dias
                            unique_overdue_count += 1
                            total_dias_atraso += dias_atraso
                            clientes_com_dias_validos += 1
                            
                            # Processar consumo médio para os clientes filtrados
                            try:
                                consumo_medio = float(row[consumo_medio_idx]) if row[consumo_medio_idx] not in [None, '', 'N/A'] else 0
                                total_consumo_medio += consumo_medio
                            except (ValueError, TypeError):
                                pass  # Ignora valores inválidos
                    except (ValueError, TypeError):
                        pass  # Ignora valores inválidos para 'dias_em_atraso'

        # Calcular média de dias de atraso
        average_delay_days = int(total_dias_atraso / clientes_com_dias_validos) if clientes_com_dias_validos > 0 else 0

        logger.info(f"Contagem de clientes com atraso na injeção <= 30 dias: {unique_overdue_count}")
        logger.info(f"Soma total de dias de atraso (<= 30d): {total_dias_atraso} (Clientes com dias válidos: {clientes_com_dias_validos})")
        logger.info(f"Soma total de consumo médio (kWh pendentes <= 30d): {total_consumo_medio}")

        return {
            'count': unique_overdue_count,
            'average_delay_days': average_delay_days,
            'pending_kwh': total_consumo_medio
        }

    except Exception as e:
        logger.error(f"Erro inesperado ao contar clientes com atraso na injeção <= 30 dias: {e}", exc_info=True)
        return {'count': 0, 'average_delay_days': 0, 'pending_kwh': 0}


# --- NOVA FUNÇÃO: count_overdue_injection_clients_over_30_days ---
def count_overdue_injection_clients_over_30_days(fornecedora: Optional[str] = None) -> dict:
    """
    Conta o número total de clientes com "Atraso na Injeção" = 'SIM'
    E cujo "Dias em Atraso" seja MAIOR que 30 dias.
    Calcula também a média de dias de atraso e soma o consumo médio (kWh pendentes)
    para esses clientes específicos.

    Args:
        fornecedora: Opcional. Filtra os resultados por uma fornecedora específica.

    Returns:
        dict: {
            'count': int,
            'average_delay_days': int,
            'pending_kwh': float
        }
    """
    log_msg = "Contando clientes com Atraso na Injeção > 30 dias"
    if fornecedora:
        log_msg += f" para a fornecedora: {fornecedora}"
    else:
        log_msg += " para TODAS as fornecedoras (Consolidado)"
    logger.info(log_msg)

    try:
        # Reutiliza a função get_boletos_por_cliente_data para obter os dados já processados.
        all_clients_data = reports_boletos.get_boletos_por_cliente_data(
            limit=None,
            export_mode=True,
            fornecedora=fornecedora
        )

        if not all_clients_data:
            logger.warning("Nenhum dado retornado do relatório de boletos para contar clientes com atraso na injeção > 30 dias.")
            return {'count': 0, 'average_delay_days': 0, 'pending_kwh': 0}

        # Buscar índices das colunas necessárias usando reports_boletos_columns_order
        try:
            codigo_idx = reports_boletos_columns_order.index('codigo')
            atraso_idx = reports_boletos_columns_order.index('atraso_na_injecao')
            dias_atraso_idx = reports_boletos_columns_order.index('dias_em_atraso')
            consumo_medio_idx = reports_boletos_columns_order.index('consumomedio')
        except ValueError as e:
            logger.error(f"Erro Crítico: Coluna '{e}' não encontrada na ordem esperada. Não é possível processar clientes com atraso > 30 dias.")
            return {'count': 0, 'average_delay_days': 0, 'pending_kwh': 0}

        # Processar dados únicos por cliente com a nova condição de > 30 dias
        seen_codigos = set()
        unique_overdue_count = 0
        total_dias_atraso = 0
        total_consumo_medio = 0
        clientes_com_dias_validos = 0

        for row in all_clients_data:
            codigo_cliente = row[codigo_idx]
            if codigo_cliente not in seen_codigos:
                seen_codigos.add(codigo_cliente)
                
                # Verifica a condição principal de atraso na injeção
                if row[atraso_idx] == 'SIM':
                    # Nova condição: Dias em Atraso > 30
                    try:
                        dias_atraso = float(row[dias_atraso_idx]) if row[dias_atraso_idx] not in [None, '', 'N/A'] else 0
                        
                        if dias_atraso > 30: # Aplica o filtro de > 30 dias
                            unique_overdue_count += 1
                            total_dias_atraso += dias_atraso
                            clientes_com_dias_validos += 1
                            
                            # Processar consumo médio para os clientes filtrados
                            try:
                                consumo_medio = float(row[consumo_medio_idx]) if row[consumo_medio_idx] not in [None, '', 'N/A'] else 0
                                total_consumo_medio += consumo_medio
                            except (ValueError, TypeError):
                                pass  # Ignora valores inválidos
                    except (ValueError, TypeError):
                        pass  # Ignora valores inválidos para 'dias_em_atraso'

        # Calcular média de dias de atraso
        average_delay_days = int(total_dias_atraso / clientes_com_dias_validos) if clientes_com_dias_validos > 0 else 0

        logger.info(f"Contagem de clientes com atraso na injeção > 30 dias: {unique_overdue_count}")
        logger.info(f"Soma total de dias de atraso (> 30d): {total_dias_atraso} (Clientes com dias válidos: {clientes_com_dias_validos})")
        logger.info(f"Soma total de consumo médio (kWh pendentes > 30d): {total_consumo_medio}")

        return {
            'count': unique_overdue_count,
            'average_delay_days': average_delay_days,
            'pending_kwh': total_consumo_medio
        }

    except Exception as e:
        logger.error(f"Erro inesperado ao contar clientes com atraso na injeção > 30 dias: {e}", exc_info=True)
        return {'count': 0, 'average_delay_days': 0, 'pending_kwh': 0}