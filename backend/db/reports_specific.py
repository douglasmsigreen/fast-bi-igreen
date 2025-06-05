# backend/db/reports_specific.py
import logging
from typing import List, Tuple, Optional
from .executor import execute_query # Import local

logger = logging.getLogger(__name__)

# --- Funções para Relatórios Específicos (Clientes por Licenciado, Boletos) ---
def get_clientes_por_licenciado_data(offset: int = 0, limit: Optional[int] = None) -> List[tuple]:
    """Busca os dados para 'Quantidade de Clientes por Licenciado'."""
    base_query = """
        SELECT c.idconsultor, c.nome, c.cpf, c.email, c.uf, COUNT(cl.idconsultor) AS quantidade_clientes_ativos
        FROM public."CONSULTOR" c LEFT JOIN public."CLIENTES" cl ON c.idconsultor = cl.idconsultor
        WHERE cl.data_ativo IS NOT NULL AND (cl.origem IS NULL OR cl.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        GROUP BY c.idconsultor, c.nome, c.cpf, c.email, c.uf ORDER BY quantidade_clientes_ativos DESC, c.nome """
    
    final_params_list = []
    limit_sql_part = ""
    if limit is not None:
        limit_sql_part = "LIMIT %s"
        final_params_list.append(limit)

    offset_sql_part = ""
    if offset > 0:
        offset_sql_part = "OFFSET %s"
        final_params_list.append(offset)
        
    paginated_query = f"{base_query.strip()} {limit_sql_part.strip()} {offset_sql_part.strip()};".replace("  ", " ").strip()
    
    actual_params_tuple = tuple(final_params_list)
    logger.debug(f"REPORTS_SPECIFIC - Query get_clientes_por_licenciado_data: [{paginated_query}], Params (tupla): [{actual_params_tuple}]")
    try: return execute_query(paginated_query, actual_params_tuple) or []
    except Exception as e: logger.error(f"Erro get_clientes_por_licenciado_data: {e}", exc_info=True); return []

def count_clientes_por_licenciado() -> int:
    """Conta o total de consultores com clientes ativos."""
    count_query_sql = """
        SELECT COUNT(DISTINCT c.idconsultor) FROM public."CONSULTOR" c
        INNER JOIN public."CLIENTES" cl ON c.idconsultor = cl.idconsultor
        WHERE cl.data_ativo IS NOT NULL AND (cl.origem IS NULL OR cl.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')); """
    logger.debug(f"REPORTS_SPECIFIC - Query count_clientes_por_licenciado: [{count_query_sql}], Params (tupla): [()]")
    try:
        result = execute_query(count_query_sql, (), fetch_one=True)
        # --- CORREÇÃO DE INDENTAÇÃO APLICADA AQUI ---
        return result[0] if result and result[0] is not None else 0
    except Exception as e:
        logger.error(f"Erro count_clientes_por_licenciado: {e}", exc_info=True)
        return 0

# --- FUNÇÕES PARA RELATÓRIO 'Quantidade de Boletos por Cliente' ---
def get_boletos_por_cliente_data(offset: int = 0, limit: Optional[int] = None, fornecedora: Optional[str] = None) -> List[tuple]:
    """Busca os dados para 'Quantidade de Boletos por Cliente'."""
    base_query = """
        SELECT c.idcliente, c.nome, c.numinstalacao, c.celular, c.cidade,
               CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN '' ELSE (c.uf || '-' || c.concessionaria) END AS regiao,
               c.fornecedora, TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo_formatado,
               CASE
                   WHEN c.data_ativo IS NOT NULL THEN EXTRACT(DAY FROM (NOW() - c.data_ativo))::INTEGER
                   ELSE NULL
               END AS dias_ativo,
               COUNT(rcb.numinstalacao) AS quantidade_registros_rcb
        FROM public."CLIENTES" c
        LEFT JOIN public."RCB_CLIENTES" rcb ON c.numinstalacao = rcb.numinstalacao """
    
    params_for_where = []  # Parâmetros para a cláusula WHERE
    where_clauses = [
        "(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))",
        "c.data_ativo IS NOT NULL",
        # --- MODIFICAÇÃO: Usar placeholder %s para o padrão ILIKE ---
        "(c.status NOT ILIKE %s OR c.status IS NULL)"
    ]
    # --- MODIFICAÇÃO: Adicionar o valor do padrão à lista de parâmetros ---
    params_for_where.append('CANCELADO%')
    
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s")
        params_for_where.append(fornecedora)  # Adicionar parâmetro da fornecedora
    
    where_sql_part = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    group_by_sql_part = " GROUP BY c.idcliente, c.nome, c.numinstalacao, c.celular, c.cidade, regiao, c.fornecedora, c.data_ativo, dias_ativo "
    order_by_sql_part = "ORDER BY c.idcliente"
    
    # Construir a lista final de parâmetros na ordem correta
    final_params_list = list(params_for_where)  # Começa com os parâmetros do WHERE
    
    limit_sql_part = ""
    if limit is not None:
        limit_sql_part = "LIMIT %s"
        final_params_list.append(limit)  # Adicionar parâmetro do LIMIT

    offset_sql_part = ""
    if offset > 0:
        offset_sql_part = "OFFSET %s"
        final_params_list.append(offset)  # Adicionar parâmetro do OFFSET
        
    paginated_query = f"{base_query.strip()} {where_sql_part.strip()} {group_by_sql_part.strip()} {order_by_sql_part.strip()} {limit_sql_part.strip()} {offset_sql_part.strip()};".replace("  ", " ").strip()
        
    actual_params_tuple = tuple(final_params_list)
    # Log para verificar a query e os parâmetros finais
    logger.debug(f"REPORTS_SPECIFIC - Query get_boletos_por_cliente_data: [{paginated_query}], Params (tupla): [{actual_params_tuple}]")
    
    try: return execute_query(paginated_query, actual_params_tuple) or []
    except Exception as e: logger.error(f"Erro get_boletos_por_cliente_data: {e}", exc_info=True); return []

def count_boletos_por_cliente(fornecedora: Optional[str] = None) -> int:
    """Conta o total de clientes para 'Boletos por Cliente'."""
    params_list = []  # Parâmetros para esta query
    where_clauses = [
        "(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))",
        "c.data_ativo IS NOT NULL",
        # --- MODIFICAÇÃO: Usar placeholder %s para o padrão ILIKE ---
        "(c.status NOT ILIKE %s OR c.status IS NULL)"
    ]
    # --- MODIFICAÇÃO: Adicionar o valor do padrão à lista de parâmetros ---
    params_list.append('CANCELADO%')
    
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s")
        params_list.append(fornecedora)  # Adicionar parâmetro da fornecedora
    
    where_sql_part = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    count_query_sql = f'SELECT COUNT(DISTINCT c.idcliente) FROM public."CLIENTES" c {where_sql_part.strip()};'.replace("  ", " ").strip()
    
    actual_params_tuple = tuple(params_list)
    # Log para verificar a query e os parâmetros finais
    logger.debug(f"REPORTS_SPECIFIC - Query count_boletos_por_cliente: [{count_query_sql}], Params (tupla): [{actual_params_tuple}]")

    try: 
        result = execute_query(count_query_sql, actual_params_tuple, fetch_one=True)
        return result[0] if result and result[0] is not None else 0
    except Exception as e: 
        logger.error(f"Erro count_boletos_por_cliente: {e}", exc_info=True)
        return 0

# --- FUNÇÕES PARA RATEIO RZK --- (Manter código original, mas aplicar .strip() e .replace("  ", " ") na construção final da query se usar f-strings)
def get_rateio_rzk_base_nova_ids() -> List[int]:
    """Busca IDs para 'Base Nova' do Rateio RZK."""
    query_base = """ SELECT DISTINCT cc.idcliente FROM public."CLIENTES_CONTRATOS" cc
        INNER JOIN public."CLIENTES_CONTRATOS_SIGNER" ccs ON cc.idcliente_contrato = ccs.idcliente_contrato
        INNER JOIN public."CLIENTES" c ON cc.idcliente = c.idcliente """
    group_by = " GROUP BY cc.idcliente_contrato, cc.idcliente HAVING bool_and(ccs.signature_at IS NOT NULL) "
    where_clauses = [
        "c.fornecedora = 'RZK'", "cc.type_document = 'procuracao_igreen'", "upper(cc.status) = 'ATIVO'",
        "c.data_ativo IS NOT NULL", "c.status IS NULL", "c.validadosucesso = 'S'",
        "c.rateio = 'N'", " (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) ",
        " NOT EXISTS ( SELECT 1 FROM public.\"DEVOLUTIVAS\" d WHERE d.idcliente = c.idcliente ) "
    ]
    full_query = f"{query_base.strip()} WHERE {' AND '.join(where_clauses)} {group_by.strip()};".replace("  ", " ").strip()
    logger.debug(f"REPORTS_SPECIFIC - Query get_rateio_rzk_base_nova_ids: [{full_query}], Params (tupla): [()]")
    try: 
        results = execute_query(full_query, ()) # Passar tupla vazia
        return [r[0] for r in results] if results else []
    except Exception as e: 
        logger.error(f"Erro get_rateio_rzk_base_nova_ids: {e}")
        return []

def get_rateio_rzk_base_enviada_ids() -> List[int]:
    """Busca IDs para 'Base Enviada' do Rateio RZK."""
    query_base = 'SELECT c.idcliente FROM public."CLIENTES" c'
    where_clauses = [ "c.fornecedora = 'RZK'", "c.rateio = 'S'", " (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) " ]
    full_query = f"{query_base.strip()} WHERE {' AND '.join(where_clauses)};".replace("  ", " ").strip()
    logger.debug(f"REPORTS_SPECIFIC - Query get_rateio_rzk_base_enviada_ids: [{full_query}], Params (tupla): [()]")
    try: 
        results = execute_query(full_query, ()) # Passar tupla vazia
        return [r[0] for r in results] if results else []
    except Exception as e: 
        logger.error(f"Erro get_rateio_rzk_base_enviada_ids: {e}")
        return []

def _get_rateio_rzk_fields() -> List[str]:
    """Retorna a lista de campos SQL EXATOS para Rateio RZK."""
    return [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao", "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo_formatado", "c.consumomedio", "c.status AS devolutiva", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "co.nome AS licenciado", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.keycontrato AS chave_contrato", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "c.nome AS nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ]

def get_rateio_rzk_client_details_by_ids(client_ids: List[int], batch_size: int = 1000) -> List[tuple]:
    """Busca detalhes completos para Rateio RZK por lista de IDs."""
    if not client_ids: return []
    all_details = []
    try:
        campos = _get_rateio_rzk_fields()
        if not campos: logger.error("Falha campos RZK details"); return []
        select_sql = f"SELECT {', '.join(campos)}"
        from_sql = 'FROM public."CLIENTES" c'
        join_sql = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor'
        where_sql = "WHERE c.idcliente = ANY(%s) AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))" # Placeholder para ANY
        order_sql = "ORDER BY c.idcliente"
        query = f"{select_sql.strip()} {from_sql.strip()}{join_sql.strip()} {where_sql.strip()} {order_sql.strip()};".replace("  ", " ").strip()
        for i in range(0, len(client_ids), batch_size):
            batch_ids = client_ids[i:i + batch_size]
            actual_params_tuple = (batch_ids,) # O parâmetro para ANY(%s) é uma lista/tupla
            logger.debug(f"REPORTS_SPECIFIC - Query get_rateio_rzk_client_details_by_ids (batch): [{query}], Params (tupla): [{actual_params_tuple}]")
            batch_results = execute_query(query, actual_params_tuple)
            if batch_results: all_details.extend(batch_results)
        return all_details
    except Exception as e: 
        logger.error(f"Erro get_rateio_rzk_client_details_by_ids: {e}", exc_info=True)
        return []

def get_rateio_rzk_data(offset: int = 0, limit: Optional[int] = None) -> List[tuple]:
    """Busca dados paginados para display Rateio RZK (Base Enviada)."""
    campos_rzk = _get_rateio_rzk_fields()
    select_sql = f"SELECT {', '.join(campos_rzk)}"
    from_sql = 'FROM public."CLIENTES" c'
    join_sql = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor'
    where_clauses = ["c.fornecedora = 'RZK'", "c.rateio = 'S'", "(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    where_sql_part = f"WHERE {' AND '.join(where_clauses)}"
    order_by_sql_part = "ORDER BY c.idcliente"
    
    final_params_list = []
    limit_sql_part = ""
    if limit is not None:
        limit_sql_part = "LIMIT %s"
        final_params_list.append(limit)

    offset_sql_part = ""
    if offset > 0:
        offset_sql_part = "OFFSET %s"
        final_params_list.append(offset)
        
    paginated_query = f"{select_sql.strip()} {from_sql.strip()}{join_sql.strip()} {where_sql_part.strip()} {order_by_sql_part.strip()} {limit_sql_part.strip()} {offset_sql_part.strip()};".replace("  ", " ").strip()
    
    actual_params_tuple = tuple(final_params_list)
    logger.debug(f"REPORTS_SPECIFIC - Query get_rateio_rzk_data: [{paginated_query}], Params (tupla): [{actual_params_tuple}]")
    try: 
        return execute_query(paginated_query, actual_params_tuple) or []
    except Exception as e: 
        logger.error(f"Erro get_rateio_rzk_data (display): {e}", exc_info=True)
        return []

def count_rateio_rzk() -> int:
    """Conta total para display Rateio RZK (Base Enviada)."""
    where_clauses = ["c.fornecedora = 'RZK'", "c.rateio = 'S'", "(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    where_sql_part = f"WHERE {' AND '.join(where_clauses)}"
    count_query_sql = f'SELECT COUNT(c.idcliente) FROM public."CLIENTES" c {where_sql_part.strip()};'.replace("  ", " ").strip()
    logger.debug(f"REPORTS_SPECIFIC - Query count_rateio_rzk: [{count_query_sql}], Params (tupla): [()]")
    try: 
        result = execute_query(count_query_sql, (), fetch_one=True) # Passar tupla vazia
        return result[0] if result and result[0] is not None else 0
    except Exception as e: 
        logger.error(f"Erro count_rateio_rzk (display): {e}", exc_info=True)
        return 0

# --- INÍCIO FUNÇÕES PARA RELATÓRIO 'Recebíveis Clientes' ---
def _get_recebiveis_clientes_fields() -> List[str]:
    """Retorna a lista de campos SQL para o relatório Recebíveis Clientes."""
    # (código original da função mantido)
    return [
        "rcb.idrcb", "c.idcliente AS codigo_cliente", "c.nome AS cliente_nome", "rcb.numinstalacao",
        "rcb.valorseria", "rcb.valorapagar", "rcb.valorcomcashback",
        "TO_CHAR(rcb.mesreferencia, 'MM/YYYY') AS data_referencia",
        "TO_CHAR(rcb.dtvencimento, 'DD/MM/YYYY') AS data_vencimento",
        "TO_CHAR(rcb.dtpagamento, 'DD/MM/YYYY') AS data_pagamento",
        "TO_CHAR(rcb.cdatavencoriginal, 'DD/MM/YYYY') AS data_vencimento_original",
        "c.celular", "c.email", "c.status_financeiro AS status_financeiro_cliente", "c.numcliente",
        "c.idconsultor AS id_licenciado", "co.nome AS nome_licenciado", "co.celular AS celular_licenciado",
        """CASE
            WHEN rcb.dtpagamento IS NOT NULL THEN 'PAGO'
            WHEN rcb.dtvencimento >= CURRENT_DATE THEN 'A RECEBER'
            ELSE 'VENCIDO'
        END AS status_calculado""",
        "rcb.urldemonstrativo", "rcb.urlboleto", "rcb.qrcode", "rcb.urlcontacemig",
        "rcb.nvalordistribuidora AS valor_distribuidora", "rcb.codigobarra", "c.ufconsumo",
        "c.fornecedora AS fornecedora_cliente", "c.concessionaria", "c.cnpj",
        'c."cpf/cnpj" AS cpf_cnpj_cliente', "rcb.nrodocumento", "rcb.idcomerc", "rcb.idbomfuturo",
        "rcb.energiainjetada", "rcb.energiacompensada", "rcb.energiaacumulada",
        "rcb.energiaajuste", "rcb.energiafaturamento", "c.desconto_cliente",
        """COUNT(rcb.idrcb) OVER (PARTITION BY c.idcliente) AS qtd_rcb_cliente"""
    ]

def get_recebiveis_clientes_data(offset: int = 0, limit: Optional[int] = None, fornecedora: Optional[str] = None) -> List[tuple]:
    """Busca os dados paginados para o relatório 'Recebíveis Clientes'."""
    campos = _get_recebiveis_clientes_fields()
    select_clause = f"SELECT {', '.join(campos)}"
    from_clause = 'FROM public."RCB_CLIENTES" rcb'
    join_clause = """
        LEFT JOIN public."CLIENTES" c ON rcb.numinstalacao = c.numinstalacao
        LEFT JOIN public."CONSULTOR" co ON c.idconsultor = co.idconsultor
    """
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    
    params_for_where = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s")
        params_for_where.append(fornecedora)
        
    where_sql_part = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    order_by_sql_part = "ORDER BY rcb.idrcb"
    
    final_params_list = list(params_for_where)

    limit_sql_part = ""
    if limit is not None:
        limit_sql_part = "LIMIT %s"
        final_params_list.append(limit)

    offset_sql_part = ""
    if offset > 0:
        offset_sql_part = "OFFSET %s"
        final_params_list.append(offset)

    paginated_query = f"{select_clause.strip()} {from_clause.strip()} {join_clause.strip()} {where_sql_part.strip()} {order_by_sql_part.strip()} {limit_sql_part.strip()} {offset_sql_part.strip()};".replace("  ", " ").strip()

    actual_params_tuple = tuple(final_params_list)
    logger.debug(f"REPORTS_SPECIFIC - Query get_recebiveis_clientes_data: [{paginated_query}], Params (tupla): [{actual_params_tuple}]")
    try: 
        return execute_query(paginated_query, actual_params_tuple) or []
    except Exception as e: 
        logger.error(f"Erro get_recebiveis_clientes_data: {e}", exc_info=True)
        return []

def count_recebiveis_clientes(fornecedora: Optional[str] = None) -> int:
    """Conta o total de registros para o relatório 'Recebíveis Clientes'."""
    from_clause = 'FROM public."RCB_CLIENTES" rcb'
    join_clause = """
        LEFT JOIN public."CLIENTES" c ON rcb.numinstalacao = c.numinstalacao
        LEFT JOIN public."CONSULTOR" co ON c.idconsultor = co.idconsultor
    """
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    params_list = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s")
        params_list.append(fornecedora)
        
    where_sql_part = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    count_query_sql = f"SELECT COUNT(rcb.idrcb) {from_clause.strip()} {join_clause.strip()} {where_sql_part.strip()};".replace("  ", " ").strip()
    
    actual_params_tuple = tuple(params_list)
    logger.debug(f"REPORTS_SPECIFIC - Query count_recebiveis_clientes: [{count_query_sql}], Params (tupla): [{actual_params_tuple}]")
    try:
        result = execute_query(count_query_sql, actual_params_tuple, fetch_one=True)
        return result[0] if result and result[0] is not None else 0
    except Exception as e: 
        logger.error(f"Erro count_recebiveis_clientes: {e}", exc_info=True)
        return 0
# --- FIM FUNÇÕES RECEBÍVEIS CLIENTES ---

# --- FUNÇÕES PARA RELATÓRIO 'Tempo até Graduação' (ATUALIZADAS) ---

def get_graduacao_licenciado_data(
    offset: int = 0, 
    limit: Optional[int] = None, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> List[tuple]:
    """
    Busca os dados para 'Tempo até Graduação', com filtros de data opcionais.
    """
    base_query = """
        SELECT
            c.idconsultor, c.nome,
            TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo_formatada,
            TO_CHAR(cp.dtgraduacao, 'DD/MM/YYYY') AS data_graduacao_formatada,
            (cp.dtgraduacao - c.data_ativo) AS dias_para_graduacao
        FROM public."CONSULTOR" c
        JOIN public."CONTROLE_PRO" cp ON c.idconsultor = cp.idconsultor
    """
    
    where_clauses = [
        "c.data_ativo IS NOT NULL",
        "cp.dtgraduacao IS NOT NULL",
        "cp.dtgraduacao >= c.data_ativo"
    ]
    params = []

    if start_date:
        where_clauses.append("c.data_ativo >= %s")
        params.append(start_date)
    
    if end_date:
        where_clauses.append("c.data_ativo <= %s")
        params.append(end_date)

    query = base_query + " WHERE " + " AND ".join(where_clauses) + " ORDER BY dias_para_graduacao ASC"
    
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)
    if offset > 0:
        query += " OFFSET %s"
        params.append(offset)
    
    query += ";"
    
    logger.debug(f"REPORTS_SPECIFIC - Query get_graduacao_licenciado_data: [{query}], Params: {params}")
    try:
        return execute_query(query, tuple(params)) or []
    except Exception as e:
        logger.error(f"Erro em get_graduacao_licenciado_data: {e}", exc_info=True)
        return []

def count_graduacao_licenciado(start_date: Optional[str] = None, end_date: Optional[str] = None) -> int:
    """
    Conta o total de registros para 'Tempo até Graduação', com filtros de data opcionais.
    """
    base_query = """
        SELECT COUNT(*)
        FROM public."CONSULTOR" c
        JOIN public."CONTROLE_PRO" cp ON c.idconsultor = cp.idconsultor
    """
    
    where_clauses = [
        "c.data_ativo IS NOT NULL",
        "cp.dtgraduacao IS NOT NULL",
        "cp.dtgraduacao >= c.data_ativo"
    ]
    params = []

    if start_date:
        where_clauses.append("c.data_ativo >= %s")
        params.append(start_date)
    
    if end_date:
        where_clauses.append("c.data_ativo <= %s")
        params.append(end_date)
        
    query = base_query + " WHERE " + " AND ".join(where_clauses) + ";"

    logger.debug(f"REPORTS_SPECIFIC - Query count_graduacao_licenciado: [{query}], Params: {params}")
    try:
        result = execute_query(query, tuple(params), fetch_one=True)
        return result[0] if result and result[0] is not None else 0
    except Exception as e:
        logger.error(f"Erro em count_graduacao_licenciado: {e}", exc_info=True)
        return 0