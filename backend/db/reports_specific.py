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
    params = []; limit_clause = ""; offset_clause = ""
    if limit is not None: limit_clause = "LIMIT %s"; params.append(limit)
    if offset > 0: offset_clause = "OFFSET %s"; params.append(offset)
    paginated_query = f"{base_query} {limit_clause} {offset_clause};"
    try: return execute_query(paginated_query, tuple(params)) or []
    except Exception as e: logger.error(f"Erro get_clientes_por_licenciado_data: {e}", exc_info=True); return []

def count_clientes_por_licenciado() -> int:
    """Conta o total de consultores com clientes ativos."""
    count_query_sql = """
        SELECT COUNT(DISTINCT c.idconsultor) FROM public."CONSULTOR" c
        INNER JOIN public."CLIENTES" cl ON c.idconsultor = cl.idconsultor
        WHERE cl.data_ativo IS NOT NULL AND (cl.origem IS NULL OR cl.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')); """
    try: result = execute_query(count_query_sql, fetch_one=True); return result[0] if result else 0
    except Exception as e: logger.error(f"Erro count_clientes_por_licenciado: {e}", exc_info=True); return 0

# --- FUNÇÕES PARA RELATÓRIO 'Quantidade de Boletos por Cliente' ---
def get_boletos_por_cliente_data(offset: int = 0, limit: Optional[int] = None, fornecedora: Optional[str] = None) -> List[tuple]:
    """Busca os dados para 'Quantidade de Boletos por Cliente'."""
    base_query = """
        SELECT c.idcliente, c.nome, c.numinstalacao, c.celular, c.cidade,
               CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN '' ELSE (c.uf || '-' || c.concessionaria) END AS regiao,
               c.fornecedora, TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo,
               CASE
                   WHEN c.data_ativo IS NOT NULL THEN EXTRACT(DAY FROM (NOW() - c.data_ativo))::INTEGER
                   ELSE NULL
               END AS dias_ativo,
               COUNT(rcb.numinstalacao) AS quantidade_registros_rcb
        FROM public."CLIENTES" c
        LEFT JOIN public."RCB_CLIENTES" rcb ON c.numinstalacao = rcb.numinstalacao """ # Assume que RCB_CLIENTES também tem numinstalacao para contar
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    group_by = " GROUP BY c.idcliente, c.nome, c.numinstalacao, c.celular, c.cidade, regiao, c.fornecedora, data_ativo, dias_ativo "
    order_by = "ORDER BY c.idcliente"
    limit_clause = "LIMIT %s" if limit is not None else ""; offset_clause = ""
    if limit is not None: params.append(limit)
    if offset > 0: offset_clause = "OFFSET %s"; params.append(offset)
    paginated_query = f"{base_query} {where} {group_by} {order_by} {limit_clause} {offset_clause};"
    try: return execute_query(paginated_query, tuple(params)) or []
    except Exception as e: logger.error(f"Erro get_boletos_por_cliente_data: {e}", exc_info=True); return []

def count_boletos_por_cliente(fornecedora: Optional[str] = None) -> int:
    """Conta o total de clientes para 'Boletos por Cliente'."""
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    count_query_sql = f'SELECT COUNT(DISTINCT c.idcliente) FROM public."CLIENTES" c {where};'
    try: result = execute_query(count_query_sql, tuple(params), fetch_one=True); return result[0] if result else 0
    except Exception as e: logger.error(f"Erro count_boletos_por_cliente: {e}", exc_info=True); return 0
# --- FIM FUNÇÕES BOLETOS POR CLIENTE ---


# --- FUNÇÕES PARA RATEIO RZK ---
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
    full_query = query_base + " WHERE " + " AND ".join(where_clauses) + group_by + ";"
    try: results = execute_query(full_query); return [r[0] for r in results] if results else []
    except Exception as e: logger.error(f"Erro get_rateio_rzk_base_nova_ids: {e}"); return []

def get_rateio_rzk_base_enviada_ids() -> List[int]:
    """Busca IDs para 'Base Enviada' do Rateio RZK."""
    query_base = 'SELECT c.idcliente FROM public."CLIENTES" c'
    where_clauses = [ "c.fornecedora = 'RZK'", "c.rateio = 'S'", " (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) " ]
    full_query = query_base + " WHERE " + " AND ".join(where_clauses) + ";"
    try: results = execute_query(full_query); return [r[0] for r in results] if results else []
    except Exception as e: logger.error(f"Erro get_rateio_rzk_base_enviada_ids: {e}"); return []

def _get_rateio_rzk_fields() -> List[str]:
    """Retorna a lista de campos SQL EXATOS para Rateio RZK."""
    return [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao", "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "c.consumomedio", "c.status AS devolutiva", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "co.nome AS licenciado", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.keycontrato AS chave_contrato", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "c.nome AS nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ]

def get_rateio_rzk_client_details_by_ids(client_ids: List[int], batch_size: int = 1000) -> List[tuple]:
    """Busca detalhes completos para Rateio RZK por lista de IDs."""
    if not client_ids: return []
    all_details = []
    try:
        campos = _get_rateio_rzk_fields()
        if not campos: logger.error("Falha campos RZK details"); return []
        select = f"SELECT {', '.join(campos)}"; from_ = 'FROM public."CLIENTES" c'
        join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor'
        where = "WHERE c.idcliente = ANY(%s) AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"
        order = "ORDER BY c.idcliente"; query = f"{select} {from_}{join} {where} {order};"
        for i in range(0, len(client_ids), batch_size):
            batch_ids = client_ids[i:i + batch_size]; params = (batch_ids,)
            batch_results = execute_query(query, params)
            if batch_results: all_details.extend(batch_results)
        return all_details
    except Exception as e: logger.error(f"Erro get_rateio_rzk_client_details_by_ids: {e}", exc_info=True); return []

def get_rateio_rzk_data(offset: int = 0, limit: Optional[int] = None) -> List[tuple]:
    """Busca dados paginados para display Rateio RZK (Base Enviada)."""
    campos_rzk = _get_rateio_rzk_fields()
    select = f"SELECT {', '.join(campos_rzk)}"; from_ = 'FROM public."CLIENTES" c'
    join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor'
    where_clauses = ["c.fornecedora = 'RZK'", "c.rateio = 'S'", "(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    where = f"WHERE {' AND '.join(where_clauses)}"; order = "ORDER BY c.idcliente"; params = []
    limit_clause = "LIMIT %s" if limit is not None else ""; offset_clause = ""
    if limit is not None: params.append(limit)
    if offset > 0: offset_clause = "OFFSET %s"; params.append(offset)
    paginated_query = f"{select} {from_}{join} {where} {order} {limit_clause} {offset_clause};"
    try: return execute_query(paginated_query, tuple(params)) or []
    except Exception as e: logger.error(f"Erro get_rateio_rzk_data (display): {e}", exc_info=True); return []

def count_rateio_rzk() -> int:
    """Conta total para display Rateio RZK (Base Enviada)."""
    where_clauses = ["c.fornecedora = 'RZK'", "c.rateio = 'S'", "(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    where = f"WHERE {' AND '.join(where_clauses)}"; count_query_sql = f'SELECT COUNT(c.idcliente) FROM public."CLIENTES" c {where};'
    try: result = execute_query(count_query_sql, fetch_one=True); return result[0] if result else 0
    except Exception as e: logger.error(f"Erro count_rateio_rzk (display): {e}", exc_info=True); return 0

# --- FIM FUNÇÕES RZK ---

# --- INÍCIO FUNÇÕES PARA RELATÓRIO 'Recebíveis Clientes' ---
def _get_recebiveis_clientes_fields() -> List[str]:
    """Retorna a lista de campos SQL para o relatório Recebíveis Clientes."""
    # (Mesma função que estava no database.py original)
    return [
        "rcb.idrcb",                            # 1
        "c.idcliente AS codigo_cliente",        # 2
        "c.nome AS cliente_nome",               # 3
        "rcb.numinstalacao",                    # 4
        "rcb.valorseria",                       # 5
        "rcb.valorapagar",                      # 6
        "rcb.valorcomcashback",                 # 7
        "TO_CHAR(rcb.mesreferencia, 'MM/YYYY') AS data_referencia", # 8
        "TO_CHAR(rcb.dtvencimento, 'DD/MM/YYYY') AS data_vencimento", # 9
        "TO_CHAR(rcb.dtpagamento, 'DD/MM/YYYY') AS data_pagamento", # 10
        "TO_CHAR(rcb.cdatavencoriginal, 'DD/MM/YYYY') AS data_vencimento_original", # 11
        "c.celular",                            # 12
        "c.email",                              # 13
        "c.status_financeiro AS status_financeiro_cliente", # 14
        "c.numcliente",                         # 15
        "c.idconsultor AS id_licenciado",       # 16
        "co.nome AS nome_licenciado",           # 17
        "co.celular AS celular_licenciado",     # 18
        """CASE
            WHEN rcb.dtpagamento IS NOT NULL THEN 'PAGO'
            WHEN rcb.dtvencimento >= CURRENT_DATE THEN 'A RECEBER'
            ELSE 'VENCIDO'
        END AS status_calculado""",             # 19
        "rcb.urldemonstrativo",                 # 20
        "rcb.urlboleto",                        # 21
        "rcb.qrcode",                           # 22
        "rcb.urlcontacemig",                    # 23
        "rcb.nvalordistribuidora AS valor_distribuidora", # 24
        "rcb.codigobarra",                      # 25
        "c.ufconsumo",                          # 26
        "c.fornecedora AS fornecedora_cliente", # 27 (Seleciona de CLIENTES)
        "c.concessionaria",                     # 28
        "c.cnpj",                               # 29
        'c."cpf/cnpj" AS cpf_cnpj_cliente',     # 30
        "rcb.nrodocumento",                     # 31
        "rcb.idcomerc",                         # 32
        "rcb.idbomfuturo",                      # 33
        "rcb.energiainjetada",                  # 34
        "rcb.energiacompensada",                # 35
        "rcb.energiaacumulada",                 # 36
        "rcb.energiaajuste",                    # 37
        "rcb.energiafaturamento",               # 38
        "c.desconto_cliente",                   # 39
        """COUNT(rcb.idrcb) OVER (PARTITION BY c.idcliente) AS qtd_rcb_cliente""" # 40
    ]

def get_recebiveis_clientes_data(offset: int = 0, limit: Optional[int] = None, fornecedora: Optional[str] = None) -> List[tuple]:
    """Busca os dados paginados para o relatório 'Recebíveis Clientes'."""
    # (Mesma função que estava no database.py original, usa _get_recebiveis_clientes_fields)
    campos = _get_recebiveis_clientes_fields()
    select_clause = f"SELECT {', '.join(campos)}"
    from_clause = 'FROM public."RCB_CLIENTES" rcb'
    join_clause = """
        LEFT JOIN public."CLIENTES" c ON rcb.numinstalacao = c.numinstalacao
        LEFT JOIN public."CONSULTOR" co ON c.idconsultor = co.idconsultor
    """
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s")
        params.append(fornecedora)
    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    order_by = "ORDER BY rcb.idrcb"
    limit_clause = "LIMIT %s" if limit is not None else ""
    offset_clause = "OFFSET %s" if offset > 0 else ""
    if limit is not None: params.append(limit)
    if offset > 0: params.append(offset)
    paginated_query = f"{select_clause} {from_clause} {join_clause} {where_clause} {order_by} {limit_clause} {offset_clause};"
    logger.debug(f"Executando query para Recebíveis Clientes (Pag): {paginated_query} com params: {params}")
    try: return execute_query(paginated_query, tuple(params)) or []
    except Exception as e: logger.error(f"Erro get_recebiveis_clientes_data: {e}", exc_info=True); return []

def count_recebiveis_clientes(fornecedora: Optional[str] = None) -> int:
    """Conta o total de registros para o relatório 'Recebíveis Clientes'."""
    # (Mesma função que estava no database.py original)
    from_clause = 'FROM public."RCB_CLIENTES" rcb'
    join_clause = """
        LEFT JOIN public."CLIENTES" c ON rcb.numinstalacao = c.numinstalacao
        LEFT JOIN public."CONSULTOR" co ON c.idconsultor = co.idconsultor
    """
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s")
        params.append(fornecedora)
    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    count_query_sql = f"SELECT COUNT(rcb.idrcb) {from_clause} {join_clause} {where_clause};"
    logger.debug(f"Executando query de contagem para Recebíveis Clientes: {count_query_sql} com params: {params}")
    try:
        result = execute_query(count_query_sql, tuple(params), fetch_one=True)
        return result[0] if result else 0
    except Exception as e: logger.error(f"Erro count_recebiveis_clientes: {e}", exc_info=True); return 0

# --- FIM FUNÇÕES RECEBÍVEIS CLIENTES ---