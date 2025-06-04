import logging
from typing import List, Tuple, Optional, Dict, Any
from enum import Enum
from .executor import execute_query # Import local

logger = logging.getLogger(__name__)

# --- Constantes ---
CONSOLIDADO_PROVIDER = "consolidado"
TABLE_CLIENTES = 'public."CLIENTES"'
TABLE_CLIENTES_CONTRATOS = 'public."CLIENTES_CONTRATOS"'
TABLE_CLIENTES_CONTRATOS_SIGNER = 'public."CLIENTES_CONTRATOS_SIGNER"'
TABLE_DEVOLUTIVAS = 'public."DEVOLUTIVAS"'
TABLE_CONSULTOR = 'public."CONSULTOR"'

# Cláusula de filtro de origem comum, com parênteses para garantir a precedência correta
ORIGEM_FILTER_CLAUSE = "(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"

class ReportType(Enum):
    BASE_CLIENTES = "base_clientes"
    RATEIO = "rateio"
    RATEIO_RZK = "rateio_rzk"
    CLIENTES_POR_LICENCIADO = "clientes_por_licenciado"
    BOLETOS_POR_CLIENTE = "boletos_por_cliente"

# --- Definições de Campos para Relatórios ---
# Movido para constantes globais para melhor organização e reutilização
BASE_CLIENTES_FIELDS: List[str] = [
    "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade",
    "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao",
    "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "(COALESCE(c.qtdeassinatura, 0)::text || '/4') AS qtdeassinatura",
    "c.consumomedio", "c.status", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad",
    "c.\"cpf/cnpj\"", "c.numcliente", "TO_CHAR(c.dtultalteracao, 'DD/MM/YYYY') AS dtultalteracao",
    "c.celular_2", "c.email", "c.rg", "c.emissor", "TO_CHAR(c.datainjecao, 'DD/MM/YYYY') AS datainjecao",
    "c.idconsultor", "co.nome AS consultor_nome", "co.celular AS consultor_celular", "c.cep",
    "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia",
    "c.ufconsumo", "c.classificacao", "c.keycontrato", "c.keysigner", "c.leadidsolatio", "c.indcli",
    "c.enviadocomerc", "c.obs", "c.posvenda", "c.retido", "c.contrato_verificado", "c.rateio",
    "c.validadosucesso", "CASE WHEN c.validadosucesso = 'S' THEN 'Aprovado' ELSE 'Rejeitado' END AS status_sucesso",
    "c.documentos_enviados", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj",
    "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial",
    "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.codigo",
    "c.elegibilidade", "c.idplanopj", "TO_CHAR(c.dtcancelado, 'DD/MM/YYYY') AS dtcancelado",
    "TO_CHAR(c.data_ativo_original, 'DD/MM/YYYY') AS data_ativo_original", "c.fornecedora",
    "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc", "c.origem",
    "c.cm_tipo_pagamento", "c.status_financeiro", "c.logindistribuidora", "c.senhadistribuidora",
    "c.nacionalidade", "c.profissao", "c.estadocivil", "c.obs_compartilhada", "c.linkassinatura1"
]

BASE_RATEIO_FIELDS: List[str] = [
    "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade",
    "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao",
    "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "c.consumomedio",
    "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad", "c.\"cpf/cnpj\"", "c.numcliente",
    "c.email", "c.rg", "c.emissor", "c.cep", "co.nome AS consultor_nome",
    "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao",
    "c.fantasia", "c.ufconsumo", "c.classificacao", "c.link_documento",
    "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1",
    "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial",
    "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf",
    "c.fornecedora", "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc",
    "c.logindistribuidora", "c.senhadistribuidora", "c.nome AS nome_cliente_rateio",
    "c.nacionalidade", "c.profissao", "c.estadocivil"
]

RATEIO_RZK_FIELDS: List[str] = [
    "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade",
    "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao",
    "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "c.consumomedio",
    "c.status AS devolutiva", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad",
    "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor",
    "co.nome AS licenciado", "c.cep", "c.endereco", "c.numero", "c.bairro",
    "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo",
    "c.classificacao", "c.keycontrato AS chave_contrato", "c.link_documento",
    "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1",
    "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial",
    "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf",
    "c.fornecedora", "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc",
    "c.logindistribuidora", "c.senhadistribuidora", "c.nome AS nome_cliente_rateio",
    "c.nacionalidade", "c.profissao", "c.estadocivil"
]

CLIENTES_POR_LICENCIADO_FIELDS: List[str] = [
    "c.idconsultor", "c.nome", "c.cpf", "c.email", "c.uf", "quantidade_clientes_ativos"
]

BOLETOS_POR_CLIENTE_FIELDS: List[str] = [
    "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao",
    "c.fornecedora", "data_ativo", "quantidade_registros_rcb"
]

REPORT_TYPE_TO_FIELDS_MAP: Dict[ReportType, List[str]] = {
    ReportType.BASE_CLIENTES: BASE_CLIENTES_FIELDS,
    ReportType.RATEIO: BASE_RATEIO_FIELDS,
    ReportType.RATEIO_RZK: RATEIO_RZK_FIELDS,
    ReportType.CLIENTES_POR_LICENCIADO: CLIENTES_POR_LICENCIADO_FIELDS,
    ReportType.BOLETOS_POR_CLIENTE: BOLETOS_POR_CLIENTE_FIELDS,
}

# --- Funções Auxiliares ---
def _execute_query_and_fetch_ids(query: str, params: tuple, error_message_prefix: str) -> List[int]:
    """Executa uma query que retorna IDs, loga erros e retorna uma lista de inteiros."""
    try:
        results = execute_query(query, params)
        return [r[0] for r in results] if results else []
    except Exception as e:
        logger.error(f"{error_message_prefix}: {e}", exc_info=True)
        return []

def _build_common_filters(fornecedora: Optional[str] = None, use_origem_filter: bool = True) -> Tuple[List[str], List[Any]]:
    """Constrói cláusulas WHERE comuns e seus parâmetros."""
    where_clauses: List[str] = []
    params_list: List[Any] = []

    if use_origem_filter:
        where_clauses.append(ORIGEM_FILTER_CLAUSE)

    if fornecedora and fornecedora.lower() != CONSOLIDADO_PROVIDER:
        where_clauses.append("c.fornecedora = %s")
        params_list.append(fornecedora)
    return where_clauses, params_list

# --- Funções Específicas para Bases Rateio (Geral) ---
def get_base_nova_ids(fornecedora: Optional[str] = None) -> List[int]:
    """Busca IDs para 'Base Nova' do Rateio Geral."""
    query_base = f"""
        SELECT DISTINCT cc.idcliente
        FROM {TABLE_CLIENTES_CONTRATOS} cc
        INNER JOIN {TABLE_CLIENTES_CONTRATOS_SIGNER} ccs ON cc.idcliente_contrato = ccs.idcliente_contrato
        INNER JOIN {TABLE_CLIENTES} c ON cc.idcliente = c.idcliente
    """
    specific_where_clauses = [
        "cc.type_document = 'procuracao_igreen'",
        "UPPER(cc.status) = 'ATIVO'",
        "c.data_ativo IS NOT NULL",
        "c.status IS NULL",
        "c.validadosucesso = 'S'",
        "c.rateio = 'N'",
        f"NOT EXISTS (SELECT 1 FROM {TABLE_DEVOLUTIVAS} d WHERE d.idcliente = c.idcliente)"
    ]
    common_where_clauses, params_list = _build_common_filters(fornecedora)
    
    all_where_clauses = specific_where_clauses + common_where_clauses
    
    group_by_having = "GROUP BY cc.idcliente_contrato, cc.idcliente HAVING bool_and(ccs.signature_at IS NOT NULL)"
    
    full_query = f"{query_base} WHERE {' AND '.join(all_where_clauses)} {group_by_having};"
    
    return _execute_query_and_fetch_ids(full_query, tuple(params_list), "Erro get_base_nova_ids")

def get_base_enviada_ids(fornecedora: Optional[str] = None) -> List[int]:
    """Busca IDs para 'Base Enviada' do Rateio Geral."""
    query_base = f"SELECT c.idcliente FROM {TABLE_CLIENTES} c"
    
    specific_where_clauses = ["c.rateio = 'S'"]
    common_where_clauses, params_list = _build_common_filters(fornecedora)

    all_where_clauses = specific_where_clauses + common_where_clauses

    full_query = f"{query_base} WHERE {' AND '.join(all_where_clauses)};"
    
    return _execute_query_and_fetch_ids(full_query, tuple(params_list), "Erro get_base_enviada_ids")


# --- Função para buscar detalhes completos por lista de IDs ---
def _get_query_fields(report_type_str: str) -> List[str]:
    """Retorna a lista de campos SQL com base no tipo de relatório string."""
    try:
        # Converte a string para o Enum para validação e para usar como chave no mapa
        report_type_enum = ReportType(report_type_str.lower()) 
        fields = REPORT_TYPE_TO_FIELDS_MAP.get(report_type_enum)
        if fields:
            return fields
        logger.warning(f"_get_query_fields: Campos não encontrados para o tipo '{report_type_str}'.")
        return []
    except ValueError: # Caso report_type_str não seja um membro válido de ReportType
        logger.warning(f"_get_query_fields: Tipo de relatório '{report_type_str}' inválido.")
        return []

def get_client_details_by_ids(report_type_str: str, client_ids: List[int], batch_size: int = 1000) -> List[tuple]:
    """Busca detalhes base para clientes por IDs, processando em lotes."""
    if not client_ids:
        return []

    all_details: List[tuple] = []
    campos = _get_query_fields(report_type_str)
    if not campos:
        logger.error(f"Campos não definidos para get_client_details_by_ids tipo: {report_type_str}")
        return []

    select_clause = f"SELECT {', '.join(campos)}"
    from_clause = f"FROM {TABLE_CLIENTES} c"
    
    join_clause = ""
    if any(field.startswith("co.") for field in campos): # Adiciona join se campos do consultor são necessários
        join_clause = f"LEFT JOIN {TABLE_CONSULTOR} co ON co.idconsultor = c.idconsultor"

    # Cláusula WHERE com filtro de origem e ANY para os IDs
    # O ORIGEM_FILTER_CLAUSE já tem parênteses, então é seguro concatenar com AND
    where_clause = f"WHERE c.idcliente = ANY(%s) AND {ORIGEM_FILTER_CLAUSE}"
    order_clause = "ORDER BY c.idcliente"
    
    base_query = f"{select_clause} {from_clause} {join_clause} {where_clause} {order_clause};"

    try:
        for i in range(0, len(client_ids), batch_size):
            batch_ids = client_ids[i:i + batch_size]
            # Psycopg2 espera uma lista/tupla para ANY(%s)
            # O parâmetro para ANY deve ser a lista diretamente, não uma tupla contendo a lista.
            # Ex: cursor.execute("... ANY(%s)", (batch_ids,))
            params_tuple = (batch_ids,) 
            batch_results = execute_query(base_query, params_tuple)
            if batch_results:
                all_details.extend(batch_results)
        return all_details
    except Exception as e:
        logger.error(f"Erro get_client_details_by_ids ({report_type_str}): {e}", exc_info=True)
        return []

# --- Funções de Construção de Query Genéricas ---
def _get_report_type_enum_or_raise(report_type_str: str, function_name: str) -> ReportType:
    """Converte string para ReportType Enum ou levanta ValueError."""
    try:
        return ReportType(report_type_str.lower())
    except ValueError:
        raise ValueError(f"{function_name} não adequado para o tipo de relatório '{report_type_str}'.")

def build_query(report_type_str: str, fornecedora: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> Tuple[str, tuple]:
    """Constrói a query paginada para relatórios Base Clientes e Rateio Geral."""
    report_type_enum = _get_report_type_enum_or_raise(report_type_str, "build_query")

    if report_type_enum not in [ReportType.BASE_CLIENTES, ReportType.RATEIO]:
        raise ValueError(f"build_query não adequado para o tipo de relatório '{report_type_str}'.")

    campos = _get_query_fields(report_type_str) # _get_query_fields espera string
    if not campos:
        raise ValueError(f"Campos não definidos para '{report_type_str}'")

    select_clause = f"SELECT {', '.join(campos)}"
    from_clause = f"FROM {TABLE_CLIENTES} c"
    
    join_clause = ""
    if any(field.startswith("co.") for field in campos):
        join_clause = f"LEFT JOIN {TABLE_CONSULTOR} co ON co.idconsultor = c.idconsultor"

    where_clauses, params_list = _build_common_filters(fornecedora)
    where_string = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    
    order_clause = "ORDER BY c.idcliente"
    limit_string = ""
    offset_string = ""

    if limit is not None:
        limit_string = "LIMIT %s"
        params_list.append(limit)
    if offset > 0:
        offset_string = "OFFSET %s"
        params_list.append(offset)
    
    # Garante que não haja espaços duplos e remove espaços no início/fim
    query_parts = [select_clause, from_clause, join_clause, where_string, order_clause, limit_string, offset_string]
    query = " ".join(part for part in query_parts if part) + ";"
    
    return query, tuple(params_list)

def count_query(report_type_str: str, fornecedora: Optional[str] = None) -> Tuple[str, tuple]:
    """Constrói a query de contagem para relatórios Base Clientes e Rateio Geral."""
    report_type_enum = _get_report_type_enum_or_raise(report_type_str, "count_query")

    if report_type_enum not in [ReportType.BASE_CLIENTES, ReportType.RATEIO]:
        raise ValueError(f"count_query não adequado para o tipo de relatório '{report_type_str}'.")

    from_clause = f"FROM {TABLE_CLIENTES} c"
    # JOIN não é necessário para contagem simples baseada apenas na tabela CLIENTES
    # Se a contagem dependesse de filtros na tabela CONSULTOR, o join seria necessário aqui também.

    where_clauses, params_list = _build_common_filters(fornecedora)
    where_string = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    
    query_parts = [f"SELECT COUNT(c.idcliente)", from_clause, where_string]
    query = " ".join(part for part in query_parts if part) + ";"

    return query, tuple(params_list)