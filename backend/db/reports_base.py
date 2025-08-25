# backend/db/reports_base.py
import logging
from typing import List, Tuple, Optional, Any, Dict
from .executor import execute_query, execute_query_one # Importa as duas funções

logger = logging.getLogger(__name__)

# --- Funções de Suporte para Relatórios ---
def get_fornecedoras() -> List[str]:
    """Busca a lista de fornecedoras para o filtro de relatórios."""
    query = "SELECT DISTINCT fornecedora FROM public.\"CLIENTES\" WHERE fornecedora IS NOT NULL ORDER BY 1;"
    try:
        results = execute_query(query)
        # CORREÇÃO: Acessando o resultado por chave
        return [row['fornecedora'] for row in results] if results else []
    except Exception as e:
        logger.error(f"Erro ao buscar fornecedoras: {e}", exc_info=True)
        return []

def get_headers(report_type: str) -> List[str]:
    """Retorna o cabeçalho das colunas para o tipo de relatório."""
    if report_type == 'base_clientes':
        return ['Código', 'Nome Cliente', 'CPF/CNPJ', 'Endereço', 'Consumo (kWh)', 'Fornecedora', 'Status']
    # Adicione outros tipos de relatórios aqui
    if report_type == 'boletos_por_cliente':
        # Retorna a lista de headers definida em reports_boletos.py
        from .reports_boletos import final_columns_order
        return [col.replace('_', ' ').title() for col in final_columns_order]
    return []

# --- Funções Específicas para Bases Rateio (Geral) ---
def get_base_nova_ids(fornecedora: Optional[str] = None) -> List[int]:
    """Busca IDs para 'Base Nova' do Rateio Geral."""
    query_base = """
        SELECT DISTINCT cc.idcliente FROM public."CLIENTES_CONTRATOS" cc
        INNER JOIN public."CLIENTES_CONTRATOS_SIGNER" ccs ON cc.idcliente_contrato = ccs.idcliente_contrato
        INNER JOIN public."CLIENTES" c ON cc.idcliente = c.idcliente """
    group_by = " GROUP BY cc.idcliente_contrato, cc.idcliente HAVING bool_and(ccs.signature_at IS NOT NULL) "
    where_clauses = [
        "cc.type_document = 'procuracao_igreen'", "upper(cc.status) = 'ATIVO'",
        "c.data_ativo IS NOT NULL", "c.status IS NULL", "c.validadosucesso = 'S'",
        "c.rateio = 'N'", " (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) ",
        " NOT EXISTS ( SELECT 1 FROM public.\"DEVOLUTIVAS\" d WHERE d.idcliente = c.idcliente ) "
    ]
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    full_query = query_base + " WHERE " + " AND ".join(where_clauses) + group_by + ";"
    try:
        results = execute_query(full_query, tuple(params))
        # CORREÇÃO: Acessando o resultado por chave
        return [r['idcliente'] for r in results] if results else []
    except Exception as e: logger.error(f"Erro get_base_nova_ids: {e}", exc_info=True); return []

def get_base_enviada_ids(fornecedora: Optional[str] = None) -> List[int]:
    """Busca IDs para 'Base Enviada' do Rateio Geral."""
    query_base = 'SELECT c.idcliente FROM public."CLIENTES" c'
    where_clauses = [ "c.rateio = 'S'", " (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) "]
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    full_query = query_base + " WHERE " + " AND ".join(where_clauses) + ";"
    try:
        results = execute_query(full_query, tuple(params))
        # CORREÇÃO: Acessando o resultado por chave
        return [r['idcliente'] for r in results] if results else []
    except Exception as e: logger.error(f"Erro get_base_enviada_ids: {e}", exc_info=True); return []

def _get_query_fields(report_type: str) -> List[str]:
    """Retorna a lista de campos SQL BASE para Base Clientes ou Rateio Geral."""
    report_type = report_type.lower()
    
    # Lista completa de campos para Base Clientes
    base_clientes_fields = [
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
    
    # Campos Rateio Geral
    base_rateio_fields = [
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
    
    # Campos para Rateio RZK
    rateio_rzk_fields = [
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
    
    # Campos para Clientes por Licenciado
    clientes_por_licenciado_fields = [
        "c.idconsultor", "c.nome", "c.cpf", "c.email", "c.uf", "quantidade_clientes_ativos"
    ]
    
    # Campos para Boletos por Cliente
    boletos_por_cliente_fields = [
        "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao",
        "c.fornecedora", "data_ativo", "quantidade_registros_rcb"
    ]

    if report_type == "base_clientes":
        return base_clientes_fields
    elif report_type == "rateio":
        return base_rateio_fields
    elif report_type == "rateio_rzk":
        return rateio_rzk_fields
    elif report_type == "clientes_por_licenciado":
        return clientes_por_licenciado_fields
    elif report_type == "boletos_por_cliente":
        return boletos_por_cliente_fields
    else:
        logger.warning(f"_get_query_fields: Tipo '{report_type}' não mapeado para campos genéricos.")
        return []

def get_client_details_by_ids(report_type: str, client_ids: List[int], batch_size: int = 1000) -> List[tuple]:
    """Busca detalhes base para Rateio Geral ou Base Clientes por IDs."""
    if not client_ids: return []
    all_details = []
    try:
        campos = _get_query_fields(report_type)
        if not campos: logger.error(f"Campos não definidos para get_client_details_by_ids tipo: {report_type}"); return []
        
        select = f"SELECT {', '.join(campos)}"
        
        # Base FROM e JOINs condicionais
        from_clause = 'FROM public."CLIENTES" c'
        join_clauses = []
        
        # Adiciona JOIN com CONSULTOR apenas se necessário
        if any(f.startswith("co.") for f in campos):
            join_clauses.append('LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor')
        
        # Monta a parte de JOIN da query
        join_str = " ".join(join_clauses)
        
        where = "WHERE c.idcliente = ANY(%s)"
        # Adiciona filtro de origem se não for um relatório específico que não o necessite
        if report_type in ["base_clientes", "rateio"]:
             where += " AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"

        order = "ORDER BY c.idcliente"
        query = f"{select} {from_clause} {join_str} {where} {order};"

        for i in range(0, len(client_ids), batch_size):
            batch_ids = client_ids[i:i + batch_size]
            params = (batch_ids,)
            batch_results = execute_query(query, params)
            if batch_results:
                all_details.extend(batch_results)
        return all_details
    except Exception as e:
        logger.error(f"Erro get_client_details_by_ids ({report_type}): {e}", exc_info=True)
        return []

def build_query(report_type: str, fornecedora: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> Tuple[str, tuple]:
     """Constrói a query paginada para relatórios Base Clientes e Rateio Geral."""
     if report_type not in ["base_clientes", "rateio"]: raise ValueError(f"build_query não adequado para '{report_type}'.")
     campos = _get_query_fields(report_type);
     if not campos: raise ValueError(f"Campos não definidos para '{report_type}'")
     select = f"SELECT {', '.join(campos)}"; from_ = 'FROM public."CLIENTES" c'
     needs_consultor_join = any(f.startswith("co.") for f in campos)
     join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor' if needs_consultor_join else ""
     where_clauses = [" (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) "]; params = []
     if fornecedora and fornecedora.lower() != "consolidado": where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
     where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""; order = "ORDER BY c.idcliente"; limit_clause = ""; offset_clause = ""
     if limit is not None: limit_clause = f"LIMIT %s"; params.append(limit)
     if offset > 0: offset_clause = f"OFFSET %s"; params.append(offset)
     query = f"{select} {from_}{join} {where} {order} {limit_clause} {offset_clause};"
     return query, tuple(params)

def count_query(report_type: str, fornecedora: Optional[str] = None) -> Tuple[str, tuple]:
     """Constrói a query de contagem para relatórios Base Clientes e Rateio Geral."""
     if report_type not in ["base_clientes", "rateio"]: raise ValueError(f"count_query não adequado para '{report_type}'.")
     from_ = 'FROM public."CLIENTES" c'; where_clauses = [" (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) "]; params = []
     if fornecedora and fornecedora.lower() != "consolidado": where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
     where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""; query = f"SELECT COUNT(c.idcliente) AS count {from_} {where};"
     return query, tuple(params)