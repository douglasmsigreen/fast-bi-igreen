# backend/db/reports_base.py
import logging
from typing import List, Tuple, Optional
from .executor import execute_query # Import local

logger = logging.getLogger(__name__)

# --- Funções Específicas para Bases Rateio (Geral) - SEM ALTERAÇÕES AQUI ---
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
    try: results = execute_query(full_query, tuple(params)); return [r[0] for r in results] if results else []
    except Exception as e: logger.error(f"Erro get_base_nova_ids: {e}"); return []

def get_base_enviada_ids(fornecedora: Optional[str] = None) -> List[int]:
    """Busca IDs para 'Base Enviada' do Rateio Geral."""
    query_base = 'SELECT c.idcliente FROM public."CLIENTES" c'
    where_clauses = [ "c.rateio = 'S'", " (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) "]
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    full_query = query_base + " WHERE " + " AND ".join(where_clauses) + ";"
    try: results = execute_query(full_query, tuple(params)); return [r[0] for r in results] if results else []
    except Exception as e: logger.error(f"Erro get_base_enviada_ids: {e}"); return []


# --- Função para buscar detalhes completos por lista de IDs (Base Clientes ou Rateio Geral) ---
def _get_query_fields(report_type: str) -> List[str]:
    """Retorna a lista de campos SQL BASE para Base Clientes ou Rateio Geral."""
    report_type = report_type.lower()
    
    ### ALTERAÇÃO INICIADA: Simplificado para retornar apenas '*' para base_clientes ###
    if report_type == "base_clientes":
        # Como vamos usar a view V_CUSTOMER, podemos selecionar todas as colunas diretamente.
        # A ordem será definida em `utils.py`.
        return ["*"] 
    ### FIM DA ALTERAÇÃO ###

    # Campos Rateio Geral (sem alteração)
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
    
    if report_type == "rateio":
        return base_rateio_fields
    else:
        logger.warning(f"_get_query_fields: Tipo '{report_type}' não mapeado para campos genéricos.")
        return []

def get_client_details_by_ids(report_type: str, client_ids: List[int], batch_size: int = 1000) -> List[tuple]:
    """Busca detalhes base para Rateio Geral ou Base Clientes por IDs."""
    if not client_ids: return []
    all_details = []
    try:
        ### ALTERAÇÃO INICIADA: Lógica para base_clientes usa a view V_CUSTOMER ###
        if report_type.lower() == 'base_clientes':
            # A view já tem todos os dados, então a busca por ID é mais simples.
            # O alias "código" na view V_CUSTOMER corresponde ao idcliente.
            from_sql = 'FROM public."V_CUSTOMER"'
            where_sql = 'WHERE "código" = ANY(%s)' # Usar o nome da coluna da view
            order_sql = 'ORDER BY "código"'
            query = f"SELECT * {from_sql} {where_sql} {order_sql};"
        else:
            # Lógica antiga para outros tipos de relatório (rateio)
            campos = _get_query_fields(report_type)
            if not campos: logger.error(f"Campos não definidos para get_client_details_by_ids tipo: {report_type}"); return []
            select = f"SELECT {', '.join(campos)}"; from_ = 'FROM public."CLIENTES" c'
            needs_consultor_join = any(f.startswith("co.") for f in campos)
            join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor' if needs_consultor_join else ""
            where = "WHERE c.idcliente = ANY(%s) AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"
            order = "ORDER BY c.idcliente"; query = f"{select} {from_}{join} {where} {order};"
        ### FIM DA ALTERAÇÃO ###

        for i in range(0, len(client_ids), batch_size):
            batch_ids = client_ids[i:i + batch_size]; params = (batch_ids,)
            batch_results = execute_query(query, params)
            if batch_results: all_details.extend(batch_results)
        return all_details
    except Exception as e: logger.error(f"Erro get_client_details_by_ids ({report_type}): {e}", exc_info=True); return []

def build_query(report_type: str, fornecedora: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> Tuple[str, tuple]:
     """Constrói a query paginada para relatórios Base Clientes e Rateio."""
     ### ALTERAÇÃO INICIADA: Lógica para 'base_clientes' agora usa a view ###
     if report_type == "base_clientes":
         select = 'SELECT *' 
         from_ = 'FROM public."V_CUSTOMER"'
         where_clauses = []
         params = []

         # A view V_CUSTOMER já tem a coluna "fornecedora".
         if fornecedora and fornecedora.lower() != "consolidado":
             where_clauses.append('"fornecedora" = %s')
             params.append(fornecedora)
         
         where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
         order = 'ORDER BY "código"' # Ordenar pela coluna "código" da view
         limit_clause = "LIMIT %s" if limit is not None else ""
         if limit is not None: params.append(limit)
         offset_clause = "OFFSET %s" if offset > 0 else ""
         if offset > 0: params.append(offset)
         
         query = f"{select} {from_} {where} {order} {limit_clause} {offset_clause};"
         return query, tuple(params)
     ### FIM DA ALTERAÇÃO ###

     # Lógica para "rateio" (sem alteração)
     if report_type == "rateio":
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
     
     raise ValueError(f"build_query não adequado para '{report_type}'.")


def count_query(report_type: str, fornecedora: Optional[str] = None) -> Tuple[str, tuple]:
     """Constrói a query de contagem para relatórios Base Clientes e Rateio."""
     ### ALTERAÇÃO INICIADA: Lógica de contagem para 'base_clientes' usa a view ###
     if report_type == "base_clientes":
        from_ = 'FROM public."V_CUSTOMER"'
        where_clauses = []
        params = []
        if fornecedora and fornecedora.lower() != "consolidado":
            where_clauses.append('"fornecedora" = %s')
            params.append(fornecedora)
        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        query = f"SELECT COUNT(*) {from_} {where};"
        return query, tuple(params)
     ### FIM DA ALTERAÇÃO ###

     # Lógica para "rateio" (sem alteração)
     if report_type == "rateio":
        from_ = 'FROM public."CLIENTES" c'; where_clauses = [" (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) "]; params = []
        if fornecedora and fornecedora.lower() != "consolidado": where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""; query = f"SELECT COUNT(c.idcliente) {from_} {where};"
        return query, tuple(params)

     raise ValueError(f"count_query não adequado para '{report_type}'.")