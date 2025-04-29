# backend/db/reports_base.py
import logging
from typing import List, Tuple, Optional
from .executor import execute_query # Import local

logger = logging.getLogger(__name__)

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
     """Retorna lista de campos SQL baseados no tipo de relatório."""
     report_type = report_type.lower()
     # Campos Base Clientes (Exemplo - Mantenha sua lista completa)
     base_clientes_fields = [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao", "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "(COALESCE(c.qtdeassinatura, 0)::text || '/4') AS qtdeassinatura", "c.consumomedio", "c.status", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "co.nome AS consultor_nome", "c.fornecedora" ] # Exemplo reduzido
     # Campos Rateio Geral (Exemplo - Mantenha sua lista completa)
     base_rateio_fields = [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao", "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "c.consumomedio", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "co.nome AS consultor_nome", "c.fornecedora" ] # Exemplo reduzido

     if report_type == "base_clientes": return base_clientes_fields # Use sua lista completa aqui
     elif report_type == "rateio": return base_rateio_fields # Use sua lista completa aqui
     # Adicionar outros tipos se get_client_details_by_ids for usado para eles
     else: logger.warning(f"_get_query_fields: Tipo '{report_type}' não mapeado para campos genéricos."); return []

def get_client_details_by_ids(report_type: str, client_ids: List[int], batch_size: int = 1000) -> List[tuple]:
    """Busca detalhes base para Rateio Geral ou Base Clientes por IDs."""
    if not client_ids: return []
    all_details = []
    try:
        campos = _get_query_fields(report_type)
        if not campos: logger.error(f"Campos não definidos para get_client_details_by_ids tipo: {report_type}"); return []
        select = f"SELECT {', '.join(campos)}"; from_ = 'FROM public."CLIENTES" c'
        needs_consultor_join = any(f.startswith("co.") for f in campos)
        join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor' if needs_consultor_join else ""
        where = "WHERE c.idcliente = ANY(%s) AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"
        order = "ORDER BY c.idcliente"; query = f"{select} {from_}{join} {where} {order};"
        for i in range(0, len(client_ids), batch_size):
            batch_ids = client_ids[i:i + batch_size]; params = (batch_ids,)
            batch_results = execute_query(query, params)
            if batch_results: all_details.extend(batch_results)
        return all_details
    except Exception as e: logger.error(f"Erro get_client_details_by_ids ({report_type}): {e}", exc_info=True); return []

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
     where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""; query = f"SELECT COUNT(c.idcliente) {from_} {where};"
     return query, tuple(params)