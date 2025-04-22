# database.py
import psycopg2
import psycopg2.pool
import logging
from flask import g
from config import Config
from typing import List, Tuple, Optional, Any

logger = logging.getLogger(__name__)
db_pool = None

# --- Funções de Pool e Conexão ---
def init_pool():
    """Inicializa o pool de conexões."""
    global db_pool
    if db_pool:
        return
    try:
        logger.info("Inicializando pool de conexões com o banco de dados...")
        db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=5, **Config.DB_CONFIG
        )
        conn = db_pool.getconn()
        db_pool.putconn(conn)
        logger.info("Pool de conexões inicializado com sucesso.")
    except (psycopg2.Error, KeyError, Exception) as e:
        logger.critical(f"Falha CRÍTICA ao inicializar pool de conexões: {e}", exc_info=True)
        db_pool = None
        raise ConnectionError(f"Não foi possível inicializar o pool DB: {e}")

def get_db():
    """Obtém uma conexão do pool para a requisição Flask atual (g)."""
    if not db_pool:
        try:
            logger.warning("Pool não inicializado. Tentando inicializar em get_db...")
            init_pool()
        except ConnectionError as e:
             logger.error(f"Tentativa de inicializar pool falhou em get_db: {e}")
             raise ConnectionError("Pool de conexões não está disponível.")

    if 'db' not in g:
        try:
            g.db = db_pool.getconn()
            # logger.debug("Conexão obtida do pool para a requisição.")
        except psycopg2.Error as e:
             logger.error(f"Falha ao obter conexão do pool: {e}", exc_info=True)
             raise ConnectionError(f"Não foi possível obter conexão do banco: {e}")
    return g.db

def close_db(e=None):
    """Fecha a conexão (devolve ao pool) ao final da requisição Flask."""
    db = g.pop('db', None)
    if db is not None and db_pool is not None:
        try:
            db_pool.putconn(db)
            # logger.debug("Conexão devolvida ao pool.")
        except psycopg2.Error as e:
             logger.error(f"Falha ao devolver conexão ao pool: {e}", exc_info=True)
             try: db.close()
             except: pass
    elif db is not None:
         try:
             db.close()
             # logger.debug("Conexão fechada (pool não disponível).")
         except: pass

# --- Função Principal para Executar Queries ---
def execute_query(query: str, params: Optional[tuple] = None, fetch_one=False) -> List[tuple] or Tuple or None:
    """Executa uma query SQL usando a conexão da requisição atual."""
    conn = get_db()
    result = None
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            if fetch_one:
                result = cur.fetchone()
            else:
                result = cur.fetchall()
        return result
    except psycopg2.OperationalError as e:
        logger.error(f"Erro operacional/conexão durante query: {e}", exc_info=False)
        g.pop('db', None)
        try: conn.close()
        except: pass
        raise RuntimeError(f"Erro de conexão ou operacional: {e}")
    except psycopg2.Error as e:
        if isinstance(e, psycopg2.errors.UndefinedColumn):
             logger.error(f"Erro de coluna indefinida: Verifique nomes na query/tabela. Detalhe: {e}", exc_info=False)
        else:
             logger.error(f"Erro de banco de dados ({type(e).__name__}): {e}", exc_info=False)
        try: conn.rollback()
        except Exception as rb_err: logger.error(f"Erro durante rollback: {rb_err}")
        raise RuntimeError(f"Erro ao executar a query: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado durante query ({type(e).__name__}): {e}", exc_info=True)
        try: conn.rollback()
        except Exception as rb_err: logger.error(f"Erro durante rollback: {rb_err}")
        raise RuntimeError(f"Erro inesperado: {e}")

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
        "c.rateio = 'N'", " (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE')) ",
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
    where_clauses = [ "c.rateio = 'S'", " (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE')) "]
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    full_query = query_base + " WHERE " + " AND ".join(where_clauses) + ";"
    try: results = execute_query(full_query, tuple(params)); return [r[0] for r in results] if results else []
    except Exception as e: logger.error(f"Erro get_base_enviada_ids: {e}"); return []

# --- Função para buscar detalhes completos por lista de IDs (Base Clientes ou Rateio Geral) ---
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
        where = "WHERE c.idcliente = ANY(%s) AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE'))"
        order = "ORDER BY c.idcliente"; query = f"{select} {from_}{join} {where} {order};"
        for i in range(0, len(client_ids), batch_size):
            batch_ids = client_ids[i:i + batch_size]; params = (batch_ids,)
            batch_results = execute_query(query, params)
            if batch_results: all_details.extend(batch_results)
        return all_details
    except Exception as e: logger.error(f"Erro get_client_details_by_ids ({report_type}): {e}", exc_info=True); return []

# --- Funções para Relatórios Específicos (Clientes por Licenciado, Boletos, Mapa) ---
def get_clientes_por_licenciado_data(offset: int = 0, limit: Optional[int] = None) -> List[tuple]:
    """Busca os dados para 'Quantidade de Clientes por Licenciado'."""
    base_query = """
        SELECT c.idconsultor, c.nome, c.cpf, c.email, c.uf, COUNT(cl.idconsultor) AS quantidade_clientes_ativos
        FROM public."CONSULTOR" c LEFT JOIN public."CLIENTES" cl ON c.idconsultor = cl.idconsultor
        WHERE cl.data_ativo IS NOT NULL AND (cl.origem IS NULL OR cl.origem IN ('', 'WEB', 'BACKOFFICE'))
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
        WHERE cl.data_ativo IS NOT NULL AND (cl.origem IS NULL OR cl.origem IN ('', 'WEB', 'BACKOFFICE')); """
    try: result = execute_query(count_query_sql, fetch_one=True); return result[0] if result else 0
    except Exception as e: logger.error(f"Erro count_clientes_por_licenciado: {e}", exc_info=True); return 0

# --- FUNÇÕES PARA RELATÓRIO 'Quantidade de Boletos por Cliente' (MODIFICADAS COM FILTRO FORNECEDORA) ---
def get_boletos_por_cliente_data(offset: int = 0, limit: Optional[int] = None, fornecedora: Optional[str] = None) -> List[tuple]:
    """Busca os dados para 'Quantidade de Boletos por Cliente', com paginação,
       FILTRO DE ORIGEM e filtro opcional de FORNECEDORA."""
    # logger.info(f"Buscando dados 'Boletos por Cliente' - Offset: {offset}, Limit: {limit}, Forn: {fornecedora}")
    base_query = """
        SELECT c.idcliente, c.nome, c.numinstalacao, c.celular, c.cidade,
               CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN '' ELSE (c.uf || '-' || c.concessionaria) END AS regiao,
               c.fornecedora, TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo,
               COUNT(rcb.numinstalacao) AS quantidade_registros_rcb
        FROM public."CLIENTES" c
        LEFT JOIN public."RCB_CLIENTES" rcb ON c.numinstalacao = rcb.numinstalacao """
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE'))"]
    params = []
    # <<< ADICIONADO FILTRO DE FORNECEDORA >>>
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    # <<< FIM ADIÇÃO >>>
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    group_by = " GROUP BY c.idcliente, c.nome, c.numinstalacao, c.celular, c.cidade, regiao, c.fornecedora, data_ativo "
    order_by = "ORDER BY c.idcliente"
    limit_clause = "LIMIT %s" if limit is not None else ""; offset_clause = ""
    if limit is not None: params.append(limit)
    if offset > 0: offset_clause = "OFFSET %s"; params.append(offset)
    paginated_query = f"{base_query} {where} {group_by} {order_by} {limit_clause} {offset_clause};"
    try:
        results = execute_query(paginated_query, tuple(params))
        # logger.info(f"Retornados {len(results) if results else 0} regs 'Boletos por Cliente' (Forn: {fornecedora}).")
        return results if results else []
    except Exception as e: logger.error(f"Erro get_boletos_por_cliente_data: {e}", exc_info=True); return []

def count_boletos_por_cliente(fornecedora: Optional[str] = None) -> int:
    """Conta o total de clientes para 'Boletos por Cliente', respeitando filtros."""
    # logger.info(f"Contando 'Boletos por Cliente' (Forn: {fornecedora})...")
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE'))"]
    params = []
    # <<< ADICIONADO FILTRO DE FORNECEDORA >>>
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    # <<< FIM ADIÇÃO >>>
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    count_query_sql = f'SELECT COUNT(DISTINCT c.idcliente) FROM public."CLIENTES" c {where};'
    try:
        result = execute_query(count_query_sql, tuple(params), fetch_one=True)
        count = result[0] if result else 0
        # logger.info(f"Contagem 'Boletos por Cliente': {count} (Forn: {fornecedora})")
        return count
    except Exception as e: logger.error(f"Erro count_boletos_por_cliente: {e}", exc_info=True); return 0
# --- FIM FUNÇÕES BOLETOS POR CLIENTE ---

def get_client_count_by_state() -> List[Tuple[str, int]]:
    """Busca a contagem de clientes ativos por estado (UF)."""
    query = """
        SELECT UPPER(c.ufconsumo) as estado_uf, COUNT(c.idcliente) as total_clientes
        FROM public."CLIENTES" c
        WHERE c.data_ativo IS NOT NULL AND c.ufconsumo IS NOT NULL AND c.ufconsumo <> ''
          AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE'))
        GROUP BY UPPER(c.ufconsumo) ORDER BY estado_uf; """
    try: return execute_query(query) or []
    except Exception as e: logger.error(f"Erro get_client_count_by_state: {e}", exc_info=True); return []

# --- >>> FUNÇÕES PARA RATEIO RZK <<< ---

def get_rateio_rzk_base_nova_ids() -> List[int]:
    """Busca IDs para 'Base Nova' do Rateio RZK."""
    query_base = """ SELECT DISTINCT cc.idcliente FROM public."CLIENTES_CONTRATOS" cc
        INNER JOIN public."CLIENTES_CONTRATOS_SIGNER" ccs ON cc.idcliente_contrato = ccs.idcliente_contrato
        INNER JOIN public."CLIENTES" c ON cc.idcliente = c.idcliente """
    group_by = " GROUP BY cc.idcliente_contrato, cc.idcliente HAVING bool_and(ccs.signature_at IS NOT NULL) "
    where_clauses = [
        "c.fornecedora = 'RZK'", "cc.type_document = 'procuracao_igreen'", "upper(cc.status) = 'ATIVO'",
        "c.data_ativo IS NOT NULL", "c.status IS NULL", "c.validadosucesso = 'S'",
        "c.rateio = 'N'", " (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE')) ",
        " NOT EXISTS ( SELECT 1 FROM public.\"DEVOLUTIVAS\" d WHERE d.idcliente = c.idcliente ) "
    ]
    full_query = query_base + " WHERE " + " AND ".join(where_clauses) + group_by + ";"
    # logger.info("Query Base Nova RZK IDs...")
    try: results = execute_query(full_query); return [r[0] for r in results] if results else []
    except Exception as e: logger.error(f"Erro get_rateio_rzk_base_nova_ids: {e}"); return []

def get_rateio_rzk_base_enviada_ids() -> List[int]:
    """Busca IDs para 'Base Enviada' do Rateio RZK."""
    query_base = 'SELECT c.idcliente FROM public."CLIENTES" c'
    where_clauses = [ "c.fornecedora = 'RZK'", "c.rateio = 'S'", " (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE')) " ]
    full_query = query_base + " WHERE " + " AND ".join(where_clauses) + ";"
    # logger.info("Query Base Enviada RZK IDs...")
    try: results = execute_query(full_query); return [r[0] for r in results] if results else []
    except Exception as e: logger.error(f"Erro get_rateio_rzk_base_enviada_ids: {e}"); return []

def _get_rateio_rzk_fields() -> List[str]:
    """Retorna a lista de campos SQL EXATOS para Rateio RZK."""
    return [
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

def get_rateio_rzk_client_details_by_ids(client_ids: List[int], batch_size: int = 1000) -> List[tuple]:
    """Busca detalhes completos para Rateio RZK por lista de IDs."""
    if not client_ids: return []
    all_details = []
    try:
        campos = _get_rateio_rzk_fields()
        if not campos: logger.error("Falha campos RZK details"); return []
        select = f"SELECT {', '.join(campos)}"; from_ = 'FROM public."CLIENTES" c'
        join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor'
        where = "WHERE c.idcliente = ANY(%s) AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE'))"
        order = "ORDER BY c.idcliente"; query = f"{select} {from_}{join} {where} {order};"
        # logger.info(f"Buscando detalhes RZK para {len(client_ids)} IDs...")
        for i in range(0, len(client_ids), batch_size):
            batch_ids = client_ids[i:i + batch_size]; params = (batch_ids,)
            batch_results = execute_query(query, params)
            if batch_results: all_details.extend(batch_results)
        # logger.info(f"Busca RZK details concluída: {len(all_details)} regs.")
        return all_details
    except Exception as e: logger.error(f"Erro get_rateio_rzk_client_details_by_ids: {e}", exc_info=True); return []

def get_rateio_rzk_data(offset: int = 0, limit: Optional[int] = None) -> List[tuple]:
    """Busca dados paginados para display Rateio RZK (Base Enviada)."""
    campos_rzk = _get_rateio_rzk_fields()
    select = f"SELECT {', '.join(campos_rzk)}"; from_ = 'FROM public."CLIENTES" c'
    join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor'
    where_clauses = ["c.fornecedora = 'RZK'", "c.rateio = 'S'", "(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE'))"]
    where = f"WHERE {' AND '.join(where_clauses)}"; order = "ORDER BY c.idcliente"; params = []
    limit_clause = "LIMIT %s" if limit is not None else ""; offset_clause = ""
    if limit is not None: params.append(limit)
    if offset > 0: offset_clause = "OFFSET %s"; params.append(offset)
    paginated_query = f"{select} {from_}{join} {where} {order} {limit_clause} {offset_clause};"
    try: return execute_query(paginated_query, tuple(params)) or []
    except Exception as e: logger.error(f"Erro get_rateio_rzk_data (display): {e}", exc_info=True); return []

def count_rateio_rzk() -> int:
    """Conta total para display Rateio RZK (Base Enviada)."""
    where_clauses = ["c.fornecedora = 'RZK'", "c.rateio = 'S'", "(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE'))"]
    where = f"WHERE {' AND '.join(where_clauses)}"; count_query_sql = f'SELECT COUNT(c.idcliente) FROM public."CLIENTES" c {where};'
    try: result = execute_query(count_query_sql, fetch_one=True); return result[0] if result else 0
    except Exception as e: logger.error(f"Erro count_rateio_rzk (display): {e}", exc_info=True); return 0

# --- FIM FUNÇÕES RZK ---

# --- Funções para Estrutura de Query e Cabeçalhos ---

def _get_query_fields(report_type: str) -> List[str]:
    """Retorna a lista de campos SQL BASE para Base Clientes ou Rateio Geral."""
    report_type = report_type.lower()
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
    if report_type == "base_clientes": return base_clientes_fields
    elif report_type == "rateio": return base_rateio_fields
    else: logger.warning(f"_get_query_fields: Tipo '{report_type}' não mapeado."); return []

def build_query(report_type: str, fornecedora: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> Tuple[str, tuple]:
    """Constrói query para Base Clientes ou Rateio Geral."""
    if report_type not in ["base_clientes", "rateio"]: raise ValueError(f"build_query não adequado para '{report_type}'.")
    campos = _get_query_fields(report_type);
    if not campos: raise ValueError(f"Campos não definidos para '{report_type}'")
    select = f"SELECT {', '.join(campos)}"; from_ = 'FROM public."CLIENTES" c'
    needs_consultor_join = any(f.startswith("co.") for f in campos)
    join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor' if needs_consultor_join else ""
    where_clauses = [" (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE')) "]; params = []
    if fornecedora and fornecedora.lower() != "consolidado": where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""; order = "ORDER BY c.idcliente"; limit_clause = ""; offset_clause = ""
    if limit is not None: limit_clause = f"LIMIT %s"; params.append(limit)
    if offset > 0: offset_clause = f"OFFSET %s"; params.append(offset)
    query = f"{select} {from_}{join} {where} {order} {limit_clause} {offset_clause};"
    return query, tuple(params)

def count_query(report_type: str, fornecedora: Optional[str] = None) -> Tuple[str, tuple]:
    """Constrói query de contagem para Base Clientes ou Rateio Geral."""
    if report_type not in ["base_clientes", "rateio"]: raise ValueError(f"count_query não adequado para '{report_type}'.")
    from_ = 'FROM public."CLIENTES" c'; where_clauses = [" (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE')) "]; params = []
    if fornecedora and fornecedora.lower() != "consolidado": where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""; query = f"SELECT COUNT(c.idcliente) {from_} {where};"
    return query, tuple(params)

def get_fornecedoras() -> List[str]:
    """Busca a lista distinta de fornecedoras."""
    query = 'SELECT DISTINCT fornecedora FROM public."CLIENTES" WHERE fornecedora IS NOT NULL AND fornecedora <> \'\' ORDER BY fornecedora;'
    try: results = execute_query(query); return sorted([str(f[0]) for f in results if f and f[0]]) if results else []
    except Exception as e: logger.error(f"Erro get_fornecedoras: {e}", exc_info=True); return []

# REVISADA: get_headers com entrada para "rateio_rzk"
def get_headers(report_type: str) -> List[str]:
    """Retorna os cabeçalhos para o tipo de relatório especificado."""
    # logger.debug(f"get_headers INICIADA: report_type='{report_type}'")
    report_type = report_type.lower()

    header_map = {
        "c.idcliente": "Código Cliente", "c.nome": "Nome", "c.numinstalacao": "Instalação", "c.celular": "Celular",
        "c.cidade": "Cidade", "regiao": "Região (UF-Conc)", "data_ativo": "Data Ativo", "qtdeassinatura": "Assinaturas",
        "c.consumomedio": "Consumo Médio", "c.status": "Status Cliente", "dtcad": "Data Cadastro",
        "c.\"cpf/cnpj\"": "CPF/CNPJ", "c.numcliente": "Número Cliente", "dtultalteracao": "Última Alteração",
        "c.celular_2": "Celular 2", "c.email": "Email", "c.rg": "RG", "c.emissor": "Emissor", "datainjecao": "Data Injeção",
        "c.idconsultor": "ID Consultor", "consultor_nome": "Representante", "consultor_celular": "Celular Consultor",
        "c.cep": "CEP", "c.endereco": "Endereço", "c.numero": "Número", "c.bairro": "Bairro", "c.complemento": "Complemento",
        "c.cnpj": "CNPJ (Empresa)", "c.razao": "Razão Social", "c.fantasia": "Nome Fantasia", "c.ufconsumo": "UF Consumo",
        "c.classificacao": "Classificação", "c.keycontrato": "Key Contrato", "c.keysigner": "Key Signer",
        "c.leadidsolatio": "Lead ID Solatio", "c.indcli": "Ind CLI", "c.enviadocomerc": "Enviado Comerci", "c.obs": "Observação",
        "c.posvenda": "Pós-venda", "c.retido": "Retido", "c.contrato_verificado": "Contrato Verificado",
        "c.rateio": "Rateio (S/N)", "c.validadosucesso": "Validação Sucesso (S/N)", "status_sucesso": "Status Validação",
        "c.documentos_enviados": "Documentos Enviados", "c.link_documento": "Link Documento",
        "c.caminhoarquivo": "Link Conta Energia", "c.caminhoarquivocnpj": "Link Cartão CNPJ",
        "c.caminhoarquivodoc1": "Link Doc Ident. 1", "c.caminhoarquivodoc2": "Link Doc Ident. 2",
        "c.caminhoarquivoenergia2": "Link Conta Energia 2", "c.caminhocontratosocial": "Link Contrato Social",
        "c.caminhocomprovante": "Link Comprovante", "c.caminhoarquivoestatutoconvencao": "Link Estatuto/Convenção",
        "c.senhapdf": "Senha PDF", "c.codigo": "Código Interno", "c.elegibilidade": "Elegibilidade",
        "c.idplanopj": "ID Plano PJ", "dtcancelado": "Data Cancelamento", "data_ativo_original": "Data Ativo Original",
        "c.fornecedora": "Fornecedora", "c.desconto_cliente": "Desconto Cliente", "dtnasc": "Data Nasc.", "c.origem": "Origem",
        "c.cm_tipo_pagamento": "Tipo Pagamento", "c.status_financeiro": "Status Financeiro",
        "c.logindistribuidora": "Login Distribuidora", "c.senhadistribuidora": "Senha Distribuidora",
        "c.nacionalidade": "Nacionalidade", "c.profissao": "Profissão", "c.estadocivil": "Estado Civil",
        "c.obs_compartilhada": "Observação Compartilhada", "c.linkassinatura1": "Link Assinatura",
        # Específicos
        "c.cpf": "CPF Consultor", "c.uf": "UF Consultor", "quantidade_clientes_ativos": "Qtd. Clientes Ativos",
        "quantidade_registros_rcb": "Qtd. Boletos (RCB)", "nome_cliente_rateio": "Cliente Rateio",
        # ALIASES RZK
        "devolutiva": "Devolutiva", "licenciado": "Licenciado", "chave_contrato": "Chave Contrato"
    }

    # Listas de CHAVES
    base_clientes_keys = [
        "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "qtdeassinatura", "c.consumomedio", "c.status", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "dtultalteracao", "c.celular_2", "c.email", "c.rg", "c.emissor", "datainjecao", "c.idconsultor", "consultor_nome", "consultor_celular", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.keycontrato", "c.keysigner", "c.leadidsolatio", "c.indcli", "c.enviadocomerc", "c.obs", "c.posvenda", "c.retido", "c.contrato_verificado", "c.rateio", "c.validadosucesso", "status_sucesso", "c.documentos_enviados", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.codigo", "c.elegibilidade", "c.idplanopj", "dtcancelado", "data_ativo_original", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.origem", "c.cm_tipo_pagamento", "c.status_financeiro", "c.logindistribuidora", "c.senhadistribuidora", "c.nacionalidade", "c.profissao", "c.estadocivil", "c.obs_compartilhada", "c.linkassinatura1"
    ]
    base_rateio_keys = [
        "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "c.cep", "consultor_nome", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil"
    ]
    rateio_rzk_keys = [
        "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", # 0-7
        "devolutiva", # 8 (Nova Col 9)
        "dtcad", # 9 (Original Col 9, agora Col 10)
        "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", # 10-14
        "licenciado", # 15 (Nova Col 16)
        "c.cep", # 16 (Original Col 14, agora Col 17)
        "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", # 17-22
        "c.fantasia", "c.ufconsumo", "c.classificacao", # 23-25
        "chave_contrato", # 26 (Nova Col 27)
        "c.link_documento",# 27 (Original Col 25, agora Col 28)
        "c.caminhoarquivo", # 28 (Original Col 26, agora Col 29)
        "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2",
        "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf",
        "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora",
        "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil"
    ]
    clientes_por_licenciado_keys = [
        "c.idconsultor", "c.nome", "c.cpf", "c.email", "c.uf", "quantidade_clientes_ativos"
    ]
    boletos_por_cliente_keys = [
        "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao",
        "c.fornecedora", "data_ativo", "quantidade_registros_rcb"
    ]

    # Seleciona a lista de chaves apropriada
    keys_for_report = []
    if report_type == "base_clientes": keys_for_report = base_clientes_keys
    elif report_type == "clientes_por_licenciado": keys_for_report = clientes_por_licenciado_keys
    elif report_type == "boletos_por_cliente": keys_for_report = boletos_por_cliente_keys
    elif report_type == "rateio": keys_for_report = base_rateio_keys
    elif report_type == "rateio_rzk": keys_for_report = rateio_rzk_keys
    else: logger.warning(f"Tipo desconhecido '{report_type}' em get_headers."); return []

    # Cria a lista final de cabeçalhos
    headers_list = []; missing_keys_in_map = []
    for i, key in enumerate(keys_for_report):
        friendly_name = header_map.get(key)
        if friendly_name: headers_list.append(friendly_name)
        else:
            fallback_name = key.split('.')[-1].replace('_', ' ').title() if '.' in key else key.replace('_', ' ').title()
            headers_list.append(fallback_name); missing_keys_in_map.append(key)
            # logger.warning(f"Header Idx {i}: Chave '{key}' s/ map. Fallback: '{fallback_name}'.")

    if missing_keys_in_map: logger.warning(f"Chaves não mapeadas em header_map p/ '{report_type}': {missing_keys_in_map}.")
    # logger.info(f"Headers gerados para '{report_type}'. Total: {len(headers_list)}")
    return headers_list