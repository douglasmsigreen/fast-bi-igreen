# database.py
import psycopg2
import psycopg2.pool
import logging
from flask import g
from config import Config
from typing import List, Tuple, Optional, Any
from datetime import datetime, timedelta # Adicionado timedelta e datetime

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
               COUNT(rcb.numinstalacao) AS quantidade_registros_rcb
        FROM public."CLIENTES" c
        LEFT JOIN public."RCB_CLIENTES" rcb ON c.numinstalacao = rcb.numinstalacao """
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"]
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    group_by = " GROUP BY c.idcliente, c.nome, c.numinstalacao, c.celular, c.cidade, regiao, c.fornecedora, data_ativo "
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

# --- >>> FUNÇÃO MODIFICADA PARA O MAPA <<< ---
# --- Função para buscar DADOS AGREGADOS POR ESTADO para o MAPA ---
def get_state_map_data() -> List[Tuple[str, int, float]]: # Retorna UF, CONTAGEM, SOMA
    """
    Busca a CONTAGEM de clientes ativos e a SOMA de 'consumomedio' desses clientes,
    agrupado por estado (UF).
    Clientes considerados são aqueles com data_ativo não nula.
    """
    query = """
        SELECT
            UPPER(c.ufconsumo) as estado_uf,
            COUNT(c.idcliente) as total_clientes,              -- Adicionada CONTAGEM
            SUM(COALESCE(c.consumomedio, 0)) as total_consumo_medio -- Mantida SOMA
        FROM public."CLIENTES" c
        WHERE
            c.data_ativo IS NOT NULL                 -- Filtra clientes ativos
            AND c.ufconsumo IS NOT NULL AND c.ufconsumo <> '' -- Garante UF válida
            AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) -- Filtro de origem
        GROUP BY
            UPPER(c.ufconsumo)
        ORDER BY
            estado_uf;
    """
    logger.info("Buscando CONTAGEM e SOMA de consumo médio por estado para o mapa...")
    try:
        results = execute_query(query)
        # Garante que os tipos estejam corretos (str, int, float)
        formatted_results = [
            (
                str(row[0]),                                # UF (string)
                int(row[1]) if row[1] is not None else 0,   # Contagem (int)
                float(row[2]) if row[2] is not None else 0.0 # Soma Consumo (float)
            )
            for row in results if row and len(row) > 2 # Garante que a linha tem 3 elementos
        ]
        logger.info(f"Dados de contagem e soma por estado encontrados: {len(formatted_results)} estados.")
        return formatted_results or []
    except Exception as e:
        logger.error(f"Erro ao buscar dados agregados por estado para o mapa: {e}", exc_info=True)
        return []
# --- >>> FIM DA FUNÇÃO MODIFICADA PARA O MAPA <<< ---


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


# --- FUNÇÃO Soma de Consumo Médio por Mês (baseado em data_ativo) ---
def get_total_consumo_medio_by_month(month_str: Optional[str] = None) -> float:
    """
    Calcula a soma total de 'consumomedio' para clientes cuja data_ativo
    cai dentro do mês especificado.

    Args:
        month_str: O mês para filtrar no formato 'YYYY-MM'.
                   Se None, busca de todos os clientes com data_ativo.

    Returns:
        A soma total como float, ou 0.0 se nenhum dado ou erro.
    """
    base_query = """
        SELECT SUM(COALESCE(c.consumomedio, 0))
        FROM public."CLIENTES" c
        WHERE
            (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
            AND {date_filter}; -- Placeholder para filtro de data
    """
    params = []
    date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            logger.info(f"Filtrando soma de consumo por data_ativo no mês: {month_str} (>= {start_date} e < {end_date_exclusive})")
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError:
            logger.warning(f"Formato de mês inválido para consumo: '{month_str}'. Usando filtro padrão (data_ativo IS NOT NULL).")
            date_filter_sql = "c.data_ativo IS NOT NULL"; params = []
    final_query = base_query.format(date_filter=date_filter_sql)
    # logger.info(f"Calculando soma de consumo médio (Mês: {month_str or 'Todos'})...") # Log menos verboso
    try:
        result = execute_query(final_query, tuple(params), fetch_one=True)
        total_consumo = float(result[0]) if result and result[0] is not None else 0.0
        # logger.info(f"Soma de consumo médio (Mês: {month_str or 'Todos'}): {total_consumo:.2f}") # Log menos verboso
        return total_consumo
    except Exception as e: logger.error(f"Erro ao calcular soma de consumo médio (Mês: {month_str or 'Todos'}): {e}", exc_info=True); return 0.0
# --- FIM FUNÇÃO ---


# --- FUNÇÃO Contagem de Clientes Ativos por Mês (baseado em data_ativo) ---
def count_clientes_ativos_by_month(month_str: Optional[str] = None) -> int:
    """
    Conta os clientes cuja data_ativo cai dentro do mês especificado.

    Args:
        month_str: O mês para filtrar no formato 'YYYY-MM'.
                   Se None, conta todos os clientes com data_ativo não nula.

    Returns:
        A contagem de clientes como inteiro.
    """
    base_query = """
        SELECT COUNT(c.idcliente)
        FROM public."CLIENTES" c
        WHERE
            (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
            AND {date_filter}; -- Placeholder para filtro de data
    """
    params = []
    date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            # logger.info(f"Contando clientes ativos por data_ativo no mês: {month_str} (>= {start_date} e < {end_date_exclusive})")
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError:
            logger.warning(f"Formato de mês inválido para contagem: '{month_str}'. Usando filtro padrão (data_ativo IS NOT NULL).")
            date_filter_sql = "c.data_ativo IS NOT NULL"; params = []
    final_query = base_query.format(date_filter=date_filter_sql)
    # logger.info(f"Contando clientes ativos (Mês: {month_str or 'Todos'})...") # Log menos verboso
    try:
        result = execute_query(final_query, tuple(params), fetch_one=True)
        count = int(result[0]) if result and result[0] is not None else 0
        # logger.info(f"Contagem de clientes ativos (Mês: {month_str or 'Todos'}): {count}") # Log menos verboso
        return count
    except Exception as e: logger.error(f"Erro ao contar clientes ativos (Mês: {month_str or 'Todos'}): {e}", exc_info=True); return 0
# --- FIM FUNÇÃO ---

# --- FUNÇÃO Contagem de Clientes REGISTRADOS por Mês (baseado em dtcad) ---
def count_clientes_registrados_by_month(month_str: Optional[str] = None) -> int:
    """
    Conta os clientes cuja data de CADASTRO (dtcad) cai dentro do mês especificado.

    Args:
        month_str: O mês para filtrar no formato 'YYYY-MM'.
                   Se None, conta todos os clientes com dtcad não nula.

    Returns:
        A contagem de clientes como inteiro.
    """
    base_query = """
        SELECT COUNT(c.idcliente)
        FROM public."CLIENTES" c
        WHERE
            (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
            AND {date_filter}; -- Placeholder para filtro de data de CADASTRO
    """
    params = []
    date_filter_sql = "c.dtcad IS NOT NULL" # Filtra por dtcad
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            # logger.info(f"Contando clientes registrados por dtcad no mês: {month_str} (>= {start_date} e < {end_date_exclusive})")
            date_filter_sql = "(c.dtcad >= %s AND c.dtcad < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError:
            logger.warning(f"Formato de mês inválido para contagem (dtcad): '{month_str}'. Usando filtro padrão (dtcad IS NOT NULL).")
            date_filter_sql = "c.dtcad IS NOT NULL"; params = []
    final_query = base_query.format(date_filter=date_filter_sql)
    # logger.info(f"Contando clientes REGISTRADOS (Mês: {month_str or 'Todos'})...") # Log menos verboso
    try:
        result = execute_query(final_query, tuple(params), fetch_one=True)
        count = int(result[0]) if result and result[0] is not None else 0
        # logger.info(f"Contagem de clientes REGISTRADOS (Mês: {month_str or 'Todos'}): {count}") # Log menos verboso
        return count
    except Exception as e: logger.error(f"Erro ao contar clientes REGISTRADOS (Mês: {month_str or 'Todos'}): {e}", exc_info=True); return 0
# --- FIM FUNÇÃO ---


# --- FUNÇÃO Resumo por Fornecedora (baseado em data_ativo) ---
def get_fornecedora_summary(month_str: Optional[str] = None) -> List[Tuple[str, int, float]] or None:
    """
    Busca um resumo (qtd clientes, soma consumo) por fornecedora,
    filtrando por clientes cuja data_ativo cai dentro do mês especificado.

    Args:
        month_str: O mês para filtrar no formato 'YYYY-MM'.
                   Se None, busca de todos os clientes com data_ativo.

    Returns:
        Lista de tuplas (fornecedora, qtd, soma_consumo) ou None em caso de erro.
    """
    base_query = """
        SELECT
            COALESCE(NULLIF(TRIM(c.fornecedora), ''), 'NÃO ESPECIFICADA') AS fornecedora_tratada,
            COUNT(c.idcliente) AS qtd_clientes,
            SUM(COALESCE(c.consumomedio, 0)) AS soma_consumo_medio_por_fornecedora
        FROM public."CLIENTES" c
        WHERE
            (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
            AND {date_filter} -- Placeholder para o filtro de data
        GROUP BY fornecedora_tratada ORDER BY fornecedora_tratada;
    """
    params = []
    date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            if start_date.month == 12: end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else: end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            # logger.info(f"Filtrando resumo fornecedora por data_ativo no mês: {month_str} (>= {start_date} e < {end_date_exclusive})")
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"; params.extend([start_date, end_date_exclusive])
        except ValueError:
            logger.warning(f"Formato de mês inválido recebido: '{month_str}'. Usando filtro padrão (data_ativo IS NOT NULL).")
            date_filter_sql = "c.data_ativo IS NOT NULL"; params = []
    final_query = base_query.format(date_filter=date_filter_sql)
    # logger.info(f"Buscando resumo por fornecedora (Mês: {month_str or 'Todos'})...") # Log menos verboso
    try:
        results = execute_query(final_query, tuple(params))
        if results:
            formatted_results = [(str(row[0]), int(row[1]), float(row[2]) if row[2] is not None else 0.0) for row in results]
            # logger.info(f"Resumo por fornecedora (Mês: {month_str or 'Todos'}) encontrado: {len(formatted_results)} registros.") # Log menos verboso
            return formatted_results
        else: # logger.info(f"Nenhum dado encontrado para o resumo por fornecedora (Mês: {month_str or 'Todos'})."); # Log menos verboso
             return []
    except Exception as e: logger.error(f"Erro ao buscar resumo por fornecedora (Mês: {month_str or 'Todos'}): {e}", exc_info=True); return None
# --- FIM DA FUNÇÃO ---

# --- NOVA FUNÇÃO: Resumo por Concessionária (baseado em data_ativo) ---
def get_concessionaria_summary(month_str: Optional[str] = None) -> List[Tuple[str, int, float]] or None:
    """
    Busca um resumo (qtd clientes, soma consumo) por CONCESSIONÁRIA,
    filtrando por clientes cuja data_ativo cai dentro do mês especificado.

    Args:
        month_str: O mês para filtrar no formato 'YYYY-MM'.
                   Se None, busca de todos os clientes com data_ativo.

    Returns:
        Lista de tuplas (concessionaria, qtd, soma_consumo) ou None em caso de erro.
    """
    base_query = """
        SELECT
            -- Trata concessionária vazia ou nula e combina com UF se disponível
            CASE
                WHEN c.concessionaria IS NULL OR TRIM(c.concessionaria) = '' THEN COALESCE(UPPER(TRIM(c.ufconsumo)), 'NÃO ESPECIFICADA')
                WHEN c.ufconsumo IS NULL OR TRIM(c.ufconsumo) = '' THEN UPPER(TRIM(c.concessionaria))
                ELSE (UPPER(TRIM(c.ufconsumo)) || '-' || UPPER(TRIM(c.concessionaria)))
            END AS regiao_concessionaria,
            COUNT(c.idcliente) AS qtd_clientes,
            SUM(COALESCE(c.consumomedio, 0)) AS soma_consumo_medio
        FROM public."CLIENTES" c
        WHERE
            (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
            AND {date_filter} -- Placeholder para o filtro de data
        GROUP BY regiao_concessionaria -- Agrupa pela coluna calculada
        ORDER BY regiao_concessionaria;
    """
    params = []
    date_filter_sql = "c.data_ativo IS NOT NULL"
    if month_str:
        try:
            start_date = datetime.strptime(month_str + '-01', '%Y-%m-%d').date()
            # Calcula o fim do mês corretamente
            if start_date.month == 12:
                end_date_exclusive = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else:
                end_date_exclusive = start_date.replace(month=start_date.month + 1, day=1)
            date_filter_sql = "(c.data_ativo >= %s AND c.data_ativo < %s)"
            params.extend([start_date, end_date_exclusive])
        except ValueError:
            logger.warning(f"[CONCESSIONARIA SUMMARY] Formato de mês inválido: '{month_str}'. Usando data_ativo IS NOT NULL.")
            date_filter_sql = "c.data_ativo IS NOT NULL"
            params = [] # Reseta params se o mês for inválido

    final_query = base_query.format(date_filter=date_filter_sql)
    # logger.info(f"Buscando resumo por concessionária (Mês: {month_str or 'Todos'})...") # Log menos verboso
    try:
        results = execute_query(final_query, tuple(params))
        if results:
            # Formata garantindo que os tipos estão corretos
            formatted_results = [
                (
                    str(row[0]), # regiao_concessionaria (string)
                    int(row[1]), # qtd_clientes (int)
                    float(row[2]) if row[2] is not None else 0.0 # soma_consumo (float)
                )
                for row in results
            ]
            # logger.info(f"Resumo por concessionária (Mês: {month_str or 'Todos'}) encontrado: {len(formatted_results)} registros.") # Log menos verboso
            return formatted_results
        else:
            # logger.info(f"Nenhum dado encontrado para o resumo por concessionária (Mês: {month_str or 'Todos'}).") # Log menos verboso
            return [] # Retorna lista vazia se não houver resultados
    except Exception as e:
        logger.error(f"Erro ao buscar resumo por concessionária (Mês: {month_str or 'Todos'}): {e}", exc_info=True)
        return None # Retorna None em caso de erro
# --- FIM DA NOVA FUNÇÃO ---


# --- FUNÇÃO para Contagem Mensal de Clientes Ativados por Ano (Gráfico) ---
def get_monthly_active_clients_by_year(year: int) -> List[int]:
    """
    Busca a contagem de clientes ativados em cada mês de um ano específico,
    baseado na coluna data_ativo.

    Args:
        year: O ano (inteiro) para filtrar.

    Returns:
        Uma lista com 12 inteiros, representando a contagem para cada mês (Jan a Dez).
        Retorna uma lista de 12 zeros se houver erro ou nenhum dado.
    """
    query = """
        SELECT EXTRACT(MONTH FROM c.data_ativo)::INTEGER AS mes, COUNT(c.idcliente) AS contagem
        FROM public."CLIENTES" c
        WHERE EXTRACT(YEAR FROM c.data_ativo) = %s
            AND (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        GROUP BY mes ORDER BY mes;
    """
    params = (year,); monthly_counts = [0] * 12
    # logger.info(f"Buscando contagem mensal de clientes ativados para o ano {year}...") # Log menos verboso
    try:
        results = execute_query(query, params)
        if results:
            for row in results:
                month_index = row[0] - 1
                if 0 <= month_index < 12: monthly_counts[month_index] = int(row[1])
            # logger.info(f"Contagem mensal para {year} encontrada: {monthly_counts}") # Log menos verboso
        # else: logger.info(f"Nenhum cliente ativado encontrado para o ano {year}.") # Log menos verboso
        return monthly_counts
    except Exception as e: logger.error(f"Erro ao buscar contagem mensal para o ano {year}: {e}", exc_info=True); return [0] * 12
# --- FIM FUNÇÃO ---


# --- Funções para Estrutura de Query e Cabeçalhos ---
def _get_query_fields(report_type: str) -> List[str]:
     report_type = report_type.lower()
     base_clientes_fields = [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao", "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "(COALESCE(c.qtdeassinatura, 0)::text || '/4') AS qtdeassinatura", "c.consumomedio", "c.status", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "TO_CHAR(c.dtultalteracao, 'DD/MM/YYYY') AS dtultalteracao", "c.celular_2", "c.email", "c.rg", "c.emissor", "TO_CHAR(c.datainjecao, 'DD/MM/YYYY') AS datainjecao", "c.idconsultor", "co.nome AS consultor_nome", "co.celular AS consultor_celular", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.keycontrato", "c.keysigner", "c.leadidsolatio", "c.indcli", "c.enviadocomerc", "c.obs", "c.posvenda", "c.retido", "c.contrato_verificado", "c.rateio", "c.validadosucesso", "CASE WHEN c.validadosucesso = 'S' THEN 'Aprovado' ELSE 'Rejeitado' END AS status_sucesso", "c.documentos_enviados", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.codigo", "c.elegibilidade", "c.idplanopj", "TO_CHAR(c.dtcancelado, 'DD/MM/YYYY') AS dtcancelado", "TO_CHAR(c.data_ativo_original, 'DD/MM/YYYY') AS data_ativo_original", "c.fornecedora", "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc", "c.origem", "c.cm_tipo_pagamento", "c.status_financeiro", "c.logindistribuidora", "c.senhadistribuidora", "c.nacionalidade", "c.profissao", "c.estadocivil", "c.obs_compartilhada", "c.linkassinatura1" ]
     base_rateio_fields = [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao", "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "c.consumomedio", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "c.cep", "co.nome AS consultor_nome", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "c.nome AS nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ]
     if report_type == "base_clientes": return base_clientes_fields
     elif report_type == "rateio": return base_rateio_fields
     else: logger.warning(f"_get_query_fields: Tipo '{report_type}' não mapeado."); return []

def build_query(report_type: str, fornecedora: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> Tuple[str, tuple]:
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
     if report_type not in ["base_clientes", "rateio"]: raise ValueError(f"count_query não adequado para '{report_type}'.")
     from_ = 'FROM public."CLIENTES" c'; where_clauses = [" (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP')) "]; params = []
     if fornecedora and fornecedora.lower() != "consolidado": where_clauses.append("c.fornecedora = %s"); params.append(fornecedora)
     where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""; query = f"SELECT COUNT(c.idcliente) {from_} {where};"
     return query, tuple(params)

def get_fornecedoras() -> List[str]:
    query = 'SELECT DISTINCT fornecedora FROM public."CLIENTES" WHERE fornecedora IS NOT NULL AND fornecedora <> \'\' ORDER BY fornecedora;'
    try: results = execute_query(query); return sorted([str(f[0]) for f in results if f and f[0]]) if results else []
    except Exception as e: logger.error(f"Erro get_fornecedoras: {e}", exc_info=True); return []

def get_headers(report_type: str) -> List[str]:
     report_type = report_type.lower()
     header_map = { "c.idcliente": "Código Cliente", "c.nome": "Nome", "c.numinstalacao": "Instalação", "c.celular": "Celular", "c.cidade": "Cidade", "regiao": "Região (UF-Conc)", "data_ativo": "Data Ativo", "qtdeassinatura": "Assinaturas", "c.consumomedio": "Consumo Médio", "c.status": "Status Cliente", "dtcad": "Data Cadastro", "c.\"cpf/cnpj\"": "CPF/CNPJ", "c.numcliente": "Número Cliente", "dtultalteracao": "Última Alteração", "c.celular_2": "Celular 2", "c.email": "Email", "c.rg": "RG", "c.emissor": "Emissor", "datainjecao": "Data Injeção", "c.idconsultor": "ID Consultor", "consultor_nome": "Representante", "consultor_celular": "Celular Consultor", "c.cep": "CEP", "c.endereco": "Endereço", "c.numero": "Número", "c.bairro": "Bairro", "c.complemento": "Complemento", "c.cnpj": "CNPJ (Empresa)", "c.razao": "Razão Social", "c.fantasia": "Nome Fantasia", "c.ufconsumo": "UF Consumo", "c.classificacao": "Classificação", "c.keycontrato": "Key Contrato", "c.keysigner": "Key Signer", "c.leadidsolatio": "Lead ID Solatio", "c.indcli": "Ind CLI", "c.enviadocomerc": "Enviado Comerci", "c.obs": "Observação", "c.posvenda": "Pós-venda", "c.retido": "Retido", "c.contrato_verificado": "Contrato Verificado", "c.rateio": "Rateio (S/N)", "c.validadosucesso": "Validação Sucesso (S/N)", "status_sucesso": "Status Validação", "c.documentos_enviados": "Documentos Enviados", "c.link_documento": "Link Documento", "c.caminhoarquivo": "Link Conta Energia", "c.caminhoarquivocnpj": "Link Cartão CNPJ", "c.caminhoarquivodoc1": "Link Doc Ident. 1", "c.caminhoarquivodoc2": "Link Doc Ident. 2", "c.caminhoarquivoenergia2": "Link Conta Energia 2", "c.caminhocontratosocial": "Link Contrato Social", "c.caminhocomprovante": "Link Comprovante", "c.caminhoarquivoestatutoconvencao": "Link Estatuto/Convenção", "c.senhapdf": "Senha PDF", "c.codigo": "Código Interno", "c.elegibilidade": "Elegibilidade", "c.idplanopj": "ID Plano PJ", "dtcancelado": "Data Cancelamento", "data_ativo_original": "Data Ativo Original", "c.fornecedora": "Fornecedora", "c.desconto_cliente": "Desconto Cliente", "dtnasc": "Data Nasc.", "c.origem": "Origem", "c.cm_tipo_pagamento": "Tipo Pagamento", "c.status_financeiro": "Status Financeiro", "c.logindistribuidora": "Login Distribuidora", "c.senhadistribuidora": "Senha Distribuidora", "c.nacionalidade": "Nacionalidade", "c.profissao": "Profissão", "c.estadocivil": "Estado Civil", "c.obs_compartilhada": "Observação Compartilhada", "c.linkassinatura1": "Link Assinatura", "c.cpf": "CPF Consultor", "c.uf": "UF Consultor", "quantidade_clientes_ativos": "Qtd. Clientes Ativos", "quantidade_registros_rcb": "Qtd. Boletos (RCB)", "nome_cliente_rateio": "Cliente Rateio", "devolutiva": "Devolutiva", "licenciado": "Licenciado", "chave_contrato": "Chave Contrato" }
     base_clientes_keys = [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "qtdeassinatura", "c.consumomedio", "c.status", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "dtultalteracao", "c.celular_2", "c.email", "c.rg", "c.emissor", "datainjecao", "c.idconsultor", "consultor_nome", "consultor_celular", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.keycontrato", "c.keysigner", "c.leadidsolatio", "c.indcli", "c.enviadocomerc", "c.obs", "c.posvenda", "c.retido", "c.contrato_verificado", "c.rateio", "c.validadosucesso", "status_sucesso", "c.documentos_enviados", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.codigo", "c.elegibilidade", "c.idplanopj", "dtcancelado", "data_ativo_original", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.origem", "c.cm_tipo_pagamento", "c.status_financeiro", "c.logindistribuidora", "c.senhadistribuidora", "c.nacionalidade", "c.profissao", "c.estadocivil", "c.obs_compartilhada", "c.linkassinatura1" ]
     base_rateio_keys = [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "c.cep", "consultor_nome", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ]
     rateio_rzk_keys = [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "devolutiva", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "licenciado", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "chave_contrato", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ]
     clientes_por_licenciado_keys = [ "c.idconsultor", "c.nome", "c.cpf", "c.email", "c.uf", "quantidade_clientes_ativos" ]
     boletos_por_cliente_keys = [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "c.fornecedora", "data_ativo", "quantidade_registros_rcb" ]
     keys_for_report = []
     if report_type == "base_clientes": keys_for_report = base_clientes_keys
     elif report_type == "clientes_por_licenciado": keys_for_report = clientes_por_licenciado_keys
     elif report_type == "boletos_por_cliente": keys_for_report = boletos_por_cliente_keys
     elif report_type == "rateio": keys_for_report = base_rateio_keys
     elif report_type == "rateio_rzk": keys_for_report = rateio_rzk_keys
     else: logger.warning(f"Tipo desconhecido '{report_type}' em get_headers."); return []
     headers_list = []; missing_keys_in_map = []
     for i, key in enumerate(keys_for_report):
        friendly_name = header_map.get(key)
        if friendly_name: headers_list.append(friendly_name)
        else:
            fallback_name = key.split('.')[-1].replace('_', ' ').title() if '.' in key else key.replace('_', ' ').title()
            headers_list.append(fallback_name); missing_keys_in_map.append(key)
     if missing_keys_in_map: logger.warning(f"Chaves não mapeadas em header_map p/ '{report_type}': {missing_keys_in_map}.")
     return headers_list