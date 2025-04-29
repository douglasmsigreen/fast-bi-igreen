# database.py
import psycopg2
import psycopg2.pool
import logging
from flask import g
from .config import Config
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
        # Importante: Commit implícito se não houver erro, mas para SELECT não é necessário.
        # Se fossem INSERT/UPDATE/DELETE, um conn.commit() explícito seria mais seguro aqui fora do 'with'.
        return result
    except psycopg2.OperationalError as e:
        logger.error(f"Erro operacional/conexão durante query: {e}", exc_info=False)
        # Tenta fechar a conexão "ruim" e removê-la do contexto g
        g.pop('db', None)
        try: conn.close()
        except: pass
        # Relança o erro para que a rota Flask saiba que algo deu errado
        raise RuntimeError(f"Erro de conexão ou operacional: {e}")
    except psycopg2.Error as e:
        # Log específico para erro comum de coluna
        if isinstance(e, psycopg2.errors.UndefinedColumn):
             logger.error(f"Erro de coluna indefinida: Verifique nomes na query/tabela. Detalhe: {e}", exc_info=False)
        else:
             logger.error(f"Erro de banco de dados ({type(e).__name__}): {e}", exc_info=False)
        # Tenta fazer rollback em caso de erro na transação (útil para INSERT/UPDATE/DELETE)
        try: conn.rollback()
        except Exception as rb_err: logger.error(f"Erro durante rollback: {rb_err}")
        # Relança o erro
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


# --- FUNÇÕES PARA RATEIO RZK ---
# (Estas funções permanecem inalteradas)
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

# <<< FUNÇÃO MODIFICADA para incluir e ordenar o campo 'qtd_rcb_cliente' >>>
def _get_recebiveis_clientes_fields() -> List[str]:
    """Retorna a lista de campos SQL para o relatório Recebíveis Clientes."""
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
        # --- INÍCIO NOVA COLUNA CALCULADA (Última Posição) ---
        """COUNT(rcb.idrcb) OVER (PARTITION BY c.idcliente) AS qtd_rcb_cliente""" # 40
        # --- FIM NOVA COLUNA CALCULADA ---
    ]

# Função com filtro de fornecedora já aplicado
def get_recebiveis_clientes_data(offset: int = 0, limit: Optional[int] = None, fornecedora: Optional[str] = None) -> List[tuple]:
    """Busca os dados paginados para o relatório 'Recebíveis Clientes'."""
    campos = _get_recebiveis_clientes_fields() # Já inclui status_calculado e qtd_rcb_cliente
    select_clause = f"SELECT {', '.join(campos)}"
    from_clause = 'FROM public."RCB_CLIENTES" rcb'
    join_clause = """
        LEFT JOIN public."CLIENTES" c ON rcb.numinstalacao = c.numinstalacao
        LEFT JOIN public."CONSULTOR" co ON c.idconsultor = co.idconsultor
    """
    # LÓGICA DO WHERE CLAUSE (com filtro de fornecedora)
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"] # Filtro base opcional
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s")
        params.append(fornecedora)
    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    # FIM DA LÓGICA DO WHERE CLAUSE

    order_by = "ORDER BY rcb.idrcb"
    limit_clause = "LIMIT %s" if limit is not None else ""
    offset_clause = "OFFSET %s" if offset > 0 else ""

    # Adiciona limit e offset aos parâmetros DEPOIS do filtro de fornecedora
    if limit is not None: params.append(limit)
    if offset > 0: params.append(offset)

    paginated_query = f"{select_clause} {from_clause} {join_clause} {where_clause} {order_by} {limit_clause} {offset_clause};"
    logger.debug(f"Executando query para Recebíveis Clientes (Pag): {paginated_query} com params: {params}")
    try:
        return execute_query(paginated_query, tuple(params)) or []
    except Exception as e:
        logger.error(f"Erro get_recebiveis_clientes_data: {e}", exc_info=True)
        return []

# Função com filtro de fornecedora já aplicado
def count_recebiveis_clientes(fornecedora: Optional[str] = None) -> int:
    """Conta o total de registros para o relatório 'Recebíveis Clientes'."""
    from_clause = 'FROM public."RCB_CLIENTES" rcb'
    join_clause = """
        LEFT JOIN public."CLIENTES" c ON rcb.numinstalacao = c.numinstalacao
        LEFT JOIN public."CONSULTOR" co ON c.idconsultor = co.idconsultor
    """
    # LÓGICA DO WHERE CLAUSE (com filtro de fornecedora)
    where_clauses = ["(c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))"] # Filtro base opcional
    params = []
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s")
        params.append(fornecedora)
    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    # FIM DA LÓGICA DO WHERE CLAUSE

    count_query_sql = f"SELECT COUNT(rcb.idrcb) {from_clause} {join_clause} {where_clause};"
    logger.debug(f"Executando query de contagem para Recebíveis Clientes: {count_query_sql} com params: {params}")
    try:
        result = execute_query(count_query_sql, tuple(params), fetch_one=True)
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"Erro count_recebiveis_clientes: {e}", exc_info=True)
        return 0

# --- FIM FUNÇÕES RECEBÍVEIS CLIENTES ---


# --- FUNÇÕES PARA O DASHBOARD (KPIs, Resumos, Gráficos) ---
# (Estas funções permanecem inalteradas)
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

# --- FIM FUNÇÕES DASHBOARD ---


# --- Funções Auxiliares para Relatórios (Campos e Cabeçalhos) ---
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

def get_fornecedoras() -> List[str]:
    """Busca lista única de fornecedoras."""
    query = 'SELECT DISTINCT fornecedora FROM public."CLIENTES" WHERE fornecedora IS NOT NULL AND fornecedora <> \'\' ORDER BY fornecedora;'
    try: results = execute_query(query); return sorted([str(f[0]) for f in results if f and f[0]]) if results else []
    except Exception as e: logger.error(f"Erro get_fornecedoras: {e}", exc_info=True); return []

# <<< FUNÇÃO MODIFICADA para incluir mapeamento e ordem de 'qtd_rcb_cliente' >>>
def get_headers(report_type: str) -> List[str]:
     """Retorna cabeçalhos legíveis baseados no tipo de relatório."""
     header_map = {
         # ... (mapeamentos existentes) ...
         "c.fornecedora": "Fornecedora", # Mapeamento genérico, pode ser sobreposto
         # Chaves de Recebíveis Clientes (baseadas no CSV e nos aliases usados)
         "rcb.idrcb": "Idrcb",
         "codigo_cliente": "Codigo Cliente",
         "cliente_nome": "Cliente",
         "rcb.numinstalacao": "Instalacao",
         "rcb.valorseria": "Quanto Seria",
         "rcb.valorapagar": "Valor A Pagar",
         "rcb.valorcomcashback": "Valor Com Cashback",
         "data_referencia": "Data Referencia",
         "data_vencimento": "Data Vencimento",
         "data_pagamento": "Data Pagamento",
         "data_vencimento_original": "Data Vencimento Original",
         "status_financeiro_cliente": "Status Financeiro Cliente",
         "id_licenciado": "Id Licenciado",
         "nome_licenciado": "Licenciado",
         "celular_licenciado": "Celular Licenciado",
         "status_calculado": "Status",
         "rcb.urldemonstrativo": "Url Demonstrativo",
         "rcb.urlboleto": "Url Boleto",
         "rcb.qrcode": "Qrcode Pix",
         "rcb.urlcontacemig": "Url Boleto Distribuidora",
         "valor_distribuidora": "Valor Distribuidora",
         "rcb.codigobarra": "Codigo Barra Boleto",
         "fornecedora_cliente": "Fornecedora", # Alias específico para Recebiveis
         "c.concessionaria": "Concessionaria",
         "cpf_cnpj_cliente": "Cpf",
         "rcb.nrodocumento": "Numero Documento",
         "rcb.idcomerc": "Idcomerc",
         "rcb.idbomfuturo": "Idbomfuturo",
         "rcb.energiainjetada": "Energia Injetada",
         "rcb.energiacompensada": "Energia Compensada",
         "rcb.energiaacumulada": "Energia Acumulada",
         "rcb.energiaajuste": "Energia Ajuste",
         "rcb.energiafaturamento": "Energia Faturamento",
         "qtd_rcb_cliente": "Qt de Rcb", # <<< ADICIONADO MAPEAMENTO
         # ... (outros mapeamentos podem existir para outras colunas/relatórios) ...
     }
     # Define a ORDEM das colunas para cada relatório
     keys_order = {
         "base_clientes": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "qtdeassinatura", "c.consumomedio", "c.status", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "dtultalteracao", "c.celular_2", "c.email", "c.rg", "c.emissor", "datainjecao", "c.idconsultor", "consultor_nome", "consultor_celular", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.keycontrato", "c.keysigner", "c.leadidsolatio", "c.indcli", "c.enviadocomerc", "c.obs", "c.posvenda", "c.retido", "c.contrato_verificado", "c.rateio", "c.validadosucesso", "status_sucesso", "c.documentos_enviados", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.codigo", "c.elegibilidade", "c.idplanopj", "dtcancelado", "data_ativo_original", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.origem", "c.cm_tipo_pagamento", "c.status_financeiro", "c.logindistribuidora", "c.senhadistribuidora", "c.nacionalidade", "c.profissao", "c.estadocivil", "c.obs_compartilhada", "c.linkassinatura1" ],
         "rateio": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "c.cep", "consultor_nome", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ],
         "rateio_rzk": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo", "c.consumomedio", "devolutiva", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "licenciado", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "chave_contrato", "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "nome_cliente_rateio", "c.nacionalidade", "c.profissao", "c.estadocivil" ],
         "clientes_por_licenciado": [ "c.idconsultor", "c.nome", "c.cpf", "c.email", "c.uf", "quantidade_clientes_ativos" ],
         "boletos_por_cliente": [ "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "c.fornecedora", "data_ativo", "dias_ativo", "quantidade_registros_rcb" ],
         # <<< LISTA MODIFICADA para incluir 'qtd_rcb_cliente' no final >>>
         "recebiveis_clientes": [
             "rcb.idrcb",                        # 1
             "codigo_cliente",                   # 2
             "cliente_nome",                     # 3
             "rcb.numinstalacao",                # 4
             "rcb.valorseria",                   # 5
             "rcb.valorapagar",                  # 6
             "rcb.valorcomcashback",             # 7
             "data_referencia",                  # 8
             "data_vencimento",                  # 9
             "data_pagamento",                   # 10
             "data_vencimento_original",         # 11
             "c.celular",                        # 12
             "c.email",                          # 13
             "status_financeiro_cliente",        # 14
             "c.numcliente",                     # 15
             "id_licenciado",                    # 16
             "nome_licenciado",                  # 17
             "celular_licenciado",               # 18
             "status_calculado",                 # 19
             "rcb.urldemonstrativo",             # 20
             "rcb.urlboleto",                    # 21
             "rcb.qrcode",                       # 22
             "rcb.urlcontacemig",                # 23
             "valor_distribuidora",              # 24
             "rcb.codigobarra",                  # 25
             "c.ufconsumo",                      # 26
             "fornecedora_cliente",              # 27
             "c.concessionaria",                 # 28
             "c.cnpj",                           # 29
             "cpf_cnpj_cliente",                 # 30
             "rcb.nrodocumento",                 # 31
             "rcb.idcomerc",                     # 32
             "rcb.idbomfuturo",                  # 33
             "rcb.energiainjetada",              # 34
             "rcb.energiacompensada",            # 35
             "rcb.energiaacumulada",             # 36
             "rcb.energiaajuste",                # 37
             "rcb.energiafaturamento",           # 38
             "c.desconto_cliente",               # 39
             # --- INÍCIO NOVA CHAVE (Última Posição) ---
             "qtd_rcb_cliente"                   # 40
             # --- FIM NOVA CHAVE ---
         ]
     }
     report_keys = keys_order.get(report_type.lower())
     if not report_keys:
         logger.warning(f"Ordem de chaves não definida para '{report_type}' em get_headers.")
         if report_type.lower() == 'recebiveis_clientes':
              try:
                  # Tenta obter aliases ou nomes de colunas da função _get_fields
                  report_keys = [f.split(' AS ')[-1].strip() for f in _get_recebiveis_clientes_fields()]
                  logger.info(f"Usando ordem de campos da query como fallback para headers de '{report_type}'.")
              except Exception: return []
         else:
              return []

     headers_list = []
     for key in report_keys:
         # Tenta mapear a chave completa primeiro (útil para aliases como 'status_calculado')
         header = header_map.get(key)
         if not header:
             # Se não encontrou, tenta mapear a parte principal (ex: 'c.idcliente' ou 'idcliente')
             base_key = key.split('.')[-1] # Remove prefixo tipo 'c.' ou 'rcb.'
             header = header_map.get(base_key, key.replace('_', ' ').title()) # Fallback final
         headers_list.append(header)

     missing_keys = [key for key in report_keys if key not in header_map and key.split('.')[-1] not in header_map]
     if missing_keys:
         logger.warning(f"Chaves/Aliases não mapeados em header_map para '{report_type}': {missing_keys}")
     return headers_list
# --- FIM DA FUNÇÃO get_headers ---


# --- FUNÇÕES PARA GRÁFICOS DO DASHBOARD ---
# (Estas funções permanecem inalteradas)
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