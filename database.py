# database.py
import psycopg2
import psycopg2.pool
import logging
from flask import g # Usaremos o 'g' object do Flask para gerir conexões por requisição
from config import Config # Assumindo que config.py está no mesmo nível
from typing import List, Tuple, Optional, Any # Adicionado List, Tuple, Optional, Any

# Configuração do Logger específico para este módulo
logger = logging.getLogger(__name__)

# Variável global para o pool de conexões
db_pool = None

# --- Funções de Pool e Conexão ---
def init_pool():
    """Inicializa o pool de conexões."""
    global db_pool
    # Só inicializa se ainda não existir
    if db_pool:
        return
    try:
        logger.info("Inicializando pool de conexões com o banco de dados...")
        db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=5, **Config.DB_CONFIG # Use as configs de config.py
        )
        # Testa a conexão inicial pegando e devolvendo uma conexão
        conn = db_pool.getconn()
        db_pool.putconn(conn)
        logger.info("Pool de conexões inicializado com sucesso.")
    except (psycopg2.Error, KeyError, Exception) as e: # Adiciona KeyError para DB_CONFIG
        logger.critical(f"Falha CRÍTICA ao inicializar pool de conexões: {e}", exc_info=True)
        db_pool = None # Garante que o pool é None se falhar
        # Levanta uma exceção para sinalizar que a inicialização falhou
        raise ConnectionError(f"Não foi possível inicializar o pool DB: {e}")

def get_db():
    """Obtém uma conexão do pool para a requisição Flask atual (g)."""
    # Se o pool não foi inicializado com sucesso antes, tenta inicializar agora.
    if not db_pool:
        try:
            logger.warning("Pool não inicializado. Tentando inicializar em get_db...")
            init_pool()
        except ConnectionError as e:
             # Se a inicialização falhar aqui, não há o que fazer.
             logger.error(f"Tentativa de inicializar pool falhou em get_db: {e}")
             raise ConnectionError("Pool de conexões não está disponível.")

    # Verifica se já existe uma conexão no contexto 'g' desta requisição
    if 'db' not in g:
        try:
            # Se não existe, pega uma conexão do pool e armazena em 'g'
            g.db = db_pool.getconn()
            logger.debug("Conexão obtida do pool para a requisição.")
        except psycopg2.Error as e:
             # Se falhar ao pegar conexão, loga e levanta erro
             logger.error(f"Falha ao obter conexão do pool: {e}", exc_info=True)
             raise ConnectionError(f"Não foi possível obter conexão do banco: {e}")
    # Retorna a conexão armazenada em 'g'
    return g.db

def close_db(e=None):
    """Fecha a conexão (devolve ao pool) ao final da requisição Flask."""
    # Remove a conexão do contexto 'g', retornando None se não existir
    db = g.pop('db', None)

    # Se uma conexão foi removida e o pool existe
    if db is not None and db_pool is not None:
        try:
            # Tenta devolver a conexão ao pool
            db_pool.putconn(db)
            logger.debug("Conexão devolvida ao pool.")
        except psycopg2.Error as e:
             # Se falhar ao devolver, loga e tenta fechar a conexão diretamente
             logger.error(f"Falha ao devolver conexão ao pool: {e}", exc_info=True)
             try: db.close()
             except: pass # Ignora erros ao fechar
    elif db is not None:
         # Se a conexão existe mas o pool não (caso raro), apenas tenta fechar
         try:
             db.close()
             logger.debug("Conexão fechada (pool não disponível).")
         except: pass

# --- Função Principal para Executar Queries ---
def execute_query(query: str, params: Optional[tuple] = None, fetch_one=False) -> List[tuple] or Tuple or None:
    """Executa uma query SQL usando a conexão da requisição atual."""
    conn = get_db() # Garante que temos uma conexão válida
    result = None # Inicializa o resultado
    try:
        # Usa um cursor dentro de um bloco 'with' para garantir que seja fechado
        with conn.cursor() as cur:
            # logger.info(f"Executando query: {query[:200]}... com params: {params}") # Log mais detalhado
            cur.execute(query, params or ()) # Executa a query com os parâmetros
            # Verifica se é para buscar apenas um resultado ou todos
            if fetch_one:
                result = cur.fetchone()
                # logger.debug(f"Query retornou 1 registro (ou None).")
            else:
                result = cur.fetchall()
                # Log apenas se houver muitos resultados para não poluir
                if len(result or []) > 10: # Adicionado 'or []' para segurança
                    logger.info(f"Query retornou {len(result)} registros.")
                elif result:
                     logger.debug(f"Query retornou {len(result)} registros.")
                else:
                     logger.debug("Query não retornou registros.")

        # conn.commit() # Geralmente não necessário para SELECTs. Descomente se fizer INSERT/UPDATE/DELETE.
        return result
    except psycopg2.OperationalError as e:
        # Erro de conexão ou operacional pode invalidar a conexão
        logger.error(f"Erro operacional/conexão durante query: {e}", exc_info=True)
        g.pop('db', None) # Remove a conexão potencialmente ruim do contexto 'g'
        try: conn.close() # Tenta fechar a conexão diretamente
        except: pass
        # Levanta um erro Runtime para ser tratado pela rota Flask
        raise RuntimeError(f"Erro de conexão ou operacional: {e}")
    except psycopg2.Error as e:
        # Outros erros do psycopg2 (sintaxe SQL, tipo de dado, etc.)
        # IMPORTANTE: Verifica se o erro é por coluna inexistente (ajuste para nomes reais)
        if isinstance(e, psycopg2.errors.UndefinedColumn):
             logger.error(f"Erro de coluna indefinida: Verifique se nomes como 'codigo', 'email', ou \"password\" estão corretos na query e na tabela 'USUARIOS'. Detalhe: {e}", exc_info=True)
        else:
             logger.error(f"Erro de banco de dados ({type(e).__name__}): {e}", exc_info=True)
        try: conn.rollback() # Tenta reverter a transação
        except Exception as rb_err: logger.error(f"Erro durante rollback: {rb_err}")
        # Levanta um erro Runtime
        raise RuntimeError(f"Erro ao executar a query: {e}")
    except Exception as e:
        # Captura qualquer outro erro inesperado
        logger.error(f"Erro inesperado durante query ({type(e).__name__}): {e}", exc_info=True)
        try: conn.rollback() # Tenta reverter
        except Exception as rb_err: logger.error(f"Erro durante rollback: {rb_err}")
        # Levanta um erro Runtime
        raise RuntimeError(f"Erro inesperado: {e}")

# --- Funções Específicas para Bases Rateio (Exportação - com filtro fornecedora) ---
def get_base_nova_ids(fornecedora: Optional[str] = None) -> List[int]:
    """
    Busca os IDs de cliente para a 'Base Nova' do Rateio,
    OPCIONALMENTE filtrando por fornecedora. Usa 'idcliente' como id (VERIFICAR).
    """
    query_base = """
        SELECT DISTINCT cc.idcliente
        FROM public."CLIENTES_CONTRATOS" cc
        INNER JOIN public."CLIENTES_CONTRATOS_SIGNER" ccs ON cc.idcliente_contrato = ccs.idcliente_contrato
        INNER JOIN public."CLIENTES" c ON cc.idcliente = c.idcliente -- Assumindo join por idcliente
    """
    group_by = """
        GROUP BY cc.idcliente_contrato, cc.idcliente
        HAVING bool_and(ccs.signature_at IS NOT NULL)
    """
    where_clauses = [
        "cc.type_document = 'procuracao_igreen'",
        "upper(cc.status) = 'ATIVO'",
        "c.data_ativo IS NOT NULL",
        "c.status IS NULL",
        "c.validadosucesso = 'S'",
        "c.rateio = 'N'"
    ]
    params = []

    # Adiciona filtro de fornecedora se aplicável (usa 'consolidado' internamente para "todos")
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s")
        params.append(fornecedora)
        logger.info(f"Executando query para buscar IDs da Base Nova (Fornecedora: {fornecedora})...")
    else:
        logger.info("Executando query para buscar IDs da Base Nova (Todas Fornecedoras)...")

    full_query = query_base + " WHERE " + " AND ".join(where_clauses) + group_by + ";"

    try:
        results = execute_query(full_query, tuple(params))
        # Assume que idcliente é o campo correto (se for c.codigo, ajuste aqui)
        return [row[0] for row in results] if results else []
    except (RuntimeError, ConnectionError) as e:
        logger.error(f"Erro ao buscar IDs da Base Nova: {e}")
        return []

def get_base_enviada_ids(fornecedora: Optional[str] = None) -> List[int]:
    """
    Busca os IDs de cliente para a 'Base Enviada' do Rateio (rateio = 'S'),
    OPCIONALMENTE filtrando por fornecedora. Usa 'idcliente' (VERIFICAR).
    """
    # IMPORTANTE: Verifique se o campo a selecionar aqui é 'idcliente' ou 'codigo'
    # Assumindo que 'idcliente' é a chave estrangeira correta em CLIENTES
    query_base = """
        SELECT c.idcliente -- <<< VERIFICAR ESTE CAMPO (idcliente ou codigo?)
        FROM public."CLIENTES" c
    """
    where_clauses = ["c.rateio = 'S'"]
    params = []

    # Adiciona filtro de fornecedora se aplicável (usa 'consolidado' internamente para "todos")
    if fornecedora and fornecedora.lower() != 'consolidado':
        where_clauses.append("c.fornecedora = %s")
        params.append(fornecedora)
        logger.info(f"Executando query para buscar IDs da Base Enviada (Fornecedora: {fornecedora})...")
    else:
        logger.info("Executando query para buscar IDs da Base Enviada (Todas Fornecedoras)...")

    full_query = query_base + " WHERE " + " AND ".join(where_clauses) + ";"

    try:
        results = execute_query(full_query, tuple(params))
        return [row[0] for row in results] if results else []
    except (RuntimeError, ConnectionError) as e:
        logger.error(f"Erro ao buscar IDs da Base Enviada: {e}")
        return []

# --- Função para buscar detalhes completos por lista de IDs (com Batching) ---
def get_client_details_by_ids(report_type: str, client_ids: List[int], batch_size: int = 1000) -> List[tuple]:
    """
    Busca os detalhes completos dos clientes para uma lista de IDs,
    usando batching e corrigindo parâmetro ANY. Usa 'idcliente' no WHERE (VERIFICAR).
    """
    if not client_ids:
        logger.warning("get_client_details_by_ids chamada com lista de IDs vazia.")
        return []

    all_details = [] # Acumula os resultados de todos os lotes
    try:
        # Prepara a estrutura base da query uma vez
        campos = _get_query_fields(report_type)
        if not campos:
             logger.error(f"Não foi possível obter campos SQL para o report_type: {report_type}")
             return [] # Retorna vazio se não conseguir os campos
        select = f"SELECT {', '.join(campos)}"
        from_ = 'FROM public."CLIENTES" c'
        needs_consultor_join = any(f.startswith("co.") for f in campos)
        join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor' if needs_consultor_join else ""
        # Cláusula WHERE com ANY(%s). Assume que o campo chave em CLIENTES é 'idcliente'.
        # Se for 'codigo', altere aqui.
        where = "WHERE c.idcliente = ANY(%s)" # <<< VERIFICAR: É idcliente ou codigo?
        order = "ORDER BY c.idcliente" # Mantém a ordem consistente (use o mesmo campo do WHERE)
        query = f"{select} {from_}{join} {where} {order};"

        logger.info(f"Iniciando busca de detalhes para {len(client_ids)} IDs em lotes de {batch_size}...")

        # Itera sobre a lista de IDs em pedaços (batches)
        for i in range(0, len(client_ids), batch_size):
            batch_ids = client_ids[i:i + batch_size] # Pega o lote atual de IDs
            params = (batch_ids,) # Cria uma tupla contendo a lista/tupla do batch
            logger.debug(f"Buscando detalhes para batch #{i//batch_size + 1} com {len(batch_ids)} IDs...")
            batch_results = execute_query(query, params) # Passa a tupla contendo a lista
            if batch_results:
                all_details.extend(batch_results)
            # logger.debug(f"Batch #{i//batch_size + 1} concluído. Total acumulado: {len(all_details)}")

        logger.info(f"Busca de detalhes concluída. Total de {len(all_details)} registros encontrados para {len(client_ids)} IDs solicitados.")
        return all_details

    except (RuntimeError, ConnectionError) as e:
        logger.error(f"Erro de Banco/Conexão em get_client_details_by_ids para {len(client_ids)} IDs: {e}", exc_info=False)
        return []
    except Exception as e:
        logger.error(f"Erro inesperado em get_client_details_by_ids: {e}", exc_info=True)
        return []

# --- FUNÇÕES PARA RELATÓRIO 'Clientes por Licenciado' ---

def get_clientes_por_licenciado_data(offset: int = 0, limit: Optional[int] = None) -> List[tuple]:
    """Busca os dados para o relatório 'Quantidade de Clientes por Licenciado', com paginação."""
    logger.info(f"Buscando dados para 'Clientes por Licenciado' - Offset: {offset}, Limit: {limit}")
    base_query = """
        SELECT
            c.idconsultor,
            c.nome,
            c.cpf,
            c.email,
            c.uf,
            COUNT(cl.idconsultor) AS quantidade_clientes_ativos
        FROM
            public."CONSULTOR" c
        LEFT JOIN
            public."CLIENTES" cl ON c.idconsultor = cl.idconsultor
        WHERE
            cl.data_ativo IS NOT NULL -- Apenas clientes ativos
        GROUP BY
            c.idconsultor, c.nome, c.cpf, c.email, c.uf
        ORDER BY
            quantidade_clientes_ativos DESC, c.nome -- Ordem secundária pelo nome
    """
    params = []
    limit_clause = ""
    offset_clause = ""

    if limit is not None:
        limit_clause = "LIMIT %s"
        params.append(limit)
    if offset > 0:
        offset_clause = "OFFSET %s"
        params.append(offset)

    paginated_query = f"{base_query} {limit_clause} {offset_clause};"
    params_t = tuple(params)

    try:
        results = execute_query(paginated_query, params_t)
        logger.info(f"Retornados {len(results) if results else 0} registros para 'Clientes por Licenciado'.")
        return results if results else []
    except (RuntimeError, ConnectionError) as e:
        logger.error(f"Erro ao buscar dados para 'Clientes por Licenciado': {e}")
        return [] # Retorna lista vazia em caso de erro
    except Exception as e:
        logger.error(f"Erro inesperado em get_clientes_por_licenciado_data: {e}", exc_info=True)
        return []


def count_clientes_por_licenciado() -> int:
    """Conta o número total de consultores (linhas) que teriam clientes ativos."""
    logger.info("Contando total de registros para 'Clientes por Licenciado'...")
    # Usamos uma subquery para contar as linhas *após* o GROUP BY da query original
    # e garantimos que só contamos consultores com clientes ativos (JOIN/WHERE)
    count_query_sql = """
        SELECT COUNT(DISTINCT c.idconsultor) -- Conta consultores distintos
        FROM public."CONSULTOR" c
        INNER JOIN public."CLIENTES" cl ON c.idconsultor = cl.idconsultor -- INNER JOIN garante que só conta quem tem cliente
        WHERE cl.data_ativo IS NOT NULL; -- Filtra por clientes ativos
    """
    # Alternativa (contando após o group by, como antes, mas mais complexa):
    # count_query_sql = """
    #     SELECT COUNT(*)
    #     FROM (
    #         SELECT
    #             c.idconsultor
    #         FROM
    #             public."CONSULTOR" c
    #         LEFT JOIN
    #             public."CLIENTES" cl ON c.idconsultor = cl.idconsultor
    #         WHERE
    #             cl.data_ativo IS NOT NULL
    #         GROUP BY
    #             c.idconsultor -- Só precisa agrupar por id para contar
    #     ) AS subquery_count;
    # """
    try:
        result = execute_query(count_query_sql, fetch_one=True)
        count = result[0] if result else 0
        logger.info(f"Contagem total para 'Clientes por Licenciado': {count}")
        return count
    except (RuntimeError, ConnectionError) as e:
        logger.error(f"Erro ao contar registros para 'Clientes por Licenciado': {e}")
        return 0 # Retorna 0 em caso de erro
    except Exception as e:
        logger.error(f"Erro inesperado em count_clientes_por_licenciado: {e}", exc_info=True)
        return 0

# --- FUNÇÕES PARA RELATÓRIO 'Quantidade de Boletos por Cliente' (MODIFICADAS) ---

def get_boletos_por_cliente_data(offset: int = 0, limit: Optional[int] = None) -> List[tuple]:
    """Busca os dados para o relatório 'Quantidade de Boletos por Cliente', com paginação."""
    logger.info(f"Buscando dados para 'Quantidade de Boletos por Cliente' - Offset: {offset}, Limit: {limit}")
    # --- Query Modificada ---
    base_query = """
        SELECT
            c.idcliente,
            c.nome,
            c.numinstalacao,
            c.celular,
            c.cidade,
            CASE
                WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN ''
                ELSE (c.uf || '-' || c.concessionaria)
            END AS regiao,
            c.fornecedora,  -- <<< COLUNA ADICIONADA AO SELECT
            TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo,
            COUNT(rcb.numinstalacao) AS quantidade_registros_rcb -- Conta registros na tabela RCB
        FROM
            public."CLIENTES" c
        LEFT JOIN
            public."RCB_CLIENTES" rcb ON c.numinstalacao = rcb.numinstalacao -- JOIN por numinstalacao
        GROUP BY
            c.idcliente, c.nome, c.numinstalacao, c.celular, c.cidade, regiao,
            c.fornecedora,  -- <<< COLUNA ADICIONADA AO GROUP BY
            data_ativo
        ORDER BY
            c.idcliente -- A ordenação principal é feita aqui
    """
    # --- Fim da Modificação da Query ---

    params = []
    limit_clause = ""
    offset_clause = ""

    if limit is not None:
        limit_clause = "LIMIT %s"
        params.append(limit)
    if offset > 0:
        offset_clause = "OFFSET %s"
        params.append(offset)

    # Adiciona paginação à query base
    paginated_query = f"{base_query} {limit_clause} {offset_clause};"
    params_t = tuple(params)

    try:
        results = execute_query(paginated_query, params_t)
        logger.info(f"Retornados {len(results) if results else 0} registros para 'Boletos por Cliente'.")
        # A query agora retorna 9 colunas por linha
        return results if results else []
    except (RuntimeError, ConnectionError) as e:
        logger.error(f"Erro ao buscar dados para 'Boletos por Cliente': {e}")
        return [] # Retorna lista vazia em caso de erro
    except Exception as e:
        logger.error(f"Erro inesperado em get_boletos_por_cliente_data: {e}", exc_info=True)
        return []


def count_boletos_por_cliente() -> int:
    """Conta o número total de clientes (linhas) que seriam retornados pela query principal."""
    logger.info("Contando total de registros para 'Quantidade de Boletos por Cliente'...")
    # Conta quantos clientes únicos existem (base da agregação)
    count_query_sql = """
        SELECT COUNT(DISTINCT c.idcliente)
        FROM public."CLIENTES" c;
        -- Não precisamos do JOIN aqui, pois a agregação na query principal é por cliente,
        -- e COUNT(*) no GROUP BY retornaria 1 para cada cliente, mesmo sem boletos.
        -- Se a intenção fosse contar *apenas* clientes que *têm* boletos, o JOIN seria necessário.
    """
    # Alternativa (contando após o group by, como antes):
    # count_query_sql = """
    #     SELECT COUNT(*)
    #     FROM (
    #         SELECT
    #             c.idcliente -- Seleciona apenas uma coluna do GROUP BY para a contagem
    #         FROM
    #             public."CLIENTES" c
    #         LEFT JOIN
    #             public."RCB_CLIENTES" rcb ON c.numinstalacao = rcb.numinstalacao
    #         GROUP BY
    #             c.idcliente -- Só precisa agrupar pelo ID do cliente
    #             # ... outros campos do group by original não são necessários para a contagem
    #     ) AS subquery_count;
    # """
    try:
        result = execute_query(count_query_sql, fetch_one=True)
        count = result[0] if result else 0
        logger.info(f"Contagem total para 'Boletos por Cliente': {count}")
        return count
    except (RuntimeError, ConnectionError) as e:
        logger.error(f"Erro ao contar registros para 'Boletos por Cliente': {e}")
        return 0 # Retorna 0 em caso de erro
    except Exception as e:
        logger.error(f"Erro inesperado em count_boletos_por_cliente: {e}", exc_info=True)
        return 0

# --- FIM DAS FUNÇÕES PARA RELATÓRIO 'Boletos por Cliente' ---

# --- FUNÇÃO PARA MAPA DO DASHBOARD --- <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< NOVO

def get_client_count_by_state() -> List[Tuple[str, int]]:
    """Busca a contagem de clientes ativos por estado (UF)."""
    logger.info("Buscando contagem de clientes ativos por estado...")
    # Certifique-se que a tabela CLIENTES tem a coluna 'uf' e 'data_ativo'
    # Use a coluna chave primária correta em COUNT() (ex: c.idcliente ou c.codigo)
    query = """
        SELECT
            UPPER(c.uf) as estado_uf, -- Garante UF em maiúsculas (padrão para mapas)
            COUNT(c.idcliente) as total_clientes -- Use a chave primária correta
        FROM
            public."CLIENTES" c
        WHERE
            c.data_ativo IS NOT NULL -- Filtra por clientes ativos (ou outra condição desejada)
            AND c.uf IS NOT NULL AND c.uf <> '' -- Garante que UF existe e não está vazia
        GROUP BY
            UPPER(c.uf)
        ORDER BY
            estado_uf;
    """
    try:
        # Usa a função execute_query existente
        results = execute_query(query)
        # Retorna como lista de tuplas ('UF', count), ex: [('SP', 150), ('MG', 120), ...]
        return results if results else []
    except (RuntimeError, ConnectionError) as e:
        logger.error(f"Erro ao buscar contagem de clientes por estado: {e}")
        return [] # Retorna lista vazia em caso de erro
    except Exception as e:
         logger.error(f"Erro inesperado em get_client_count_by_state: {e}", exc_info=True)
         return []

# --- FIM FUNÇÃO PARA MAPA --- <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

# --- Funções para Estrutura de Query e Cabeçalhos ---

def _get_query_fields(report_type: str) -> List[str]:
     """Retorna a lista de campos SQL a serem selecionados na query,
        baseado no tipo de relatório. Adapta nomes como 'idcliente' se necessário."""
     report_type = report_type.lower()

     # Campos para Base Clientes (Adapte 'c.idcliente' para 'c.codigo' se for o PK)
     # <<< VERIFICAR: Usar c.idcliente ou c.codigo como chave primária em CLIENTES?
     #    Assumindo c.idcliente por enquanto. Se for c.codigo, substituir onde apropriado.
     base_clientes_fields = [
        "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade",
        "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao", # Usa UF se concessionaria vazia
        "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "(COALESCE(c.qtdeassinatura, 0)::text || '/4') AS qtdeassinatura", # COALESCE para evitar erro com NULL
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
        "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.codigo", # Aqui usa 'codigo' (é diferente de idcliente?)
        "c.elegibilidade", "c.idplanopj", "TO_CHAR(c.dtcancelado, 'DD/MM/YYYY') AS dtcancelado",
        "TO_CHAR(c.data_ativo_original, 'DD/MM/YYYY') AS data_ativo_original", "c.fornecedora",
        "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc", "c.origem",
        "c.cm_tipo_pagamento", "c.status_financeiro", "c.logindistribuidora", "c.senhadistribuidora",
        "c.nacionalidade", "c.profissao", "c.estadocivil", "c.obs_compartilhada", "c.linkassinatura1"
     ]

     # Campos para Rateio (Adapte 'c.idcliente' se necessário)
     rateio_fields = [
        "c.idcliente", # <<< VERIFICAR CHAVE
        "c.nome", "c.numinstalacao", "c.celular", "c.cidade",
        "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN c.uf ELSE (c.uf || '-' || c.concessionaria) END AS regiao",
        "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "c.consumomedio", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad",
        "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor", "c.cep", "c.endereco", "c.numero",
        "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao",
        "c.link_documento", "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2",
        "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante", "c.caminhoarquivoestatutoconvencao",
        "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc",
        "c.logindistribuidora", "c.senhadistribuidora", "c.nacionalidade", "co.nome AS consultor_nome", # Adicionado alias para nome do cliente se precisar c.nome as nome_cliente
        "c.profissao", "c.estadocivil" # Removido c.nome duplicado, adicionado alias para co.nome
     ]

     # Não definimos campos aqui para 'clientes_por_licenciado' nem 'boletos_por_cliente',
     # pois as queries são customizadas e não usam esta função.
     if report_type == "rateio":
          logger.debug(f"Selecionando campos SQL para o tipo: {report_type}")
          return rateio_fields
     elif report_type == "base_clientes":
          logger.debug(f"Selecionando campos SQL para o tipo: {report_type}")
          return base_clientes_fields
     else:
          # Se um tipo desconhecido for passado, retorna vazio.
          logger.debug(f"Tipo '{report_type}' não mapeado em _get_query_fields ou possui query customizada. Retornando lista vazia.")
          return []


def build_query(report_type: str, fornecedora: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> Tuple[str, tuple]:
    """Constrói a query principal para buscar dados para exibição na tela (paginada).
       NÃO USAR para tipos com query customizada ('clientes_por_licenciado', 'boletos_por_cliente')."""
    campos = _get_query_fields(report_type)
    if not campos:
        # Levanta erro se for um tipo que deveria ter campos definidos aqui
        if report_type in ["base_clientes", "rateio"]:
             raise ValueError(f"Campos não definidos para report_type '{report_type}' em _get_query_fields")
        else:
             # Para tipos com query customizada, isso não deve ser chamado.
             raise ValueError(f"build_query não deve ser chamado para report_type '{report_type}' com query customizada.")

    select = f"SELECT {', '.join(campos)}"
    from_ = 'FROM public."CLIENTES" c'

    needs_consultor_join = any(f.startswith("co.") for f in campos)
    join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor' if needs_consultor_join else ""

    where_clauses = []
    params = []
    # Adiciona filtro de fornecedora (usa 'consolidado' internamente para "todos")
    if fornecedora and fornecedora.lower() != "consolidado":
         where_clauses.append("c.fornecedora = %s")
         params.append(fornecedora)

    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    # Ordenação para exibição paginada (use o campo chave primária correto)
    order = "ORDER BY c.idcliente" # <<< VERIFICAR: É idcliente ou codigo?
    limit_clause = f"LIMIT %s" if limit is not None else ""
    if limit is not None: params.append(limit)
    offset_clause = f"OFFSET %s" if offset > 0 else ""
    if offset > 0: params.append(offset)

    query = f"{select} {from_}{join} {where} {order} {limit_clause} {offset_clause};"
    params_t = tuple(params)
    # logger.debug(f"Query Tela ({report_type}): {query[:300]}... Params: {params_t}")
    return query, params_t

def count_query(report_type: str, fornecedora: Optional[str] = None) -> Tuple[str, tuple]:
    """Constrói uma query para contar o total de registros com os mesmos filtros da exibição na tela.
       NÃO USAR para tipos com query de contagem própria."""
    if report_type in ['clientes_por_licenciado', 'boletos_por_cliente']:
        raise ValueError(f"count_query não deve ser chamado para '{report_type}'. Use a função de contagem específica.")

    from_ = 'FROM public."CLIENTES" c'
    # Não precisamos de JOIN para contar clientes se o filtro é só na tabela CLIENTES
    where_clauses = []
    params = []
    # Aplica o mesmo filtro de fornecedora da tela para a contagem (usa 'consolidado' internamente para "todos")
    if fornecedora and fornecedora.lower() != "consolidado":
         where_clauses.append("c.fornecedora = %s")
         params.append(fornecedora)

    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    # Contar pela chave primária é geralmente mais eficiente e seguro (use o campo chave correto)
    query = f"SELECT COUNT(c.idcliente) {from_} {where};" # <<< VERIFICAR: É idcliente ou codigo?
    params_t = tuple(params)
    # logger.debug(f"Count Query ({report_type}): {query} Params: {params_t}")
    return query, params_t

def get_fornecedoras() -> List[str]:
    """Busca a lista distinta de fornecedoras."""
    query = 'SELECT DISTINCT fornecedora FROM public."CLIENTES" WHERE fornecedora IS NOT NULL AND fornecedora <> \'\' ORDER BY fornecedora;'
    try:
        results = execute_query(query)
        # Retorna a lista ordenada, garantindo que são strings não vazias
        return sorted([str(f[0]) for f in results if f and f[0]]) if results else []
    except (ConnectionError, RuntimeError) as e:
        logger.error(f"Erro ao buscar fornecedoras: {e}")
        return []
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar fornecedoras: {e}", exc_info=True)
        return []

def get_headers(report_type: str) -> List[str]:
    """Retorna os cabeçalhos legíveis e ordenados baseado no tipo de relatório."""
    report_type = report_type.lower()

    # Mapa de chaves (alias/nomes de campo) para nomes de cabeçalho amigáveis
    # (Este mapa parece correto, vamos mantê-lo)
    header_map = {
        # Campos Comuns (revisar se todos são usados e mapeados corretamente)
        "c.idcliente": "Código Cliente", "c.nome": "Nome", "c.numinstalacao": "Instalação",
        "c.celular": "Celular", "c.cidade": "Cidade", "regiao": "Região (UF-Conc)", # Alias 'regiao'
        "data_ativo": "Data Ativo", # Alias 'data_ativo'
        "qtdeassinatura": "Assinaturas", # Alias 'qtdeassinatura'
        "c.consumomedio": "Consumo Médio", "c.status": "Status Cliente", "dtcad": "Data Cadastro", # Alias 'dtcad'
        "c.\"cpf/cnpj\"": "CPF/CNPJ", "c.numcliente": "Número Cliente", "dtultalteracao": "Última Alteração", # Alias 'dtultalteracao'
        "c.celular_2": "Celular 2", "c.email": "Email", "c.rg": "RG", "c.emissor": "Emissor",
        "datainjecao": "Data Injeção", # Alias 'datainjecao'
        "c.idconsultor": "ID Consultor", "consultor_nome": "Nome Consultor", # Alias 'consultor_nome'
        "consultor_celular": "Celular Consultor", # Alias 'consultor_celular'
        "c.cep": "CEP", "c.endereco": "Endereço", "c.numero": "Número", "c.bairro": "Bairro",
        "c.complemento": "Complemento", "c.cnpj": "CNPJ (Empresa)", "c.razao": "Razão Social",
        "c.fantasia": "Nome Fantasia", "c.ufconsumo": "UF Consumo", "c.classificacao": "Classificação",
        "c.keycontrato": "Key Contrato", "c.keysigner": "Key Signer", "c.leadidsolatio": "Lead ID Solatio",
        "c.indcli": "Ind CLI", "c.enviadocomerc": "Enviado Comerci", "c.obs": "Observação",
        "c.posvenda": "Pós-venda", "c.retido": "Retido", "c.contrato_verificado": "Contrato Verificado",
        "c.rateio": "Rateio (S/N)", "c.validadosucesso": "Validação Sucesso (S/N)",
        "status_sucesso": "Status Validação", # Alias 'status_sucesso'
        "c.documentos_enviados": "Documentos Enviados", "c.link_documento": "Link Documento",
        "c.caminhoarquivo": "Link Conta Energia", "c.caminhoarquivocnpj": "Link Cartão CNPJ",
        "c.caminhoarquivodoc1": "Link Doc Ident. 1", "c.caminhoarquivodoc2": "Link Doc Ident. 2",
        "c.caminhoarquivoenergia2": "Link Conta Energia 2", "c.caminhocontratosocial": "Link Contrato Social",
        "c.caminhocomprovante": "Link Comprovante", "c.caminhoarquivoestatutoconvencao": "Link Estatuto/Convenção",
        "c.senhapdf": "Senha PDF", "c.codigo": "Código Interno",
        "c.elegibilidade": "Elegibilidade", "c.idplanopj": "ID Plano PJ", "dtcancelado": "Data Cancelamento", # Alias 'dtcancelado'
        "data_ativo_original": "Data Ativo Original", # Alias 'data_ativo_original'
        "c.fornecedora": "Fornecedora", # <<< Certifique-se que está mapeado
        "c.desconto_cliente": "Desconto Cliente", "dtnasc": "Data Nasc.", # Alias 'dtnasc'
        "c.origem": "Origem", "c.cm_tipo_pagamento": "Tipo Pagamento", "c.status_financeiro": "Status Financeiro",
        "c.logindistribuidora": "Login Distribuidora", "c.senhadistribuidora": "Senha Distribuidora",
        "c.nacionalidade": "Nacionalidade", "c.profissao": "Profissão", "c.estadocivil": "Estado Civil",
        "c.obs_compartilhada": "Observação Compartilhada", "c.linkassinatura1": "Link Assinatura",
        # Campos Específicos 'Clientes por Licenciado'
        "c.cpf": "CPF Consultor", "c.uf": "UF Consultor", # c.uf aqui é do consultor
        "quantidade_clientes_ativos": "Qtd. Clientes Ativos",
        # Campos Específicos 'Boletos por Cliente'
        "quantidade_registros_rcb": "Qtd. Boletos (RCB)",
    }

    # --- Define a ORDEM EXATA das colunas para cada relatório ---
    # Use as chaves que correspondem aos ALIASES ou nomes de campos simples
    # A ordem nesta lista determinará a ordem das colunas na tabela/Excel

    base_clientes_keys = [
        "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo",
        "qtdeassinatura", "c.consumomedio", "c.status", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente",
        "dtultalteracao", "c.celular_2", "c.email", "c.rg", "c.emissor", "datainjecao", "c.idconsultor",
        "consultor_nome", "consultor_celular", "c.cep", "c.endereco", "c.numero", "c.bairro",
        "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao",
        "c.keycontrato", "c.keysigner", "c.leadidsolatio", "c.indcli", "c.enviadocomerc", "c.obs",
        "c.posvenda", "c.retido", "c.contrato_verificado", "c.rateio", "c.validadosucesso",
        "status_sucesso", "c.documentos_enviados", "c.link_documento", "c.caminhoarquivo",
        "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2",
        "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante",
        "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.codigo", "c.elegibilidade",
        "c.idplanopj", "dtcancelado", "data_ativo_original", "c.fornecedora", "c.desconto_cliente",
        "dtnasc", "c.origem", "c.cm_tipo_pagamento", "c.status_financeiro", "c.logindistribuidora",
        "c.senhadistribuidora", "c.nacionalidade", "c.profissao", "c.estadocivil",
        "c.obs_compartilhada", "c.linkassinatura1"
    ]

    rateio_keys = [
        "c.idcliente", "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "regiao", "data_ativo",
        "c.consumomedio", "dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor",
        "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao",
        "c.fantasia", "c.ufconsumo", "c.classificacao", "c.link_documento", "c.caminhoarquivo",
        "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2",
        "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante",
        "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente",
        "dtnasc", "c.logindistribuidora", "c.senhadistribuidora", "c.nacionalidade",
        "consultor_nome", # <-- Era "co.nome AS consultor_nome" na query
        "c.profissao", "c.estadocivil"
        # Nota: "c.nome" aparece duas vezes na lista de campos original de _get_query_fields para rateio.
        # Removi a duplicata aqui. Se precisar do nome do cliente E do representante, ajuste os aliases na query e aqui.
    ]

    clientes_por_licenciado_keys = [
        # Ordem conforme SELECT em get_clientes_por_licenciado_data
        "c.idconsultor", "c.nome", "c.cpf", "c.email", "c.uf", "quantidade_clientes_ativos"
        # Note que aqui usamos 'c.nome', 'c.cpf', 'c.email', 'c.uf' que vêm da tabela CONSULTOR
    ]

    # --- Lista Modificada ---
    boletos_por_cliente_keys = [
        # Ordem conforme SELECT em get_boletos_por_cliente_data (AJUSTADA)
        "c.idcliente",                  # 1
        "c.nome",                       # 2
        "c.numinstalacao",              # 3
        "c.celular",                    # 4
        "c.cidade",                     # 5
        "regiao",                       # 6
        "c.fornecedora",                # 7 <<< ADICIONADO AQUI (índice 6)
        "data_ativo",                   # 8
        "quantidade_registros_rcb"      # 9
    ]
    # --- Fim da Modificação ---

    # Seleciona a lista de chaves correta para o tipo de relatório
    keys_for_report = []
    if report_type == "base_clientes":
        keys_for_report = base_clientes_keys
    elif report_type == "rateio":
        keys_for_report = rateio_keys
    elif report_type == "clientes_por_licenciado":
        keys_for_report = clientes_por_licenciado_keys
    elif report_type == "boletos_por_cliente":
        keys_for_report = boletos_por_cliente_keys # Usa a lista modificada
    else:
        logger.warning(f"Tipo de relatório desconhecido '{report_type}' em get_headers. Retornando lista vazia.")
        return [] # Retorna vazio se o tipo for inválido

    # Cria a lista final de cabeçalhos usando o mapa e a ordem definida
    headers_list = []
    for key in keys_for_report:
        # Busca o nome amigável no mapa. Se não encontrar, usa a própria chave como fallback (melhorado)
        friendly_name = header_map.get(key)
        if friendly_name:
            headers_list.append(friendly_name)
        else:
            # Fallback: tenta limpar a chave para algo legível
            fallback_name = key.split('.')[-1].replace('_', ' ').title() # Pega a parte após 'c.' ou 'co.', troca _ por espaço, capitaliza
            headers_list.append(fallback_name)
            logger.warning(f"Chave '{key}' não encontrada no header_map para relatório '{report_type}'. Usando fallback: '{fallback_name}'")

    if not headers_list and report_type in ["base_clientes", "rateio", "clientes_por_licenciado", "boletos_por_cliente"]:
        logger.error(f"Lista de cabeçalhos final ficou vazia para o tipo de relatório conhecido: '{report_type}'. Verifique as listas de chaves e o header_map.")

    return headers_list