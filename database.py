# database.py
import psycopg2
import psycopg2.pool
import logging
from flask import g # Usaremos o 'g' object do Flask para gerir conexões por requisição
from config import Config # Assumindo que config.py está no mesmo nível
from typing import List, Tuple, Optional, Any # Adicionar Any

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
                if len(result) > 10:
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
    OPCIONALMENTE filtrando por fornecedora. Usa 'codigo' como id.
    """
    query_base = """
        SELECT DISTINCT cc.idcliente
        FROM public."CLIENTES_CONTRATOS" cc
        INNER JOIN public."CLIENTES_CONTRATOS_SIGNER" ccs ON cc.idcliente_contrato = ccs.idcliente_contrato
        INNER JOIN public."CLIENTES" c ON cc.idcliente = c.idcliente
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
        # Assume que idcliente é o campo correto (ou deve ser c.codigo se for diferente?)
        # Se idcliente em CLIENTES_CONTRATOS refere-se a CLIENTES.codigo, está ok.
        return [row[0] for row in results] if results else []
    except (RuntimeError, ConnectionError) as e:
        logger.error(f"Erro ao buscar IDs da Base Nova: {e}")
        return []

def get_base_enviada_ids(fornecedora: Optional[str] = None) -> List[int]:
    """
    Busca os IDs de cliente para a 'Base Enviada' do Rateio (rateio = 'S'),
    OPCIONALMENTE filtrando por fornecedora. Usa 'idcliente' (ou 'codigo'?)
    """
    # IMPORTANTE: Verifique se o campo a selecionar aqui é 'idcliente' ou 'codigo'
    # Assumindo que 'idcliente' é a chave estrangeira correta em CLIENTES
    query_base = """
        SELECT c.idcliente
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
    usando batching e corrigindo parâmetro ANY. Usa 'idcliente' no WHERE.
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
        where = "WHERE c.idcliente = ANY(%s)"
        order = "ORDER BY c.idcliente" # Mantém a ordem consistente
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
            cl.data_ativo IS NOT NULL
        GROUP BY
            c.idconsultor, c.nome, c.cpf, c.email, c.uf
        ORDER BY
            quantidade_clientes_ativos DESC
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
    """Conta o número total de consultores (linhas) que seriam retornados pela query principal."""
    logger.info("Contando total de registros para 'Clientes por Licenciado'...")
    # Usamos uma subquery para contar as linhas *após* o GROUP BY da query original
    count_query_sql = """
        SELECT COUNT(*)
        FROM (
            SELECT
                c.idconsultor
            FROM
                public."CONSULTOR" c
            LEFT JOIN
                public."CLIENTES" cl ON c.idconsultor = cl.idconsultor
            WHERE
                cl.data_ativo IS NOT NULL
            GROUP BY
                c.idconsultor, c.nome, c.cpf, c.email, c.uf
        ) AS subquery_count;
    """
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

# --- FIM DAS FUNÇÕES PARA RELATÓRIO ---

# --- Funções para Estrutura de Query e Cabeçalhos ---

def _get_query_fields(report_type: str) -> List[str]:
     """Retorna a lista de campos SQL a serem selecionados na query,
        baseado no tipo de relatório. Adapta nomes como 'idcliente' se necessário."""
     report_type = report_type.lower()

     # Campos para Base Clientes (Adapte 'c.idcliente' para 'c.codigo' se for o PK)
     base_clientes_fields = [
        "c.idcliente", # <<< VERIFICAR: É idcliente ou codigo a chave primária em CLIENTES?
        "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN '' ELSE (c.uf || '-' || c.concessionaria) END AS regiao",
        "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "(c.qtdeassinatura::text || '/4') AS qtdeassinatura", "c.consumomedio", "c.status", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad",
        "c.\"cpf/cnpj\"", "c.numcliente", "TO_CHAR(c.dtultalteracao, 'DD/MM/YYYY') AS dtultalteracao", "c.celular_2", "c.email", "c.rg", "c.emissor", "TO_CHAR(c.datainjecao, 'DD/MM/YYYY') AS datainjecao",
        "c.idconsultor", "co.nome AS consultor_nome", "co.celular AS consultor_celular", "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia",
        "c.ufconsumo", "c.classificacao", "c.keycontrato", "c.keysigner", "c.leadidsolatio", "c.indcli", "c.enviadocomerc", "c.obs", "c.posvenda", "c.retido", "c.contrato_verificado",
        "c.rateio", "c.validadosucesso", "CASE WHEN c.validadosucesso = 'S' THEN 'Aprovado' ELSE 'Rejeitado' END AS status_sucesso", "c.documentos_enviados", "c.link_documento",
        "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante",
        "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.codigo", # Aqui usa 'codigo'
        "c.elegibilidade", "c.idplanopj", "TO_CHAR(c.dtcancelado, 'DD/MM/YYYY') AS dtcancelado",
        "TO_CHAR(c.data_ativo_original, 'DD/MM/YYYY') AS data_ativo_original", "c.fornecedora", "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc", "c.origem",
        "c.cm_tipo_pagamento", "c.status_financeiro", "c.logindistribuidora", "c.senhadistribuidora", "c.nacionalidade", "c.profissao", "c.estadocivil", "c.obs_compartilhada", "c.linkassinatura1"
     ]

     # Campos para Rateio (Adapte 'c.idcliente' se necessário)
     rateio_fields = [
        "c.idcliente", # <<< VERIFICAR: É idcliente ou codigo a chave primária em CLIENTES?
        "c.nome", "c.numinstalacao", "c.celular", "c.cidade", "CASE WHEN c.concessionaria IS NULL OR c.concessionaria = '' THEN '' ELSE (c.uf || '-' || c.concessionaria) END AS regiao",
        "TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo", "c.consumomedio", "TO_CHAR(c.dtcad, 'DD/MM/YYYY') AS dtcad", "c.\"cpf/cnpj\"", "c.numcliente", "c.email", "c.rg", "c.emissor",
        "c.cep", "c.endereco", "c.numero", "c.bairro", "c.complemento", "c.cnpj", "c.razao", "c.fantasia", "c.ufconsumo", "c.classificacao", "c.link_documento",
        "c.caminhoarquivo", "c.caminhoarquivocnpj", "c.caminhoarquivodoc1", "c.caminhoarquivodoc2", "c.caminhoarquivoenergia2", "c.caminhocontratosocial", "c.caminhocomprovante",
        "c.caminhoarquivoestatutoconvencao", "c.senhapdf", "c.fornecedora", "c.desconto_cliente", "TO_CHAR(c.dtnasc, 'DD/MM/YYYY') AS dtnasc", "c.logindistribuidora", "c.senhadistribuidora",
        "c.nacionalidade", "c.nome", "co.nome AS consultor_nome", "c.profissao", "c.estadocivil"
     ]

     # Não definimos campos aqui para 'clientes_por_licenciado', pois a query é customizada.
     # Retornamos os campos apropriados para 'rateio' ou 'base_clientes'.
     if report_type == "rateio":
          logger.debug(f"Selecionando campos SQL para o tipo: {report_type}")
          return rateio_fields
     elif report_type == "base_clientes":
          logger.debug(f"Selecionando campos SQL para o tipo: {report_type}")
          return base_clientes_fields
     else:
          # Se um tipo desconhecido for passado (ex: 'clientes_por_licenciado'),
          # retornamos vazio, pois esta função não é usada para ele.
          logger.debug(f"Tipo '{report_type}' não mapeado em _get_query_fields. Retornando lista vazia.")
          return []


def build_query(report_type: str, fornecedora: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> Tuple[str, tuple]:
    """Constrói a query principal para buscar dados para exibição na tela (paginada).
       NÃO USAR para 'clientes_por_licenciado' que tem query própria."""
    campos = _get_query_fields(report_type)
    if not campos:
        # Levanta erro se for um tipo que deveria ter campos definidos aqui
        if report_type in ["base_clientes", "rateio"]:
             raise ValueError(f"Campos não definidos para report_type '{report_type}' em _get_query_fields")
        else:
             # Para tipos com query customizada (como clientes_por_licenciado), isso não deve ser chamado.
             # Mas se for, retorna erro para indicar uso incorreto.
             raise ValueError(f"build_query não deve ser chamado para report_type '{report_type}' com query customizada.")

    select = f"SELECT {', '.join(campos)}"
    from_ = 'FROM public."CLIENTES" c'

    needs_consultor_join = any(f.startswith("co.") for f in campos)
    join = ' LEFT JOIN public."CONSULTOR" co ON co.idconsultor = c.idconsultor' if needs_consultor_join else ""

    where_clauses = []
    params = []
    # Adiciona filtro de fornecedora (usa 'consolidado' internamente para "todos")
    # Relevante apenas para 'base_clientes' e 'rateio' aqui
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
       NÃO USAR para 'clientes_por_licenciado' que tem query de contagem própria."""
    if report_type == 'clientes_por_licenciado':
        raise ValueError("count_query não deve ser chamado para 'clientes_por_licenciado'. Use count_clientes_por_licenciado().")

    from_ = 'FROM public."CLIENTES" c'
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
        return sorted([f[0] for f in results if f[0]]) if results else []
    except (ConnectionError, RuntimeError) as e:
        logger.error(f"Erro ao buscar fornecedoras: {e}")
        return []

def get_headers(report_type: str) -> List[str]:
    """Retorna os cabeçalhos baseado no tipo de relatório."""
    report_type = report_type.lower()

    column_headers = {
         # Chave 'base_clientes' corresponde aos campos em base_clientes_fields
         "base_clientes": [
            "Código", # Assume que refere-se a c.idcliente (ou c.codigo?)
            "Nome", "Instalação", "Celular", "Cidade", "Região", "Data Ativo",
            "Assinaturas", "Consumo Médio", "Status", "Data Cadastro", "CPF/CNPJ",
            "Número Cliente", "Última Alteração", "Celular 2", "Email", "RG", "Emissor",
            "Data Injeção", "ID Consultor", "Nome Consultor", "Celular Consultor", "CEP",
            "Endereço", "Número", "Bairro", "Complemento", "CNPJ", "Razão Social",
            "Nome Fantasia", "UF Consumo", "Classificação", "Key Contrato", "Key Signer",
            "Lead ID Solatio", "Ind CLI", "Enviado Comerci", "Observação", "Pós-venda",
            "Retido", "Contrato Verificado", "Rateio", "Validação Sucesso", "Status Sucesso",
            "Documentos Enviados", "Link Documento", "Link Conta Energia", "Link Cartão CNPJ",
            "Caminho Documento 1", "Caminho Documento 2", "Caminho Energia 2", "Caminho Contrato Social",
            "Caminho Comprovante", "Caminho Estatuto/Convenção", "Senha PDF",
            "Código", # <<< CUIDADO: Header "Código" aparece duas vezes. Qual campo é este? c.codigo?
            "Elegibilidade",
            "ID Plano PJ", "Data Cancelamento", "Data Ativo Original", "Fornecedora", "Desconto Cliente",
            "Data Nasc.", "Origem", "Tipo Pagamento", "Status Financeiro", "Login Distribuidora",
            "Senha Distribuidora", "Nacionalidade", "Profissão", "Estado Civil", "Observação Compartilhada",
            "Link Assinatura"
         ],
         # Chave 'rateio' corresponde aos campos em rateio_fields
         "rateio": [
            "Código", # Assume que refere-se a c.idcliente (ou c.codigo?)
            "Nome", "Instalação", "Celular", "Cidade", "Região", "Data Ativo", "Consumo Médio",
            "Data Cadastro", "CPF/CNPJ", "Número Cliente", "Email", "RG", "Emissor", "CEP", "Endereço", "Número",
            "Bairro", "Complemento", "CNPJ", "Razão Social", "Nome Fantasia", "UF Consumo", "Classificação",
            "Link Documento", "Link Conta Energia", "Link Cartão CNPJ", "Link Documento Frente", "Link Documento Verso",
            "Link Conta Energia 2", "Link Contrato Social", "Link Comprovante De Pagamento", "Link Estatuto Convenção",
            "Senha PDF", "Fornecedora", "Desconto Cliente", "Data Nascimento", "Login Distribuidora", "Senha Distribuidora",
            "Nacionalidade", "Cliente", "Representante", "Profissão", "Estado Civil"
         ],
         # --- NOVO TIPO DE RELATÓRIO ---
         "clientes_por_licenciado": [
            "ID Consultor",
            "Nome Consultor",
            "CPF",
            "Email",
            "UF",
            "Qtd. Clientes Ativos"
         ]
         # --- FIM NOVO TIPO ---
     }
    # Retorna a lista de headers para o tipo solicitado, ou a de 'base_clientes' como padrão
    headers_list = column_headers.get(report_type, column_headers.get("base_clientes", [])) # Default para base_clientes se report_type inválido
    if not headers_list and report_type not in column_headers:
        logger.warning(f"Tipo de relatório desconhecido '{report_type}' e 'base_clientes' não encontrado em get_headers. Retornando lista vazia.")
        return []
    elif not headers_list:
         logger.error(f"Lista de cabeçalhos vazia para o tipo de relatório: {report_type}")
         # Retorna uma lista vazia para evitar erros posteriores
         return []
    return headers_list