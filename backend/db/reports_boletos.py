# backend/db/reports_boletos.py
import logging
import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Optional
from .executor import execute_query

logger = logging.getLogger(__name__)

CTE_BASE = """
WITH BaseQuery AS (
    SELECT 
        c.idcliente AS codigo, 
        c.nome, 
        c.numinstalacao AS instalacao,  
        c.numcliente AS numero_cliente,
        COALESCE(NULLIF(c.cnpj, ''), c."cpf/cnpj") AS cpf_cnpj,  
        c.cidade,
        c.ufconsumo,
        c.concessionaria,
        CASE
            WHEN COALESCE(c.fornecedora, ''::character varying)::text <> ''::text THEN c.fornecedora::text
            ELSE
            CASE
                WHEN COALESCE(c.idcomerc, ''::character varying)::text <> ''::text AND c.ufconsumo::text IN ('MG', 'PE') THEN 'COMERC'::text
                WHEN c.ufconsumo::text = 'MT'::text THEN 'BOM FUTURO'::text
                WHEN c.ufconsumo::text = 'GO'::text THEN 'BC ENERGIA'::text
                WHEN COALESCE(c.idcomerc, ''::character varying)::text = ''::text AND c.ufconsumo::text = 'MG'::text THEN 'SOLATIO'::text
                ELSE ''::text
            END
        END AS fornecedora,
        TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo,
        CASE
            WHEN c.data_ativo IS NOT NULL 
            THEN EXTRACT(DAY FROM (NOW() - c.data_ativo))::INTEGER
            ELSE NULL
        END AS dias_desde_ativacao,
        c.validadosucesso AS validado_sucesso,
        c.status AS devolutiva,
        c.idconsultor AS id_licenciado,
        cons.nome AS licenciado,
        CASE
            WHEN cp.dtgraduacao IS NULL THEN 'NÃO'
            ELSE 'SIM'
        END AS status_pro,
        TO_CHAR(cp.dtgraduacao, 'DD/MM/YYYY') AS data_graduacao_pro,
        COUNT(rcb.numinstalacao) AS quantidade_boletos
    FROM 
        public."CLIENTES" c
    LEFT JOIN 
        public."RCB_CLIENTES" rcb ON c.numinstalacao = rcb.numinstalacao
    LEFT JOIN public."CONSULTOR" cons ON c.idconsultor = cons.idconsultor
    LEFT JOIN (
        SELECT idconsultor, MAX(dtgraduacao) AS dtgraduacao
        FROM public."CONTROLE_PRO"
        GROUP BY idconsultor
    ) cp ON c.idconsultor = cp.idconsultor
    WHERE 
        (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        AND (c.status NOT ILIKE %s OR c.status IS NULL)
    GROUP BY 
        c.idcliente, c.nome, c.numinstalacao, c.celular, c.cidade,
        c.ufconsumo, c.concessionaria, fornecedora, c.data_ativo,
        dias_desde_ativacao, c.validadosucesso, c.status, c.idconsultor,
        cons.nome, cp.dtgraduacao, c.cnpj, c."cpf/cnpj", c.numcliente
)
"""

def get_boletos_por_cliente_data(offset: int = 0, limit: Optional[int] = None, fornecedora: Optional[str] = None) -> List[tuple]:
    """Busca dados, junta com CSV e calcula as colunas 'Atraso na Injeção' e 'Dias em Atraso'."""
    
    # 1. Busca os dados do banco de dados (SQL)
    query_final_sql = "SELECT * FROM BaseQuery"
    params_sql = ['CANCELADO%']
    if fornecedora and fornecedora.lower() != 'consolidado':
        query_final_sql += " WHERE fornecedora = %s"
        params_sql.append(fornecedora)
    query_final_sql += " ORDER BY codigo"
    if limit is not None:
        query_final_sql += " LIMIT %s"
        params_sql.append(limit)
    if offset > 0:
        query_final_sql += " OFFSET %s"
        params_sql.append(offset)
    full_query_sql = CTE_BASE + query_final_sql + ";"
    
    try:
        results_sql = execute_query(full_query_sql, tuple(params_sql)) or []
        if not results_sql:
            return []
    except Exception as e:
        logger.error(f"Erro ao buscar dados de boletos (SQL): {e}", exc_info=True)
        return []

    # 2. Carrega o arquivo CSV de prazos e padroniza as chaves
    try:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        csv_path = os.path.join(project_root, 'data', 'prazos.csv')
        df_prazos = pd.read_csv(csv_path, delimiter=';')
        for col in ['ufconsumo', 'concessionaria', 'fornecedora']:
            if col in df_prazos.columns:
                df_prazos[col] = df_prazos[col].astype(str).str.strip().str.upper()
    except FileNotFoundError:
        logger.error(f"Arquivo 'prazos.csv' não encontrado: {csv_path}")
        # Adapta o retorno para incluir as novas colunas vazias em caso de erro
        return [row + (None, None, None) for row in results_sql]

    # 3. Converte os resultados do SQL para um DataFrame e padroniza as chaves
    sql_columns = [
        "codigo", "nome", "instalacao", "numero_cliente", "cpf_cnpj", "cidade",
        "ufconsumo", "concessionaria", "fornecedora", "data_ativo", "dias_desde_ativacao",
        "validado_sucesso", "devolutiva", "id_licenciado", "licenciado", "status_pro",
        "data_graduacao_pro", "quantidade_boletos"
    ]
    df_sql = pd.DataFrame(results_sql, columns=sql_columns)
    for col in ['ufconsumo', 'concessionaria', 'fornecedora']:
        if col in df_sql.columns:
            df_sql[col] = df_sql[col].astype(str).str.strip().str.upper()

    # 4. Faz o merge (junção) dos DataFrames
    df_merged = pd.merge(
        df_sql, 
        df_prazos, 
        on=['ufconsumo', 'concessionaria', 'fornecedora'], 
        how='left'
    )
    df_merged['injecao'] = df_merged['injecao'].fillna('')

    # ***** INÍCIO DA NOVA LÓGICA DE CÁLCULO *****
    
    # 5. Extrai o valor numérico do prazo de injeção
    # Converte a coluna 'injecao' para string ANTES de usar o acessor .str
    df_merged['prazo_numerico'] = pd.to_numeric(df_merged['injecao'].astype(str).str.extract(r'(\d+)', expand=False), errors='coerce')

    # 6. Calcula a coluna "Atraso na Injeção"
    # Condições para o atraso
    cond_qtd_boletos = df_merged['quantidade_boletos'] == 0
    cond_data_ativo = df_merged['data_ativo'].notna() & (df_merged['data_ativo'] != '')
    cond_prazo_estourado = (df_merged['dias_desde_ativacao'] - df_merged['prazo_numerico']) > 0
    
    # Combina as condições
    atraso_mask = cond_qtd_boletos & cond_data_ativo & cond_prazo_estourado
    df_merged['atraso_na_injecao'] = np.where(atraso_mask, 'SIM', 'NÃO')

    # 7. Calcula a coluna "Dias em Atraso"
    dias_em_atraso_calculado = df_merged['dias_desde_ativacao'] - df_merged['prazo_numerico']
    df_merged['dias_em_atraso'] = np.where(df_merged['atraso_na_injecao'] == 'SIM', dias_em_atraso_calculado, np.nan)
    # Converte para inteiro e depois para string, tratando valores nulos
    df_merged['dias_em_atraso'] = df_merged['dias_em_atraso'].astype('Int64').astype(str).replace('<NA>', '')


    # 8. Define a ordem final das colunas, incluindo as novas
    final_columns_order = [
         "codigo", "nome", "instalacao", "numero_cliente", "cpf_cnpj", "cidade",
         "ufconsumo", "concessionaria", "fornecedora", "data_ativo", 
         "dias_desde_ativacao",
         "injecao",
         "atraso_na_injecao", # <-- NOVA COLUNA
         "dias_em_atraso",     # <-- NOVA COLUNA
         "validado_sucesso", "devolutiva", "id_licenciado", "licenciado", "status_pro",
         "data_graduacao_pro", "quantidade_boletos"
    ]
    df_final = df_merged[final_columns_order]
    
    # ***** FIM DA NOVA LÓGICA DE CÁLCULO *****
    return [tuple(x) for x in df_final.to_numpy()]


def count_boletos_por_cliente(fornecedora: Optional[str] = None) -> int:
    """Conta o total de clientes. Esta função não é alterada."""
    query_final = "SELECT COUNT(*) FROM BaseQuery"
    params = ['CANCELADO%']

    if fornecedora and fornecedora.lower() != 'consolidado':
        query_final += " WHERE fornecedora = %s"
        params.append(fornecedora)

    full_query = CTE_BASE + query_final + ";"

    try: 
        result = execute_query(full_query, tuple(params), fetch_one=True)
        return result[0] if result and result[0] is not None else 0
    except Exception as e: 
        logger.error(f"Erro ao contar boletos por cliente: {e}", exc_info=True)
        return 0