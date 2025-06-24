# backend/db/reports_boletos.py
import logging
import os
import pandas as pd
import numpy as np
from typing import List, Tuple, Optional
from .executor import execute_query

logger = logging.getLogger(__name__)

# --- NOVO: Variável final_columns_order movida para o nível do módulo ---
final_columns_order = [
     "codigo", "nome", "instalacao", "numero_cliente", "cpf_cnpj", "cidade",
     "ufconsumo", "concessionaria", "fornecedora",
     "consumomedio",
     "data_ativo", "dias_desde_ativacao",
     "injecao",
     "atraso_na_injecao", # <-- Esta é a 14ª coluna (índice 13)
     "dias_em_atraso",
     "validado_sucesso", "devolutiva", "retorno_fornecedora",
     "id_licenciado", "licenciado", "status_pro",
     "data_graduacao_pro", "quantidade_boletos"
]
# --- FIM da movimentação ---

CTE_BASE = """
WITH LatestDevolutiva AS (
    -- 1. Seleciona a devolução mais recente para cada cliente, incluindo a flag 'corrigida'
    SELECT DISTINCT ON (idcliente)
        idcliente,
        obs,
        corrigida -- <<< COLUNA ADICIONADA AQUI
    FROM public."DEVOLUTIVAS"
    -- Ordena por cliente e depois pela data de atualização em ordem decrescente
    -- DISTINCT ON pega a primeira linha desta ordenação, que será a mais recente
    ORDER BY idcliente, updated_at DESC
),
BaseQuery AS (
    -- 2. A query principal agora usa o resultado da CTE acima
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
        c.consumomedio,
        TO_CHAR(c.data_ativo, 'DD/MM/YYYY') AS data_ativo,
        CASE
            WHEN c.data_ativo IS NOT NULL 
            THEN EXTRACT(DAY FROM (NOW() - c.data_ativo))::INTEGER
            ELSE NULL
        END AS dias_desde_ativacao,
        c.validadosucesso AS validado_sucesso,
        
        -- <<< LÓGICA DO CASE ATUALIZADA COM A NOVA REGRA >>>
        CASE
            -- Se a devolução mais recente foi corrigida, usa o status do cliente
            WHEN d.corrigida = TRUE THEN c.status
            -- Se não foi corrigida E o status do cliente está vazio, usa a observação da devolução
            WHEN COALESCE(c.status, '')::text = '' THEN d.obs
            -- Caso contrário (status do cliente preenchido e devolução não corrigida), usa o status do cliente
            ELSE c.status
        END AS devolutiva,

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
        AND rcb.dtvencimento >= c.data_ativo
    LEFT JOIN public."CONSULTOR" cons ON c.idconsultor = cons.idconsultor
    LEFT JOIN (
        SELECT idconsultor, MAX(dtgraduacao) AS dtgraduacao
        FROM public."CONTROLE_PRO"
        GROUP BY idconsultor
    ) cp ON c.idconsultor = cp.idconsultor
    
    -- <<< JOIN ALTERADO PARA USAR A CTE COM A DEVOLUÇÃO MAIS RECENTE >>>
    LEFT JOIN LatestDevolutiva d ON c.idcliente = d.idcliente

    WHERE 
        (c.origem IS NULL OR c.origem IN ('', 'WEB', 'BACKOFFICE', 'APP'))
        AND (c.status NOT ILIKE %s OR c.status IS NULL)
    GROUP BY 
        c.idcliente, c.nome, c.numinstalacao, c.celular, c.cidade,
        c.ufconsumo, c.concessionaria, fornecedora, c.consumomedio,
        c.data_ativo, dias_desde_ativacao, c.validadosucesso, c.status, 
        c.idconsultor, cons.nome, cp.dtgraduacao, c.cnpj, c."cpf/cnpj", c.numcliente,
        d.obs,
        d.corrigida -- <<< CAMPO ADICIONADO AO GROUP BY >>>
)
"""

def load_csv_prazos(project_root: str) -> pd.DataFrame:
    """Carrega o CSV de prazos, padronizando colunas de string."""
    csv_path_prazos = os.path.join(project_root, 'data', 'prazos.csv')
    try:
        df = pd.read_csv(csv_path_prazos, delimiter=';')
        for col in ['ufconsumo', 'concessionaria', 'fornecedora']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()
        return df
    except FileNotFoundError:
        logger.error(f"Arquivo 'prazos.csv' não encontrado: {csv_path_prazos}")
        return pd.DataFrame()

def load_csv_devolutivas(project_root: str) -> pd.DataFrame:
    """Carrega o CSV de devolutivas, renomeando colunas conforme necessário."""
    csv_path_devolutivas = os.path.join(project_root, 'data', 'devolutivas.csv')
    try:
        df = pd.read_csv(csv_path_devolutivas, delimiter=';', dtype={'idcliente': 'Int64'})
        df.rename(columns={'idcliente': 'codigo', 'retorno_fornecedora': 'retorno_fornecedora'}, inplace=True)
        return df
    except FileNotFoundError:
        logger.error(f"Arquivo 'devolutivas.csv' não encontrado: {csv_path_devolutivas}")
        return pd.DataFrame()

def calcular_colunas_atraso(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula as colunas 'atraso_na_injecao' e 'dias_em_atraso' no DataFrame."""
    df['prazo_numerico'] = pd.to_numeric(df['injecao'].astype(str).str.extract(r'(\d+)', expand=False), errors='coerce')
    cond_qtd_boletos = (df['quantidade_boletos'] == 0)
    cond_data_ativo = df['data_ativo'].notna() & (df['data_ativo'] != '')
    cond_prazo_estourado = (df['dias_desde_ativacao'] - df['prazo_numerico']) > 0
    cond_devolutiva_vazia = (df['devolutiva'].isnull()) | (df['devolutiva'] == '')
    cond_retorno_fornecedora_nao_vazia = ~(df['retorno_fornecedora'].fillna('').astype(str).str.strip().eq(''))
    df['atraso_na_injecao'] = np.where(
        cond_retorno_fornecedora_nao_vazia,
        'NÃO',
        np.where(
            cond_qtd_boletos & cond_data_ativo & cond_prazo_estourado & cond_devolutiva_vazia,
            'SIM',
            'NÃO'
        )
    )
    dias_em_atraso_calculado = df['dias_desde_ativacao'] - df['prazo_numerico']
    df['dias_em_atraso'] = np.where(df['atraso_na_injecao'] == 'SIM', dias_em_atraso_calculado, np.nan)
    return df

def get_boletos_por_cliente_data(offset: int = 0, limit: Optional[int] = None, fornecedora: Optional[str] = None, export_mode: bool = False) -> List[tuple]:
    """
    Busca dados, junta com CSVs e calcula as colunas 'Atraso na Injeção' e 'Dias em Atraso'.
    Modularizado para facilitar manutenção e testes.
    """
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

    # 2. Carrega os arquivos CSV usando funções auxiliares
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    df_prazos = load_csv_prazos(project_root)
    df_devolutivas = load_csv_devolutivas(project_root)

    # 3. Converte os resultados do SQL para um DataFrame
    cte_columns_from_base_query = [
        "codigo", "nome", "instalacao", "numero_cliente", "cpf_cnpj", "cidade",
        "ufconsumo", "concessionaria", "fornecedora", "consumomedio", "data_ativo", "dias_desde_ativacao",
        "validado_sucesso", "devolutiva", "id_licenciado", "licenciado", "status_pro",
        "data_graduacao_pro", "quantidade_boletos"
    ]
    df_sql = pd.DataFrame(results_sql, columns=cte_columns_from_base_query)
    for col in ['ufconsumo', 'concessionaria', 'fornecedora']:
        if col in df_sql.columns:
            df_sql[col] = df_sql[col].astype(str).str.strip().str.upper()

    # 4. Faz o merge dos DataFrames
    df_merged = pd.merge(df_sql, df_prazos, on=['ufconsumo', 'concessionaria', 'fornecedora'], how='left')
    if not df_devolutivas.empty:
        df_merged = pd.merge(df_merged, df_devolutivas, on='codigo', how='left')
    else:
        df_merged['retorno_fornecedora'] = ''
    df_merged['retorno_fornecedora'] = df_merged['retorno_fornecedora'].fillna('')
    df_merged['injecao'] = df_merged['injecao'].fillna('')

    # 5. Calcula as colunas de atraso usando função utilitária
    df_merged = calcular_colunas_atraso(df_merged)

    # 6. Define a ordem final das colunas
    df_final = df_merged.reindex(columns=final_columns_order, fill_value='')
    for col in ['injecao', 'atraso_na_injecao', 'dias_em_atraso', 'retorno_fornecedora']:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna('')
    if not export_mode:
        for col in ['consumomedio']:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notna(x) else '')
    if not export_mode and 'dias_desde_ativacao' in df_final.columns:
        df_final['dias_desde_ativacao'] = df_final['dias_desde_ativacao'].astype('Int64').astype(str).replace('<NA>', '')
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