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

def get_boletos_por_cliente_data(offset: int = 0, limit: Optional[int] = None, fornecedora: Optional[str] = None, export_mode: bool = False) -> List[tuple]:
    """Busca dados, junta com CSVs e calcula as colunas 'Atraso na Injeção' e 'Dias em Atraso'."""
    
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

    # 2. Carrega os arquivos CSV
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    
    try:
        csv_path_prazos = os.path.join(project_root, 'data', 'prazos.csv')
        df_prazos = pd.read_csv(csv_path_prazos, delimiter=';')
        for col in ['ufconsumo', 'concessionaria', 'fornecedora']:
            if col in df_prazos.columns:
                df_prazos[col] = df_prazos[col].astype(str).str.strip().str.upper()
    except FileNotFoundError:
        logger.error(f"Arquivo 'prazos.csv' não encontrado: {csv_path_prazos}")
        df_prazos = pd.DataFrame()

    try:
        csv_path_devolutivas = os.path.join(project_root, 'data', 'devolutivas.csv')
        df_devolutivas = pd.read_csv(csv_path_devolutivas, delimiter=';', dtype={'idcliente': 'Int64'})
        df_devolutivas.rename(columns={'idcliente': 'codigo', 'retorno_fornecedora': 'retorno_fornecedora'}, inplace=True)
    except FileNotFoundError:
        logger.error(f"Arquivo 'devolutivas.csv' não encontrado: {csv_path_devolutivas}")
        df_devolutivas = pd.DataFrame()

    # 3. Converte os resultados do SQL para um DataFrame
    # <-- ALTERAÇÃO NA LISTA DE COLUNAS -->
    # Usa a variável global final_columns_order
    sql_columns = final_columns_order[:-2] # Exclui as 2 últimas colunas, pois 'injecao' e 'atraso_na_injecao' são calculadas
    # Na verdade, o CTE já retorna "injecao", "atraso_na_injecao" e "dias_em_atraso" calculados.
    # O ideal é que as colunas da CTE sejam listadas aqui para um mapeamento mais robusto.
    # No entanto, se o CTE já retorna tudo na ordem do final_columns_order, podemos usar o próprio.
    # Vamos usar os nomes do CTE e depois reordenar com reindex.
    # Isso é para garantir que a leitura inicial do DataFrame do SQL esteja correta.
    # O CTE_BASE retorna as seguintes colunas, que serão os 'columns' do DataFrame:
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

    # ***** INÍCIO DA LÓGICA DE CÁLCULO ATUALIZADA *****
    
    # 5. Extrai o valor numérico do prazo de injeção
    df_merged['prazo_numerico'] = pd.to_numeric(df_merged['injecao'].astype(str).str.extract(r'(\d+)', expand=False), errors='coerce')

    # 6. Calcula a coluna "Atraso na Injeção"
    # Condições para o atraso
    cond_qtd_boletos = df_merged['quantidade_boletos'] == 0
    cond_data_ativo = df_merged['data_ativo'].notna() & (df_merged['data_ativo'] != '')
    cond_prazo_estourado = (df_merged['dias_desde_ativacao'] - df_merged['prazo_numerico']) > 0
    # <-- NOVA CONDIÇÃO ADICIONADA AQUI -->
    cond_devolutiva_vazia = (df_merged['devolutiva'].isnull()) | (df_merged['devolutiva'] == '')

    # Combina TODAS as condições
    # <-- MÁSCARA ATUALIZADA PARA INCLUIR A NOVA CONDIÇÃO -->
    atraso_mask = cond_qtd_boletos & cond_data_ativo & cond_prazo_estourado & cond_devolutiva_vazia
    df_merged['atraso_na_injecao'] = np.where(atraso_mask, 'SIM', 'NÃO')

    # 7. Calcula a coluna "Dias em Atraso"
    dias_em_atraso_calculado = df_merged['dias_desde_ativacao'] - df_merged['prazo_numerico']
    df_merged['dias_em_atraso'] = np.where(df_merged['atraso_na_injecao'] == 'SIM', dias_em_atraso_calculado, np.nan)
    
    # <-- 2. ADICIONAR LÓGICA CONDICIONAL PARA FORMATAÇÃO -->
    if not export_mode:
        # Para a visualização na tela, converte para string e remove <NA>
        df_merged['dias_em_atraso'] = df_merged['dias_em_atraso'].astype('Int64').astype(str).replace('<NA>', '')
    # Se export_mode for True, a coluna permanece como numérica (float64 com np.nan),
    # que o openpyxl interpretará corretamente como número ou célula vazia.

    # 8. Define a ordem final das colunas (AGORA USANDO A VARIÁVEL GLOBAL)
    df_final = df_merged.reindex(columns=final_columns_order, fill_value='')
    
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