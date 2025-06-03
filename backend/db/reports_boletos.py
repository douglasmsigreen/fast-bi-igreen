# backend/db/reports_boletos.py
import logging
from typing import List, Tuple, Optional
from .executor import execute_query

logger = logging.getLogger(__name__)

def get_boletos_por_cliente_data(fornecedora: Optional[str] = None, offset: int = 0, limit: Optional[int] = None) -> List[tuple]:
    """Busca dados completos para o relatório Boletos por Cliente com a nova estrutura."""
    try:
        query = """
        SELECT 
            c.idcliente AS codigo, 
            c.nome, 
            c.numinstalacao AS instalacao,  
            c.numcliente AS numero_cliente,
            COALESCE(NULLIF(c.cnpj, ''), c."cpf/cnpj") AS cpf_cnpj,  
            c.cidade,
            CASE 
                WHEN c.concessionaria IS NULL OR c.concessionaria = '' 
                THEN '' 
                ELSE (c.uf || '-' || c.concessionaria) 
            END AS regiao,
            CASE
                WHEN COALESCE(c.fornecedora, ''::character varying)::text <> ''::text THEN c.fornecedora::text
                ELSE
                CASE
                    WHEN COALESCE(c.idcomerc, ''::character varying)::text <> ''::text AND c.ufconsumo::text = 'MG'::text THEN 'COMERC MG'::text
                    ELSE
                    CASE
                        WHEN c.ufconsumo::text = 'MT'::text THEN 'BOM FUTURO'::text
                        ELSE
                        CASE
                            WHEN c.ufconsumo::text = 'PE'::text THEN 'COMERC PE'::text
                            ELSE
                            CASE
                                WHEN c.ufconsumo::text = 'GO'::text THEN 'BC ENERGIA'::text
                                ELSE
                                CASE
                                    WHEN COALESCE(c.idcomerc, ''::character varying)::text = ''::text AND c.ufconsumo::text = 'MG'::text THEN 'SOLATIO'::text
                                    ELSE ''::text
                                END
                            END
                        END
                    END
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
            AND (c.status NOT ILIKE 'CANCELADO%' OR c.status IS NULL)
        """
        
        params = []
        if fornecedora and fornecedora.lower() != 'consolidado':
            query += " AND c.fornecedora = %s"
            params.append(fornecedora)
            
        query += """
        GROUP BY 
            c.idcliente, 
            c.nome, 
            c.numinstalacao, 
            c.celular, 
            c.cidade,
            CASE 
                WHEN c.concessionaria IS NULL OR c.concessionaria = '' 
                THEN '' 
                ELSE (c.uf || '-' || c.concessionaria) 
            END, 
            c.fornecedora, 
            c.data_ativo, 
            CASE
                WHEN c.data_ativo IS NOT NULL 
                THEN EXTRACT(DAY FROM (NOW() - c.data_ativo))::INTEGER
                ELSE NULL
            END, 
            c.ufconsumo, 
            c.idcomerc,
            c.validadosucesso,
            c.status,
            c.idconsultor,
            cons.nome,
            cp.dtgraduacao,
            c.cnpj,
            c."cpf/cnpj",
            c.numcliente
        ORDER BY 
            c.idcliente
        """
        
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        if offset > 0:
            query += " OFFSET %s"
            params.append(offset)
            
        logger.debug(f"REPORTS_BOLETOS - Query get_boletos_por_cliente_data: [{query}], Params: {params}")
        results = execute_query(query, tuple(params))
        return results if results else []
        
    except Exception as e:
        logger.error(f"Erro ao buscar dados boletos por cliente: {e}", exc_info=True)
        return []

def count_boletos_por_cliente(fornecedora: Optional[str] = None) -> int:
    """Conta total de registros para paginação no relatório Boletos por Cliente."""
    try:
        query = """
        SELECT COUNT(DISTINCT c.idcliente)
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
            AND (c.status NOT ILIKE 'CANCELADO%' OR c.status IS NULL)
        """
        
        params = []
        if fornecedora and fornecedora.lower() != 'consolidado':
            query += " AND c.fornecedora = %s"
            params.append(fornecedora)
            
        logger.debug(f"REPORTS_BOLETOS - Query count_boletos_por_cliente: [{query}], Params: {params}")
        result = execute_query(query, tuple(params), fetch_one=True)
        return result[0] if result else 0
        
    except Exception as e:
        logger.error(f"Erro ao contar boletos por cliente: {e}", exc_info=True)
        return 0