# backend/db/tv_dashboard.py
import logging
from .executor import execute_query, execute_query_one

logger = logging.getLogger(__name__)

def get_tv_dashboard_data():
    """
    Busca todos os dados necessários para o dashboard da TV.
    Inclui KPIs de ativações e consumo, top 5 regiões/fornecedoras e ativações por mês.
    """
    logger.info("Buscando todos os dados para o Dashboard da TV.")
    data = {}

    try:
        # 1. Total de Ativações
        query_ativacoes = """
            SELECT
              (
                SELECT
                  COUNT(*)
                FROM
                  public."V_CUSTOMER"
                WHERE
                  "data ativo" BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE
              ) AS "contagem_mes_atual",
              (
                SELECT
                  COUNT(*)
                FROM
                  public."V_CUSTOMER"
                WHERE
                  "data ativo" BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AND (CURRENT_DATE - INTERVAL '1 month')
              ) AS "contagem_mes_anterior";
        """
        ativacoes = execute_query_one(query_ativacoes)
        data['ativacoes'] = ativacoes

        # 2. Total de kWh
        query_kwh = """
            SELECT
              (
                SELECT
                  SUM("média consumo")
                FROM
                  public."V_CUSTOMER"
                WHERE
                  "data ativo" BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE
              ) AS "soma_consumo_mes_atual",
              (
                SELECT
                  SUM("média consumo")
                FROM
                  public."V_CUSTOMER"
                WHERE
                  "data ativo" BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AND (CURRENT_DATE - INTERVAL '1 month')
              ) AS "soma_consumo_mes_anterior";
        """
        kwh = execute_query_one(query_kwh)
        data['kwh'] = kwh

        # 3. Top 5 Regiões
        query_regioes = """
            SELECT
              "região",
              COUNT(*) AS "quantidade_registros",
              SUM("média consumo") AS "soma_consumo"
            FROM
              public."V_CUSTOMER"
            WHERE
              "data ativo" BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE
            GROUP BY
              "região"
            ORDER BY
              "quantidade_registros" DESC
            LIMIT 5;
        """
        regioes = execute_query(query_regioes)
        data['top_regioes'] = regioes

        # 4. Top 5 Fornecedoras
        query_fornecedoras = """
            SELECT
              "fornecedora",
              COUNT(*) AS "quantidade_registros",
              SUM("média consumo") AS "soma_consumo"
            FROM
              public."V_CUSTOMER"
            WHERE
              "data ativo" BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE
            GROUP BY
              "fornecedora"
            ORDER BY
              "quantidade_registros" DESC
            LIMIT 5;
        """
        fornecedoras = execute_query(query_fornecedoras)
        data['top_fornecedoras'] = fornecedoras

        # 5. Gráfico de ativações por mês
        query_grafico_mes = """
            SELECT
              EXTRACT(MONTH FROM "data ativo") AS "mes",
              COUNT(*) AS "quantidade"
            FROM
              public."V_CUSTOMER"
            WHERE
              EXTRACT(YEAR FROM "data ativo") = EXTRACT(YEAR FROM CURRENT_DATE)
            GROUP BY
              "mes"
            ORDER BY
              "mes";
        """
        grafico_mes = execute_query(query_grafico_mes)
        data['grafico_ativacoes_mes'] = grafico_mes
        
    except Exception as e:
        logger.error(f"Erro ao buscar dados para o dashboard da TV: {e}", exc_info=True)
        # Retorna um dicionário vazio para o frontend lidar com o erro
        return None

    return data