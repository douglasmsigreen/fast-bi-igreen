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

                # --- NOVO: Cadastros, Validados e Cancelados ---
        query_cadastros = """
            SELECT
              (
                SELECT
                  COUNT(*)
                FROM
                  public."V_CUSTOMER"
                WHERE
                  DATE_TRUNC('month', "data cadastro") = DATE_TRUNC('month', CURRENT_DATE)
              ) AS "cadastrados_quantidade",
              (
                SELECT
                  SUM("média consumo")
                FROM
                  public."V_CUSTOMER"
                WHERE
                  DATE_TRUNC('month', "data cadastro") = DATE_TRUNC('month', CURRENT_DATE)
              ) AS "cadastrados_soma_consumo",
              (
                SELECT
                  COUNT(*)
                FROM
                  public."V_CUSTOMER"
                WHERE
                  "validado sucesso" = 'S'
                  AND DATE_TRUNC('month', "data ativo") = DATE_TRUNC('month', CURRENT_DATE)
              ) AS "validados_quantidade",
              (
                SELECT
                  SUM("média consumo")
                FROM
                  public."V_CUSTOMER"
                WHERE
                  "validado sucesso" = 'S'
                  AND DATE_TRUNC('month', "data ativo") = DATE_TRUNC('month', CURRENT_DATE)
              ) AS "validados_soma_consumo",
              (
                SELECT
                  COUNT(*)
                FROM
                  public."V_CUSTOMER"
                WHERE
                  DATE_TRUNC('month', "data cancelamento") = DATE_TRUNC('month', CURRENT_DATE)
              ) AS "cancelados_quantidade",
              (
                SELECT
                  SUM("média consumo")
                FROM
                  public."V_CUSTOMER"
                WHERE
                  DATE_TRUNC('month', "data cancelamento") = DATE_TRUNC('month', CURRENT_DATE)
              ) AS "cancelados_soma_consumo";
        """
        cadastros = execute_query_one(query_cadastros)

        # Query para "Backlog - A Validar"
        query_backlog_a_validar = """
            SELECT
                COUNT(*) AS a_validar_quantidade,
                SUM(c.consumomedio)::bigint AS a_validar_soma_consumo
            FROM "CLIENTES" c
                LEFT JOIN "MV_DEVOLUTIVAS" d ON d.idcliente = c.idcliente
            WHERE
                c.data_ativo >= DATE_TRUNC('year', CURRENT_DATE)
                AND c.data_ativo < DATE_TRUNC('month', CURRENT_DATE)
                AND (c.status IS NULL OR c.status = '')
                AND (d.msgdevolutiva IS NULL OR d.msgdevolutiva = '')
                AND c.validadosucesso = 'N'
                AND (c.fornecedora IS NOT NULL OR c.fornecedora <> '');
        """
        backlog_a_validar = execute_query_one(query_backlog_a_validar)

        # Query para "Mês Atual - A Validar"
        query_mes_atual_a_validar = """
            SELECT
                COUNT(*) AS a_validar_quantidade,
                SUM(c.consumomedio)::bigint AS a_validar_soma_consumo
            FROM "CLIENTES" c
                LEFT JOIN "MV_DEVOLUTIVAS" d ON d.idcliente = c.idcliente
            WHERE
                c.data_ativo >= DATE_TRUNC('month', CURRENT_DATE)
                AND c.data_ativo <= CURRENT_DATE
                AND (c.status IS NULL OR c.status = '')
                AND (d.msgdevolutiva IS NULL OR d.msgdevolutiva = '')
                AND c.validadosucesso = 'N'
                AND (c.fornecedora IS NOT NULL OR c.fornecedora <> '');
        """
        mes_atual_a_validar = execute_query_one(query_mes_atual_a_validar)
        
        if cadastros:
            # Converte para dict para poder adicionar novas chaves
            cadastros = dict(cadastros)
            
            if backlog_a_validar:
                cadastros['backlog_a_validar_quantidade'] = backlog_a_validar.get('a_validar_quantidade', 0)
                cadastros['backlog_a_validar_soma_consumo'] = backlog_a_validar.get('a_validar_soma_consumo', 0)
            else:
                cadastros['backlog_a_validar_quantidade'] = 0
                cadastros['backlog_a_validar_soma_consumo'] = 0

            if mes_atual_a_validar:
                cadastros['a_validar_quantidade'] = mes_atual_a_validar.get('a_validar_quantidade', 0)
                cadastros['a_validar_soma_consumo'] = mes_atual_a_validar.get('a_validar_soma_consumo', 0)
            else:
                cadastros['a_validar_quantidade'] = 0
                cadastros['a_validar_soma_consumo'] = 0

        data['cadastros'] = cadastros
        # --- FIM NOVO ---

        # 3. Top 5 Regiões
        query_regioes = """
            SELECT
              "região",
              COUNT(*) AS "quantidade_registros",
              SUM("média consumo") AS "soma_consumo",
              COUNT(CASE WHEN "validado sucesso" = 'S' THEN 1 END) AS "registros_validados",
              SUM(CASE WHEN "validado sucesso" = 'S' THEN "média consumo" ELSE 0 END) AS "consumo_validados"
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
              SUM("média consumo") AS "soma_consumo",
              COUNT(CASE WHEN "validado sucesso" = 'S' THEN 1 END) AS "registros_validados",
              SUM(CASE WHEN "validado sucesso" = 'S' THEN "média consumo" ELSE 0 END) AS "consumo_validados"
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

        # --- NOVO: Top 5 Licenciados ---
        query_licenciados = """
            SELECT
              vc."licenciado",
              co.uf,
              COUNT(vc.*) AS "quantidade_registros",
              SUM(vc."média consumo") AS "soma_consumo"
            FROM
              public."V_CUSTOMER" AS vc
            INNER JOIN
              public."CONSULTOR" AS co ON vc."id licenciado" = co.idconsultor
            WHERE
              vc."data ativo" BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE
            GROUP BY
              vc."licenciado",
              co.uf
            ORDER BY
              "quantidade_registros" DESC
            LIMIT 5;
        """
        licenciados = execute_query(query_licenciados)
        data['top_licenciados'] = licenciados
        # --- FIM NOVO ---

        # 5. Gráfico de ativações por mês
        query_grafico_mes = """
            WITH daily_cumulative_counts AS (
              SELECT
                EXTRACT(MONTH FROM "data ativo") AS mes,
                EXTRACT(DAY FROM "data ativo") AS dia,
                COUNT(*) AS contagem_diaria,
                SUM(COUNT(*)) OVER (PARTITION BY EXTRACT(MONTH FROM "data ativo") ORDER BY EXTRACT(DAY FROM "data ativo")) AS soma_acumulada
              FROM
                public."V_CUSTOMER"
              WHERE
                EXTRACT(YEAR FROM "data ativo") = EXTRACT(YEAR FROM CURRENT_DATE)
                AND "data ativo" <= CURRENT_DATE
              GROUP BY
                mes,
                dia
            )
            SELECT
              mes,
              SUM(CASE WHEN dia = 1 THEN soma_acumulada ELSE 0 END) AS "dia_1",
              SUM(CASE WHEN dia = 2 THEN soma_acumulada ELSE 0 END) AS "dia_2",
              SUM(CASE WHEN dia = 3 THEN soma_acumulada ELSE 0 END) AS "dia_3",
              SUM(CASE WHEN dia = 4 THEN soma_acumulada ELSE 0 END) AS "dia_4",
              SUM(CASE WHEN dia = 5 THEN soma_acumulada ELSE 0 END) AS "dia_5",
              SUM(CASE WHEN dia = 6 THEN soma_acumulada ELSE 0 END) AS "dia_6",
              SUM(CASE WHEN dia = 7 THEN soma_acumulada ELSE 0 END) AS "dia_7",
              SUM(CASE WHEN dia = 8 THEN soma_acumulada ELSE 0 END) AS "dia_8",
              SUM(CASE WHEN dia = 9 THEN soma_acumulada ELSE 0 END) AS "dia_9",
              SUM(CASE WHEN dia = 10 THEN soma_acumulada ELSE 0 END) AS "dia_10",
              SUM(CASE WHEN dia = 11 THEN soma_acumulada ELSE 0 END) AS "dia_11",
              SUM(CASE WHEN dia = 12 THEN soma_acumulada ELSE 0 END) AS "dia_12",
              SUM(CASE WHEN dia = 13 THEN soma_acumulada ELSE 0 END) AS "dia_13",
              SUM(CASE WHEN dia = 14 THEN soma_acumulada ELSE 0 END) AS "dia_14",
              SUM(CASE WHEN dia = 15 THEN soma_acumulada ELSE 0 END) AS "dia_15",
              SUM(CASE WHEN dia = 16 THEN soma_acumulada ELSE 0 END) AS "dia_16",
              SUM(CASE WHEN dia = 17 THEN soma_acumulada ELSE 0 END) AS "dia_17",
              SUM(CASE WHEN dia = 18 THEN soma_acumulada ELSE 0 END) AS "dia_18",
              SUM(CASE WHEN dia = 19 THEN soma_acumulada ELSE 0 END) AS "dia_19",
              SUM(CASE WHEN dia = 20 THEN soma_acumulada ELSE 0 END) AS "dia_20",
              SUM(CASE WHEN dia = 21 THEN soma_acumulada ELSE 0 END) AS "dia_21",
              SUM(CASE WHEN dia = 22 THEN soma_acumulada ELSE 0 END) AS "dia_22",
              SUM(CASE WHEN dia = 23 THEN soma_acumulada ELSE 0 END) AS "dia_23",
              SUM(CASE WHEN dia = 24 THEN soma_acumulada ELSE 0 END) AS "dia_24",
              SUM(CASE WHEN dia = 25 THEN soma_acumulada ELSE 0 END) AS "dia_25",
              SUM(CASE WHEN dia = 26 THEN soma_acumulada ELSE 0 END) AS "dia_26",
              SUM(CASE WHEN dia = 27 THEN soma_acumulada ELSE 0 END) AS "dia_27",
              SUM(CASE WHEN dia = 28 THEN soma_acumulada ELSE 0 END) AS "dia_28",
              SUM(CASE WHEN dia = 29 THEN soma_acumulada ELSE 0 END) AS "dia_29",
              SUM(CASE WHEN dia = 30 THEN soma_acumulada ELSE 0 END) AS "dia_30",
              SUM(CASE WHEN dia = 31 THEN soma_acumulada ELSE 0 END) AS "dia_31"
            FROM
              daily_cumulative_counts
            GROUP BY
              mes
            ORDER BY
              mes;
        """
        grafico_mes = execute_query(query_grafico_mes)
        data['grafico_ativacoes_mes'] = grafico_mes
        
    except Exception as e:
        logger.error(f"Erro ao buscar dados para o dashboard da TV: {e}", exc_info=True)
        # Retorna um dicionário vazio para o frontend lidar com o erro
        return None

    return data