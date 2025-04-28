# Fast BI (iGreen) - Aplicação Web de Business Intelligence

## Descrição

Esta é uma aplicação web de Business Intelligence (BI) desenvolvida para visualização e análise de dados da iGreen Energy. Ela permite a visualização de dashboards interativos, geração de relatórios detalhados, visualização de dados geográficos e exportação de informações para Excel.

A aplicação utiliza Python (Flask) no backend para processamento de dados e lógica de negócios, e HTML, CSS e JavaScript no frontend para a interface do utilizador.

## Funcionalidades

* **Autenticação:** Sistema de login seguro para utilizadores registrados.
* **Dashboard Interativo:**
    * Visualização de KPIs chave (Total kWh vendido, Clientes Ativados no mês, Clientes Registrados no mês) com filtro por mês.
    * Gráfico de evolução mensal de ativações de clientes (Chart.js) com filtro por ano.
    * Tabelas de resumo de clientes e consumo por Fornecedora e por Região/Concessionária, atualizadas dinamicamente via AJAX e com ordenação de colunas.
* **Relatórios Detalhados:**
    * Geração de múltiplos relatórios: Base Clientes, Rateio Geral, Rateio RZK (Especial), Clientes por Licenciado, Boletos por Cliente[cite: 3].
    * Filtros por tipo de relatório e fornecedora (quando aplicável)[cite: 3].
    * Paginação para lidar com grandes volumes de dados[cite: 3].
    * Funcionalidade de pesquisa em tempo real na tabela exibida[cite: 2, 3].
    * Cálculo de "Dias Ativo" no relatório de Boletos por Cliente.
* **Mapa de Clientes:**
    * Visualização geográfica da quantidade de clientes ativos e consumo médio por estado brasileiro usando Plotly.js.
    * Dados carregados via API.
* **Exportação para Excel:**
    * Funcionalidade para exportar os dados completos de qualquer relatório visualizado para um arquivo `.xlsx`.
    * Geração de arquivos multi-abas para os relatórios de Rateio (Base Nova / Base Enviada).
    * Formatação automática (cabeçalhos coloridos, largura de coluna ajustada, painéis congelados, autofiltro).

## Tecnologias Utilizadas

* **Backend:**
    * Python 3
    * Flask (Web Framework)
    * Flask-Login (Autenticação)
    * Flask-WTF (Formulários)
    * psycopg2-binary (Driver PostgreSQL)
    * openpyxl (Manipulação de arquivos Excel)
    * python-dotenv (Variáveis de ambiente)
* **Frontend:**
    * HTML5 [cite: 3]
    * CSS3 (Estilos customizados, veja `static/css/style.css`) [cite: 1]
    * JavaScript (Vanilla JS, Fetch API) [cite: 2]
    * Chart.js (Gráfico de evolução no dashboard)
    * Plotly.js (Mapa de clientes)
    * Font Awesome (Ícones)
* **Banco de Dados:**
    * PostgreSQL (Implícito pelo uso de `psycopg2` e queries SQL)

## Estrutura do Projeto (Simplificada)