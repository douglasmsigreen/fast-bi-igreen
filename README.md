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

fast-bi-igreen/
├── backend/
│   ├── db/
│   │   ├── __init__.py                 # Agrupa e exporta funções do diretório db
│   │   ├── connection.py             # Gerencia o pool de conexões com o PostgreSQL
│   │   ├── dashboard.py              # Funções específicas para buscar dados do dashboard
│   │   ├── executor.py               # Função central para executar queries SQL
│   │   ├── reports_base.py           # Funções base para relatórios (Rateio Geral, Base Clientes)
│   │   └── reports_specific.py       # Funções para relatórios específicos (RZK, Licenciado, Boletos, Recebíveis)
│   │   └── utils.py                  # Funções utilitárias do DB (ex: get_headers, get_fornecedoras)
│   ├── routes/
│   │   ├── __init__.py                 # Inicialização do subpacote de rotas
│   │   ├── api.py                    # Rotas da API REST para o frontend (AJAX)
│   │   ├── auth.py                   # Rotas de autenticação (login, logout)
│   │   ├── dashboard.py              # Rota para a página principal do dashboard e mapa
│   │   └── reports.py                # Rota para a página de relatórios e exportação Excel
│   ├── __init__.py                     # Inicialização do pacote backend (App Factory: create_app)
│   ├── config.py                     # Configurações da aplicação (Secret Key, DB Config, etc.)
│   ├── exporter.py                   # Classe para exportar dados para Excel (openpyxl)
│   ├── forms.py                      # Definição de formulários (ex: LoginForm com Flask-WTF)
│   └── models.py                     # Modelo de dados (ex: classe User para Flask-Login)
├── static/
│   ├── css/
│   │   └── style.css                 # Folha de estilos principal [cite: 1]
│   ├── js/
│   │   ├── script.js                 # Script JS principal (ex: filtro de tabela) [cite: 2]
│   │   └── dashboard_charts.js       # (ARQUIVO NÃO INCLUÍDO NO ZIP - mas mencionado na estrutura anterior, manter?)
│   ├── img/
│   │   ├── favicon.png               # Ícone da aba do navegador
│   │   ├── logo_igreen.png           # Logo principal (usado no login)
│   │   ├── logo_igreen_branco.png    # Logo branco (usado na sidebar)
│   │   └── telalogin.jpg             # Imagem de fundo da tela de login
│   └── geojson/
│       └── brasil-estados.geojson    # Arquivo GeoJSON para o mapa de estados
├── templates/
│   ├── components/                   # Pasta para componentes reutilizáveis do template
│   │   ├── footer.html               # Componente do rodapé
│   │   ├── flash_messages.html       # Componente para exibir mensagens flash
│   │   ├── header.html               # Componente do cabeçalho da área de conteúdo
│   │   └── sidebar.html              # Componente da barra lateral de navegação
│   ├── layouts/                      # Pasta para layouts base
│   │   └── base_layout.html          # Layout HTML base para páginas autenticadas
│   ├── dashboard.html                # Template da página principal do dashboard
│   ├── login.html                    # Template da página de login
│   ├── mapa_clientes.html            # Template da página do mapa de clientes
│   └── relatorios.html               # Template da página de relatórios [cite: 3]
├── .env                              # Arquivo para variáveis de ambiente (não versionar!)
├── README.md                         # Documentação do projeto
├── requirements.txt                  # Lista de dependências Python
├── run.py                            # Script para iniciar a aplicação Flask
└── script inicialização.txt          # Instruções básicas para setup e execução (provavelmente local)
