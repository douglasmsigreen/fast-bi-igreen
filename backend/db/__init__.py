# backend/db/__init__.py
import logging

# Importações do connection.py
from .connection import init_app, get_db, close_db, close_pool, db_pool

# Importações do executor.py
from .executor import execute_query, execute_query_one

# Importações do reports_base.py
from .reports_base import (
    get_base_nova_ids,
    get_base_enviada_ids,
    get_client_details_by_ids,
    build_query,
    count_query,
)

# Importações do reports_specific.py
from .reports_specific import (
    get_clientes_por_licenciado_data,
    count_clientes_por_licenciado,
    get_rateio_rzk_base_nova_ids,
    get_rateio_rzk_base_enviada_ids,
    get_rateio_rzk_client_details_by_ids,
    get_rateio_rzk_data,
    count_rateio_rzk,
    get_recebiveis_clientes_data,
    count_recebiveis_clientes,
    get_graduacao_licenciado_data,
    count_graduacao_licenciado
)

# Bloco de importação para o arquivo de boletos
from .reports_boletos import (
    get_boletos_por_cliente_data,
    count_boletos_por_cliente
)

# Importações do dashboard.py
from .dashboard import (
    get_total_consumo_medio_by_month,
    count_clientes_ativos_by_month,
    count_clientes_registrados_by_month,
    get_fornecedora_summary,
    get_concessionaria_summary,
    get_monthly_active_clients_by_year,
    get_active_clients_count_by_fornecedora_month,
    get_active_clients_count_by_concessionaria_month,
    get_state_map_data,
    get_fornecedora_summary_no_rcb,
    get_overdue_payments_by_fornecedora,
    get_green_score_by_fornecedora,
    get_overdue_clients_by_state_for_map,
)

# Importa a função do novo dashboard da TV
from .tv_dashboard import get_tv_dashboard_data