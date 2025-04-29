# backend/routes/dashboard.py
import logging
import re
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, flash, current_app, url_for, redirect
from flask_login import login_required, current_user # <--- Adicionar current_user aqui
from .. import db # Importa o módulo database do __init__.py

logger = logging.getLogger(__name__)

dashboard_bp = Blueprint('dashboard_bp', __name__,
                         template_folder='../templates',
                         static_folder='../static')

@dashboard_bp.route('/')
@login_required
def dashboard():
    """Rota para a página inicial do dashboard."""
    logger.info(f"Acessando dashboard. Utilizador: {current_user.email if hasattr(current_user, 'email') else 'N/A'}")
    # Dados iniciais para placeholders (a maioria é carregada via AJAX)
    total_kwh_mes = 0.0
    clientes_ativos_count = 0
    clientes_registrados_count = 0
    error_dashboard = None

    # Valida e define o mês selecionado (padrão: mês atual)
    selected_month_str = request.args.get('month')
    if not selected_month_str or not re.match(r'^\d{4}-\d{2}$', selected_month_str):
        selected_month_str = datetime.now().strftime('%Y-%m')
        logger.debug(f"Mês não fornecido ou inválido no dashboard, usando padrão: {selected_month_str}")
        # Opcional: redirecionar para a URL com o mês padrão para consistência
        # return redirect(url_for('dashboard_bp.dashboard', month=selected_month_str))

    # Busca inicial de placeholders (melhora UX, mas principal carga é AJAX)
    try:
        # O ideal é que estas funções usem `current_app.logger` se precisarem logar
        # ou que o logger seja configurado adequadamente no módulo db.
        total_kwh_mes = db.get_total_consumo_medio_by_month(month_str=selected_month_str)
        clientes_ativos_count = db.count_clientes_ativos_by_month(month_str=selected_month_str)
        clientes_registrados_count = db.count_clientes_registrados_by_month(month_str=selected_month_str)
        logger.debug(f"KPIs iniciais carregados para {selected_month_str}: kWH={total_kwh_mes}, Ativos={clientes_ativos_count}, Registrados={clientes_registrados_count}")
    except Exception as e:
        logger.error(f"Erro ao carregar KPIs iniciais do dashboard para {selected_month_str}: {e}", exc_info=True)
        error_dashboard = "Erro ao carregar indicadores iniciais do painel."
        # Não precisa flash aqui se a carga principal for AJAX, mas pode ser útil
        # flash(error_dashboard, "warning")
        # Zera os valores em caso de erro
        total_kwh_mes = 0.0
        clientes_ativos_count = 0
        clientes_registrados_count = 0

    # Gera opções para o dropdown de mês (lógica movida do app.py)
    month_options = []
    current_date = datetime.now()
    try:
        for i in range(12): # Últimos 12 meses
            dt = current_date - timedelta(days=i * 30) # Aproximação
            month_val = dt.strftime('%Y-%m')
            month_text = dt.strftime('%b/%Y').upper().replace('.', '') # Formato PT-BR
            # Mapeamento manual (alternativa a locale)
            month_map_pt = {'JAN': 'JAN', 'FEB': 'FEV', 'MAR': 'MAR', 'APR': 'ABR', 'MAY': 'MAI', 'JUN': 'JUN', 'JUL': 'JUL', 'AUG': 'AGO', 'SEP': 'SET', 'OCT': 'OUT', 'NOV': 'NOV', 'DEC': 'DEZ'}
            for en, pt in month_map_pt.items(): month_text = month_text.replace(en, pt)
            month_options.append({'value': month_val, 'text': month_text})

        # Garante que o mês selecionado esteja na lista
        if selected_month_str not in [m['value'] for m in month_options]:
            sel_dt = datetime.strptime(selected_month_str + '-01', '%Y-%m-%d')
            sel_text = sel_dt.strftime('%b/%Y').upper().replace('.', '')
            for en, pt in month_map_pt.items(): sel_text = sel_text.replace(en, pt)
            month_options.insert(0, {'value': selected_month_str, 'text': sel_text})
    except Exception as e:
        logger.error(f"Erro ao gerar opções de mês para o dashboard: {e}")
        # Adiciona pelo menos o mês atual como fallback
        if not month_options:
            month_options.append({'value': selected_month_str, 'text': selected_month_str})


    # Renderiza o template (procurará em ../templates/dashboard.html)
    return render_template(
        'dashboard.html',
        title="Dashboard - Fast BI",
        total_kwh=total_kwh_mes,
        clientes_ativos_count=clientes_ativos_count,
        clientes_registrados_count=clientes_registrados_count,
        month_options=month_options,
        selected_month=selected_month_str,
        # Não passamos mais os resumos, eles vêm via API
        error_summary=error_dashboard # Apenas para erros de KPIs iniciais
    )

# --- ROTA ADICIONADA PARA O MAPA DE CLIENTES ---
@dashboard_bp.route('/mapa-clientes')
@login_required
def mapa_clientes():
    """Rota para a página do Mapa de Clientes."""
    logger.info(f"Acessando /mapa-clientes. Utilizador: {current_user.email if hasattr(current_user, 'email') else 'N/A'}")
    # A página HTML em si não precisa de dados extras do Flask aqui
    # O template será buscado em ../templates/mapa_clientes.html
    return render_template('mapa_clientes.html', title="Mapa de Clientes - Fast BI")
# --- FIM DA ROTA ADICIONADA ---