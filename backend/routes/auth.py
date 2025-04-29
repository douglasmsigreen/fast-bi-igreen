# backend/routes/auth.py
import logging
from flask import Blueprint, render_template, request, flash, redirect, url_for
from flask_login import login_user, logout_user, login_required, current_user
from urllib.parse import urlparse, urljoin
from ..models import User  # Import relativo do diretório pai
from ..forms import LoginForm # Import relativo do diretório pai

logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth_bp', __name__,
                    template_folder='../templates', # Opcional se já definido na app
                    static_folder='../static')     # Opcional se já definido na app

# --- Validador de URL 'next' (pode ser movido para um utilitário) ---
def is_safe_url(target):
    if not target: return True
    ref_url = urlparse(request.host_url); test_url = urlparse(urljoin(request.host_url, target))
    return test_url.scheme in ('http', 'https') and ref_url.netloc == test_url.netloc

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Rota para autenticação do utilizador."""
    if current_user.is_authenticated:
        return redirect(url_for('dashboard_bp.dashboard')) # Redireciona para o dashboard blueprint

    form = LoginForm()
    if form.validate_on_submit():
        user = User.get_by_email(form.email.data)
        if user and user.verify_password(form.password.data):
            login_user(user, remember=form.remember_me.data)
            logger.info(f"Login OK: '{form.email.data}'")
            next_page = request.args.get('next')
            # Valida o next_page antes de redirecionar
            if not is_safe_url(next_page):
                logger.warning(f"Tentativa de redirect inseguro bloqueada: '{next_page}'")
                next_page = url_for('dashboard_bp.dashboard') # Redireciona para dashboard por segurança

            flash('Login bem-sucedido!', 'success') # Mensagem de sucesso opcional
            return redirect(next_page or url_for('dashboard_bp.dashboard'))
        else:
            logger.warning(f"Tentativa de login falhou para: '{form.email.data}'")
            flash('Email ou senha inválidos.', 'danger')

    # Renderiza o template de login (procurará em ../templates/login.html)
    return render_template('login.html', title='Login', form=form)


@auth_bp.route('/logout')
@login_required
def logout():
    """Rota para fazer logout do utilizador."""
    user_email = current_user.email if hasattr(current_user, 'email') else '?'
    logger.info(f"Logout solicitado para: '{user_email}'")
    logout_user()
    flash('Logout efetuado com sucesso.', 'success')
    return redirect(url_for('auth_bp.login')) # Redireciona para a página de login deste blueprint