# forms.py
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField
from wtforms.validators import DataRequired, Length, Email # Adicionar validador de Email

class LoginForm(FlaskForm):
    """Formulário de Login."""
    # Campo renomeado para email, label atualizado, validador Email adicionado
    email = StringField('Email', validators=[
        DataRequired(message="O email é obrigatório."),
        Email(message="Por favor, insira um email válido.") # Valida o formato do email
    ])
    password = PasswordField('Senha', validators=[
        DataRequired(message="A senha é obrigatória.")
    ])
    remember_me = BooleanField('Lembrar-me')
    submit = SubmitField('Entrar')