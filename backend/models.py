# models.py
from flask_login import UserMixin
import bcrypt # Importa a biblioteca bcrypt para verificação de senha
from . import db as database # Importa nosso módulo de banco de dados para acesso às funções de query
import logging # Adiciona import para logger

# Obtém o logger configurado em app.py ou database.py
logger = logging.getLogger(__name__) # Usa o logger do módulo atual

class User(UserMixin):
    """Modelo de Utilizador para Flask-Login.

    Usa bcrypt para verificação de senha e mapeia para as colunas
    'codigo', 'email', 'password' e 'nome' da tabela public."USUARIOS".
    """

    # --- MÉTODO __init__ MODIFICADO ---
    def __init__(self, user_id, email, password_hash_from_db, nome): # Adicionado 'nome'
        """
        Inicializa o objeto User.
        Args:
            user_id: O valor da coluna 'codigo' do utilizador.
            email: O valor da coluna 'email' do utilizador.
            password_hash_from_db: O valor da coluna 'password' (que contém o hash bcrypt).
            nome: O valor da coluna 'nome' do utilizador.
        """
        self.id = user_id # Flask-Login espera um atributo 'id'. Mapeamos 'codigo' para 'id'.
        self.email = email # Atributo para armazenar o email.
        self.password_hash = password_hash_from_db # Atributo para armazenar o hash bcrypt lido do banco.
        self.nome = nome # <<< NOVO ATRIBUTO ARMAZENADO

    # --- MÉTODO verify_password (com bcrypt) ---
    def verify_password(self, password_input):
        """Verifica se a senha fornecida corresponde ao hash bcrypt armazenado."""
        # Verifica se temos um hash e uma senha para comparar
        if not self.password_hash or not password_input:
            logger.debug(f"Tentativa de verificar senha com hash ou input vazio para user ID {self.id}.")
            return False
        try:
            # bcrypt.checkpw exige que ambos os argumentos sejam bytes.
            password_bytes = password_input.encode('utf-8')
            hash_bytes = self.password_hash.encode('utf-8')
            # Compara a senha digitada (bytes) com o hash armazenado (bytes)
            is_valid = bcrypt.checkpw(password_bytes, hash_bytes)
            if not is_valid:
                logger.debug(f"Senha incorreta fornecida para user ID {self.id} (email: {self.email}).")
            return is_valid
        except ValueError as e:
             logger.error(f"Erro ao verificar hash bcrypt (formato inválido?) para user ID {self.id}: {e}")
             return False
        except Exception as e:
             logger.error(f"Erro inesperado durante bcrypt.checkpw para user ID {self.id}: {e}", exc_info=True)
             return False

    # --- Métodos Estáticos para buscar Utilizadores ---

    @staticmethod
    def get_by_id(user_id):
        """Busca um utilizador no banco de dados pelo seu ID ('codigo'). Usado pelo user_loader."""
        if not user_id: return None
        # Seleciona as colunas necessárias, incluindo 'nome'
        query = 'SELECT codigo, email, "password", nome FROM public."USUARIOS" WHERE codigo = %s;'
        logger.debug(f"Buscando utilizador por ID (codigo): {user_id}")
        try:
            # CORREÇÃO: Usando execute_query_one e removendo 'fetch_one=True'
            result = database.execute_query_one(query, (user_id,))
            if result:
                # CORREÇÃO: Acessando os resultados por chave (dicionário)
                user = User(
                    user_id=result['codigo'], 
                    email=result['email'], 
                    password_hash_from_db=result['password'], 
                    nome=result['nome']
                )
                logger.debug(f"Utilizador encontrado por ID: {user.email}, Nome: {user.nome}")
                return user
            else:
                logger.debug(f"Utilizador com ID (codigo) {user_id} não encontrado.")
                return None
        except (RuntimeError, ConnectionError) as e:
            logger.error(f"Erro de banco ao buscar utilizador por ID (codigo={user_id}): {e}")
            return None
        except Exception as e:
            logger.error(f"Erro inesperado em get_by_id para ID {user_id}: {e}", exc_info=True)
            return None

    @staticmethod
    def get_by_email(email):
        """Busca um utilizador no banco de dados pelo seu email. Usado na rota de login."""
        if not email: return None
        # Seleciona as colunas necessárias, incluindo 'nome'
        query = 'SELECT codigo, email, "password", nome FROM public."USUARIOS" WHERE email = %s;'
        logger.debug(f"Buscando utilizador por email: {email}")
        try:
            # CORREÇÃO: Usando execute_query_one e removendo 'fetch_one=True'
            result = database.execute_query_one(query, (email,))
            if result:
                # CORREÇÃO: Acessando os resultados por chave (dicionário)
                user = User(
                    user_id=result['codigo'], 
                    email=result['email'], 
                    password_hash_from_db=result['password'], 
                    nome=result['nome']
                )
                logger.debug(f"Utilizador encontrado por email: {user.email} (ID: {user.id}, Nome: {user.nome})")
                return user
            else:
                 logger.debug(f"Utilizador com email {email} não encontrado.")
                 return None
        except (RuntimeError, ConnectionError) as e:
            logger.error(f"Erro de banco ao buscar utilizador por email {email}: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro inesperado em get_by_email para email {email}: {e}", exc_info=True)
            return None

    # --- (Opcional) Geração de Hash ---
    # Se precisar gerar hashes bcrypt em outros lugares
    # from werkzeug.security import generate_password_hash # Pode usar werkzeug OU bcrypt para gerar
    # @staticmethod
    # def generate_hash(password):
    #     if not password: return None
    #     password_bytes = password.encode('utf-8')
    #     hashed_bytes = bcrypt.hashpw(password_bytes, bcrypt.gensalt())
    #     return hashed_bytes.decode('utf-8') # Retorna como string

    # def set_password(self, password):
    #     """Gera o hash bcrypt para a senha e atualiza o atributo."""
    #     self.password_hash = User.generate_hash(password)
    #     # Chamar função em database.py para salvar self.password_hash no DB para self.id
    #     # ex: database.update_user_password(self.id, self.password_hash)