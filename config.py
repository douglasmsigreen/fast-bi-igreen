# config.py
import os
from dotenv import load_dotenv

load_dotenv() # Carrega variáveis do .env

class Config:
    SECRET_KEY = os.getenv('FLASK_SECRET_KEY', 'default_secret_key_for_dev')
    DB_CONFIG = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "connect_timeout": 10
    }
    # Adicione outras configurações se necessário (ex: itens por página)
    ITEMS_PER_PAGE = 50