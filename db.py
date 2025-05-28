import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()  # Carrega .env se estiver em ambiente local

def get_connection():
    prod_connection_string = os.getenv("CONNECTION_STRING_PROD")

    if prod_connection_string:
        # Estamos em produção (Render)
        return psycopg2.connect(prod_connection_string)
    else: 
        # Ambiente local
        return psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", 5432),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "senha"),
            dbname=os.getenv("DB_NAME", "simplefeed-local")
        )
