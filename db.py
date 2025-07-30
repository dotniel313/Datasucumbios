# db.py
# -----------------------------------------------------------------------------
# Módulo de conexión a la base de datos.
# Centraliza la lógica para conectarse a PostgreSQL (Supabase).
# -----------------------------------------------------------------------------

import os
import psycopg2
from dotenv import load_dotenv

# Carga las variables de entorno desde un archivo .env (ideal para desarrollo local)
load_dotenv()

def get_db_connection():
    """
    Crea y retorna una nueva conexión a la base de datos.
    Lee la URL de conexión desde las variables de entorno para mayor seguridad.
    """
    try:
        # Render y Supabase proveen la URL en una variable de entorno llamada DATABASE_URL
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        return conn
    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")
        return None