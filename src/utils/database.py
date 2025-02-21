import psycopg2  # type: ignore
from utils.config import Config


def get_main_connection():
    try:
        conn = psycopg2.connect(
            host=Config.DB_MAIN_HOST,
            database=Config.DB_MAIN_NAME,
            user=Config.DB_MAIN_USER,
            password=Config.DB_MAIN_PASSWORD,
            port=Config.DB_MAIN_PORT,
        )
        print("Connected to the database")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


def get_collector_connection():
    try:
        conn = psycopg2.connect(
            host=Config.DB_COLLECTOR_HOST,
            database=Config.DB_COLLECTOR_NAME,
            user=Config.DB_COLLECTOR_USER,
            password=Config.DB_COLLECTOR_PASSWORD,
            port=Config.DB_COLLECTOR_PORT,
        )
        return conn
    except Exception as e:
        raise Exception("Error connecting to the database: {e}")
