from dotenv import load_dotenv  # type: ignore
import os

load_dotenv()


class Config:
    DB_MAIN_HOST = os.getenv("DB_MAIN_HOST")
    DB_MAIN_PORT = os.getenv("DB_MAIN_PORT")
    DB_MAIN_USER = os.getenv("DB_MAIN_USER")
    DB_MAIN_PASSWORD = os.getenv("DB_MAIN_PASSWORD")
    DB_MAIN_NAME = os.getenv("DB_MAIN_NAME")

    DB_COLLECTOR_HOST = os.getenv("DB_COLLECTOR_HOST")
    DB_COLLECTOR_PORT = os.getenv("DB_COLLECTOR_PORT")
    DB_COLLECTOR_USER = os.getenv("DB_COLLECTOR_USER")
    DB_COLLECTOR_PASSWORD = os.getenv("DB_COLLECTOR_PASSWORD")
    DB_COLLECTOR_NAME = os.getenv("DB_COLLECTOR_NAME")
