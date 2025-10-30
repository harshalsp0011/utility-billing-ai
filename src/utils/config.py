# src/config.py
import os
from dotenv import load_dotenv
from sqlalchemy.engine.url import URL

load_dotenv()

DB_TYPE = os.getenv("DB_TYPE", "sqlite")
DB_URL = None

if DB_TYPE == "postgres":
    DB_URL = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
    )
else:
    DB_PATH = os.getenv("DB_PATH", "data/project.db")
    DB_URL = f"sqlite:///{DB_PATH}"

# Removed print statement to avoid output during DAG import in Airflow
# print(f"✅ Using database: {DB_URL}")
ENV = os.getenv("ENV", "dev")