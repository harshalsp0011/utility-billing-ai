# src/config.py
import os
from dotenv import load_dotenv
from sqlalchemy.engine.url import URL

# Load from .env file for local development
load_dotenv()

def get_env(key: str, default=None):
    """
    Get environment variable from Streamlit secrets (if running on Streamlit Cloud)
    or from os.environ/.env (if running locally).
    
    Parameters
    ----------
    key : str
        Environment variable name
    default : str, optional
        Default value if key not found
        
    Returns
    -------
    str
        Environment variable value or default
    """
    # Try Streamlit secrets first (only available when running on Streamlit Cloud)
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except (ImportError, AttributeError, KeyError, FileNotFoundError):
        pass
    
    # Fall back to os.environ/.env
    return os.getenv(key, default)

DB_TYPE = get_env("DB_TYPE", "sqlite")
DB_URL = None

if DB_TYPE == "postgres":
    DB_URL = URL.create(
        drivername="postgresql+psycopg2",
        username=get_env("DB_USER"),
        password=get_env("DB_PASSWORD"),
        host=get_env("DB_HOST"),
        port=get_env("DB_PORT"),
        database=get_env("DB_NAME"),
    )
else:
    DB_PATH = get_env("DB_PATH", "data/project.db")
    DB_URL = f"sqlite:///{DB_PATH}"

# Removed print statement to avoid output during DAG import in Airflow
# print(f"✅ Using database: {DB_URL}")
ENV = get_env("ENV", "dev")

# -------------------------
# Airflow API configuration
# -------------------------
# These are read by the Streamlit UI and helper modules. Set them in your
# project .env (see .env.example) or via Streamlit Secrets in production.
AIRFLOW_API_URL = get_env("AIRFLOW_API_URL", "http://localhost:8080/api/v2")
AIRFLOW_API_USER = get_env("AIRFLOW_API_USER")
AIRFLOW_API_PASSWORD = get_env("AIRFLOW_API_PASSWORD")
AIRFLOW_DAG_ID = get_env("AIRFLOW_DAG_ID", "utility_billing_pipeline")



# -------------------------
# LLM Configuration
# -------------------------
OPENAI_API_KEY = get_env("OPENAI_API_KEY", "")
OPENAI_MODEL = get_env("OPENAI_MODEL", "gpt-4o-mini")
