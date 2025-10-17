from sqlalchemy import create_engine, text
from src.config import DB_URL, DB_TYPE

def test_db_connection():
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            if DB_TYPE == "postgres":
                result = conn.execute(text("SELECT version();"))
            else:
                result = conn.execute(text("SELECT sqlite_version();"))
            print("✅ Connected successfully!")
            print("Database Version:", result.fetchone())
    except Exception as e:
        print("❌ Connection failed:", e)

if __name__ == "__main__":
    test_db_connection()
