# src/database/init_db.py

from sqlalchemy import create_engine
from src.config import DB_URL  # Adjust path if config.py changes
from src.database.models import Base

def init_db():
    """
    Creates all tables defined in models.py inside the database.
    """
    print("🔄 Connecting to database...")
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    print("✅ Tables created successfully!")

if __name__ == "__main__":
    init_db()
