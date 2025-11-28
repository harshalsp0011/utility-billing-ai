# src/database/init_db.py

from sqlalchemy import create_engine, text
from src.utils.config import DB_URL  # Adjust path if config.py changes
from src.database.models import Base

def init_db():
    """
    Creates all tables defined in models.py inside the database.
    """
    print("🔄 Connecting to database...")
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    print("✅ Tables created successfully!")

    # Post-create safety migrations
    with engine.begin() as conn:
        # Ensure tariff_logic.sc_code can hold long composite codes
        try:
            res = conn.execute(text("""
                SELECT character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'tariff_logic'
                  AND column_name = 'sc_code'
            """)).fetchone()
            if res and res[0] is not None and res[0] < 100:
                print("🔧 Migrating column tariff_logic.sc_code to VARCHAR(100)...")
                conn.execute(text("ALTER TABLE tariff_logic ALTER COLUMN sc_code TYPE VARCHAR(100)"))
                print("✅ Migration complete: sc_code widened to 100 chars")
        except Exception as e:
            print(f"⚠️ Skipped sc_code migration: {e}")

if __name__ == "__main__":
    init_db()


#python -m src.database.init_db