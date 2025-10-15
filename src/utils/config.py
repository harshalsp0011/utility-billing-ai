# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# -------- Paths --------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# -------- Local Database --------
DB_URL = os.getenv("DB_URL", "sqlite:///local.db")

# -------- General Settings --------
DEBUG = os.getenv("DEBUG", "True").lower() == "true"

if DEBUG:
    print("✅ Local configuration loaded successfully!")
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"OUTPUT_DIR: {OUTPUT_DIR}")
