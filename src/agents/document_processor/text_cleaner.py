"""
text_cleaner.py
----------------
🧹 Cleans extracted text/tables from PDF and Excel data.

Purpose:
--------
Standardizes data before passing it to Tariff Analysis.
Cleans up anomalies such as:
    - Extra whitespace
    - Currency symbols ($)
    - Commas in numbers
    - Non-numeric characters in numeric fields

Workflow:
---------
1️⃣ Accepts raw DataFrame (from PDF or Excel).
2️⃣ Applies cleaning transformations.
3️⃣ Returns standardized DataFrame.

Inputs:
-------
- Raw pandas DataFrame

Outputs:
--------
- Clean DataFrame (ready for Tariff Analysis)

Depends On:
-----------
- pandas
- src.utils.logger
"""

import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

def clean_text_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans a DataFrame by removing formatting artifacts (currency, commas, etc.).
    """
    try:
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].replace(r"[\$,]", "", regex=True)
                df[col] = df[col].replace(r"\s+", " ", regex=True)
        logger.info(f"🧽 Cleaned text fields in DataFrame (Cols: {len(df.columns)})")
        return df
    except Exception as e:
        logger.error(f"❌ Error cleaning text data: {e}")
        return df
