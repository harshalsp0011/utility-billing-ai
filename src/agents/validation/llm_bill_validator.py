import os
import json
from datetime import datetime

import pandas as pd
import psycopg2
from openai import OpenAI

# ============= CONFIG =============

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL_NAME = "gpt-4.1-mini"          # change if you like
MAX_BILLS_PER_REQUEST = 24           # keep prompts manageable

client = OpenAI(api_key=OPENAI_API_KEY)

# Postgres connection defaults – override in __main__ if needed
PG_DEFAULTS = dict(
    dbname="utility_billing",
    user="postgres",
    password="your_password",        # <-- change this
    host="localhost",
    port=5432,
)

# ============= LLM SYSTEM PROMPT =============

SYSTEM_PROMPT = """
You are an expert utility-billing quality-control analyst.
Your job is to do a FIRST-PASS anomaly review on electricity bills.

You are given a list of bills for a single account, as structured JSON.
Fields you may see (some may be null/missing):
- bill_id: integer (internal bill identifier, from user_bills.id)
- period_start: ISO date string or null
- period_end: ISO date string or null
- bill_days: integer
- kwh_usage: float
- kw_demand: float
- total_amount: float
- sales_tax_amount: float or null
- is_holiday_month: boolean
- is_municipality: boolean
- load_factor: float (0–1 or similar)
- notes: string (optional context)

You must only apply the FIRST-LOOK rules below, and output anomalies in strict JSON.

---------------- RULES TO APPLY ----------------

R1) Unusual spikes or drops in usage, demand, or charges
    - Compare kwh_usage, kw_demand, and total_amount to a typical pattern
      based on the rest of the history (e.g., median or recent average).
    - If a value changes by +50% or more OR –50% or more vs typical,
      flag an anomaly.
    - Use these rule_ids when appropriate:
        * "R1_USAGE_SPIKE" or "R1_USAGE_DROP"
        * "R1_DEMAND_SPIKE" or "R1_DEMAND_DROP"
        * "R1_CHARGE_SPIKE" or "R1_CHARGE_DROP"

R2) Bill period (bill_days) out of range
    - bill_days should be between 25 and 35 days inclusive.
    - Flag anything < 25 or > 35 as "R2_BILL_DAYS_OUT_OF_RANGE".
    - This is a review item, not automatically an overcharge.

R3) Zero, missing, or negative values
    - For non-holiday months (is_holiday_month == false or missing), flag:
        * kwh_usage <= 0
        * kw_demand < 0
        * total_amount < 0
    - Zero usage will never be an overcharge but may be suspicious.
    - Negative total_amount often means a true-up or credit.
    - Use rule_id "R3_ZERO_OR_NEGATIVE_USAGE_OR_CHARGE".

R4) Sales tax always zero or missing
    - If is_municipality == true, zero or missing tax is usually fine.
    - If is_municipality == false and sales_tax_amount is zero or null
      for most bills, flag a condition "R4_SALES_TAX_SUSPECT".
    - This does NOT imply overcharge; is_overcharge_risk should be false.

R5) Big swings in load factor or demand
    - If load_factor is provided, compare to a typical pattern.
    - If it changes by +/- 50% or more, flag as "R5_LOAD_FACTOR_SWING".
    - Large swings in kw_demand are already covered by R1 demand rules.

R6) Repeated billing periods or duplicated charges
    - If two or more bills have identical period_start and period_end
      dates, or clearly duplicated/overlapping date ranges that look like
      the same service period billed twice, flag as "R6_DUPLICATE_PERIOD".
    - This is a serious overcharge risk; is_overcharge_risk should be true.

GENERAL:
- You are doing TRIAGE, not full tariff validation.
- Be conservative: when unsure, you may flag "needs review" anomalies
  but mark is_overcharge_risk = false.
- Many anomalies are not overcharges; they just need follow-up.

---------------- OUTPUT FORMAT ----------------

Return ONE JSON object with this structure:

{
  "summary": {
    "total_bills": int,
    "bills_with_anomalies": int
  },
  "bill_anomalies": [
    {
      "bill_id": int,
      "anomalies": [
        {
          "rule_id": "R1_USAGE_SPIKE" | "R1_USAGE_DROP" | "R1_DEMAND_SPIKE" |
                     "R1_DEMAND_DROP" | "R1_CHARGE_SPIKE" | "R1_CHARGE_DROP" |
                     "R2_BILL_DAYS_OUT_OF_RANGE" |
                     "R3_ZERO_OR_NEGATIVE_USAGE_OR_CHARGE" |
                     "R4_SALES_TAX_SUSPECT" |
                     "R5_LOAD_FACTOR_SWING" |
                     "R6_DUPLICATE_PERIOD" |
                     "OTHER",
          "severity": "low" | "medium" | "high",
          "is_overcharge_risk": true | false,
          "field_names": ["kwh_usage", "kw_demand", "total_amount", ...],
          "message": "Short human-readable explanation"
        }
      ]
    }
  ]
}

If a bill has no anomalies, you may omit it from bill_anomalies.
Answer with ONLY this JSON object and no extra commentary.
"""


# ============= DB HELPERS =============

def load_user_bills_from_postgres(
    bill_account: str,
    dbname: str,
    user: str,
    password: str,
    host: str = "localhost",
    port: int = 5432,
    limit_rows: int | None = None,
) -> pd.DataFrame:
    """
    Load billing history for a single bill_account from user_bills.
    """
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )

    query = """
        SELECT *
        FROM user_bills
        WHERE bill_account = %s
        ORDER BY bill_date
    """
    params = (bill_account,)

    if limit_rows is not None:
        query += " LIMIT %s"
        params = (bill_account, limit_rows)

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


def dataframe_to_bill_dicts(df: pd.DataFrame) -> list[dict]:
    """
    Convert user_bills DataFrame to list[dict] for the LLM.
    bill_id is user_bills.id so we can link anomalies back to the DB.
    """
    records: list[dict] = []

    for _, row in df.iterrows():
        records.append(
            {
                # direct link back to user_bills.id
                "bill_id": int(row["id"]),

                # We only know bill_date reliably; treat as period_end
                "period_start": None,
                "period_end": row.get("bill_date"),

                "bill_days": row.get("days_used"),
                "kwh_usage": row.get("billed_kwh"),
                "kw_demand": row.get("billed_demand"),
                "total_amount": row.get("bill_amount"),
                "sales_tax_amount": row.get("sales_tax_amt"),
                "load_factor": row.get("load_factor"),

                # For now, assume non-holiday, non-municipality
                "is_holiday_month": False,
                "is_municipality": False,

                "notes": f"account={row.get('bill_account')}, "
                         f"customer={row.get('customer')}, "
                         f"read_date={row.get('read_date')}",
            }
        )

    return records


# ============= LLM CALL HELPERS =============

def build_user_prompt(bills: list[dict]) -> str:
    """
    Build the user message for the LLM using the bills JSON.
    """
    bills_json = json.dumps(bills, indent=2, default=str)
    prompt = (
        "Below is the billing history for ONE electricity account as JSON.\n"
        "Apply the rules from the system message and return anomalies in the required JSON format.\n\n"
        "BILLS_JSON:\n```json\n"
        f"{bills_json}\n"
        "```\n"
    )
    return prompt


def call_llm_for_validation(bills: list[dict]) -> dict:
    """
    Call the OpenAI chat model and parse the JSON response.
    """
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_user_prompt(bills)},
    ]

    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        response_format={"type": "json_object"},
        temperature=0.0,
    )

    content = resp.choices[0].message.content
    return json.loads(content)


# ============= SAVE TO validation_results =============

def save_llm_anomalies_to_validation_results(
    anomalies: dict,
    account_id: str,
    dbname: str,
    user: str,
    password: str,
    host: str = "localhost",
    port: int = 5432,
):
    """
    Save LLM anomalies into validation_results table.
    Expects validation_results to have columns:
      id (serial), account_id, user_bill_id, issue_type, description, detected_on, status
    """
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cur = conn.cursor()

    for bill_entry in anomalies.get("bill_anomalies", []):
        user_bill_id = bill_entry.get("bill_id")  # equals user_bills.id

        for a in bill_entry.get("anomalies", []):
            issue_type = a.get("rule_id")
            description = a.get("message")
            detected_on = datetime.utcnow()

            cur.execute(
                """
                INSERT INTO validation_results
                  (account_id, user_bill_id, issue_type, description, detected_on, status)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (
                    account_id,
                    user_bill_id,
                    issue_type,
                    description,
                    detected_on,
                    "new",
                ),
            )

    conn.commit()
    cur.close()
    conn.close()

    print("LLM anomalies saved to validation_results with user_bill_id.")


# ============= HIGH-LEVEL PIPELINE =============

def validate_account_with_llm(
    bill_account: str,
    db_kwargs: dict,
) -> dict:
    """
    Full pipeline for a single bill_account:
      1. Load bills from user_bills
      2. Prepare LLM input
      3. Call LLM
      4. Save anomalies to validation_results
      5. Return anomalies dict
    """
    print(f"Loading bills for account {bill_account} ...")
    df = load_user_bills_from_postgres(bill_account, **db_kwargs)

    if df.empty:
        raise ValueError(f"No bills found in user_bills for account_id={bill_account}")

    bills = dataframe_to_bill_dicts(df)

    # Limit number of bills per request to keep prompt small
    if len(bills) > MAX_BILLS_PER_REQUEST:
        bills = bills[-MAX_BILLS_PER_REQUEST:]

    print(f"Calling LLM for {len(bills)} bills ...")
    anomalies = call_llm_for_validation(bills)

    print("Saving anomalies to validation_results ...")
    save_llm_anomalies_to_validation_results(
        anomalies,
        account_id=bill_account,
        **db_kwargs,
    )

    return anomalies


# ============= MAIN =============

if __name__ == "__main__":
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set in environment.")

    # Change this to whichever account you want to validate
    BILL_ACCOUNT = "YOUR_ACCOUNT_ID_HERE"

    anomalies = validate_account_with_llm(
        BILL_ACCOUNT,
        db_kwargs=PG_DEFAULTS,
    )

    # Optional: print output JSON nicely
    print(json.dumps(anomalies, indent=2))
