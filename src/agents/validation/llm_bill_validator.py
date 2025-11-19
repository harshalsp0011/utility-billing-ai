import os 
import json
from datetime import datetime
from pathlib import Path
import sys

# When running this module directly (python src/agents/validation/llm_bill_validator.py)
# ensure the repository root is on sys.path so `from src...` imports resolve.
# The file is at <repo>/src/agents/validation/llm_bill_validator.py, so parents[3]
# points to the repository root.
repo_root = str(Path(__file__).resolve().parents[3])
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

import pandas as pd
from sqlalchemy import text
from src.utils.llm_client import LLMClient
from src.utils.config import OPENAI_API_KEY, OPENAI_MODEL
from src.database.db_utils import get_engine, insert_validation_result, fetch_user_bills,insert_bill_validation_result
from src.utils.logger import get_logger


logger = get_logger(__name__)

# ============= CONFIG =============


try:
    client = LLMClient(api_key=OPENAI_API_KEY, model=OPENAI_MODEL)
    logger.info("LLM client initialized with model=%s", OPENAI_MODEL)
except Exception as e:
    client = None
    logger.error("Failed to initialize LLM client: %s", e)

# Example: direct insert helpers you can call from scripts or REPL
# Use these helpers to write single records without manual SQL.
#
# from src.database.db_utils import insert_user_bill, insert_validation_result
#
# Example: insert a validation result directly:
# insert_validation_result({
#     "account_id": "ACCT-9981",
#     "user_bill_id": 4,
#     "issue_type": "Usage Spike",
#     "description": "KWh increased by 62% vs last month — anomaly detected.",
#     "status": "open",
# })

# Example: insert a single user bill record:
# insert_user_bill({
#     "bill_account": "ACCT-9981",
#     "customer": "Example Customer",
#     "read_date": "2025-09-01",
#     "billed_kwh": 123.4,
#     "bill_amount": 45.67,
# })

# ============= LLM SYSTEM PROMPT =============

SYSTEM_PROMPT = """
You are an expert utility-billing quality-control analyst.
Your job is to do a FIRST-PASS anomaly review on electricity bills.

You are given a list of bills for a single account, as structured JSON.
Fields you may see:
- bill_id: integer (internal bill identifier from user_bills.id)
- period_start: ISO date string or null
- period_end: ISO date string or null
- bill_days: integer
- kwh_usage: float
- kw_demand: float
- total_amount: float
- sales_tax_amount: float or null
- is_holiday_month: boolean
- is_municipality: boolean
- load_factor: float (0–1)
- notes: string (optional context)

------------------------------------------------------------
IMPORTANT CLARIFICATIONS FOR SMALL LOADS / MUNICIPAL ACCOUNTS
------------------------------------------------------------

• Many small-load accounts (SC-1, SC-1C, municipal buildings, parks, traffic signals)
  have **no demand meter**, so:
    → kw_demand = 0 is NORMAL and MUST NOT be flagged.

• Municipalities frequently have **NO SALES TAX**.
  If is_municipality = true:
    → Do NOT flag R4 (sales tax always zero).

• For small accounts with no demand meter:
    → Load factor may be 0 or undefined; do NOT flag R5 unless
      the account has meaningful >0 demand values in history.

------------------------------------------------------------
RULES TO APPLY (FIRST-LOOK QC)
------------------------------------------------------------

R1) Unusual spikes or drops in usage or charges
    - Compare kwh_usage and total_amount against typical history
      (median or middle 50% of values).
    - Flag only if the change is ≥ +50% or ≤ -50%.
    - Allowed rule_ids:
        "R1_USAGE_SPIKE", "R1_USAGE_DROP",
        "R1_CHARGE_SPIKE", "R1_CHARGE_DROP".

R2) Bill period (bill_days) out of normal range
    - Normal range: 25–35 days inclusive.
    - Flag bill_days < 25 or > 35.
    - rule_id: "R2_BILL_DAYS_OUT_OF_RANGE".

R3) Zero, missing, or negative values
    - Flag only if:
        * kwh_usage <= 0 (non-holiday months), OR
        * kw_demand < 0  (NOTE: kw_demand == 0 is NORMAL), OR
        * total_amount < 0 (credit bill)
    - rule_id: "R3_ZERO_OR_NEGATIVE_USAGE_OR_CHARGE".

R4) Sales tax always zero or missing
    - Apply ONLY IF is_municipality == false.
    - If the account normally has tax and most bills show zero or null:
        → flag as "R4_SALES_TAX_SUSPECT".
    - This is NOT an overcharge by itself; mark is_overcharge_risk = false.

R5) Big swings in load factor or demand (ONLY if the account has non-zero demand)
    - If kw_demand > 0 at any point in history:
         → compare load_factor or kw_demand swings ≥ 50%.
    - If kw_demand is ALWAYS zero:
         → skip R5 completely.
    - rule_id: "R5_LOAD_FACTOR_SWING".

R6) Repeated billing periods or duplicated charges
    - If two bills share identical period_start & period_end OR clearly duplicate:
         → rule_id "R6_DUPLICATE_PERIOD", is_overcharge_risk = true.

------------------------------------------------------------
OUTPUT FORMAT (STRICT JSON)
------------------------------------------------------------

Return ONE JSON object:

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
          "rule_id": "...",
          "severity": "low" | "medium" | "high",
          "is_overcharge_risk": true | false,
          "field_names": ["kwh_usage", "total_amount", ...],
          "message": "Short explanation"
        }
      ]
    }
  ]
}

If a bill has no anomalies, omit it from bill_anomalies.
Output ONLY the JSON. No extra text.
"""


# ============= DB HELPERS =============

def load_user_bills_from_db(bill_account: str, limit_rows: int | None = None) -> pd.DataFrame:
    """Load billing history for a single bill_account by reusing db_utils.fetch_user_bills.

    fetch_user_bills returns a limited set of recent UserBills rows; we filter the
    returned DataFrame for the requested `bill_account`. If `limit_rows` is not
    provided, a sensible large default is used to increase the chance the account
    appears in the result set.
    """
    # Use a reasonable default ceiling so fetch_user_bills returns enough history
    #default_limit = 1000
    #limit = limit_rows if limit_rows is not None else default_limit

    logger.info("Fetching user bills (DB filter) for account=%s", bill_account)
    df = fetch_user_bills(bill_account)
    if df.empty:
        logger.warning("fetch_user_bills returned empty DataFrame")
        return df

    # Sort by read_date to preserve ordering if present
    if "read_date" in df.columns:
        df = df.sort_values("read_date")
    logger.info("Found %d rows for account %s", len(df), bill_account)

    return df


def is_municipality_customer(customer_name: str | None) -> bool:
    """
    Basic heuristic to detect municipality / govt accounts.
    Expand as needed.
    """
    if not customer_name:
        return False

    tokens = customer_name.upper()
    municipal_keywords = [
        "CITY", "TOWN", "VILLAGE", "COUNTY",
        "CITY OF", "TOWN OF", "VILLAGE OF",
        "SCHOOL", "UNIVERSITY", "STATE OF",
        "DEPT", "DEPARTMENT", "MUNICIPAL"
    ]

    return any(key in tokens for key in municipal_keywords)



def dataframe_to_bill_dicts(df: pd.DataFrame) -> list[dict]:
    """
    Convert user_bills rows to LLM bill objects.
    Auto-detect municipality from customer name.
    Skip R5 rules automatically if demand is always zero.
    """
    records = []

    # Detect if the entire account ever has demand > 0
    any_demand = (df["billed_demand"].fillna(0) > 0).any()

    for _, row in df.iterrows():

        customer_name = row.get("customer", "")
        municipal_flag = is_municipality_customer(customer_name)

        records.append(
            {
                "bill_id": int(row["id"]),
                "period_start": None,
                "period_end": row.get("bill_date"),
                "bill_days": row.get("days_used"),
                "kwh_usage": row.get("billed_kwh"),
                "kw_demand": row.get("billed_demand"),

                "total_amount": row.get("bill_amount"),
                "sales_tax_amount": row.get("sales_tax_amt"),
                "load_factor": row.get("load_factor"),

                # FOUND AUTOMATICALLY:
                "is_municipality": municipal_flag,

                # You can add holiday logic later if needed:
                "is_holiday_month": False,

                # This lets the LLM skip R5 automatically
                "account_has_real_demand": any_demand,

                "notes": (
                    f"account={row.get('bill_account')}, "
                    f"customer={customer_name}, "
                    f"read_date={row.get('read_date')}"
                ),
            }
        )
    logger.info("Converted %d DataFrame rows to bill dicts", len(records))
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
    logger.debug("Built user prompt for %d bills (len=%d)", len(bills), len(bills_json))
    return prompt


def call_llm_for_validation(bills: list[dict]) -> dict:
    """
    Call the OpenAI chat model and parse the JSON response.
    """
    # Combine system prompt and user prompt into a single prompt string
    full_prompt = SYSTEM_PROMPT.strip() + "\n\n" + build_user_prompt(bills)

    if client is None:
        logger.error("LLM client is not initialized; cannot call LLM")
        raise RuntimeError("LLM client not available")

    logger.info("Calling LLM for validation with %d bills", len(bills))
    # Use the project's LLM client which returns the raw string content
    resp_text = client.ask(full_prompt, temperature=0.0)
    logger.debug("LLM raw response length: %d", len(str(resp_text)))

    # Try to parse JSON directly; if the model returns extra text, attempt
    # to extract the first JSON object substring.
    try:
        parsed = json.loads(resp_text)
        logger.info("Parsed JSON from LLM response successfully")
        return parsed
    except Exception as e:
        logger.warning("Direct JSON parse failed: %s", e)
        # Attempt to find first '{' and last '}' and parse that slice
        try:
            start = resp_text.find('{')
            end = resp_text.rfind('}')
            if start != -1 and end != -1 and end > start:
                candidate = resp_text[start:end+1]
                parsed = json.loads(candidate)
                logger.info("Parsed JSON after extracting substring")
                return parsed
            else:
                logger.error("Could not locate JSON object in LLM output. Raw output:\n%s", resp_text)
        except Exception as e2:
            logger.error("Failed to parse JSON from LLM response: %s", e2)
        raise RuntimeError("Failed to parse JSON from LLM response")


# ============= SAVE TO validation_results =============

def save_llm_anomalies_to_validation_results(anomalies: dict, account_id: str):
    """Save LLM anomalies into the `validation_results` table using `insert_validation_result`.

    Delegate insertion to `src.database.db_utils.insert_validation_result` so
    session handling and logging remain centralized.
    """
    total_saved = 0
    for bill_entry in anomalies.get("bill_anomalies", []):
        user_bill_id = bill_entry.get("bill_id")
        for a in bill_entry.get("anomalies", []):
            record = {
                "account_id": account_id,
                "user_bill_id": user_bill_id,
                "issue_type": a.get("rule_id"),
                "description": a.get("message"),
                "detected_on": datetime.utcnow(),
                "status": "new",
            }
            try:
                insert_bill_validation_result(record)
                logger.info("inserted anomaly for bill_id=%s, rule_id=%s", user_bill_id, a.get("rule_id"))
                total_saved += 1
            except Exception as e:
                logger.error("Failed to insert validation result for bill_id=%s: %s", user_bill_id, e)

    logger.info("Saved %d LLM anomalies to validation_results for account=%s", total_saved, account_id)


# ============= HIGH-LEVEL PIPELINE =============

def validate_account_with_llm(
    bill_account: str,
) -> dict:
    """
    Full pipeline for a single bill_account:
      1. Load bills from user_bills
      2. Prepare LLM input
      3. Call LLM
      4. Save anomalies to validation_results
      5. Return anomalies dict
    """
    logger.info("Starting LLM validation pipeline for account=%s", bill_account)
    # Use the project's db_utils engine to load bills for the account
    df = load_user_bills_from_db(bill_account)

    if df.empty:
        logger.warning("No bills found in user_bills for account_id=%s", bill_account)
        raise ValueError(f"No bills found in user_bills for account_id={bill_account}")

    bills = dataframe_to_bill_dicts(df)

    # Limit number of bills per request to keep prompt small
    #if len(bills) > MAX_BILLS_PER_REQUEST:
     #   bills = bills[-MAX_BILLS_PER_REQUEST:]

    logger.info("Calling LLM for %d bills", len(bills))
    anomalies = call_llm_for_validation(bills)

    logger.info("Saving anomalies to validation_results for account=%s", bill_account)
    save_llm_anomalies_to_validation_results(anomalies, account_id=bill_account)

    logger.info("Completed LLM validation for account=%s; anomalies summary: %s", bill_account, json.dumps(anomalies.get("summary", {})))
    return anomalies


# ============= MAIN =============

if __name__ == "__main__":
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set in environment.")

    # Change this to whichever account you want to validate
    BILL_ACCOUNT = "1031293107"

    anomalies = validate_account_with_llm(BILL_ACCOUNT)

    # Optional: print output JSON nicely
    print(json.dumps(anomalies, indent=2))
