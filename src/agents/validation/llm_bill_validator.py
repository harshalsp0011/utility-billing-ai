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

                # We only know read_date reliably; treat as period_end
                "period_start": None,
                "period_end": row.get("read_date"),

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
    logger.info("Converted %d DataFrame rows to bill dicts", len(records))


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
