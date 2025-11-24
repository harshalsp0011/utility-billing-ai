import json
import logging
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuditEngine:
    def __init__(self, tariff_definitions_path: str):
        self.tariff_map = self._load_logic(tariff_definitions_path)

    def _load_logic(self, path: str):
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            # Support both list format and {"tariffs": [...]} format
            if isinstance(data, dict) and 'tariffs' in data:
                data = data['tariffs']
            
            mapping = {item['sc_code']: item for item in data}
            logger.info(f"✅ Engine loaded logic for: {list(mapping.keys())}")
            return mapping
        except FileNotFoundError:
            logger.error(f"❌ Logic file {path} not found.")
            return {}

    def calculate_expected_bill(self, row: pd.Series):
        # Default to SC1 if missing, but Runner will usually override this
        sc_code = row.get('service_class', 'SC1') 
        
        logic = self.tariff_map.get(sc_code)
        if not logic:
            return {
                "status": "SKIPPED", 
                "reason": f"No logic for {sc_code}",
                "expected_amount": 0.0,
                "variance": 0.0,
                "trace": []
            }

        total_expected = 0.0
        trace_log = []

        # Prepare context specifically for your UserBills schema
        bill_date = row.get('bill_date')
        if isinstance(bill_date, str):
            try:
                bill_date = pd.to_datetime(bill_date)
            except:
                pass
        
        user_context = pd.Series({
            'billed_kwh': float(row.get('billed_kwh', 0) or 0),
            'billed_demand': float(row.get('billed_demand', 0) or 0),
            'billed_rkva': float(row.get('billed_rkva', 0) or 0),
            'days_used': int(row.get('days_used', 30)),
            'bill_date': bill_date
        })

        eval_context = {"user": user_context, "pd": pd}

        for step in logic.get('logic_steps', []):
            step_name = step.get('step_name', 'Unknown')
            charge_type = step.get('charge_type')
            condition = step.get('condition', "Always")
            
            if condition != "Always":
                try:
                    if not eval(condition, {"__builtins__": None}, eval_context):
                        continue
                except Exception as e:
                    logger.warning(f"⚠️ Condition Error in {step_name}: {e}")
                    continue

            cost = 0.0
            try:
                if charge_type == 'fixed_fee':
                    cost = float(step.get('value', 0))
                elif charge_type == 'formula':
                    formula = step.get('python_formula', "0")
                    cost = float(eval(formula, {"__builtins__": None}, eval_context))

                total_expected += cost
                trace_log.append(f"{step_name}: ${cost:.2f}")

            except Exception as e:
                logger.error(f"❌ Math Error in {step_name}: {e}")

        actual_bill = float(row.get('bill_amount', 0.0) or 0.0)
        variance = total_expected - actual_bill

        return {
            "status": "SUCCESS",
            "sc_code": sc_code,
            "actual_bill": round(actual_bill, 2),
            "expected_bill": round(total_expected, 2),
            "variance": round(variance, 2),
            "trace": trace_log
        }