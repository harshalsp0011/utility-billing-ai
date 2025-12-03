from pathlib import Path
import subprocess
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[3]

def run_tariff_pipeline(pdf_path: Path):

    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    # ======================================================
    # 1) pagewise_text_extractor.py
    # ======================================================
    step1 = PROJECT_ROOT / "src" / "agents" / "tariff_analysis_agent" / "pagewise_text_extractor.py"
    if not step1.exists():
        raise FileNotFoundError(f"Missing: {step1}")

    subprocess.run([sys.executable, str(step1)], check=True)

    grouped_json = PROJECT_ROOT / "data" / "processed" / "grouped_tariffs.json"
    if not grouped_json.exists():
        raise RuntimeError("grouped_tariffs.json was not created.")

    # ======================================================
    # 2) group_extracted_raw_text.py
    # ======================================================
    step2 = PROJECT_ROOT / "src" / "agents" / "tariff_analysis_agent" / "group_extracted_raw_text.py"
    if not step2.exists():
        raise FileNotFoundError(f"Missing: {step2}")

    subprocess.run([sys.executable, str(step2)], check=True)

    extracted_json = PROJECT_ROOT / "data" / "processed" / "tariff_definitions.json"
    if not extracted_json.exists():
        raise RuntimeError("tariff_definitions.json was not created.")

    # ======================================================
    # 3) extract_logic_llm_call.py (FINAL LLM OUTPUT)
    # ======================================================
    step3 = PROJECT_ROOT / "src" / "agents" / "tariff_analysis_agent" / "extract_logic_llm_call.py"
    if not step3.exists():
        raise FileNotFoundError(f"Missing: {step3}")

    subprocess.run([sys.executable, str(step3)], check=True)

    logic_json = PROJECT_ROOT / "data" / "processed" / "final_logic_output.json"
    if not logic_json.exists():
        raise RuntimeError("final_logic_output.json was not created.")

    return {
        "grouped_tariffs": grouped_json,
        "tariff_definitions": extracted_json,
        "final_logic": logic_json
    }
