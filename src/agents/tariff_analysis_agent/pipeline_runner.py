from pathlib import Path
import subprocess
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.aws_app import file_exists_in_s3, get_s3_key

def run_tariff_pipeline(pdf_path: Path):

    pdf_path = Path(pdf_path)
    print("PATH:", pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    # ======================================================
    # 1) pagewise_text_extractor.py
    # ======================================================
    print("\n🔄 Step 1/3: Extracting text from PDF pages...")
    step1 = PROJECT_ROOT / "src" / "agents" / "tariff_analysis_agent" / "pagewise_text_extractor.py"
    if not step1.exists():
        raise FileNotFoundError(f"Missing: {step1}")

    subprocess.run([sys.executable, str(step1), str(pdf_path)], check=True)

    # Check if output exists in S3 only
    s3_key_raw = get_s3_key("processed", "raw_extracted_tarif.json")
    if not file_exists_in_s3(s3_key_raw):
        raise RuntimeError(f"raw_extracted_tarif.json was not created in S3: {s3_key_raw}")
    print("✅ Step 1/3: Text extraction completed!")

    # ======================================================
    # 2) group_extracted_raw_text.py
    # ======================================================
    print("\n🔄 Step 2/3: Grouping tariffs by service class...")
    step2 = PROJECT_ROOT / "src" / "agents" / "tariff_analysis_agent" / "group_extracted_raw_text.py"
    if not step2.exists():
        raise FileNotFoundError(f"Missing: {step2}")

    subprocess.run([sys.executable, str(step2)], check=True)

    # Note: step2 creates grouped_tariffs.json (already validated above)
    print("✅ Step 2/3: Tariff grouping completed!")

    # ======================================================
    # 3) extract_logic_llm_call.py (FINAL LLM OUTPUT)
    # ======================================================
    print("\n🔄 Step 3/3: Extracting tariff logic using LLM...")
    step3 = PROJECT_ROOT / "src" / "agents" / "tariff_analysis_agent" / "extract_logic_llm_call.py"
    if not step3.exists():
        raise FileNotFoundError(f"Missing: {step3}")

    subprocess.run([sys.executable, str(step3), str(pdf_path)], check=True)

    # Check if output exists in S3 only
    s3_key_logic = get_s3_key("processed", "final_logic_output.json")
    if not file_exists_in_s3(s3_key_logic):
        raise RuntimeError(f"final_logic_output.json was not created in S3: {s3_key_logic}")
    print("✅ Step 3/3: Logic extraction completed!")

    print("\n" + "="*60)
    print("✅ TARIFF PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*60)
    print(f"📄 Grouped Tariffs: s3://{get_s3_key('processed', 'grouped_tariffs.json')}")
    print(f"🎯 Final Logic Output: s3://{get_s3_key('processed', 'final_logic_output.json')}")
    print("="*60 + "\n")

    return {
        "grouped_tariffs": get_s3_key('processed', 'grouped_tariffs.json'),
        "final_logic": get_s3_key('processed', 'final_logic_output.json')
    }
