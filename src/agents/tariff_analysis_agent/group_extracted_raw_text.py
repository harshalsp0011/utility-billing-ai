import json
import re
import os
from pathlib import Path
from datetime import datetime

def parse_effective_date(text_block):
    """
    Scans a text block for the 'Effective Date'.
    Patterns to look for:
    1. "Effective Date: MM/DD/YYYY"
    2. "INITIAL EFFECTIVE DATE: MONTH DD, YYYY"
    """
    # Pattern 1: MM/DD/YYYY (e.g., 09/01/2025)
    match_short = re.search(r"Effective Date:\s*(\d{2}/\d{2}/\d{4})", text_block, re.IGNORECASE)
    if match_short:
        return match_short.group(1)

    # Pattern 2: Long Format (e.g., SEPTEMBER 1, 2025)
    match_long = re.search(r"EFFECTIVE DATE:\s*([A-Z]+\s+\d{1,2},\s+\d{4})", text_block, re.IGNORECASE)
    if match_long:
        try:
            date_str = match_long.group(1)
            # Convert "SEPTEMBER 1, 2025" -> "2025-09-01"
            # Removing extra spaces if any
            date_str = re.sub(r'\s+', ' ', date_str)
            dt = datetime.strptime(date_str, "%B %d, %Y")
            return dt.strftime("%Y-%m-%d")
        except Exception as e:
            print(f"Date parse error: {e}")
            pass
            
    # Default fallback if not found (should be rare if PDF is standard)
    return None

def group_tariffs_v3(input_file, output_file):
    print(f"Loading {input_file}...")
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.")
        return

    with open(input_file, 'r') as f:
        data = json.load(f)

    # Header Regex
    header_pattern = re.compile(
        r"^\s*(?:SERVICE CLASSIFICATION|S\.C\.)\s*(?:NO\.|NUMBER)\s*([0-9]+(?:-[A-Z]|[A-Z])?)", 
        re.IGNORECASE | re.MULTILINE
    )

    grouped_data = {}
    current_sc_id = None
    current_text_buffer = []
    start_page = 0
    
    pages = data.get('pages', []) if 'pages' in data else []
    if not pages and isinstance(data, dict):
         for k, v in data.items():
             if isinstance(v, dict) and 'text' in v:
                 pages.append({'page_number': int(k) if k.isdigit() else 0, 'text': v['text']})
         pages.sort(key=lambda x: x['page_number'])

    print(f"Scanning {len(pages)} pages for headers...")

    for page in pages:
        text = page.get('text', "")
        page_num = page.get('page_number', 0)
        
        header_sample = text[:1000] 
        match = header_pattern.search(header_sample)
        
        is_new_section = False
        found_id = None

        if match:
            raw_id = match.group(1).upper().replace("-", "").replace(" ", "")
            found_id = f"SC{raw_id}"
            
            if found_id != current_sc_id:
                is_new_section = True

        if is_new_section:
            # A. Save the Previous Section
            if current_sc_id:
                full_text = "\n".join(current_text_buffer)
                # Extract date from the first 2000 chars of the block
                eff_date = parse_effective_date(full_text[:2000]) or "2025-09-01"
                
                grouped_data[current_sc_id] = {
                    "sc_code": current_sc_id,
                    "start_page": start_page,
                    "end_page": page_num - 1,
                    "effective_date": eff_date,  # <--- NEW FIELD
                    "full_text": full_text
                }
                print(f"Captured {current_sc_id} (Pages {start_page}-{page_num - 1}, Date: {eff_date})")

            # B. Start New Section
            current_sc_id = found_id
            current_text_buffer = [text]
            start_page = page_num
        
        else:
            if current_sc_id:
                current_text_buffer.append(text)

    # Save the final section
    if current_sc_id and current_text_buffer:
        full_text = "\n".join(current_text_buffer)
        eff_date = parse_effective_date(full_text[:2000]) or "2025-09-01"
        
        grouped_data[current_sc_id] = {
            "sc_code": current_sc_id,
            "start_page": start_page,
            "end_page": pages[-1]['page_number'],
            "effective_date": eff_date,
            "full_text": full_text
        }
        print(f"Captured {current_sc_id} (Pages {start_page}-{pages[-1]['page_number']}, Date: {eff_date})")

    # Write Output
    with open(output_file, 'w') as f:
        json.dump(grouped_data, f, indent=2)
    
    print(f"\nSuccess! Grouped data saved to {output_file}")


def _get_default_paths():
    """Resolve default input/output paths relative to repo root."""
    # group_tariffs.py is in: <root>/src/agents/tariff_analysis/
    # root is 3 levels up
    root = Path(__file__).resolve().parents[3]
    input_path = root / "data" / "processed" / "raw_extracted_tarif.json"
    output_path = root / "data" / "processed" / "grouped_tariffs.json"
    return input_path, output_path


if __name__ == "__main__":
    # Check relative path first (for direct execution)
    if Path('raw_extracted_tarif.json').exists():
        group_tariffs_v3('raw_extracted_tarif.json', 'grouped_tariffs.json')
    else:
        # Use project structure paths
        input_file, output_file = _get_default_paths()
        
        if not input_file.exists():
            # Just a fallback message, raising error might be too harsh if running standalone for test
            print(f"Input file not found at default path: {input_file}")
        else:
            group_tariffs_v3(str(input_file), str(output_file))