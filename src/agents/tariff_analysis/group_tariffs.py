import json
import re
from pathlib import Path

def group_tariffs_v3(input_file, output_file):
    print(f"Loading {input_file}...")
    with open(input_file, 'r') as f:
        data = json.load(f)

    # --- THE FIX ---
    # 1. STRICTER REGEX: Only matches "SERVICE CLASSIFICATION NO." or "S.C. NO."
    #    Removed "SC" to avoid false positives like "SC2D" in lists.
    # 2. CAPTURE GROUPS: Handles "1", "1-C", "3A", "1H"
    header_pattern = re.compile(
        r"^\s*(?:SERVICE CLASSIFICATION|S\.C\.)\s*(?:NO\.|NUMBER)\s*([0-9]+(?:-[A-Z]|[A-Z])?)", 
        re.IGNORECASE | re.MULTILINE
    )

    grouped_data = {}
    current_sc_id = None
    current_text_buffer = []
    start_page = 0
    
    # Handle list vs dict structure
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
        
        # --- THE FIX ---
        # Scan first 1000 chars to catch headers pushed down by metadata
        header_sample = text[:1000] 
        match = header_pattern.search(header_sample)
        
        is_new_section = False
        found_id = None

        if match:
            # Normalize ID: "1-C" -> "SC1C", "3A" -> "SC3A"
            raw_id = match.group(1).upper().replace("-", "").replace(" ", "")
            found_id = f"SC{raw_id}"
            
            # Logic: Only switch if the ID actually changes
            if found_id != current_sc_id:
                is_new_section = True

        if is_new_section:
            # A. Save the Previous Section
            if current_sc_id:
                grouped_data[current_sc_id] = {
                    "sc_code": current_sc_id,
                    "start_page": start_page,
                    "end_page": page_num - 1,
                    "full_text": "\n".join(current_text_buffer)
                }
                print(f"Captured {current_sc_id}: Pages {start_page} to {page_num - 1}")

            # B. Start New Section
            current_sc_id = found_id
            current_text_buffer = [text]
            start_page = page_num
        
        else:
            # Append text if we are tracking a valid section
            if current_sc_id:
                current_text_buffer.append(text)

    # Save the final section
    if current_sc_id and current_text_buffer:
        grouped_data[current_sc_id] = {
            "sc_code": current_sc_id,
            "start_page": start_page,
            "end_page": pages[-1]['page_number'],
            "full_text": "\n".join(current_text_buffer)
        }
        print(f"Captured {current_sc_id}: Pages {start_page} to {pages[-1]['page_number']}")

    # Write Output
    with open(output_file, 'w') as f:
        json.dump(grouped_data, f, indent=2)
    
    print(f"\nSuccess! Fixed groups saved to {output_file}")


def _get_default_paths():
    """Resolve default input/output paths relative to repo root."""
    # group_tariffs.py is in: <root>/src/agents/tariff_analysis/
    # root is 3 levels up
    root = Path(__file__).resolve().parents[3]
    input_path = root / "data" / "processed" / "raw_extracted_tarif.json"
    output_path = root / "data" / "processed" / "grouped_tariffs.json"
    return input_path, output_path


if __name__ == "__main__":
    input_file, output_file = _get_default_paths()
    
    if not input_file.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_file}\n"
            "Please ensure raw_extracted_tarif.json exists in data/processed/"
        )
    
    group_tariffs_v3(str(input_file), str(output_file))