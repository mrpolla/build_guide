import json
import psycopg2
import os
import argparse
import time
from datetime import datetime
from dotenv import load_dotenv
from helper_scripts.llm_utils import query_llm

# Load environment variables
load_dotenv()

# Configuration
DEFAULT_LLM_MODEL = "winkefinger/alma-13b"  # Default model
# mistral
# llama3
# gemma:2b
# qwen:1.8b
# phi3:mini
# winkefinger/alma-13b  - good for translation
# zongwei/gemma3-translator:1b  - good for translation
# nuextract  - good for text extraction
MAX_ITEMS = None  # Process 10 items by default (for testing)
BATCH_SIZE = 50  # Number of items to process in a batch before saving progress

# Output folder for logs and results
OUTPUT_FOLDER = "./logs/translate_missing_fields"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# DB connection settings
DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}

def create_short_description(product, model, log_file=None):
    """
    Create a short description (around 30 words) based on available English product information
    """
    # Build context from product info - prefer name_en, fallback to name_en_ai
    product_name = product.get('name_en') if product.get('name_en') else product.get('name_en_ai', '')
    description = product.get('description_en_ai', '')
    tech_descr = product.get('tech_descr_en_ai', '')
    tech_applic = product.get('tech_applic_en_ai', '')
    
    # Combine all relevant text fields for analysis
    combined_text = f"""
Product Name: {product_name}

Description: {description}

Technical Description: {tech_descr}

Technical Application: {tech_applic}
"""
    
    # Create the short description prompt
    short_desc_prompt = f"""You are a construction industry specialist creating concise product descriptions.

Your task: Create a SHORT, informative summary of this construction product in about 30 words.

Here is the product information:

{combined_text}

Focus on:
1. What the product IS (specific type of product/material)
2. Its PRIMARY purpose and application
3. Any KEY distinguishing features

RESPOND WITH ONLY the short description text.
DO NOT include any explanations, headers, or other text.
DO NOT exceed 30-35 words.
Use professional, technical language appropriate for construction industry professionals."""

    # Log the short description prompt if requested
    if log_file:
        log_file.write(f"\n\n--- SHORT DESCRIPTION PROMPT FOR {product_name} ---\n")
        log_file.write(short_desc_prompt)
    
    # Query the LLM for short description
    short_description = query_llm(short_desc_prompt, model)
    
    # Log the short description response if requested
    if log_file:
        log_file.write(f"\n\n--- SHORT DESCRIPTION RESPONSE ---\n")
        log_file.write(short_description)
    
    return short_description.strip()

def translate_text(text, field_name, model, log_file=None):
    """
    Translate German text to English using the LLM
    """
    if not text or text.strip() == '':
        return ''
        
    # Create the translation prompt
    prompt = f"""You are a construction industry translator specializing in technical terminology.
    
Translate this German construction industry text to English with precise technical accuracy:

"{text}"

INSTRUCTIONS:
- Maintain all technical terminology accuracy
- Preserve any measurements, units, or specific values
- Keep the same level of technical detail
- Use standard technical English for the construction industry
- Do not add any information not present in the original text

Return only the English translation, nothing else."""
    
    # Log the translation prompt if requested
    if log_file:
        log_file.write(f"\n\n--- TRANSLATION PROMPT ({field_name}) ---\n")
        log_file.write(prompt)
    
    translated_text = query_llm(prompt, model)
    
    # Log the translation response if requested
    if log_file:
        log_file.write(f"\n\n--- TRANSLATION RESPONSE ---\n")
        log_file.write(translated_text)
    
    return translated_text.strip()

def process_products(conn, model, max_items=None, batch_size=BATCH_SIZE, log_file=None):
    """
    Process all products in the database, translating missing English fields
    and creating short descriptions
    """
    cur = conn.cursor()
    
    # Get products with at least one missing English field
    limit_clause = f"LIMIT {max_items}" if max_items is not None else ""
    cur.execute(f"""
        SELECT process_id, name_en, name_de, description_en, description_de, 
               tech_descr_en, tech_descr_de, tech_applic_en, tech_applic_de
        FROM products
        WHERE (name_en IS NULL AND name_de IS NOT NULL)
           OR (description_en IS NULL AND description_de IS NOT NULL)
           OR (tech_descr_en IS NULL AND tech_descr_de IS NOT NULL)
           OR (tech_applic_en IS NULL AND tech_applic_de IS NOT NULL)
           OR short_desc_en_ai IS NULL
        {limit_clause}
    """)
    
    products = cur.fetchall()
    print(f"Found {len(products)} products with missing English fields")
    
    results = []
    
    # Process each product
    for i, (process_id, name_en, name_de, description_en, description_de, 
            tech_descr_en, tech_descr_de, tech_applic_en, tech_applic_de) in enumerate(products):
        
        product_name = name_en if name_en else name_de
        print(f"Processing product {i+1}/{len(products)}: {product_name[:40]}...")
        
        if log_file:
            log_file.write(f"\n\n{'='*80}\nPRODUCT {i+1}/{len(products)}: {product_name} (ID: {process_id})\n{'='*80}\n")
        
        # Initialize product object to store translations
        product = {
            "process_id": process_id,
            "name_en": name_en,
            "name_de": name_de,
            "description_en": description_en,
            "description_de": description_de,
            "tech_descr_en": tech_descr_en,
            "tech_descr_de": tech_descr_de,
            "tech_applic_en": tech_applic_en,
            "tech_applic_de": tech_applic_de,
            "translations": {}
        }
        
        # Translate each missing field
        if not name_en and name_de:
            print(f"  > Translating name_de to name_en_ai...")
            product["translations"]["name_en_ai"] = translate_text(name_de, "name_de", model, log_file)
        
        if not description_en and description_de:
            print(f"  > Translating description_de to description_en_ai...")
            product["translations"]["description_en_ai"] = translate_text(description_de, "description_de", model, log_file)
        
        if not tech_descr_en and tech_descr_de:
            print(f"  > Translating tech_descr_de to tech_descr_en_ai...")
            product["translations"]["tech_descr_en_ai"] = translate_text(tech_descr_de, "tech_descr_de", model, log_file)
        
        if not tech_applic_en and tech_applic_de:
            print(f"  > Translating tech_applic_de to tech_applic_en_ai...")
            product["translations"]["tech_applic_en_ai"] = translate_text(tech_applic_de, "tech_applic_de", model, log_file)
        
        # Create short description if we have any English content
        print(f"  > Creating short_desc_en_ai...")
        
        # Create product object with all translated fields for short description generation
        enriched_product = {
            "name_en": name_en,
            "name_en_ai": product["translations"].get("name_en_ai", ""),
            "description_en_ai": description_en or product["translations"].get("description_en_ai", ""),
            "tech_descr_en_ai": tech_descr_en or product["translations"].get("tech_descr_en_ai", ""),
            "tech_applic_en_ai": tech_applic_en or product["translations"].get("tech_applic_en_ai", "")
        }
        
        product["translations"]["short_desc_en_ai"] = create_short_description(enriched_product, model, log_file)
        
        # Update database with translations
        update_sql_parts = []
        update_values = []
        
        for field, value in product["translations"].items():
            if value:  # Only update if we have a value
                update_sql_parts.append(f"{field} = %s")
                update_values.append(value)
        
        if update_sql_parts:
            update_sql = f"UPDATE products SET {', '.join(update_sql_parts)} WHERE process_id = %s"
            update_values.append(process_id)
            
            try:
                cur.execute(update_sql, update_values)
                conn.commit()
                print(f"  > Database updated with translations.")
            except Exception as e:
                conn.rollback()
                print(f"  > Error updating database: {str(e)}")
                if log_file:
                    log_file.write(f"\n\nERROR UPDATING DATABASE: {str(e)}\n")
        
        # Include the actual translated content in the results
        results.append({
            "process_id": process_id,
            "name": product_name,
            "fields_translated": list(product["translations"].keys()),
            "translations": product["translations"],
            "success": True
        })
        
        # Save progress after each batch (for backup/recovery purposes)
        if (i + 1) % batch_size == 0 or i == len(products) - 1:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Sanitize model name for filenames
            safe_model_name = model.replace("/", "_").replace(":", "_").replace(".", "p")
            progress_file = os.path.join(OUTPUT_FOLDER, f"products_translation_progress_{safe_model_name}_{timestamp}.json")
            with open(progress_file, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"Progress backup saved to {progress_file}")
    
    cur.close()
    return results

def process_lcia_results(conn, model, max_items=None, batch_size=BATCH_SIZE, log_file=None):
    """
    Process LCIA results with missing English method names
    """
    cur = conn.cursor()
    
    # Get LCIA results with missing English method names
    limit_clause = f"LIMIT {max_items}" if max_items is not None else ""
    cur.execute(f"""
        SELECT lcia_id, process_id, method_en, method_de
        FROM lcia_results
        WHERE method_en IS NULL AND method_de IS NOT NULL
        {limit_clause}
    """)
    
    lcia_results = cur.fetchall()
    print(f"Found {len(lcia_results)} LCIA results with missing English method names")
    
    results = []
    
    # Process each LCIA result
    for i, (lcia_id, process_id, method_en, method_de) in enumerate(lcia_results):
        print(f"Processing LCIA result {i+1}/{len(lcia_results)}: ID {lcia_id}...")
        
        if log_file:
            log_file.write(f"\n\n{'='*80}\nLCIA RESULT {i+1}/{len(lcia_results)}: ID {lcia_id}\n{'='*80}\n")
        
        # Translate method_de to method_en_ai
        print(f"  > Translating method_de to method_en_ai...")
        method_en_ai = translate_text(method_de, "method_de", model, log_file)
        
        # Update database
        try:
            cur.execute("UPDATE lcia_results SET method_en_ai = %s WHERE lcia_id = %s", 
                       (method_en_ai, lcia_id))
            conn.commit()
            print(f"  > Database updated with translation.")
        except Exception as e:
            conn.rollback()
            print(f"  > Error updating database: {str(e)}")
            if log_file:
                log_file.write(f"\n\nERROR UPDATING DATABASE: {str(e)}\n")
        
        results.append({
            "lcia_id": lcia_id,
            "process_id": process_id,
            "field_translated": "method_en_ai",
            "translation": {
                "method_en_ai": method_en_ai
            },
            "success": True
        })
        
        # Save progress after each batch (for backup/recovery purposes)
        if (i + 1) % batch_size == 0 or i == len(lcia_results) - 1:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Sanitize model name for filenames
            safe_model_name = model.replace("/", "_").replace(":", "_").replace(".", "p")
            progress_file = os.path.join(OUTPUT_FOLDER, f"lcia_results_translation_progress_{safe_model_name}_{timestamp}.json")
            with open(progress_file, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"Progress backup saved to {progress_file}")
    
    cur.close()
    return results

def process_exchanges(conn, model, max_items=None, batch_size=BATCH_SIZE, log_file=None):
    """
    Process exchanges with missing English flow names
    """
    cur = conn.cursor()
    
    # Get exchanges with missing English flow names
    limit_clause = f"LIMIT {max_items}" if max_items is not None else ""
    cur.execute(f"""
        SELECT exchange_id, process_id, flow_en, flow_de
        FROM exchanges
        WHERE flow_en IS NULL AND flow_de IS NOT NULL
        {limit_clause}
    """)
    
    exchanges = cur.fetchall()
    print(f"Found {len(exchanges)} exchanges with missing English flow names")
    
    results = []
    
    # Process each exchange
    for i, (exchange_id, process_id, flow_en, flow_de) in enumerate(exchanges):
        print(f"Processing exchange {i+1}/{len(exchanges)}: ID {exchange_id}...")
        
        if log_file:
            log_file.write(f"\n\n{'='*80}\nEXCHANGE {i+1}/{len(exchanges)}: ID {exchange_id}\n{'='*80}\n")
        
        # Translate flow_de to flow_en_ai
        print(f"  > Translating flow_de to flow_en_ai...")
        flow_en_ai = translate_text(flow_de, "flow_de", model, log_file)
        
        # Update database
        try:
            cur.execute("UPDATE exchanges SET flow_en_ai = %s WHERE exchange_id = %s", 
                       (flow_en_ai, exchange_id))
            conn.commit()
            print(f"  > Database updated with translation.")
        except Exception as e:
            conn.rollback()
            print(f"  > Error updating database: {str(e)}")
            if log_file:
                log_file.write(f"\n\nERROR UPDATING DATABASE: {str(e)}\n")
        
        results.append({
            "exchange_id": exchange_id,
            "process_id": process_id,
            "field_translated": "flow_en_ai",
            "translation": {
                "flow_en_ai": flow_en_ai
            },
            "success": True
        })
        
        # Save progress after each batch (for backup/recovery purposes)
        if (i + 1) % batch_size == 0 or i == len(exchanges) - 1:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Sanitize model name for filenames
            safe_model_name = model.replace("/", "_").replace(":", "_").replace(".", "p")
            progress_file = os.path.join(OUTPUT_FOLDER, f"exchanges_translation_progress_{safe_model_name}_{timestamp}.json")
            with open(progress_file, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"Progress backup saved to {progress_file}")
    
    cur.close()
    return results

def process_flow_properties(conn, model, max_items=None, batch_size=BATCH_SIZE, log_file=None):
    """
    Process flow properties with missing English names
    """
    cur = conn.cursor()
    
    # Get flow properties with missing English names
    limit_clause = f"LIMIT {max_items}" if max_items is not None else ""
    cur.execute(f"""
        SELECT flow_property_id, process_id, name_en, name_de
        FROM flow_properties
        WHERE name_en IS NULL AND name_de IS NOT NULL
        {limit_clause}
    """)
    
    flow_properties = cur.fetchall()
    print(f"Found {len(flow_properties)} flow properties with missing English names")
    
    results = []
    
    # Process each flow property
    for i, (flow_property_id, process_id, name_en, name_de) in enumerate(flow_properties):
        print(f"Processing flow property {i+1}/{len(flow_properties)}: ID {flow_property_id}...")
        
        if log_file:
            log_file.write(f"\n\n{'='*80}\nFLOW PROPERTY {i+1}/{len(flow_properties)}: ID {flow_property_id}\n{'='*80}\n")
        
        # Translate name_de to name_en_ai
        print(f"  > Translating name_de to name_en_ai...")
        name_en_ai = translate_text(name_de, "name_de", model, log_file)
        
        # Update database
        try:
            cur.execute("UPDATE flow_properties SET name_en_ai = %s WHERE flow_property_id = %s", 
                       (name_en_ai, flow_property_id))
            conn.commit()
            print(f"  > Database updated with translation.")
        except Exception as e:
            conn.rollback()
            print(f"  > Error updating database: {str(e)}")
            if log_file:
                log_file.write(f"\n\nERROR UPDATING DATABASE: {str(e)}\n")
        
        results.append({
            "flow_property_id": flow_property_id,
            "process_id": process_id,
            "field_translated": "name_en_ai",
            "translation": {
                "name_en_ai": name_en_ai
            },
            "success": True
        })
        
        # Save progress after each batch (for backup/recovery purposes)
        if (i + 1) % batch_size == 0 or i == len(flow_properties) - 1:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Sanitize model name for filenames
            safe_model_name = model.replace("/", "_").replace(":", "_").replace(".", "p")
            progress_file = os.path.join(OUTPUT_FOLDER, f"flow_properties_translation_progress_{safe_model_name}_{timestamp}.json")
            with open(progress_file, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"Progress backup saved to {progress_file}")
    
    cur.close()
    return results

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Translate missing English fields in the database')
    parser.add_argument('--model', type=str, default=DEFAULT_LLM_MODEL, 
                        help=f'LLM model to use (default: {DEFAULT_LLM_MODEL})')
    parser.add_argument('--max-items', type=int, default=MAX_ITEMS, 
                        help=f'Maximum number of items to process per table (default: {MAX_ITEMS})')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, 
                        help=f'Number of items to process before saving progress (default: {BATCH_SIZE})')
    parser.add_argument('--skip-products', action='store_true', 
                        help='Skip processing products table')
    parser.add_argument('--skip-lcia', action='store_true', 
                        help='Skip processing lcia_results table')
    parser.add_argument('--skip-exchanges', action='store_true', 
                        help='Skip processing exchanges table')
    parser.add_argument('--skip-flow-properties', action='store_true', 
                        help='Skip processing flow_properties table')
    parser.add_argument('--output-file', type=str, default=None,
                        help='Custom output filename for all translations (default: auto-generated)')
    args = parser.parse_args()
    
    # Get selected model and create timestamp
    selected_model = args.model
    # Sanitize model name for filenames (replace slashes, colons, etc.)
    safe_model_name = selected_model.replace("/", "_").replace(":", "_").replace(".", "p")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create log file
    log_filename = os.path.join(OUTPUT_FOLDER, f"translation_{safe_model_name}_{timestamp}.log")
    print(f"Creating log file: {log_filename}")
    log_file = open(log_filename, "w", encoding="utf-8")
    log_file.write(f"TRANSLATION LOG - Model: {selected_model}, Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Set output file name
    if args.output_file:
        output_filename = args.output_file
    else:
        output_filename = os.path.join(OUTPUT_FOLDER, f"translations_all_{safe_model_name}_{timestamp}.json")
    
    # Connect to the database
    conn = psycopg2.connect(**DB_PARAMS)
    
    try:
        # Combined results from all processes
        all_results = {
            "timestamp": datetime.now().isoformat(),
            "model": selected_model,
            "products": [],
            "lcia_results": [],
            "exchanges": [],
            "flow_properties": []
        }
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "model": selected_model,
            "results": {}
        }
        
        # Process each table unless skipped
        if not args.skip_products:
            print("\n=== Processing Products ===")
            start_time = time.time()
            product_results = process_products(conn, selected_model, args.max_items, args.batch_size, log_file)
            products_time = time.time() - start_time
            all_results["products"] = product_results
            summary["results"]["products"] = {
                "count": len(product_results),
                "time_seconds": products_time
            }
            print(f"Completed processing {len(product_results)} products in {products_time:.2f} seconds")
        
        if not args.skip_lcia:
            print("\n=== Processing LCIA Results ===")
            start_time = time.time()
            lcia_results = process_lcia_results(conn, selected_model, args.max_items, args.batch_size, log_file)
            lcia_time = time.time() - start_time
            all_results["lcia_results"] = lcia_results
            summary["results"]["lcia_results"] = {
                "count": len(lcia_results),
                "time_seconds": lcia_time
            }
            print(f"Completed processing {len(lcia_results)} LCIA results in {lcia_time:.2f} seconds")
        
        if not args.skip_exchanges:
            print("\n=== Processing Exchanges ===")
            start_time = time.time()
            exchange_results = process_exchanges(conn, selected_model, args.max_items, args.batch_size, log_file)
            exchanges_time = time.time() - start_time
            all_results["exchanges"] = exchange_results
            summary["results"]["exchanges"] = {
                "count": len(exchange_results),
                "time_seconds": exchanges_time
            }
            print(f"Completed processing {len(exchange_results)} exchanges in {exchanges_time:.2f} seconds")
        
        if not args.skip_flow_properties:
            print("\n=== Processing Flow Properties ===")
            start_time = time.time()
            flow_prop_results = process_flow_properties(conn, selected_model, args.max_items, args.batch_size, log_file)
            flow_props_time = time.time() - start_time
            all_results["flow_properties"] = flow_prop_results
            summary["results"]["flow_properties"] = {
                "count": len(flow_prop_results),
                "time_seconds": flow_props_time
            }
            print(f"Completed processing {len(flow_prop_results)} flow properties in {flow_props_time:.2f} seconds")
        
        # Save all results to a single file
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"\nAll translations saved to: {output_filename}")
        
        # Save final summary
        summary_file = os.path.join(OUTPUT_FOLDER, f"translation_summary_{safe_model_name}_{timestamp}.json")
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        print(f"Summary saved to: {summary_file}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        log_file.write(f"\n\nERROR: {str(e)}")
    finally:
        conn.close()
        log_file.close()
        print(f"Log file closed: {log_filename}")

if __name__ == "__main__":
    main()