import os
import json
import psycopg2
from psycopg2 import DatabaseError
import logging
from dotenv import load_dotenv
import re
import csv
from datetime import datetime, timezone
from tqdm import tqdm
import traceback

# --- Load environment and configure logging ---
load_dotenv()
log_dir = 'logs/02_store_epds_in_db'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=f'{log_dir}/store_epds_in_db.log')
logger = logging.getLogger(__name__)

MAX_FILES = None  # Set to None to process all files
# --- Database parameters ---
DB_PARAMS = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT")
}

# --- Translation file path ---
TRANSLATIONS_FILE = "./scripts/database/data/translations.csv"

# Global list to track missing translations
not_found_translations = []

def load_translations(csv_file=TRANSLATIONS_FILE):
    """Load translations from a CSV file."""
    translations = {}
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    german = row[0].strip()
                    english = row[1].strip()
                    if german and english:
                        translations[german.lower()] = english
        logger.info(f"Loaded {len(translations)} translations.")
    except FileNotFoundError:
        logger.warning(f"Translation file {csv_file} not found.")
    return translations


def translate_text(text, translations):
    """Translate text from German to English if a translation exists."""
    if not text:
        return text
        
    cleaned_text = text.replace(",", "").lower().strip()
    if cleaned_text not in translations:
        logger.debug(f"Warning: Translation for '{text}' not found.")
        if text not in not_found_translations:
            not_found_translations.append(text)
        return text

    return translations.get(cleaned_text)

target_indicators = set([
    "PERE", "PERM", "PERT", "PENRE", "PENRM", "PENRT", "SM", "RSF", "NRSF", "FW",
    "HWD", "NHWD", "RWD", "CRU", "MFR", "MER", "EEE", "EET",
    "GWP-total", "GWP-biogenic", "GWP-fossil", "GWP-luluc", "ODP", "POCP",
    "AP", "EP-terrestrial", "EP-freshwater", "EP-marine", "WDP",
    "ADPE", "ADPF", "HTP-c", "HTP-nc", "PM", "IR", "ETP-fw", "SQP"
])

def get_indicator_key(multilang_dict):
    if not isinstance(multilang_dict, dict):
        return None
    for lang_text in multilang_dict.values():
        for key in target_indicators:
            if re.search(rf'\b{re.escape(key)}\b', lang_text, re.IGNORECASE):
                return key
    return None

def extract_multilang(entry_list):
    if not isinstance(entry_list, list):
        return {}
    return {entry.get("lang", "unknown"): entry.get("value") for entry in entry_list if isinstance(entry, dict)}

def convert_timestamp(unix_timestamp):
    """Convert Unix timestamp (in milliseconds) to ISO 8601 format."""
    if not unix_timestamp:
        return None
    try:
        # Convert milliseconds to seconds
        timestamp_in_seconds = int(unix_timestamp) / 1000
        # Convert to datetime object
        dt = datetime.fromtimestamp(timestamp_in_seconds, tz=timezone.utc)
        # Format as ISO 8601 with timezone info
        return dt.isoformat()
    except (ValueError, TypeError):
        logger.warning(f"Failed to convert timestamp: {unix_timestamp}")
        return None

def extract_original_epd_url(data):
    """Extract the original EPD URL from the JSON data."""
    try:
        original_epd = data.get("modellingAndValidation", {}).get("dataSourcesTreatmentAndRepresentativeness", {}).get("other", {}).get("anies", [])
        for item in original_epd:
            if item.get("name") == "referenceToOriginalEPD":
                value = item.get("value", {})
                resource_urls = value.get("resourceURLs", [])
                if resource_urls:
                    return resource_urls[0]
    except (KeyError, TypeError) as e:
        logger.warning(f"Failed to extract original EPD URL: {e}")
    return None
    
def parse_json(json_path, translations):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    uuid = data.get("processInformation", {}).get("dataSetInformation", {}).get("UUID", "")
    version = data.get("administrativeInformation", {}).get("publicationAndOwnership", {}).get("dataSetVersion", "")
    process_id = f"{uuid}_{version}"

    name = extract_multilang(data["processInformation"]["dataSetInformation"].get("name", {}).get("baseName", []))
    comment = extract_multilang(data["processInformation"]["dataSetInformation"].get("generalComment", []))
    reference_year = data["processInformation"].get("time", {}).get("referenceYear")
    valid_until = data["processInformation"].get("time", {}).get("dataSetValidUntil")
    time_representativeness = {}

    safety_margins = {
        'margin': None,
        'description': {}
    }

    geo = data["processInformation"].get("geography", {}).get("locationOfOperationSupplyOrProduction", {})
    geography = {
        'location': geo.get("location"),
        'description': {}
    }

    tech = data["processInformation"].get("technology", {})
    technology_description = extract_multilang(tech.get("technologyDescriptionAndIncludedProcesses", []))
    tech_applicability = extract_multilang(tech.get("technologicalApplicability", []))

    method_info = data["modellingAndValidation"].get("LCIMethodAndAllocation", {})
    dataset_type = method_info.get("typeOfDataSet")
    dataset_subtype = next((el["value"] for el in method_info.get("other", {}).get("anies", []) if el.get("name") == "subType"), None)

    sources = ', '.join(
        el["shortDescription"][0]["value"]
        for el in data["modellingAndValidation"].get("dataSourcesTreatmentAndRepresentativeness", {}).get("referenceToDataSource", [])
        if el.get("shortDescription")
    )

    use_advice = extract_multilang(data["modellingAndValidation"].get("dataSourcesTreatmentAndRepresentativeness", {}).get("useAdviceForDataSet", []))

    original_epd_url = extract_original_epd_url(data)

    reviews = []
    for r in data["modellingAndValidation"].get("validation", {}).get("review", []):
        reviewers = [e["shortDescription"][0]["value"] for e in r.get("referenceToNameOfReviewerAndInstitution", []) if e.get("shortDescription")]
        reviews.append({"reviewers": reviewers, "details": {}})

    compliances = []
    for c in data["modellingAndValidation"].get("complianceDeclarations", {}).get("compliance", []):
        short = c.get("referenceToComplianceSystem", {}).get("shortDescription", [])
        system = extract_multilang(short)
        compliances.append({"system": system, "approval": None})

    admin = data.get("administrativeInformation", {})
    entry = admin.get("dataEntryBy", {})
    admin_info = {
        'generator': extract_multilang(admin.get("dataGenerator", {}).get("referenceToPersonOrEntityGeneratingTheDataSet", [{}])[0].get("shortDescription", [])),
        'entry_by': extract_multilang(entry.get("referenceToDataSetFormat", [{}])[0].get("shortDescription", [])),
        'timestamp': convert_timestamp(entry.get("timeStamp")),
        'formats': [
                    next((desc.get("value") for desc in fmt.get("shortDescription", []) if isinstance(desc, dict) and "value" in desc), None)
                    for fmt in entry.get("referenceToDataSetFormat", [])
                    ],
        'version': admin.get("publicationAndOwnership", {}).get("dataSetVersion"),
        'license': admin.get("publicationAndOwnership", {}).get("licenseType"),
        'access': extract_multilang(admin.get("publicationAndOwnership", {}).get("accessRestrictions", []))
    }

    classifications = []
    for c in data["processInformation"]["dataSetInformation"].get("classificationInformation", {}).get("classification", []):
        for cl in c.get("class", []):
            classifications.append({
                "name": c.get("name"),
                "level": cl.get("level", ""),
                "classId": cl.get("classId", ""),
                "classification": cl.get("value")
            })

    exchanges = []
    for ex in data.get("exchanges", {}).get("exchange", []):
        flow = extract_multilang(ex.get("referenceToFlowDataSet", {}).get("shortDescription", []))
        indicator_key = get_indicator_key(flow)
        direction = ex.get("exchange direction")
        meanAmount = ex.get("meanAmount")
        unit = None
        for item in ex.get("other", {}).get("anies", []):
            if item.get("name") == "referenceToUnitGroupDataSet":
                unit_value = item.get("value")
                if isinstance(unit_value, dict) and "shortDescription" in unit_value:
                    for desc in unit_value.get("shortDescription", []):
                        if isinstance(desc, dict) and "value" in desc:
                            unit = desc.get("value")
                            break
        module_amounts = {}
        for x in ex.get("other", {}).get("anies", []):
            if "module" in x:
                # If value doesn't exist or is empty, use None
                if "value" not in x or x["value"] == "":
                    amount = None
                else:
                    try:
                        amount = float(x["value"])
                    except (ValueError, TypeError):
                        amount = None
                
                module_amounts[x.get("module")] = {
                    "amount": amount, 
                    "scenario": x.get("scenario", "")
                }
        exchanges.append({
            'data_set_internal_id': ex.get("dataSetInternalID"),
            'reference_to_flow': None,
            'uri': ex.get("referenceToFlowDataSet", {}).get("uri"),
            'ref_object_id': ex.get("referenceToFlowDataSet", {}).get("refObjectId"),
            'flow': flow,
            'indicator_key': indicator_key,
            'direction': direction,
            'meanAmount': meanAmount,
            'unit': unit,
            'module_amounts': module_amounts
        })

    lcia_results = []
    for lcia in data.get("LCIAResults", {}).get("LCIAResult", []):
        method = extract_multilang(lcia.get("referenceToLCIAMethodDataSet", {}).get("shortDescription", []))
        indicator_key = get_indicator_key(method)
        meanAmount = lcia.get("meanAmount")
        unit = None
        for item in lcia.get("other", {}).get("anies", []):
            if item.get("name") == "referenceToUnitGroupDataSet":
                unit_value = item.get("value")
                if isinstance(unit_value, dict) and "shortDescription" in unit_value:
                    for desc in unit_value.get("shortDescription", []):
                        if isinstance(desc, dict) and "value" in desc:
                            unit = desc.get("value")
                            break
        module_amounts = {}
        for x in lcia.get("other", {}).get("anies", []):
            if "module" in x:
                # If value doesn't exist or is empty, use None
                if "value" not in x or x["value"] == "":
                    amount = None
                else:
                    try:
                        amount = float(x["value"])
                    except (ValueError, TypeError):
                        amount = None
                
                module_amounts[x.get("module")] = {
                    "amount": amount,
                    "scenario": x.get("scenario", "")
                }

        lcia_results.append({
            'method': method,
            'indicator_key': indicator_key,
            'meanAmount': meanAmount,
            'unit': unit,
            'module_amounts': module_amounts
        })

    reference_flow_id = data["processInformation"]["quantitativeReference"]["referenceToReferenceFlow"][0]
    reference_flow = []
    for ex in data.get("exchanges", {}).get("exchange", []):
        if ex.get("dataSetInternalID") == reference_flow_id:
            mat_props = ex.get("materialProperties", [])
            flow_props = ex.get("flowProperties", [])
            ref_flow_props = []
            for prop in flow_props:
                ref_flow_props.append({
                    "name_en": next((n["value"] for n in prop.get("name", []) if n.get("lang") == "en"), None),
                    "name_de": next((n["value"] for n in prop.get("name", []) if n.get("lang") == "de"), None),
                    "mean_value": prop.get("meanValue"),
                    "unit": prop.get("referenceUnit"),
                    "is_reference": prop.get("referenceFlowProperty", False),
                    "dataSetInternalID": prop.get("uuid")
                })
            reference_flow.append({
                'material_properties': {mp['name']: {
                    'value': mp['value'],
                    'units': mp['unit'],
                    'description': mp.get('unitDescription')
                } for mp in mat_props},
                'flow_properties': ref_flow_props
            })

    # Extract categories
    category_level_1 = None
    category_level_2 = None
    category_level_3 = None
    
    for classification in classifications:
        if classification.get("name").lower() == "oekobau.dat":
            classes = [c for c in classifications if c.get("name").lower() == "oekobau.dat"]
            for c in classes:
                level = int(c.get("level", "0"))
                classification_value = c.get("classification")
                # Translate the classification value
                translated_classification = translate_text(classification_value, translations)
                if level == 0:
                    category_level_1 = translated_classification
                elif level == 1:
                    category_level_2 = translated_classification
                elif level == 2:
                    category_level_3 = translated_classification

    return {
        'process_id': process_id,
        'uuid': uuid,
        'version': version,
        'name': name,
        'description': comment,
        'category_level_1': category_level_1,
        'category_level_2': category_level_2,
        'category_level_3': category_level_3,
        'classifications': classifications,
        'reference_year': reference_year,
        'valid_until': valid_until,
        'time_representativeness': time_representativeness,
        'safety_margins': safety_margins,
        'geography': geography,
        'technology_description': technology_description,
        'tech_applicability': tech_applicability,
        'dataset_type': dataset_type,
        'dataset_subtype': dataset_subtype,
        'sources': sources,
        'use_advice': use_advice,
        'reviews': reviews,
        'compliances': compliances,
        'admin_info': admin_info,
        'exchanges': exchanges,
        'lcia_results': lcia_results,
        'reference_flow': reference_flow,
        'original_epd_url': original_epd_url
    }

def connect_to_db():
    try:
        return psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def convert_to_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def extract_datastock_info(folder_name):
    """Extract datastock name and UUID from folder name."""
    pattern = r'data_stock_(.+)_uuid_([a-f0-9\-]+)_'
    match = re.search(pattern, folder_name)
    if match:
        datastock_name = match.group(1)
        datastock_uuid = match.group(2)
        return datastock_name, datastock_uuid
    return None, None

def get_or_create_datastock(conn, datastock_name, datastock_uuid):
    """Get existing datastock ID or create a new one."""
    try:
        with conn.cursor() as cursor:
            # Check if the datastock exists
            cursor.execute('''
                SELECT datastock_id FROM DataStocks WHERE uuid = %s
            ''', (datastock_uuid,))
            result = cursor.fetchone()
            
            if result:
                return result[0]
            
            # Create new datastock
            cursor.execute('''
                INSERT INTO DataStocks (name, uuid)
                VALUES (%s, %s)
                RETURNING datastock_id
            ''', (datastock_name, datastock_uuid))
            datastock_id = cursor.fetchone()[0]
            conn.commit()
            
            logger.info(f"Created new datastock: {datastock_name} (ID: {datastock_id})")
            return datastock_id
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating datastock: {e}")
        return None

def store_data_in_db(collected_data, conn, datastock_id):
    with conn.cursor() as cursor:
        for item in collected_data:
            # Check if the product with the given process_id already exists
            cursor.execute('''
                SELECT 1 FROM products WHERE process_id = %s
            ''', (item['process_id'],))
            exists = cursor.fetchone()

            if exists:
                logger.info(f"Product with process_id {item['process_id']} already exists. Skipping insertion.")
                continue

            # Insert the product if it does not exist
            cursor.execute('''
                INSERT INTO products (
                    process_id, uuid, version, name_de, name_en, 
                    category_level_1, category_level_2, category_level_3,
                    description_de, description_en, reference_year,
                    valid_until, time_repr_de, time_repr_en, safety_margin, safety_descr_de, safety_descr_en,
                    geo_location, geo_descr_de, geo_descr_en,
                    tech_descr_de, tech_descr_en, tech_applic_de, tech_applic_en,
                    dataset_type, dataset_subtype, sources,
                    use_advice_de, use_advice_en,
                    generator_de, generator_en, entry_by_de, entry_by_en,
                    admin_version, license_type, access_de, access_en,
                    timestamp, formats, original_epd_url, datastock_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s,
                          %s, %s, %s,
                          %s, %s, %s, %s, 
                          %s, %s, %s,
                          %s, %s,
                          %s, %s, %s, %s,
                          %s, %s, %s, %s,
                          %s, %s, %s, %s)
            ''', (
                item['process_id'], item['uuid'], item['version'],
                item['name'].get('de'), item['name'].get('en'),
                item['category_level_1'], item['category_level_2'], item['category_level_3'],
                item['description'].get('de'), item['description'].get('en'),
                item['reference_year'], item['valid_until'],
                item['time_representativeness'].get('de'), item['time_representativeness'].get('en'),
                item['safety_margins']['margin'],
                item['safety_margins']['description'].get('de'), item['safety_margins']['description'].get('en'),
                item['geography']['location'],
                item['geography']['description'].get('de'), item['geography']['description'].get('en'),
                item['technology_description'].get('de'), item['technology_description'].get('en'),
                item['tech_applicability'].get('de'), item['tech_applicability'].get('en'),
                item['dataset_type'], item['dataset_subtype'], item['sources'],
                item['use_advice'].get('de'), item['use_advice'].get('en'),
                item['admin_info']['generator'].get('de'), item['admin_info']['generator'].get('en'),
                item['admin_info']['entry_by'].get('de'), item['admin_info']['entry_by'].get('en'),
                item['admin_info']['version'], item['admin_info']['license'],
                item['admin_info']['access'].get('de'), item['admin_info']['access'].get('en'),
                item['admin_info']['timestamp'],
                ', '.join(filter(None, item['admin_info']['formats'])),  # Filter None values
                item['original_epd_url'],
                datastock_id
            ))

            # Insert related data (e.g., classifications, exchanges, LCIA results, reviews, compliances)
            for classification in item['classifications']:
                cursor.execute('''
                    INSERT INTO classifications (process_id, name, level, classId, classification)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (item['process_id'], classification['name'], classification['level'], classification['classId'], classification['classification']))

            for exchange in item['exchanges']:
                cursor.execute('''
                    INSERT INTO exchanges (process_id, flow_de, flow_en, indicator_key, direction, meanAmount, unit)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING exchange_id
                ''', (
                    item['process_id'],
                    exchange['flow'].get('de'),
                    exchange['flow'].get('en'),
                    exchange['indicator_key'],
                    exchange['direction'],
                    exchange['meanAmount'],
                    exchange['unit']
                ))
                exchange_id = cursor.fetchone()[0]

                for module, data in exchange['module_amounts'].items():
                   scenario = data['scenario']
                   amount = data['amount']
                   cursor.execute('''
                       INSERT INTO exchange_moduleamounts (exchange_id, module, scenario, amount)
                       VALUES (%s, %s, %s, %s)
                   ''', (exchange_id, module, scenario, amount))

            for lcia in item['lcia_results']:
                cursor.execute('''
                    INSERT INTO lcia_results (process_id, method_de, method_en, indicator_key, meanAmount, unit)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING lcia_id
                ''', (
                    item['process_id'],
                    lcia['method'].get('de'),
                    lcia['method'].get('en'),
                    lcia['indicator_key'],
                    lcia['meanAmount'],
                    lcia['unit']
                ))
                lcia_id = cursor.fetchone()[0]

                for module, data in lcia['module_amounts'].items():
                    scenario = data['scenario']
                    amount = data['amount']
                    cursor.execute('''
                        INSERT INTO lcia_moduleamounts (lcia_id, module, scenario, amount)
                        VALUES (%s, %s, %s, %s)
                    ''', (lcia_id, module, scenario, amount))

            for review in item['reviews']:
                for reviewer in review['reviewers']:
                    cursor.execute('''
                        INSERT INTO reviews (process_id, reviewer, detail_de, detail_en)
                        VALUES (%s, %s, %s, %s)
                    ''', (
                        item['process_id'], reviewer,
                        review['details'].get('de'), review['details'].get('en')
                    ))

            for compliance in item['compliances']:
                cursor.execute('''
                    INSERT INTO compliances (process_id, system_de, system_en, approval)
                    VALUES (%s, %s, %s, %s)
                ''', (
                    item['process_id'],
                    compliance['system'].get('de'), compliance['system'].get('en'),
                    compliance['approval']
                ))

            for refFlow in item['reference_flow']:

                # Insert the reference flow properties 
                for flow_property in refFlow['flow_properties']:
                    cursor.execute('''
                        INSERT INTO flow_properties (process_id, name_en, name_de, meanamount, unit, is_reference)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (
                        item['process_id'],
                        flow_property.get('name_en'),
                        flow_property.get('name_de'),
                        flow_property.get('mean_value'),
                        flow_property.get('unit'),
                        flow_property.get('is_reference', False)
                    ))

                # Insert the material properties 
                for property_id, property_data in refFlow['material_properties'].items():
                    cursor.execute('''
                        INSERT INTO material_properties (process_id, property_id, property_name, value, units, description)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (
                        item['process_id'],
                        property_id,
                        property_data.get('name'),
                        convert_to_float(property_data.get('value')),
                        property_data.get('units'),
                        property_data.get('description')
                    ))
                    
        conn.commit()

def process_datastock_folder(data_dir, folder_name, conn, translations):
    """Process all JSON files in a datastock folder."""
    folder_path = os.path.join(data_dir, folder_name)
    
    # Extract datastock information
    datastock_name, datastock_uuid = extract_datastock_info(folder_name)
    if not datastock_name or not datastock_uuid:
        logger.warning(f"Could not extract datastock info from folder: {folder_name}")
        return 0
    
    # Get or create datastock
    datastock_id = get_or_create_datastock(conn, datastock_name, datastock_uuid)
    if not datastock_id:
        logger.error(f"Failed to get or create datastock for: {folder_name}")
        return 0
    
    # Get all JSON files in the directory
    json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]
    file_count = 0
    
    total_files = len(json_files) if MAX_FILES is None else min(len(json_files), MAX_FILES)
    
    for file_name in tqdm(json_files, desc=f"Processing EPD files in {folder_name}", total=total_files):
        if MAX_FILES is not None and file_count >= MAX_FILES:
            logger.info(f"Processed {MAX_FILES} files. Stopping for folder {folder_name}.")
            break
            
        json_path = os.path.join(folder_path, file_name)
        logger.info(f"Processing file: {json_path}")
            
        try:
            # Pass translations dictionary to parse_json
            item_data = parse_json(json_path, translations)
            if item_data:
                try:
                    store_data_in_db([item_data], conn, datastock_id)
                    logger.info(f"Data from {file_name} stored successfully.")
                    file_count += 1
                except Exception as store_error:
                    logger.error(f"Failed to store data from {file_name}: {store_error}")
                    logger.error(traceback.format_exc())
                    conn.rollback() 
        except Exception as parse_error:
            logger.error(f"Failed to parse {file_name}: {parse_error}")
            logger.error(traceback.format_exc())

    logger.info(f"Processing complete for folder {folder_name}. Successfully processed {file_count} out of {len(json_files)} files.")
    return file_count

def store_data(data_dir):
    conn = None
    try:
        conn = connect_to_db()
        logger.info(f"Connected to database {DB_PARAMS['dbname']} on {DB_PARAMS['host']}")

        # Load translations
        translations = load_translations()
        
        # Get all subdirectories in the data directory
        data_folders = [f for f in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, f)) 
                        and f.startswith("data_stock_")]
        
        total_processed = 0
        
        for folder in data_folders:
            logger.info(f"Processing datastock folder: {folder}")
            processed = process_datastock_folder(data_dir, folder, conn, translations)
            total_processed += processed
        
        logger.info(f"Total EPDs processed across all datastocks: {total_processed}")
        
        # Save missing translations
        if not_found_translations:
            translation_file = os.path.join(log_dir, "translations_not_found.txt")
            with open(translation_file, "w", encoding="utf-8") as f:
                for item in not_found_translations:
                    if item:  # Only write non-empty strings
                        f.write(f"{item}\n")
            logger.info(f"Saved {len(not_found_translations)} missing translations to {translation_file}")

    except Exception as e:
        logger.error(f"Error in store_data function: {e}")
        logger.error(f"Detailed error traceback:\n{traceback.format_exc()}")
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

def convert_to_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
    
def main():
    data_dir = os.path.join(os.getcwd(), "data")
    store_data(data_dir)

if __name__ == "__main__":
    main()