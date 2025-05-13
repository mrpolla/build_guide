import os
import psycopg2
import pandas as pd
import numpy as np
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}

def normalize_unit(unit):
    """
    Standardize unit names to a consistent format.
    Comprehensive version that handles all units found in the CSV files.
    """
    if not isinstance(unit, str):
        return unit
    
    # Handle NULL values
    if unit.upper() == "NULL" or unit.strip() == "":
        return "-"
    
    unit = unit.strip().lower()
    
    # Standard mappings with expanded patterns
    mappings = {
        # Volume units
        r"^m[\^³]?[3]?$": "m3",
        r"^m\s*[3³]$": "m3",
        r"^m\^3$": "m3",
        r"^m³.*$": "m3",
        
        # Carbon dioxide equivalents with various notations
        r"^kg.*co.*?2.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg CO2 eq",
        r"^kg.*co.*?\(?2\)?.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg CO2 eq",
        r"^kg.*co_?\(?2\)?[- ]?äq\.?$": "kg CO2 eq",
        r"^kg\s*co_?\(?2\)?(?:\s|-|_).*$": "kg CO2 eq",
        
        # CFC equivalents
        r"^kg.*(?:cfc|r)\s*11.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg CFC-11 eq",
        r"^kg.*(?:cfc|r)11.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg CFC-11 eq",
        r"^kg\s*cfc-11\s*eq\.?$": "kg CFC-11 eq",
        
        # Phosphorus equivalents
        r"^kg.*p(?:[ -]?eq|\s?äq|\s?aeq|\s?äqv|\s?eqv|[- ]?äquiv|\s?equivalent).*$": "kg P eq",
        r"^kg.*p[- ]?äq.*$": "kg P eq",
        r"^kg.*phosphat.*$": "kg PO4 eq",
        
        # NMVOC equivalents
        r"^kg.*n.*mvoc.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg NMVOC eq",
        r"^kg.*nmvoc.*$": "kg NMVOC eq",
        
        # Ethene/Ethylene equivalents
        r"^kg.*(?:ethen|ethene|ethylen).*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg C2H4 eq",
        r"^kg.*(?:ethen|ethene|ethylen)[- ]?äq.*$": "kg C2H4 eq",
        r"^kg.*c2h[24].*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg C2H4 eq",
        
        # Antimony equivalents
        r"^kg.*sb.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg Sb eq",
        
        # Sulfur dioxide equivalents
        r"^kg.*so.*?2.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg SO2 eq",
        r"^kg.*so_?\(?2\)?.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg SO2 eq",
        
        # Phosphate equivalents
        r"^kg.*po.*?4.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg PO4 eq",
        r"^kg.*po_?\(?4\)?.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg PO4 eq",
        r"^kg.*po.*\(?3-?\)?.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg PO4 eq",
        r"^kg.*\(po4\)3-.*$": "kg PO4 eq",
        r"^kg.*phosphate.*$": "kg PO4 eq",
        
        # Nitrogen equivalents
        r"^kg.*n.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kg N eq",
        r"^mol.*n.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "mol N eq",
        r"^mole.*n.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "mol N eq",
        
        # Hydrogen ion equivalents
        r"^mol.*h.*[\+\^].*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "mol H+ eq",
        r"^mole.*h.*[\+\^].*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "mol H+ eq",
        r"^mol.*h.*[-\+](?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "mol H+ eq",
        
        # Uranium equivalents
        r"^k?bq.*u235.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*$": "kBq U235 eq",
        
        # World water equivalents
        r"^m.*?3.*world.*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*(?:deprived|entzogen)?$": "m3 world eq deprived",
        r"^m\^?\(?3\)?.*w(?:orld|elt).*(?:eq|äq|aeq|äqv|eqv|[- ]?äquiv|\s?equivalent).*(?:deprived|entzogen)$": "m3 world eq deprived",
        
        # Disease incidence
        r"^disease\s*incidence$": "disease incidence",
        r"^krankheitsfälle$": "disease incidence",
        
        # Other specific units
        r"^ctuh$": "CTUh",
        r"^ctue$": "CTUe",
        r"^sqp$": "SQP",
        r"^dimensionless$": "dimensionless",
        
        # Simple units
        r"^-?$": "-",
        r"^mj$": "MJ",
        r"^kg$": "kg",
        
        # Per unit conversions
        r"^kg\/pce$": "kg/pce",
        r"^kg\s*\/\s*pce$": "kg/pce",
        
        # Compound units with divisions or per - volume-based
        r"^kg\s*\/\s*m[\^]?3$": "kg/m3",
        r"^kg\s*\/\s*m3$": "kg/m3",
        r"^kg\s*per\s*m3$": "kg/m3",
        r"^kg\s*per\s*m[\^]?3$": "kg/m3",
        
        # Compound units with divisions or per - area-based
        r"^kg\s*\/\s*m[\^]?2$": "kg/m2",
        r"^kg\s*\/\s*m2$": "kg/m2",
        r"^kg\s*per\s*m2$": "kg/m2",
        r"^kg\s*per\s*m[\^]?2$": "kg/m2"
    }

    for pattern, standard in mappings.items():
        if re.match(pattern, unit):
            return standard
    
    return unit

def connect_to_db():
    return psycopg2.connect(**DB_PARAMS)

def create_statistics_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS indicator_statistics (
            stat_id SERIAL PRIMARY KEY,
            category_level_1 TEXT,
            category_level_2 TEXT,
            category_level_3 TEXT,
            indicator_key TEXT,
            source TEXT,
            module TEXT,
            mean REAL,
            median REAL,
            std_dev REAL,
            min REAL,
            max REAL,
            unit TEXT,
            count INTEGER,
            last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
        );
        """)
        conn.commit()

def get_combined_data(conn):
    # Load LCIA data
    lcia_query = """
        SELECT lr.process_id, lr.indicator_key, lr.unit, lma.module, lma.amount, 'lcia' AS source
        FROM lcia_results lr
        JOIN lcia_moduleamounts lma ON lr.lcia_id = lma.lcia_id
    """
    lcia_df = pd.read_sql(lcia_query, conn)

    # Load exchange data
    exchange_query = """
        SELECT e.process_id, e.indicator_key, e.unit, ema.module, ema.amount, 'exchange' AS source
        FROM exchanges e
        JOIN exchange_moduleamounts ema ON e.exchange_id = ema.exchange_id
    """
    exchange_df = pd.read_sql(exchange_query, conn)

    # Load product categories
    products_query = """
        SELECT process_id, category_level_1, category_level_2, category_level_3
        FROM products
    """
    products_df = pd.read_sql(products_query, conn)

    # Combine and merge with product categories
    data = pd.concat([lcia_df, exchange_df], ignore_index=True)
    data = data.merge(products_df, on='process_id', how='left')
    
    # Standardize units
    data['standardized_unit'] = data['unit'].apply(normalize_unit)
    
    return data

def calculate_and_store_statistics(conn, data):
    def insert_stats(grouped_df):
        with conn.cursor() as cur:
            for _, row in grouped_df.iterrows():
                # Use the most common unit for each group
                cur.execute("""
                    INSERT INTO indicator_statistics (
                        category_level_1, category_level_2, category_level_3,
                        indicator_key, source, module,
                        mean, median, std_dev, min, max, unit, count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row.get('category_level_1'),
                    row.get('category_level_2'),
                    row.get('category_level_3'),
                    row['indicator_key'], row['source'], row['module'],
                    row['mean'], row['median'], row['std'], row['min'], row['max'], 
                    row['common_unit'], int(row['count'])
                ))
            conn.commit()

    # Function to get the most common standardized unit for each group
    def get_most_common_unit(group_df):
        common_unit = group_df['standardized_unit'].mode().iloc[0] if not group_df['standardized_unit'].empty else 'N/A'
        return common_unit

    # Create groups without unit in the groupby
    # Stats for level 3
    group3_keys = ['category_level_1', 'category_level_2', 'category_level_3', 
                   'indicator_key', 'source', 'module']
    # First, calculate the common unit for each group
    unit_lookup3 = data.groupby(group3_keys).apply(get_most_common_unit).reset_index(name='common_unit')
    
    # Calculate statistics
    group3 = data.groupby(group3_keys)['amount']
    stats3 = group3.agg(['mean', 'median', 'std', 'min', 'max', 'count']).reset_index()
    
    # Merge with the unit information
    stats3 = pd.merge(stats3, unit_lookup3, on=group3_keys)
    insert_stats(stats3)

    # Stats for level 2
    group2_keys = ['category_level_1', 'category_level_2', 
                  'indicator_key', 'source', 'module']
    # First, calculate the common unit for each group
    unit_lookup2 = data.groupby(group2_keys).apply(get_most_common_unit).reset_index(name='common_unit')
    
    # Calculate statistics
    group2 = data.groupby(group2_keys)['amount']
    stats2 = group2.agg(['mean', 'median', 'std', 'min', 'max', 'count']).reset_index()
    
    # Merge with the unit information
    stats2 = pd.merge(stats2, unit_lookup2, on=group2_keys)
    stats2['category_level_3'] = None
    insert_stats(stats2)

    # Stats for level 1
    group1_keys = ['category_level_1', 'indicator_key', 'source', 'module']
    # First, calculate the common unit for each group
    unit_lookup1 = data.groupby(group1_keys).apply(get_most_common_unit).reset_index(name='common_unit')
    
    # Calculate statistics
    group1 = data.groupby(group1_keys)['amount']
    stats1 = group1.agg(['mean', 'median', 'std', 'min', 'max', 'count']).reset_index()
    
    # Merge with the unit information
    stats1 = pd.merge(stats1, unit_lookup1, on=group1_keys)
    stats1['category_level_2'] = None
    stats1['category_level_3'] = None
    insert_stats(stats1)

def main():
    conn = connect_to_db()
    try:
        create_statistics_table(conn)
        data = get_combined_data(conn)
        calculate_and_store_statistics(conn, data)
        print("Statistics successfully stored in the database.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()