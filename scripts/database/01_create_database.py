import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
log_dir = 'logs/01_create_database_logs'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=f'{log_dir}/database_setup.log')
logger = logging.getLogger(__name__)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# Get database connection parameters from .env
DB_NAME = os.getenv("DB_NAME")
DB_PARAMS = {
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT")
}

def create_database_and_tables():
    """Creates the database and tables needed for the EPD data."""

    try:
        # Connect to the PostgreSQL server (without specifying the database)
        conn = psycopg2.connect(**DB_PARAMS)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) #important for database creation

        cur = conn.cursor()

        # Check if the database already exists
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}'")
        exists = cur.fetchone()

        if not exists:
            # Create the database if it doesn't exist
            cur.execute(f"CREATE DATABASE {DB_NAME}")
            logger.info(f"Database '{DB_NAME}' created successfully.")
        else:
            logger.info(f"Database '{DB_NAME}' already exists.")

        # Close the initial connection
        cur.close()
        conn.close()

        # Connect to the newly created database
        conn = psycopg2.connect(dbname=DB_NAME, **DB_PARAMS)
        cursor = conn.cursor()

        # Create DataStocks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS DataStocks (
                datastock_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                uuid TEXT UNIQUE
            )
        ''')
        logger.info("DataStocks table created successfully.")

        # Create Products table with datastock reference
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Products (
                process_id TEXT PRIMARY KEY,
                uuid TEXT,
                version TEXT,
                name_en TEXT,
                name_en_AI TEXT,
                name_de TEXT,
                category_level_1 TEXT,
                category_level_2 TEXT,
                category_level_3 TEXT,
                description_en TEXT,
                description_en_AI TEXT,
                description_de TEXT,
                short_desc_en_AI TEXT,
                reference_year TEXT,
                valid_until TEXT,
                time_repr_en TEXT,
                time_repr_de TEXT,
                safety_margin TEXT,
                safety_descr_en TEXT,
                safety_descr_de TEXT,
                geo_location TEXT,
                geo_descr_en TEXT,
                geo_descr_de TEXT,
                tech_descr_en TEXT,
                tech_descr_en_AI TEXT,
                tech_descr_de TEXT,
                tech_applic_en TEXT,
                tech_applic_en_AI TEXT,
                tech_applic_de TEXT,
                dataset_type TEXT,
                dataset_subtype TEXT,
                sources TEXT,
                use_advice_en TEXT,
                use_advice_de TEXT,
                generator_en TEXT,
                generator_de TEXT,
                entry_by_en TEXT,
                entry_by_de TEXT,
                admin_version TEXT,
                license_type TEXT,
                access_en TEXT,
                access_de TEXT,
                timestamp TIMESTAMP,
                formats TEXT,
                original_epd_url TEXT,
                datastock_id INTEGER,
                FOREIGN KEY (datastock_id) REFERENCES DataStocks (datastock_id)
            )
        ''')
        logger.info("Products table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Compliances (
                compliance_id SERIAL PRIMARY KEY,
                process_id TEXT,
                system_en TEXT,
                system_de TEXT,
                approval TEXT,
                FOREIGN KEY (process_id) REFERENCES Products (process_id) ON DELETE CASCADE
            )
        ''')
        logger.info("Compliances table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Classifications (
                process_id TEXT,
                name TEXT,
                level TEXT,
                classId TEXT,
                classification TEXT,
                FOREIGN KEY (process_id) REFERENCES Products (process_id) ON DELETE CASCADE
            )
        ''')    
        logger.info("Classifications table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Exchanges (
                exchange_id SERIAL PRIMARY KEY,
                process_id TEXT,
                flow_en TEXT,
                flow_en_AI TEXT,
                flow_de TEXT,
                indicator_key TEXT,
                direction TEXT,
                meanAmount REAL,
                unit TEXT,
                FOREIGN KEY (process_id) REFERENCES Products (process_id) ON DELETE CASCADE
            )
        ''')
        logger.info("Exchanges table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Exchange_ModuleAmounts (
                module_amount_id SERIAL PRIMARY KEY,
                exchange_id INTEGER,
                module TEXT,
                scenario TEXT,
                amount REAL,
                FOREIGN KEY (exchange_id) REFERENCES Exchanges (exchange_id) ON DELETE CASCADE
            )
        ''')
        logger.info("Exchange_ModuleAmounts table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS LCIA_Results (
                lcia_id SERIAL PRIMARY KEY,
                process_id TEXT,
                method_en TEXT,
                method_en_AI TEXT,
                method_de TEXT,
                indicator_key TEXT,
                meanAmount REAL,
                unit TEXT,
                FOREIGN KEY (process_id) REFERENCES Products (process_id) ON DELETE CASCADE
            )
        ''')
        logger.info("LCIA_Results table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS LCIA_ModuleAmounts (
                lcia_module_amount_id SERIAL PRIMARY KEY,
                lcia_id INTEGER,
                module TEXT,
                scenario TEXT,
                amount REAL,
                FOREIGN KEY (lcia_id) REFERENCES LCIA_Results (lcia_id) ON DELETE CASCADE
            )
        ''')
        logger.info("LCIA_ModuleAmounts table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Reviews (
                review_id SERIAL PRIMARY KEY,
                process_id TEXT,
                reviewer TEXT,
                detail_en TEXT,
                detail_de TEXT,
                FOREIGN KEY (process_id) REFERENCES Products (process_id) ON DELETE CASCADE
            )
        ''')
        logger.info("Reviews table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Flow_Properties (
                flow_property_id SERIAL PRIMARY KEY,
                process_id TEXT,
                name_en TEXT,
                name_en_AI TEXT,
                name_de TEXT,
                meanAmount TEXT,
                unit TEXT,
                is_reference BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (process_id) REFERENCES Products (process_id) ON DELETE CASCADE
            )
        ''')
        logger.info("Flow_Properties table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Material_Properties (
                material_prop_id SERIAL PRIMARY KEY,
                process_id TEXT,
                property_id TEXT,
                property_name TEXT,
                value TEXT,
                units TEXT,
                description TEXT,
                FOREIGN KEY (process_id) REFERENCES Products (process_id) ON DELETE CASCADE
            )
        ''')
        logger.info("Material_Properties table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Materials (
                material_id SERIAL PRIMARY KEY,
                process_id TEXT,
                material TEXT,
                list_order TEXT,
                FOREIGN KEY (process_id) REFERENCES Products (process_id) ON DELETE CASCADE
            )
        ''')
        logger.info("Materials table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Uses (
                use_id SERIAL PRIMARY KEY,
                process_id TEXT,
                use_case TEXT,
                list_order TEXT,
                FOREIGN KEY (process_id) REFERENCES Products (process_id) ON DELETE CASCADE
            )
        ''')
        logger.info("Uses table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Indicators (
                indicator_key TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                short_description TEXT,
                long_description TEXT
            )
        ''')
        logger.info("Indicators table created successfully.")

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Modules (
                module_code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                short_description TEXT,
                long_description TEXT
            )
        ''')
        logger.info("Modules table created successfully.")

        conn.commit()
        logger.info("Tables created successfully.")

    except psycopg2.Error as e:
        error_msg = f"Error creating database or tables: {e}"
        logger.error(error_msg)
        print(f"ERROR: {error_msg}")  # Also print to console
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_database_and_tables()