import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json
from typing import Dict, List
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import time
load_dotenv()
import ssl

class CustomAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context(cert_reqs=None)
        context.check_hostname = False
        context.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
        context.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)

class EPADataProcessor:
    def __init__(self, snowflake_conn_params: Dict):
        self.conn_params = snowflake_conn_params
        self.base_url = "https://ordspub.epa.gov/ords/pesticides/cswu/ppls"
        # Set up session with custom adapter
        self.session = requests.Session()
        self.session.mount('https://', CustomAdapter())
        self.session.verify = False
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def get_snowflake_connection(self):
        return snowflake.connector.connect(
            user=self.conn_params['user'],
            password=self.conn_params['password'],
            account=self.conn_params['account'],
            warehouse=self.conn_params['warehouse'],
            database=self.conn_params['database'],
            schema=self.conn_params['schema']
        )

    def fetch_epa_data(self, epa_number: str) -> Dict:
        url = f"{self.base_url}/{epa_number}"
        try:
            logging.info(f"Fetching data for EPA number: {epa_number}")
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()['items'][0]
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching EPA number {epa_number}: {str(e)}")
            raise

    def create_tables(self):
        conn = self.get_snowflake_connection()
        cur = conn.cursor()

        # Main product table
        cur.execute("""
        CREATE OR REPLACE TABLE EPA_PRODUCTS (
            eparegno STRING PRIMARY KEY,
            productname STRING,
            registereddate STRING,
            cancel_flag STRING,
            cancellationreason STRING,
            product_status STRING,
            product_status_date STRING,
            signal_word STRING,
            rup_yn STRING,
            transfer_flag STRING
        )
        """)

        # Company info table
        cur.execute("""
        CREATE OR REPLACE TABLE EPA_COMPANY_INFO (
            eparegno STRING,
            name STRING,
            contact_person STRING,
            co_division_name STRING,
            phone STRING,
            fax STRING,
            email STRING,
            street STRING,
            po_box STRING,
            city STRING,
            state STRING,
            zip_code STRING,
            FOREIGN KEY (eparegno) REFERENCES EPA_PRODUCTS(eparegno)
        )
        """)

        # Similar CREATE TABLE statements for other nested structures
        # Active ingredients table
        cur.execute("""
        CREATE OR REPLACE TABLE EPA_ACTIVE_INGREDIENTS (
            eparegno STRING,
            pc_code STRING,
            active_ing STRING,
            cas_number STRING,
            active_ing_percent FLOAT,
            FOREIGN KEY (eparegno) REFERENCES EPA_PRODUCTS(eparegno)
        )
        """)

        # Sites table
        cur.execute("""
        CREATE OR REPLACE TABLE EPA_SITES (
            eparegno STRING,
            site STRING,
            FOREIGN KEY (eparegno) REFERENCES EPA_PRODUCTS(eparegno)
        )
        """)

        # Pests table
        cur.execute("""
        CREATE OR REPLACE TABLE EPA_PESTS (
            eparegno STRING,
            pest STRING,
            FOREIGN KEY (eparegno) REFERENCES EPA_PRODUCTS(eparegno)
        )
        """)


        # Types table
        cur.execute("""
        CREATE OR REPLACE TABLE EPA_TYPES (
            eparegno STRING,
            type STRING,
            FOREIGN KEY (eparegno) REFERENCES EPA_PRODUCTS(eparegno)
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE EPA_TRANSFER_HISTORY (
            eparegno STRING,
            previous_eparegno STRING,
            previous_company STRING,
            transferred_date STRING,
            FOREIGN KEY (eparegno) REFERENCES EPA_PRODUCTS(eparegno)
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE EPA_PDF_FILES (
            epa_reg_num STRING,
            pdffile STRING,
            pdffile_accepted_date STRING,
            FOREIGN KEY (epa_reg_num) REFERENCES EPA_PRODUCTS(eparegno)
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE EPA_ALT_BRAND_NAMES (
            eparegno STRING,
            altbrandname STRING,
            FOREIGN KEY (eparegno) REFERENCES EPA_PRODUCTS(eparegno)
        )
        """)

        cur.execute("""
        CREATE OR REPLACE TABLE EPA_FORMULATIONS (
            eparegno STRING,
            formulation STRING,
            FOREIGN KEY (eparegno) REFERENCES EPA_PRODUCTS(eparegno)
        )
        """)

        conn.close()

    def process_and_load_data(self, epa_numbers: List[str]):
        for epa_number in epa_numbers:
            try:
                # Fetch data from API
                logging.info(f"Processing EPA number: {epa_number}")
                data = self.fetch_epa_data(epa_number)
                
                # Process main product data
                product_df = pd.DataFrame([{
                    'eparegno': data['eparegno'],
                    'productname': data['productname'],
                    'registereddate': pd.to_datetime(data['registereddate']),
                    'cancel_flag': data['cancel_flag'],
                    'cancellationreason': data['cancellationreason'],
                    'product_status': data['product_status'],
                    'product_status_date': pd.to_datetime(data['product_status_date']),
                    'signal_word': data['signal_word'],
                    'rup_yn': data['rup_yn'],
                    'transfer_flag': data['transfer_flag']
                }])
                product_df.columns = product_df.columns.str.upper()

                # Initialize all DataFrames as None
                company_df = None
                active_ing_df = None
                sites_df = None
                pests_df = None
                formulations_df = None
                altbrands_df = None
                pdf_df = None
                types_df = None
                transfer_df = None

                # Process company info if exists
                if data.get('companyinfo'):
                    company_df = pd.DataFrame(data['companyinfo'])
                    company_df['eparegno'] = data['eparegno']
                    company_df.columns = company_df.columns.str.upper()

                # Process active ingredients if exists
                if data.get('active_ingredients'):
                    active_ing_df = pd.DataFrame(data['active_ingredients'])
                    active_ing_df['eparegno'] = data['eparegno']
                    active_ing_df.columns = active_ing_df.columns.str.upper()

                # Process sites if exists
                if data.get('sites'):
                    sites_df = pd.DataFrame(data['sites'])
                    sites_df['eparegno'] = data['eparegno']
                    sites_df.columns = sites_df.columns.str.upper()

                # Process pests if exists
                if data.get('pests'):
                    pests_df = pd.DataFrame(data['pests'])
                    pests_df['eparegno'] = data['eparegno']
                    pests_df.columns = pests_df.columns.str.upper()

                # Process formulations if exists
                if data.get('formulations'):
                    formulations_df = pd.DataFrame(data['formulations'])
                    formulations_df['eparegno'] = data['eparegno']
                    formulations_df.columns = formulations_df.columns.str.upper()

                # Process alternative brand names if exists
                if data.get('altbrandnames'):
                    altbrands_df = pd.DataFrame(data['altbrandnames'])
                    altbrands_df['eparegno'] = data['eparegno']
                    altbrands_df.columns = altbrands_df.columns.str.upper()

                # Process PDF files if exists
                if data.get('pdffiles'):
                    pdf_df = pd.DataFrame(data['pdffiles'])
                    #pdf_df['eparegno'] = data['eparegno']
                    pdf_df['pdffile_accepted_date'] = pd.to_datetime(pdf_df['pdffile_accepted_date'])
                    pdf_df.columns = pdf_df.columns.str.upper()

                # Process types if exists
                if data.get('types'):
                    types_df = pd.DataFrame(data['types'])
                    types_df['eparegno'] = data['eparegno']
                    types_df.columns = types_df.columns.str.upper()

                # Process transfer history if exists
                if data.get('transfer_history'):
                    transfer_df = pd.DataFrame(data['transfer_history'])
                    transfer_df['eparegno'] = data['eparegno']
                    if 'transfer_date' in transfer_df.columns:
                        transfer_df['transfer_date'] = pd.to_datetime(transfer_df['transfer_date'])
                    transfer_df.columns = transfer_df.columns.str.upper()

                # Load data to Snowflake
                conn = self.get_snowflake_connection()
                
                # Write each DataFrame if it exists
                write_pandas(conn, product_df, 'EPA_PRODUCTS')
                
                if company_df is not None:
                    write_pandas(conn, company_df, 'EPA_COMPANY_INFO')
                if active_ing_df is not None:
                    write_pandas(conn, active_ing_df, 'EPA_ACTIVE_INGREDIENTS')
                if sites_df is not None:
                    write_pandas(conn, sites_df, 'EPA_SITES')
                if pests_df is not None:
                    write_pandas(conn, pests_df, 'EPA_PESTS')
                if types_df is not None:
                    write_pandas(conn, types_df, 'EPA_TYPES')
                if pdf_df is not None:
                    write_pandas(conn, pdf_df, 'EPA_PDF_FILES')
                if altbrands_df is not None:
                    write_pandas(conn, altbrands_df, 'EPA_ALT_BRAND_NAMES')
                if formulations_df is not None:
                    write_pandas(conn, formulations_df, 'EPA_FORMULATIONS')
                if transfer_df is not None:
                    write_pandas(conn, transfer_df, 'EPA_TRANSFER_HISTORY')

                conn.close()
                logging.info(f"Successfully processed EPA number: {epa_number}")
                
                # Add delay between requests
                time.sleep(5)

            except Exception as e:
                print(f"Error processing EPA number {epa_number}: {str(e)}")

# Usage example
if __name__ == "__main__":
    # Snowflake connection parameters
    snowflake_params = {
        'user': os.getenv('user'),
        'password': os.getenv('password'),
        'account': os.getenv('account'),
        'warehouse': 'COMPUTE_WH',
        'database': 'DEV_SRC_INGEST',
        'schema': 'EPA_RAW'
    }

    # Read EPA numbers from CSV
    epa_list = pd.read_csv('EPA_LIST.csv')
    epa_numbers = epa_list['EPA'].tolist()

    # Initialize processor and create tables
    processor = EPADataProcessor(snowflake_params)
    processor.create_tables()

    # Process and load data
    processor.process_and_load_data(epa_numbers)