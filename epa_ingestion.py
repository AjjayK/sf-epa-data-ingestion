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
import tempfile
import re
from pathlib import Path

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
    
    def get_focus_products(self):
        conn = self.get_snowflake_connection()
        query = """
            SELECT * FROM DEV_DP_APP.MODELED.FOCUS_PRODUCTS
        """

        try:
            df = pd.read_sql(query, conn)
            print(f"Retrieved {len(df)} focus products")
            return df
        except Exception as e:
            print(f"Error fetching data from Snowflake: {str(e)}")
            raise

        conn.close()


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
            eparegno STRING,
            epa_reg_num STRING,
            pdffile STRING,
            pdffile_accepted_date STRING,
            FOREIGN KEY (eparegno) REFERENCES EPA_PRODUCTS(eparegno)
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

        # Metadata table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS EPA_PDF_INGESTION_METADATA (
            id INTEGER AUTOINCREMENT,
            product_name VARCHAR,
            stage_file_path VARCHAR,
            original_url VARCHAR,
            file_size_bytes INTEGER,
            upload_timestamp TIMESTAMP_NTZ,
            processing_status VARCHAR DEFAULT 'PENDING',
            EPAREGNO VARCHAR,
            PDF_FILE_NAME VARCHAR,
            PDFFILE_ACCEPTED_DATE VARCHAR,
            PDFFILE VARCHAR
        )
        """)

        # Chunk table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS EPA_PDF_INGESTION_CHUNK (
                    RELATIVE_PATH VARCHAR(16777216), 
                    SIZE NUMBER(38,0), 
                    FILE_URL VARCHAR(16777216),
                    SCOPED_FILE_URL VARCHAR(16777216), 
                    CHUNK VARCHAR(16777216), 
                    CATEGORY VARCHAR(16777216)
                    )
        """)
        conn.close()

    def process_and_load_data(self, epa_numbers: List[str]):
        # For development check
        #epa_numbers = epa_numbers[:1]

        for epa_number in epa_numbers:
            try:
                # Fetch data from API
                logging.info(f"Processing EPA number: {epa_number}")
                data = self.fetch_epa_data(epa_number)
                
                # Process main product data
                product_df = pd.DataFrame([{
                    'eparegno': data['eparegno'],
                    'productname': data['productname'],
                    'registereddate': data['registereddate'],
                    'cancel_flag': data['cancel_flag'],
                    'cancellationreason': data['cancellationreason'],
                    'product_status': data['product_status'],
                    'product_status_date': data['product_status_date'],
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
                    pdf_df['eparegno'] = data['eparegno']
                    pdf_df['pdffile_accepted_date'] = pdf_df['pdffile_accepted_date']
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
                        transfer_df['transfer_date'] = transfer_df['transfer_date']
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
                logging.error(f"Error processing EPA number {epa_number}: {str(e)}")
    def pdf_to_download(self):
        """Fetch pdf files data to download from Snowflake using the specified query"""
        conn = self.get_snowflake_connection()
        query = """
            SELECT *
            FROM DEV_SRC_INGEST.EPA_RAW.VW_PDF_TO_DOWNLOAD
        """

        try:
            df = pd.read_sql(query, conn)
            print(f"Retrieved {len(df)} records from Snowflake")
            return df
        except Exception as e:
            print(f"Error fetching data from Snowflake: {str(e)}")
            raise

    def pdf_to_chunk(self):
        """Fetch pdf files data to download from Snowflake using the specified query"""
        conn = self.get_snowflake_connection()
        query = """
            SELECT *
            FROM DEV_SRC_INGEST.EPA_RAW.VW_PDF_TO_CHUNK
        """

        try:
            df = pd.read_sql(query, conn)
            print(f"Retrieved {len(df)} records from Snowflake")
            return df
        except Exception as e:
            print(f"Error fetching data from Snowflake: {str(e)}")
            raise
    def clean_filename(self, filename):
        """Clean filename to remove invalid characters and spaces"""
        # Remove invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        # Replace spaces with underscores
        filename = filename.replace(' ', '_')
        # Limit length
        if len(filename) > 200:
            filename = filename[:200]
        return filename
    
    def upload_to_snowflake_stage(self, local_file_path, stage_path, file_name):
        """Upload file to Snowflake stage"""
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        try:
            # Remove '@' from the beginning of stage path if present
            clean_stage_path = stage_path.lstrip('@')
            snowflake_path = local_file_path.replace('\\', '\\\\')
            put_command = f"PUT 'file://{snowflake_path}' '@{clean_stage_path}/' AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cursor.execute(put_command)
            return True
        except Exception as e:
            print(f"Error uploading to stage: {str(e)}")
            return False
        finally:
            cursor.close()

    def store_pdf_metadata_snowflake(self, metadata_records):
        """Store PDF metadata in Snowflake"""
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        metadata_df = pd.DataFrame(metadata_records)
        metadata_df.columns = metadata_df.columns.str.upper()
        temp_table_name = 'TEMP_EPA_PDF_INGESTION_METADATA'
        table_name = 'EPA_PDF_INGESTION_METADATA'
        # Create table if it doesn't exist
        cursor = conn.cursor()
        try:
            # Write to temporary table

                    # Metadata table
            cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS TEMP_EPA_PDF_INGESTION_METADATA (
                id INTEGER AUTOINCREMENT,
                product_name VARCHAR,
                stage_file_path VARCHAR,
                original_url VARCHAR,
                file_size_bytes INTEGER,
                upload_timestamp TIMESTAMP_NTZ,
                processing_status VARCHAR DEFAULT 'PENDING',
                EPAREGNO VARCHAR,
                PDF_FILE_NAME VARCHAR,
                PDFFILE_ACCEPTED_DATE VARCHAR,
                PDFFILE VARCHAR
            )
            """)

            success = write_pandas(conn, metadata_df, temp_table_name)
            
            if success:
                # Perform MERGE operation
                merge_sql = f"""
                MERGE INTO {table_name} t
                USING {temp_table_name} s
                ON t.EPAREGNO = s.EPAREGNO
                WHEN MATCHED THEN UPDATE SET
                    t.PRODUCT_NAME = s.PRODUCT_NAME,
                    t.STAGE_FILE_PATH = s.STAGE_FILE_PATH,
                    t.ORIGINAL_URL = s.ORIGINAL_URL,
                    t.FILE_SIZE_BYTES = s.FILE_SIZE_BYTES,
                    t.UPLOAD_TIMESTAMP = s.UPLOAD_TIMESTAMP,
                    t.PROCESSING_STATUS = s.PROCESSING_STATUS,
                    t.PDF_FILE_NAME = s.PDF_FILE_NAME,
                    t.PDFFILE_ACCEPTED_DATE = s.PDFFILE_ACCEPTED_DATE,
                    t.PDFFILE = s.PDFFILE
                WHEN NOT MATCHED THEN INSERT (
                    PRODUCT_NAME,
                    STAGE_FILE_PATH,
                    ORIGINAL_URL,
                    FILE_SIZE_BYTES,
                    UPLOAD_TIMESTAMP,
                    PROCESSING_STATUS,
                    EPAREGNO,
                    PDF_FILE_NAME,
                    PDFFILE_ACCEPTED_DATE,
                    PDFFILE
                ) VALUES (
                    s.PRODUCT_NAME,
                    s.STAGE_FILE_PATH,
                    s.ORIGINAL_URL,
                    s.FILE_SIZE_BYTES,
                    s.UPLOAD_TIMESTAMP,
                    s.PROCESSING_STATUS,
                    s.EPAREGNO,
                    s.PDF_FILE_NAME,
                    s.PDFFILE_ACCEPTED_DATE,
                    s.PDFFILE
                )
                """
                cursor.execute(merge_sql)
                
                # Clean up temporary table
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                
                conn.commit()
                success = True
        except Exception as e:
            print(f"Error writing to Snowflake: {str(e)}")
            conn.rollback()
            success = False
        finally:
            cursor.close()
            
        return success

    def download_and_store_pdfs(self, df, stage_path):
        """
        Download PDFs and store them in a file system for Document AI processing
        
        Parameters:
        df (pd.DataFrame): DataFrame containing 'PDFFILE' and 'PRODUCTNAME' columns
        storage_path (str): Base path to store PDFs
        snowflake_credentials (dict): Snowflake connection credentials
        """
        
        # Connect to Snowflake
        conn = self.get_snowflake_connection()


        try:
            # Fetch data from Snowflake
        
            base_url = "https://www3.epa.gov/pesticides/chem_search/ppls/"
            results = {'success': [], 'failed': []}
            metadata_records = []
            
            # Create temporary directory for intermediate storage
            with tempfile.TemporaryDirectory() as temp_dir:
                for index, row in df.iterrows():
                    try:
                        pdf_file = row['PDFFILE']
                        product_name = row['PRODUCTNAME']
                        epa_number = row['EPAREGNO']
                        url = f"{base_url}{pdf_file}"
                        unclean_filename = f"{product_name}_{epa_number}"
                        
                        # Create clean filename
                        clean_name = self.clean_filename(unclean_filename)
                        filename = f"{clean_name}.pdf"
                        temp_file_path = os.path.join(temp_dir, filename)
                        
                        # Download PDF
                        response = requests.get(url)
                        response.raise_for_status()
                        
                        # Save to temporary file
                        with open(temp_file_path, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                        
                        # Upload to Snowflake stage
                        if self.upload_to_snowflake_stage(temp_file_path, stage_path, filename):
                            # Prepare metadata
                            metadata_records.append({
                                'product_name': product_name,
                                'stage_file_path': f"{stage_path}/{filename}",
                                'original_url': url,
                                'file_size_bytes': len(response.content),
                                'upload_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'processing_status': 'PENDING',
                                'EPAREGNO': epa_number,
                                'PDF_FILE_NAME': filename,
                                'PDFFILE_ACCEPTED_DATE': row['PDFFILE_ACCEPTED_DATE'],
                                'PDFFILE': row['PDFFILE']
                            })
                            
                            results['success'].append({
                                'product_name': product_name,
                                'stage_file_path': f"{stage_path}/{filename}"
                            })
                            
                            print(f"Successfully uploaded: {filename}")
                        else:
                            raise Exception("Failed to upload to Snowflake stage")
                        
                        # Add delay to be respectful to the server
                        time.sleep(1)
                        
                    except Exception as e:
                        print(f"Error processing {product_name}: {str(e)}")
                        results['failed'].append({
                            'product_name': product_name,
                            'error': str(e)
                        })
                
                # Store metadata in Snowflake
                if metadata_records:
                    store_success = self.store_pdf_metadata_snowflake(metadata_records)

                    if not store_success:
                        print("Warning: Failed to store metadata in Snowflake")
            
        finally:
            conn.close()
        
        return results
    
    def process_pdf_chunks(self):
        """
        Process PDF chunks by:
        1. Deleting existing chunks for PDFs that need to be rechunked
        2. Inserting new chunks using the text_chunker function
        3. Updating the processing status to 'CHUNKED' for processed PDFs
        """
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        
        try:
            delete_query = """
            DELETE FROM DEV_SRC_INGEST.EPA_PROCESSED.DOCS_CHUNKS_TABLE AS T1
            USING DEV_SRC_INGEST.EPA_RAW.VW_PDF_TO_CHUNK AS T2
            WHERE T1.RELATIVE_PATH = T2.RELATIVE_PATH
            """
            cursor.execute(delete_query)
            
            insert_query = """
            INSERT INTO DEV_SRC_INGEST.EPA_PROCESSED.DOCS_CHUNKS_TABLE (relative_path, size, file_url, scoped_file_url, chunk)
            WITH pdf_to_chunk AS (
                SELECT 
                    new_docs.relative_path, 
                    new_docs.size, 
                    new_docs.file_url, 
                    build_scoped_file_url(@DEV_SRC_INGEST.EPA_RAW.PDF_STORE, new_docs.relative_path) AS scoped_file_url
                FROM DEV_SRC_INGEST.EPA_RAW.VW_PDF_TO_CHUNK as new_docs
            )
            SELECT *
            FROM pdf_to_chunk,
            TABLE(DEV_SRC_INGEST.EPA_PROCESSED.TEXT_CHUNKER(TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@DEV_SRC_INGEST.EPA_RAW.PDF_STORE, 
                                        relative_path, {'mode': 'LAYOUT'}))))
            """
            cursor.execute(insert_query)
            

            update_query = """
            UPDATE EPA_PDF_INGESTION_METADATA m
            SET PROCESSING_STATUS = 'CHUNKED'
            FROM DEV_SRC_INGEST.EPA_RAW.VW_PDF_TO_CHUNK c
            WHERE m.STAGE_FILE_PATH LIKE '%' || c.RELATIVE_PATH
            """
            cursor.execute(update_query)
            
            # Commit all changes
            conn.commit()
            logging.info("Successfully processed PDF chunks")
            return True
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Error processing PDF chunks: {str(e)}")
            return False
            
        finally:
            cursor.close()
            conn.close()
    
    
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


    # PDF store path and metadata table name
    stage_path = '@DEV_SRC_INGEST.EPA_RAW.PDF_STORE/EPA_LABEL_PDF'

    # Initialize processor and create tables
    processor = EPADataProcessor(snowflake_params)
    
    epa_df = processor.get_focus_products()
    epa_list = epa_numbers['EPAREGNO'].tolist()
    processor.create_tables()

    # Process and load data
    if len(epa_list) > 0:
        processor.process_and_load_data(epa_numbers)
    else:
        logging.info("No data to process")

    # Download and store PDFs
    pdf_to_download_df = processor.pdf_to_download()

    # For development check
    pdf_to_download_df = pdf_to_download_df[0:1]

    if len(pdf_to_download_df) > 0:
        logging.info("Downloading and storing PDFs...")
        processor.download_and_store_pdfs(pdf_to_download_df, stage_path)
    else:
        logging.info("No PDFs to download")

    pdf_to_chunk_df = processor.pdf_to_chunk()

    if len(pdf_to_chunk_df) > 0:
        logging.info("Chunking PDFs...")
        processor.process_pdf_chunks()
    else:
        logging.info("No PDFs to chunk")

