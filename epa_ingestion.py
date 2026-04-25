import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from enum import Enum
import yaml
import sys
class CustomAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context(cert_reqs=None)
        context.check_hostname = False
        context.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
        context.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)
class Environment(Enum):
    DEV = "DEV"
    TEST = "TEST"
    PROD = "PROD"

class DatabaseConfig:
    def __init__(self, env: Environment, yaml_cfg: Dict = None):
        self.env = env
        self._yaml = yaml_cfg or {}

    def _get(self, *keys, default=None):
        node = self._yaml
        for k in keys:
            if not isinstance(node, dict) or k not in node:
                return default
            node = node[k]
        return node

    @property
    def database_prefix(self) -> str:
        return self.env.value

    def get_database(self, name: str) -> str:
        return f"{self.database_prefix}_{name}"

    @property
    def config(self) -> Dict:
        src_db      = self.get_database(self._get('databases', 'src_ingest_suffix', default='SRC_INGEST'))
        dp_db       = self.get_database(self._get('databases', 'dp_app_suffix',     default='DP_APP'))
        raw_schema  = self._get('schemas', 'raw',       default='EPA_RAW')
        proc_schema = self._get('schemas', 'processed', default='EPA_PROCESSED')
        mod_schema  = self._get('schemas', 'modeled',   default='MODELED')
        stage_name   = self._get('stage', 'name',      default='PDF_STORE')
        stage_folder = self._get('stage', 'subfolder', default='EPA_LABEL_PDF')
        return {
            # existing keys (names unchanged for backward compatibility)
            'SRC_INGEST_DB':  src_db,
            'DP_APP_DB':      dp_db,
            'PDF_STORE_PATH': f"@{src_db}.{raw_schema}.{stage_name}/{stage_folder}",
            # schemas
            'RAW_SCHEMA':  raw_schema,
            'PROC_SCHEMA': proc_schema,
            'MOD_SCHEMA':  mod_schema,
            # warehouse
            'WAREHOUSE': self._get('snowflake', 'warehouse', default='COMPUTE_WH'),
            # stage
            'STAGE_NAME':   stage_name,
            'STAGE_FOLDER': stage_folder,
            # raw schema tables
            'TBL_PRODUCTS':      self._get('tables', 'products',            default='EPA_PRODUCTS'),
            'TBL_COMPANY_INFO':  self._get('tables', 'company_info',        default='EPA_COMPANY_INFO'),
            'TBL_ACTIVE_ING':    self._get('tables', 'active_ingredients',  default='EPA_ACTIVE_INGREDIENTS'),
            'TBL_SITES':         self._get('tables', 'sites',               default='EPA_SITES'),
            'TBL_PESTS':         self._get('tables', 'pests',               default='EPA_PESTS'),
            'TBL_TYPES':         self._get('tables', 'types',               default='EPA_TYPES'),
            'TBL_TRANSFER_HIST': self._get('tables', 'transfer_history',    default='EPA_TRANSFER_HISTORY'),
            'TBL_PDF_FILES':     self._get('tables', 'pdf_files',           default='EPA_PDF_FILES'),
            'TBL_ALT_BRANDS':    self._get('tables', 'alt_brand_names',     default='EPA_ALT_BRAND_NAMES'),
            'TBL_FORMULATIONS':  self._get('tables', 'formulations',        default='EPA_FORMULATIONS'),
            'TBL_PDF_META':      self._get('tables', 'pdf_ingestion_meta',  default='EPA_PDF_INGESTION_METADATA'),
            'TBL_TEMP_PDF_META': self._get('tables', 'temp_pdf_meta',       default='TEMP_EPA_PDF_INGESTION_METADATA'),
            # processed schema tables
            'TBL_DOCS_CHUNKS':    self._get('tables', 'docs_chunks',    default='DOCS_CHUNKS_TABLE'),
            'TBL_FOCUS_PRODUCTS': self._get('tables', 'focus_products', default='FOCUS_PRODUCTS'),
            # views
            'VW_PDF_DOWNLOAD': self._get('views', 'pdf_to_download', default='VW_PDF_TO_DOWNLOAD'),
            'VW_PDF_CHUNK':    self._get('views', 'pdf_to_chunk',    default='VW_PDF_TO_CHUNK'),
            # UDFs
            'UDF_TEXT_CHUNKER': self._get('udfs', 'text_chunker', default='text_chunker'),
            # Cortex search
            'CORTEX_SVC_PREFIX': self._get('cortex_search', 'service_prefix', default='CC_SEARCH_SERVICE_CS'),
        }



class EPADataProcessor:
    def __init__(self, snowflake_conn_params: Dict, env: str = "DEV", yaml_cfg: Dict = None):
        self.env = Environment(env.upper())
        self.db_config = DatabaseConfig(self.env, yaml_cfg)
        self.conn_params = {
            **snowflake_conn_params,
            'database': self.db_config.config['SRC_INGEST_DB'],
            'schema': self.db_config.config['RAW_SCHEMA']
        }
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
        cfg = self.db_config.config
        query = f"""
            SELECT * FROM {cfg['DP_APP_DB']}.{cfg['MOD_SCHEMA']}.{cfg['TBL_FOCUS_PRODUCTS']}
        """

        try:
            df = pd.read_sql(query, conn)
            logging.info(f"Retrieved {len(df)} focus products")
            return df
        except Exception as e:
            logging.error(f"Error fetching data from Snowflake: {str(e)}")
            raise
        finally:
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
        cfg = self.db_config.config

        # Main product table
        cur.execute(f"""
        CREATE OR REPLACE TABLE {cfg['TBL_PRODUCTS']} (
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
        cur.execute(f"""
        CREATE OR REPLACE TABLE {cfg['TBL_COMPANY_INFO']} (
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
            FOREIGN KEY (eparegno) REFERENCES {cfg['TBL_PRODUCTS']}(eparegno)
        )
        """)

        # Similar CREATE TABLE statements for other nested structures
        # Active ingredients table
        cur.execute(f"""
        CREATE OR REPLACE TABLE {cfg['TBL_ACTIVE_ING']} (
            eparegno STRING,
            pc_code STRING,
            active_ing STRING,
            cas_number STRING,
            active_ing_percent FLOAT,
            FOREIGN KEY (eparegno) REFERENCES {cfg['TBL_PRODUCTS']}(eparegno)
        )
        """)

        # Sites table
        cur.execute(f"""
        CREATE OR REPLACE TABLE {cfg['TBL_SITES']} (
            eparegno STRING,
            site STRING,
            FOREIGN KEY (eparegno) REFERENCES {cfg['TBL_PRODUCTS']}(eparegno)
        )
        """)

        # Pests table
        cur.execute(f"""
        CREATE OR REPLACE TABLE {cfg['TBL_PESTS']} (
            eparegno STRING,
            pest STRING,
            FOREIGN KEY (eparegno) REFERENCES {cfg['TBL_PRODUCTS']}(eparegno)
        )
        """)


        # Types table
        cur.execute(f"""
        CREATE OR REPLACE TABLE {cfg['TBL_TYPES']} (
            eparegno STRING,
            type STRING,
            FOREIGN KEY (eparegno) REFERENCES {cfg['TBL_PRODUCTS']}(eparegno)
        )
        """)

        cur.execute(f"""
        CREATE OR REPLACE TABLE {cfg['TBL_TRANSFER_HIST']} (
            eparegno STRING,
            previous_eparegno STRING,
            previous_company STRING,
            transferred_date STRING,
            FOREIGN KEY (eparegno) REFERENCES {cfg['TBL_PRODUCTS']}(eparegno)
        )
        """)

        cur.execute(f"""
        CREATE OR REPLACE TABLE {cfg['TBL_PDF_FILES']} (
            eparegno STRING,
            epa_reg_num STRING,
            pdffile STRING,
            pdffile_accepted_date STRING,
            FOREIGN KEY (eparegno) REFERENCES {cfg['TBL_PRODUCTS']}(eparegno)
        )
        """)

        cur.execute(f"""
        CREATE OR REPLACE TABLE {cfg['TBL_ALT_BRANDS']} (
            eparegno STRING,
            altbrandname STRING,
            FOREIGN KEY (eparegno) REFERENCES {cfg['TBL_PRODUCTS']}(eparegno)
        )
        """)

        cur.execute(f"""
        CREATE OR REPLACE TABLE {cfg['TBL_FORMULATIONS']} (
            eparegno STRING,
            formulation STRING,
            FOREIGN KEY (eparegno) REFERENCES {cfg['TBL_PRODUCTS']}(eparegno)
        )
        """)

        # Metadata table
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {cfg['TBL_PDF_META']} (
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
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {cfg['PROC_SCHEMA']}.{cfg['TBL_DOCS_CHUNKS']} (
                    RELATIVE_PATH VARCHAR(16777216),
                    SIZE NUMBER(38,0),
                    FILE_URL VARCHAR(16777216),
                    SCOPED_FILE_URL VARCHAR(16777216),
                    CHUNK VARCHAR(16777216),
                    CATEGORY VARCHAR(16777216)
                    )
        """)

        # chunk function

        cur.execute(f"""
create or replace function {cfg['PROC_SCHEMA']}.{cfg['UDF_TEXT_CHUNKER']}(pdf_text string)
returns table (chunk varchar)
language python
runtime_version = '3.9'
handler = 'text_chunker'
packages = ('snowflake-snowpark-python', 'langchain')
as
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class text_chunker:

    def process(self, pdf_text: str):

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1512, #Adjust this as you see fit
            chunk_overlap  = 256, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len
        )

        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])

        yield from df.itertuples(index=False, name=None)
$$
        """
        )

        # PDF STAGE
        cur.execute(f"""
        create or replace stage {cfg['RAW_SCHEMA']}.{cfg['STAGE_NAME']} DIRECTORY = (
        ENABLE = TRUE)
        """)
        cur.close()
        conn.close()

    def _fetch_and_transform_product(self, epa_number: str) -> Dict:
        """Fetch and transform one product into a dict of DataFrames. Called in parallel."""
        logging.info(f"Processing EPA number: {epa_number}")
        data = self.fetch_epa_data(epa_number)
        cfg = self.db_config.config

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

        result = {cfg['TBL_PRODUCTS']: product_df}

        if data.get('companyinfo'):
            df = pd.DataFrame(data['companyinfo'])
            df['eparegno'] = data['eparegno']
            result[cfg['TBL_COMPANY_INFO']] = df.rename(columns=str.upper)

        if data.get('active_ingredients'):
            df = pd.DataFrame(data['active_ingredients'])
            df['eparegno'] = data['eparegno']
            result[cfg['TBL_ACTIVE_ING']] = df.rename(columns=str.upper)

        if data.get('sites'):
            df = pd.DataFrame(data['sites'])
            df['eparegno'] = data['eparegno']
            result[cfg['TBL_SITES']] = df.rename(columns=str.upper)

        if data.get('pests'):
            df = pd.DataFrame(data['pests'])
            df['eparegno'] = data['eparegno']
            result[cfg['TBL_PESTS']] = df.rename(columns=str.upper)

        if data.get('formulations'):
            df = pd.DataFrame(data['formulations'])
            df['eparegno'] = data['eparegno']
            result[cfg['TBL_FORMULATIONS']] = df.rename(columns=str.upper)

        if data.get('altbrandnames'):
            df = pd.DataFrame(data['altbrandnames'])
            df['eparegno'] = data['eparegno']
            result[cfg['TBL_ALT_BRANDS']] = df.rename(columns=str.upper)

        if data.get('pdffiles'):
            df = pd.DataFrame(data['pdffiles'])
            df['eparegno'] = data['eparegno']
            result[cfg['TBL_PDF_FILES']] = df.rename(columns=str.upper)

        if data.get('types'):
            df = pd.DataFrame(data['types'])
            df['eparegno'] = data['eparegno']
            result[cfg['TBL_TYPES']] = df.rename(columns=str.upper)

        if data.get('transfer_history'):
            df = pd.DataFrame(data['transfer_history'])
            df['eparegno'] = data['eparegno']
            result[cfg['TBL_TRANSFER_HIST']] = df.rename(columns=str.upper)

        time.sleep(1)
        return result

    def process_and_load_data(self, epa_numbers: List[str]):
        succeeded = []
        failed = []
        accumulated: Dict[str, List] = {}

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(self._fetch_and_transform_product, n): n for n in epa_numbers}
            for future in as_completed(futures):
                epa_number = futures[future]
                try:
                    dfs = future.result()
                    for table, df in dfs.items():
                        accumulated.setdefault(table, []).append(df)
                    succeeded.append(epa_number)
                    logging.info(f"Successfully processed EPA number: {epa_number}")
                except Exception as e:
                    logging.error(f"Error processing EPA number {epa_number}: {str(e)}")
                    failed.append({'eparegno': epa_number, 'error': str(e)})

        # Single connection, one write_pandas per table
        if accumulated:
            conn = self.get_snowflake_connection()
            try:
                for table, df_list in accumulated.items():
                    combined = pd.concat(df_list, ignore_index=True)
                    write_pandas(conn, combined, table)
            finally:
                conn.close()

        logging.info(
            f"process_and_load_data summary: {len(succeeded)} succeeded, "
            f"{len(failed)} failed out of {len(epa_numbers)} total"
        )
        if failed:
            logging.warning(f"Failed EPA numbers: {failed}")
        return {'succeeded': succeeded, 'failed': failed}

    def pdf_to_download(self):
        """Fetch pdf files data to download from Snowflake using the specified query"""
        conn = self.get_snowflake_connection()
        cfg = self.db_config.config

        create_view = f"""
            create or replace view {cfg['SRC_INGEST_DB']}.{cfg['RAW_SCHEMA']}.{cfg['VW_PDF_DOWNLOAD']}(
            EPAREGNO,
            PRODUCTNAME,
            REGISTEREDDATE,
            CANCEL_FLAG,
            CANCELLATIONREASON,
            PRODUCT_STATUS,
            PRODUCT_STATUS_DATE,
            SIGNAL_WORD,
            RUP_YN,
            TRANSFER_FLAG,
            PDFFILE,
            PDFFILE_ACCEPTED_DATE
        ) as
        with updated as (
            select prd.*, PDF.PDFFILE, PDF.PDFFILE_ACCEPTED_DATE
            from {cfg['SRC_INGEST_DB']}.{cfg['RAW_SCHEMA']}.{cfg['TBL_PRODUCTS']} as prd
            join {cfg['SRC_INGEST_DB']}.{cfg['RAW_SCHEMA']}.{cfg['TBL_PDF_FILES']} as pdf
              on prd.eparegno = pdf.eparegno
            qualify row_number() over (
                partition by PRD.eparegno
                order by pdffile_accepted_date desc nulls last
            ) = 1
        )
        select UPDATED.EPAREGNO,
               UPDATED.PRODUCTNAME,
               UPDATED.REGISTEREDDATE,
               UPDATED.CANCEL_FLAG,
               UPDATED.CANCELLATIONREASON,
               UPDATED.PRODUCT_STATUS,
               UPDATED.PRODUCT_STATUS_DATE,
               UPDATED.SIGNAL_WORD,
               UPDATED.RUP_YN,
               UPDATED.TRANSFER_FLAG,
               UPDATED.PDFFILE,
               UPDATED.PDFFILE_ACCEPTED_DATE
        from UPDATED
        left join {cfg['SRC_INGEST_DB']}.{cfg['RAW_SCHEMA']}.{cfg['TBL_PDF_META']} as M
          on M.EPAREGNO = UPDATED.EPAREGNO
        where M.EPAREGNO is null
           or M.PDFFILE              is distinct from UPDATED.PDFFILE
           or M.PDFFILE_ACCEPTED_DATE is distinct from UPDATED.PDFFILE_ACCEPTED_DATE
        """

        try:
            cursor = conn.cursor()
            try:
                cursor.execute(create_view)
            finally:
                cursor.close()

            query = f"""
                SELECT *
                FROM {cfg['SRC_INGEST_DB']}.{cfg['RAW_SCHEMA']}.{cfg['VW_PDF_DOWNLOAD']}
            """
            df = pd.read_sql(query, conn)
            logging.info(f"Retrieved {len(df)} records from Snowflake")
            return df
        except Exception as e:
            logging.error(f"Error in pdf_to_download: {str(e)}")
            raise
        finally:
            conn.close()

    def pdf_to_chunk(self):
        """Fetch pdf files data to download from Snowflake using the specified query"""
        conn = self.get_snowflake_connection()
        cfg = self.db_config.config

        create_view = f"""
        create or replace view {cfg['SRC_INGEST_DB']}.{cfg['RAW_SCHEMA']}.{cfg['VW_PDF_CHUNK']}(
            RELATIVE_PATH,
            SIZE,
            LAST_MODIFIED,
            MD5,
            ETAG,
            FILE_URL
        ) as
        select store.*
        from directory(@{cfg['SRC_INGEST_DB']}.{cfg['RAW_SCHEMA']}.{cfg['STAGE_NAME']}) as store
        inner join (
            select *, REGEXP_SUBSTR(stage_file_path, '{cfg['STAGE_FOLDER']}/.*$') as META_relative_path
            from {cfg['SRC_INGEST_DB']}.{cfg['RAW_SCHEMA']}.{cfg['TBL_PDF_META']}
            where processing_status = 'PENDING'
            ) as meta
            on meta.META_relative_path = store.relative_path
        """

        try:
            cursor = conn.cursor()
            try:
                cursor.execute(create_view)
            finally:
                cursor.close()

            query = f"""
                SELECT *
                FROM {cfg['SRC_INGEST_DB']}.{cfg['RAW_SCHEMA']}.{cfg['VW_PDF_CHUNK']}
            """
            df = pd.read_sql(query, conn)
            logging.info(f"Retrieved {len(df)} records from Snowflake")
            return df
        except Exception as e:
            logging.error(f"Error in pdf_to_chunk: {str(e)}")
            raise
        finally:
            conn.close()

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
            logging.error(f"Error uploading to stage: {str(e)}")
            return False
        finally:
            cursor.close()
            conn.close()

    def store_pdf_metadata_snowflake(self, metadata_records):
        """Store PDF metadata in Snowflake"""
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        cfg = self.db_config.config
        src_ingest_db = cfg['SRC_INGEST_DB']
        metadata_df = pd.DataFrame(metadata_records)
        metadata_df.columns = metadata_df.columns.str.upper()
        temp_table_name = cfg['TBL_TEMP_PDF_META']
        table_name = cfg['TBL_PDF_META']
        orphans = []
        try:
            cursor.execute(f"""
            CREATE TEMP TABLE IF NOT EXISTS {temp_table_name} (
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
                # Capture old stage paths for EPAREGNOs whose STAGE_FILE_PATH is changing,
                # so we can REMOVE the orphaned files from the stage after the MERGE commits.
                cursor.execute(f"""
                SELECT t.EPAREGNO,
                       REGEXP_SUBSTR(t.STAGE_FILE_PATH, '{cfg['STAGE_FOLDER']}/.*$') AS OLD_REL_PATH
                FROM {table_name} t
                JOIN {temp_table_name} s ON t.EPAREGNO = s.EPAREGNO
                WHERE t.STAGE_FILE_PATH IS DISTINCT FROM s.STAGE_FILE_PATH
                  AND t.STAGE_FILE_PATH IS NOT NULL
                """)
                orphans = cursor.fetchall()

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

                # After metadata commit, remove any orphaned stage files whose paths changed.
                # REMOVE on a missing path is a no-op; per-iteration try/except keeps a single
                # failure from aborting the rest. Metadata is already the source of truth.
                for epa, old_rel in orphans:
                    if not old_rel:
                        continue
                    try:
                        cursor.execute(
                            f"REMOVE @{src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['STAGE_NAME']}/{old_rel}"
                        )
                        logging.info(f"Removed orphaned stage file for {epa}: {old_rel}")
                    except Exception as e:
                        logging.warning(f"Failed to REMOVE orphan {old_rel} for {epa}: {e}")
        except Exception as e:
            logging.error(f"Error writing to Snowflake: {str(e)}")
            conn.rollback()
            success = False
        finally:
            cursor.close()
            conn.close()

        return success

    def _download_single_pdf(self, row, temp_dir: str, stage_path: str) -> Dict:
        """Download one PDF and upload to Snowflake stage. Called in parallel."""
        base_url = "https://www3.epa.gov/pesticides/chem_search/ppls/"
        pdf_file = row['PDFFILE']
        product_name = row['PRODUCTNAME']
        epa_number = row['EPAREGNO']
        url = f"{base_url}{pdf_file}"
        filename = f"{self.clean_filename(f'{product_name}_{epa_number}')}.pdf"
        temp_file_path = os.path.join(temp_dir, filename)

        response = requests.get(url)
        response.raise_for_status()

        with open(temp_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        if not self.upload_to_snowflake_stage(temp_file_path, stage_path, filename):
            raise Exception("Failed to upload to Snowflake stage")

        logging.info(f"Successfully uploaded: {filename}")
        time.sleep(1)

        return {
            'metadata': {
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
            },
            'success': {'product_name': product_name, 'stage_file_path': f"{stage_path}/{filename}"}
        }

    def download_and_store_pdfs(self, df, stage_path):
        results = {'success': [], 'failed': []}
        metadata_records = []

        with tempfile.TemporaryDirectory() as temp_dir:
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {
                    executor.submit(self._download_single_pdf, row, temp_dir, stage_path): row['PRODUCTNAME']
                    for _, row in df.iterrows()
                }
                for future in as_completed(futures):
                    product_name = futures[future]
                    try:
                        result = future.result()
                        metadata_records.append(result['metadata'])
                        results['success'].append(result['success'])
                    except Exception as e:
                        logging.error(f"Error processing {product_name}: {str(e)}")
                        results['failed'].append({'product_name': product_name, 'error': str(e)})

            if metadata_records:
                if not self.store_pdf_metadata_snowflake(metadata_records):
                    logging.warning("Failed to store metadata in Snowflake")

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
        cfg = self.db_config.config
        src_ingest_db = cfg['SRC_INGEST_DB']

        try:
            # Delete chunks for every EPAREGNO about to be re-chunked, regardless of the
            # old RELATIVE_PATH. Filename convention is {PRODUCTNAME}_{EPAREGNO}.pdf, so any
            # chunk whose path ends with _<EPAREGNO>.pdf belongs to that product. This catches
            # orphaned chunks left behind when a PDF was previously chunked under a different
            # filename. ESCAPE neutralizes SQL's '_' wildcard.
            delete_query = f"""
            DELETE FROM {src_ingest_db}.{cfg['PROC_SCHEMA']}.{cfg['TBL_DOCS_CHUNKS']} T1
            USING (
                SELECT DISTINCT M.EPAREGNO
                FROM {src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['TBL_PDF_META']} M
                JOIN {src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['VW_PDF_CHUNK']} V
                  ON REGEXP_SUBSTR(M.STAGE_FILE_PATH, '{cfg['STAGE_FOLDER']}/.*$') = V.RELATIVE_PATH
            ) TGT
            WHERE T1.RELATIVE_PATH ILIKE '%\\\\_' || TGT.EPAREGNO || '.pdf' ESCAPE '\\\\'
            """
            cursor.execute(delete_query)

            insert_query = f"""
            INSERT INTO {src_ingest_db}.{cfg['PROC_SCHEMA']}.{cfg['TBL_DOCS_CHUNKS']} (relative_path, size, file_url, scoped_file_url, chunk)
            WITH pdf_to_chunk AS (
                SELECT
                    new_docs.relative_path,
                    new_docs.size,
                    new_docs.file_url,
                    build_scoped_file_url(@{src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['STAGE_NAME']}, new_docs.relative_path) AS scoped_file_url
                FROM {src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['VW_PDF_CHUNK']} as new_docs
            )
            SELECT *
            FROM pdf_to_chunk,
            TABLE({src_ingest_db}.{cfg['PROC_SCHEMA']}.{cfg['UDF_TEXT_CHUNKER'].upper()}(TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@{src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['STAGE_NAME']},
                                        relative_path, {{'mode': 'LAYOUT'}}))))
            """
            cursor.execute(insert_query)

            update_query = f"""
            UPDATE {src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['TBL_PDF_META']} m
            SET PROCESSING_STATUS = 'CHUNKED'
            FROM {src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['VW_PDF_CHUNK']} c
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
    
    def update_category(self):
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        cfg = self.db_config.config
        src_ingest_db = cfg['SRC_INGEST_DB']
        try:
            alter_table = f"""
                ALTER TABLE {src_ingest_db}.{cfg['PROC_SCHEMA']}.{cfg['TBL_DOCS_CHUNKS']}
                ADD COLUMN
                    IF NOT EXISTS COMPANYNAME VARCHAR,
                    IF NOT EXISTS PRODUCTNAME VARCHAR,
                    IF NOT EXISTS SIGNAL_WORD VARCHAR,
                    IF NOT EXISTS CATEGORY_EPA_TYPE ARRAY
            """
            cursor.execute(alter_table)

            update_query = f"""
            UPDATE {src_ingest_db}.{cfg['PROC_SCHEMA']}.{cfg['TBL_DOCS_CHUNKS']} A
                SET
                A.COMPANYNAME = C.NAME,
                A.PRODUCTNAME = B.PRODUCT_NAME,
                A.SIGNAL_WORD = D.SIGNAL_WORD,
                A.CATEGORY_EPA_TYPE = TYPE_ARRAY
                FROM {src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['TBL_PDF_META']} B
                LEFT JOIN {src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['TBL_COMPANY_INFO']} AS C ON B.EPAREGNO =C.EPAREGNO
                LEFT JOIN {src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['TBL_PRODUCTS']} AS D ON D.EPAREGNO = B.EPAREGNO
                LEFT JOIN (
                    SELECT
                        EPAREGNO,
                        ARRAY_AGG(TYPE) AS TYPE_ARRAY
                    FROM {src_ingest_db}.{cfg['RAW_SCHEMA']}.{cfg['TBL_TYPES']}
                    GROUP BY EPAREGNO
                ) T ON T.EPAREGNO = B.EPAREGNO
                WHERE A.RELATIVE_PATH = REGEXP_SUBSTR(B.stage_file_path, '{cfg['STAGE_FOLDER']}/.*$')
            """
            cursor.execute(update_query)
            conn.commit()
            logging.info("Successfully updated PDF category")
            return True
        except Exception as e:
            conn.rollback()
            logging.error(f"Error updating PDF category: {str(e)}")
            return False
        finally:
            cursor.close()
            conn.close()

    def create_cortex_search_(self):
        conn = self.get_snowflake_connection()
        cursor = conn.cursor()
        cfg = self.db_config.config
        src_ingest_db = cfg['SRC_INGEST_DB']

        try:
            query = f"""
            create or replace CORTEX SEARCH SERVICE {src_ingest_db}.{cfg['PROC_SCHEMA']}.{cfg['CORTEX_SVC_PREFIX']}_{src_ingest_db}
            ON chunk
            ATTRIBUTES PRODUCTNAME, SIGNAL_WORD, CATEGORY_EPA_TYPE, COMPANYNAME
            warehouse = {cfg['WAREHOUSE']}
            TARGET_LAG = '1 minute'
            as (
                select *
                from {src_ingest_db}.{cfg['PROC_SCHEMA']}.{cfg['TBL_DOCS_CHUNKS']}
            )
            """
            cursor.execute(query)
            conn.commit()
            logging.info("Successfully created search service")
            return True
        except Exception as e:
            conn.rollback()
            logging.error(f"Error creating search service: {str(e)}")
            return False
        finally:
            cursor.close()
            conn.close()

# Usage example
if __name__ == "__main__":
    # Load config.yaml if present (falls back to built-in defaults when absent)
    config_path = Path(__file__).parent / 'config.yaml'
    yaml_cfg = {}
    if config_path.exists():
        with open(config_path, 'r') as f:
            yaml_cfg = yaml.safe_load(f) or {}

    # Environment variable
    env = os.getenv('ENVIRONMENT', 'DEV')

    # Snowflake connection parameters
    snowflake_params = {
        'user': os.getenv('user'),
        'password': os.getenv('password'),
        'account': os.getenv('account'),
        'warehouse': yaml_cfg.get('snowflake', {}).get('warehouse', 'COMPUTE_WH')
    }

    # Initialize processor and create tables
    processor = EPADataProcessor(snowflake_params, env, yaml_cfg)

    
    epa_df = processor.get_focus_products()
    # For development check
    #epa_df = epa_df[:1]
    epa_list = epa_df['EPAREGNO'].tolist()



    # Process and load data
    if len(epa_list) > 0:
        processor.create_tables()
        processor.process_and_load_data(epa_list)
    else:
        logging.info("No data to process")
        sys.exit(0)

    # Download and store PDFs
    pdf_to_download_df = processor.pdf_to_download()

    # For development check
    #pdf_to_download_df = pdf_to_download_df[0:1]

    if len(pdf_to_download_df) > 0:
        logging.info("Downloading and storing PDFs...")
        processor.download_and_store_pdfs(pdf_to_download_df, processor.db_config.config['PDF_STORE_PATH'])
    else:
        logging.info("No PDFs to download")

    pdf_to_chunk_df = processor.pdf_to_chunk()

    if len(pdf_to_chunk_df) > 0:
        logging.info("Chunking PDFs...")
        processor.process_pdf_chunks()
        processor.update_category()
        processor.create_cortex_search_()

    else:
        logging.info("No PDFs to chunk")

