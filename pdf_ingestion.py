import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import requests
import os
from pathlib import Path
import time
import re
from datetime import datetime
import tempfile

def get_epa_data(conn):
    """Fetch pdf files data to download from Snowflake using the specified query"""
    query = """
    select *
    from DEV_SRC_INGEST.EPA_RAW.EPA_PRODUCTS as prd
    join DEV_SRC_INGEST.EPA_RAW.EPA_PDF_FILES as pdf 
        on prd.eparegno = pdf.epa_reg_num
    qualify row_number() over (
        partition by eparegno 
        order by pdffile_accepted_date desc
    ) = 1
    """
    
    try:
        df = pd.read_sql(query, conn)
        print(f"Retrieved {len(df)} records from Snowflake")
        return df
    except Exception as e:
        print(f"Error fetching data from Snowflake: {str(e)}")
        raise

def clean_filename(filename):
    """Clean filename to remove invalid characters and spaces"""
    # Remove invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    # Replace spaces with underscores
    filename = filename.replace(' ', '_')
    # Limit length
    if len(filename) > 200:
        filename = filename[:200]
    return filename

def upload_to_snowflake_stage(conn, local_file_path, stage_path, file_name):
    """Upload file to Snowflake stage"""
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
def store_pdf_metadata_snowflake(conn, table_name, metadata_records):
    """Store PDF metadata in Snowflake"""
    metadata_df = pd.DataFrame(metadata_records)
    metadata_df.columns = metadata_df.columns.str.upper()

    # Create table if it doesn't exist
    cursor = conn.cursor()
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER AUTOINCREMENT,
        product_name VARCHAR,
        stage_file_path VARCHAR,
        original_url VARCHAR,
        file_size_bytes INTEGER,
        upload_timestamp TIMESTAMP_NTZ,
        processing_status VARCHAR DEFAULT 'PENDING'
    )
    """
    cursor.execute(create_table_sql)
    cursor.close()
    
    # Write metadata to Snowflake
    success = False
    try:
        success = write_pandas(conn, metadata_df, table_name)
    except Exception as e:
        print(f"Error writing to Snowflake: {str(e)}")
    return success

def download_and_store_pdfs(stage_path, snowflake_credentials, metadata_table):
    """
    Download PDFs and store them in a file system for Document AI processing
    
    Parameters:
    df (pd.DataFrame): DataFrame containing 'PDFFILE' and 'PRODUCTNAME' columns
    storage_path (str): Base path to store PDFs
    snowflake_credentials (dict): Snowflake connection credentials
    metadata_table (str): Snowflake table name for storing metadata
    """
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=snowflake_credentials['user'],
        password=snowflake_credentials['password'],
        account=snowflake_credentials['account'],
        warehouse=snowflake_credentials['warehouse'],
        database=snowflake_credentials['database'],
        schema=snowflake_credentials['schema']
    )

    try:
        # Fetch data from Snowflake
        df = get_epa_data(conn)
        #df = df[0:3]
    
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
                    clean_name = clean_filename(unclean_filename)
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
                    if upload_to_snowflake_stage(conn, temp_file_path, stage_path, filename):
                        # Prepare metadata
                        metadata_records.append({
                            'product_name': product_name,
                            'stage_file_path': f"{stage_path}/{filename}",
                            'original_url': url,
                            'file_size_bytes': len(response.content),
                            'upload_timestamp': datetime.now(),
                            'processing_status': 'PENDING'
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
                store_success = store_pdf_metadata_snowflake(conn, metadata_table, metadata_records)
                if not store_success:
                    print("Warning: Failed to store metadata in Snowflake")
        
    finally:
        conn.close()
    
    return results


stage_path = '@DEV_SRC_INGEST.EPA_RAW.PDF_STORE/EPA_LABEL_PDF'
snowflake_credentials = {
    'user': os.getenv('user'),
    'password': os.getenv('password'),
    'account': os.getenv('account'),
    'warehouse': 'COMPUTE_WH',
    'database': 'DEV_SRC_INGEST',
    'schema': 'EPA_RAW'
}
metadata_table = 'EPA_PDF_INGESTION_METADATA'


results = download_and_store_pdfs(stage_path, snowflake_credentials, metadata_table)