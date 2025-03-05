import os
import base64
import boto3
import botocore.session as bc
import pandas as pd
from pandas import read_excel
import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import StringIO, BytesIO
import openpyxl
import re
import sys
import time
import pytz
import json
from datetime import datetime, timedelta
from dateutil.parser import parse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
REDSHIFT_CONFIG_TABLE = "red_main_emir.tr_emir_filerecon_configdetails"  # Hardcoded schema and table for config

# S3 and boto3 initialization (removed unnecessary S3 client for bucket checks)
s3 = boto3.resource('s3', region_name=os.getenv('region_name', 'eu-west-1'), use_ssl=True)

bc_session = bc.get_session()
session = boto3.Session(
    botocore_session=bc_session,
    region_name=os.getenv('region_name', 'eu-west-1')
)

# Import required packages (assume pre-installed or manage via requirements.txt)
import pybase64
from Crypto.Cipher import AES
import psycopg2

def get_secret(secret_name):
    start_time_get_secret = time.time()

    # Use environment variable for region_name or default to a safe value
    region_name = os.getenv('region_name', 'eu-west-1')
    get_secret_value_response = ""

    try:
        # Create a Secrets Manager session with VPC endpoint
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name,
            endpoint_url=os.environ.get('SECRETS_MANAGER_ENDPOINT', None)  # Use VPC endpoint if available
        )

        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

    except Exception as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            logging.error(f"Decryption failure for secret {secret_name}: {e}")
            raise
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            logging.error(f"Internal service error for secret {secret_name}: {e}")
            raise
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logging.error(f"Invalid parameter for secret {secret_name}: {e}")
            raise
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logging.error(f"Invalid request for secret {secret_name}: {e}")
            raise
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            logging.error(f"Resource not found for secret {secret_name}: {e}")
            raise
        else:
            logging.error(f"Unexpected error retrieving secret {secret_name}: {e}")
            raise

    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    end_time_get_secret = time.time()
    logging.info(f"Total time taken by get_secret {end_time_get_secret - start_time_get_secret}secs")
    return get_secret_value_response

def get_redshift_connection():
    try:
        # Safely get environment variables with defaults
        environment = os.environ.get('Environment', 'bobcat')  # Default to 'bobcat' if not set
        logging.info(f"Environment: {environment}")
        
        # Set configuration based on environment
        if environment == "bobcat":
            os.environ['configbucket'] = 'fca-dev-mm-emr-config'
            os.environ['configfile'] = 'SEC_Config_DEV.json'
        elif environment == "ocelot":
            os.environ['configbucket'] = 'fca-sit-mm-emr-config'
            os.environ['configfile'] = 'SEC_Config_SIT.json'
        elif environment == "lynx":
            os.environ['configbucket'] = 'fca-pre-prod-euw-emr-config'
            os.environ['configfile'] = 'TR_Config_PREPROD.json'
        elif environment == "panther":
            os.environ['configbucket'] = 'fca-prod-euw-emr-config'
            os.environ['configfile'] = 'SEC_Config_PROD.json'
        else:
            raise ValueError(f"Unsupported environment: {environment}")

        # Use os.getenv for region_name with a fallback
        os.environ['region_name'] = os.getenv('REGION', 'eu-west-1')
        s3 = boto3.resource('s3', region_name=os.getenv('region_name'), use_ssl=True)
        logging.info(f"Config Bucket Name: {os.getenv('configbucket')}")

        # Fetch config from S3
        obj = s3.Object(os.getenv('configbucket'), os.getenv('configfile'))
        filedata = obj.get()['Body'].read()
        contents = filedata.decode('utf-8')
        cfg_json_dict = json.loads(contents)
        if not isinstance(cfg_json_dict, dict) or 'config' not in cfg_json_dict:
            raise ValueError("Invalid configuration JSON: 'config' key not found or JSON is malformed")

        # Fetch Redshift credentials from Secrets Manager
        secret_name = cfg_json_dict['config']['RedshiftSecretName']
        SecretDetails = get_secret(secret_name)
        SecretContent = json.dumps(SecretDetails, indent=4, sort_keys=True, default=str)
        json_secret = json.loads(SecretContent)
        RedshiftDetails = json_secret['SecretString']
        json_redshift = json.loads(RedshiftDetails)
        logging.info("Redshift details Loaded from secret manager")
        
        DatabasePassword = json_redshift['password']
        Key = str(json_redshift['Key'])
        encd_bytes = Key.encode('utf-8')
        decdbase64_bytes = pybase64.b64decode(encd_bytes).strip()
        decdbase64_Key = decdbase64_bytes.decode('utf-8')
        obj_EncryptDecryptCode = EncryptDecryptCode(decdbase64_Key)
        decoded_RedshiftPassword = obj_EncryptDecryptCode.decrypt(DatabasePassword)

        # Set environment variables for Redshift connection
        os.environ['DatabaseLogin'] = json_redshift['Database']
        os.environ['decoded_RedshiftPassword'] = decoded_RedshiftPassword
        os.environ['DatabaseServer'] = str(json_redshift['Server'])
        os.environ['DatabaseUser'] = str(json_redshift['username'])
        os.environ['DatabasePort'] = str(json_redshift['Port'])

        # Fetch config and SQL details from the hardcoded Redshift table
        details_query = f"select key, value from {REDSHIFT_CONFIG_TABLE} where active_flag=true"

        try:
            details_dataframe = redshift_connect(
                os.getenv('DatabaseServer'),
                os.getenv('DatabaseLogin'),
                os.getenv('DatabaseUser'),
                os.getenv('decoded_RedshiftPassword'),
                os.getenv('DatabasePort'),
                details_query
            )
            for row in details_dataframe.itertuples(index=False):
                os.environ[row.key] = row.value
            
        except Exception as error:
            logging.error(f"Error fetching Config and SQL Details from Redshift: {error}")
            raise Exception("ERROR: Issue in fetching Config and SQL Details from Redshift")

        # Return the connection object for use in other functions
        connection = redshift_connect(
            os.getenv('DatabaseServer'),
            os.getenv('DatabaseLogin'),
            os.getenv('DatabaseUser'),
            os.getenv('decoded_RedshiftPassword'),
            os.getenv('DatabasePort'),
            f"SELECT key, value FROM {REDSHIFT_CONFIG_TABLE}"
        )
        config = {row[0]: row[1] for row in connection.fetchall()}
        connection.close()  # Close the cursor, but return the connection if needed
        return connection  # Return the psycopg2 connection object

    except Exception as e:
        logging.error(f"Error in Redshift connection: {e}")
        raise

def redshift_connect(host, database, user, password, port, query):
    start_time_redshift_connect = time.time()
    query_str = query

    connection = psycopg2.connect(user=user,
                                 password=password,
                                 host=host,
                                 port=port,
                                 database=database)
    redshiftCursor = connection.cursor()
    redshiftCursor.execute(query_str)
    res_df = pd.DataFrame(redshiftCursor.fetchall(), columns=[desc[0] for desc in redshiftCursor.description])
    while redshiftCursor.rowcount is None:
        time.sleep(5)  # Use time.sleep instead of sleep for clarity
    logging.info("End of query execution. Total time taken {}secs".format(time.time() - start_time_redshift_connect))
    return res_df

def redshift_connect_execute(host, database, user, password, port, query):
    start_time_redshift_connect = time.time()
    query_str = query

    connection = psycopg2.connect(user=user,
                                 password=password,
                                 host=host,
                                 port=port,
                                 database=database)
    redshiftCursor = connection.cursor()
    redshiftCursor.execute(query_str)
    while redshiftCursor.rowcount is None:
        time.sleep(5)
    connection.commit()
    end_time_redshift_connect = time.time()
    logging.info("End of redshift_connect_execute function. Total time taken {}secs".format(end_time_redshift_connect - start_time_redshift_connect))
    return redshiftCursor

class EncryptDecryptCode:
    def __init__(self, key):
        self.key = key.encode("utf8")
        self.length = AES.block_size
        self.aes = AES.new(self.key, AES.MODE_ECB)
        self.unpad = lambda data: data[0:-ord(data[-1])]

    def pad(self, text):
        count = len(text.encode("utf-8"))
        add = self.length - (count % self.length)
        entext = text + (chr(add) * add)
        return entext

    def encrypt(self, encrData):
        res = self.aes.encrypt(self.pad(encrData).encode("utf8"))
        msg = str(base64.b64encode(res))
        return msg

    def decrypt(self, decrData):
        res = base64.standard_b64decode(decrData.encode("utf8"))
        msg = self.aes.decrypt(res).decode("utf8")
        return self.unpad(msg)

def fetch_config(redshift_conn, config_table_name="red_main_emir.tr_emir_filerecon_configdetails"):
    """
    Fetch configuration values from the specified Redshift config table.
    All config parameters must be fetched from the table, no hardcoded defaults.
    """
    if not redshift_conn:
        raise ValueError("Redshift connection is not established")
    
    cursor = redshift_conn.cursor()
    try:
        config = {}
        cursor.execute(f"SELECT key, value FROM {config_table_name}")
        for row in cursor.fetchall():
            # Store values directly from the table, ensuring clean strings
            config[row[0]] = str(row[1]).strip("[]'\"")  # Remove any brackets or quotes

        logging.info(f"Loaded config from {config_table_name}: {config}")

        # Validate that all required config parameters are present
        required_params = [
            'main_schema', 'temp_schema', 'permanent_recon_table', 'count_logs_table',
            'firms', 'datatypes', 'Recon_emailSubject', 'email_intro_text',
            'email_end_quote', 'smtp_server', 'smtp_port', 'frommail', 'tomail', 'ccmail',
            'temp_table_tsr', 'temp_table_mds', 'temp_table_tar', 'temp_table_mda',
            'temp_table_rec', 'temp_table_rej', 'temp_table_wrn',
            'main_table_tsr_mds_tar_mda', 'main_table_rec_rej_wrn'
        ]
        
        missing_params = [param for param in required_params if param not in config]
        if missing_params:
            raise ValueError(f"Missing required configuration parameters in {config_table_name}: {missing_params}")

        return config
    except psycopg2.Error as e:
        logging.error(f"Database error fetching config from {config_table_name}: {e}")
        raise
    except Exception as e:
        logging.error(f"Error fetching config from {config_table_name}: {e}")
        raise
    finally:
        cursor.close()

def update_permanent_recon_table(redshift_conn, config, trade_dates):
    try:
        cursor = redshift_conn.cursor()
        main_schema = config['main_schema']  # Fetch from config table
        permanent_table_name = config['permanent_recon_table']  # Fetch from config table
        
        # Create permanent table if it doesn’t exist under main_schema
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {main_schema}.{permanent_table_name} (
            file_name VARCHAR,
            file_type VARCHAR,
            file_timestamp TIMESTAMP,
            firm VARCHAR,
            datatype VARCHAR,
            trade_date VARCHAR
        )
        """
        cursor.execute(create_table_sql)

        # Truncate (clear) the permanent table before repopulating
        cursor.execute(f"TRUNCATE TABLE {main_schema}.{permanent_table_name}")

        # Get today and yesterday dates in YYYY-MM-DD format
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        today_str = today.strftime('%Y-%m-%d')
        yesterday_str = yesterday.strftime('%Y-%m-%d')

        logging.info(f"Processing files for today: {today_str} and yesterday: {yesterday_str}")

        # Insert filtered data for today and yesterday timestamps from audit tables
        firms = config['firms'].split(',')  # Fetch from config table
        datatypes = config['datatypes'].split(',')  # Fetch from config table
        for firm in firms:
            for datatype in datatypes:
                # Map datatype to temp table under temp_schema
                temp_table_map = {
                    'TSR': config['temp_table_tsr'],
                    'MDS': config['temp_table_mds'],
                    'TAR': config['temp_table_tar'],
                    'MDA': config['temp_table_mda'],
                    'REC': config['temp_table_rec'],
                    'REJ': config['temp_table_rej'],
                    'WRN': config['temp_table_wrn']
                }
                temp_table = temp_table_map.get(datatype, f"tr_fileaudit_{datatype.lower()}")

                # Map datatype to main table under main_schema
                main_table_map = {
                    'TSR': config['main_table_tsr_mds_tar_mda'],
                    'MDS': config['main_table_tsr_mds_tar_mda'],
                    'TAR': config['main_table_tsr_mds_tar_mda'],
                    'MDA': config['main_table_tsr_mds_tar_mda'],
                    'REC': config['main_table_rec_rej_wrn'],
                    'REJ': config['main_table_rec_rej_wrn'],
                    'WRN': config['main_table_rec_rej_wrn']
                }
                main_table = main_table_map.get(datatype, 'tr_fileaudit' if datatype in ['TSR', 'MDS', 'TAR', 'MDA'] else 'tr_fileaudit_stat')

                insert_sql = f"""
                INSERT INTO {main_schema}.{permanent_table_name}
                SELECT file_name, file_type, file_timestamp, firm, datatype, trade_date
                FROM {config['temp_schema']}.{temp_table}
                WHERE file_timestamp::date IN ('{yesterday_str}', '{today_str}')
                UNION ALL
                SELECT file_name, file_type, file_timestamp, firm, datatype, trade_date
                FROM {main_schema}.{main_table}
                WHERE file_timestamp::date IN ('{yesterday_str}', '{today_str}')
                """
                cursor.execute(insert_sql)
                logging.info(f"Inserted data for {firm}/{datatype} with timestamps from {yesterday_str} and {today_str} into permanent table in {main_schema}")

        redshift_conn.commit()
    except Exception as e:
        logging.error(f"Error updating permanent recon table: {e}")
        raise
    finally:
        cursor.close()

def fetch_file_names(redshift_conn, schema, table, trade_date):
    try:
        cursor = redshift_conn.cursor()
        # Get today and yesterday dates in YYYY-MM-DD format
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        today_str = today.strftime('%Y-%m-%d')
        yesterday_str = yesterday.strftime('%Y-%m-%d')

        logging.info(f"Fetching files for trade_date: {trade_date}, checking today: {today_str} and yesterday: {yesterday_str}")
        
        cursor.execute(f"""
            SELECT file_name FROM {schema}.{table}
            WHERE file_timestamp::date IN ('{yesterday_str}', '{today_str}')
        """)
        files = [row[0] for row in cursor.fetchall()]
        logging.info(f"Found files: {files}")
        cursor.close()
        return files
    except Exception as e:
        logging.error(f"Error fetching file names from {schema}.{table}: {e}")
        raise

def parse_file_name(file_name):
    try:
        logging.debug(f"Parsing file name: {file_name}")
        if not file_name.endswith('.zip'):
            logging.warning(f"Invalid file name format (not .zip): {file_name}")
            return None
        parts = file_name.rstrip('.zip').split('_')
        if len(parts) < 6:
            logging.warning(f"Invalid file name format: {file_name}")
            return None
        firm = parts[0]  # TRDDR or FIRM1, etc.
        datatype = parts[1][-5:]  # DATTAR (last 5 characters of second portion)
        trade_date = parts[4]  # 250303 (yymmdd)
        part_info = parts[5].split('-')[0]  # 001001
        total_parts = int(part_info[:3])  # 001 (expected parts)
        sequence_number = int(part_info[3:])  # 001 (sequence number)
        parsed = {
            "firm": firm,
            "datatype": datatype,
            "trade_date": trade_date,
            "total_parts": total_parts,
            "sequence_number": sequence_number
        }
        logging.debug(f"Parsed result: {parsed}")
        return parsed
    except Exception as e:
        logging.error(f"Error parsing file name {file_name}: {e}")
        return None

def calculate_counts(temp_files, main_files, config, trade_date):
    logging.debug(f"Calculating counts with temp_files: {temp_files}, main_files: {main_files}")
    temp_parsed = {}
    main_parsed = {}

    # Parse and group temp files (from permanent_recon_table, representing temp audit tables)
    for file_name in temp_files:
        parsed = parse_file_name(file_name)
        if parsed:
            key = (parsed["firm"], parsed["datatype"], parsed["trade_date"])
            if key not in temp_parsed:
                temp_parsed[key] = {"expected": parsed["total_parts"], "files": set()}
            temp_parsed[key]["files"].add(parsed["sequence_number"])

    # Parse and group main files (from permanent_recon_table, representing main audit tables)
    for file_name in main_files:
        parsed = parse_file_name(file_name)
        if parsed:
            key = (parsed["firm"], parsed["datatype"], parsed["trade_date"])
            if key not in main_parsed:
                main_parsed[key] = {"received": set()}
            main_parsed[key]["received"].add(parsed["sequence_number"])

    # Get all firms and datatypes from config (fetched from table)
    firms = config['firms'].split(',')
    datatypes = config['datatypes'].split(',')

    # Consolidate counts for all firm-datatype combinations, showing zeros if no data
    consolidated = []
    for firm in firms:
        for datatype in datatypes:
            key = (firm, datatype, trade_date)
            expected = temp_parsed[key]['expected'] if key in temp_parsed else 0
            received = len(main_parsed.get(key, {}).get("received", set())) if key in main_parsed else 0
            yet_to_receive = max(0, expected - received)  # Ensure non-negative
            
            # Set stuck_count to 0 since we’re avoiding S3 bucket checks
            stuck_count = 0

            consolidated.append({
                "firm": firm,
                "datatype": datatype,
                "trade_date": trade_date,
                "expected": expected,
                "received": received,
                "yet_to_receive": yet_to_receive,
                "stuck_count": stuck_count
            })
    logging.debug(f"Consolidated counts: {consolidated}")
    return consolidated

def generate_html_report(report_data, config):
    trade_date = report_data[0]['trade_date'] if report_data else "Unknown"
    has_data = any(row['expected'] > 0 or row['received'] > 0 or row['yet_to_receive'] > 0 or row['stuck_count'] > 0 for row in report_data)

    if not has_data:
        # No trade records available message
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>File Reconciliation Report</title>
        </head>
        <body style="font-family: Calibri, Segoe UI, Arial, sans-serif; background-color: #f9f9f9; margin: 0; padding: 20px;">
            <table style="width: 600px; background: #ffffff; border-radius: 10px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15); padding: 20px; margin: auto; border-collapse: collapse; border: 1px solid #D3D3D3;">
                <tr>
                    <td>
                        <h2 style="color: #FFFFFF; text-align: center; margin-bottom: 20px; background: linear-gradient(to right, #FF4040, #FF8000); padding: 20px; border-radius: 10px; border: 1px solid #D3D3D3; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 24px; font-weight: 600;">
                            File Reconciliation Report
                        </h2>
                        <div style="text-align: center; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 18px; color: #FF4040; padding: 20px;">
                            No trade records available for {trade_date}
                        </div>
                        <div style="margin-top: 20px; text-align: center; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 14px; color: #888888; background: #FFFFFF; padding: 15px; border: 1px solid #D3D3D3;">
                            This report is generated automatically. Please contact support for any queries.
                        </div>
                    </td>
                </tr>
            </table>
        </body>
        </html>
        """
        return html

    # Generate non-chart HTML report (simple table with all firms, trade dates, and counts, showing zeros if no data)
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>File Reconciliation Report</title>
    </head>
    <body style="font-family: Calibri, Segoe UI, Arial, sans-serif; background-color: #f9f9f9; margin: 0; padding: 20px;">
        <table style="width: 600px; background: #ffffff; border-radius: 10px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15); padding: 20px; margin: auto; border-collapse: collapse; border: 1px solid #D3D3D3;">
            <tr>
                <td>
                    <h2 style="color: #FFFFFF; text-align: center; margin-bottom: 20px; background: linear-gradient(to right, #FF4040, #FF8000); padding: 20px; border-radius: 10px; border: 1px solid #D3D3D3; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 24px; font-weight: 600;">
                        File Reconciliation Report
                    </h2>
                    <h3 style="color: #FF4040; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 18px; font-weight: 600; text-align: center; margin-bottom: 10px;">Trade Date: {trade_date}</h3>
                    <table style="width: 100%; border-collapse: collapse; margin-top: 10px; overflow-x: auto;">
                        <tr>
                            <th style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background: linear-gradient(to right, #FF4040, #FF8000); color: #FFFFFF; font-weight: 600;">Firm</th>
                            <th style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background: linear-gradient(to right, #FF4040, #FF8000); color: #FFFFFF; font-weight: 600;">Datatype</th>
                            <th style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background: linear-gradient(to right, #FF4040, #FF8000); color: #FFFFFF; font-weight: 600;">Expected</th>
                            <th style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background: linear-gradient(to right, #FF4040, #FF8000); color: #FFFFFF; font-weight: 600;">Received</th>
                            <th style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background: linear-gradient(to right, #FF4040, #FF8000); color: #FFFFFF; font-weight: 600;">Yet to Receive</th>
                            <th style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background: linear-gradient(to right, #FF4040, #FF8000); color: #FFFFFF; font-weight: 600;">Stuck</th>
                        </tr>
                        {generate_table_rows_no_charts(report_data)}
                    </table>
                    <div style="margin-top: 20px; text-align: center; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 14px; color: #888888; background: #FFFFFF; padding: 15px; border: 1px solid #D3D3D3;">
                        This report is generated automatically. Please contact support for any queries.
                    </div>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    return html

def generate_table_rows_no_charts(report_data):
    rows = ''
    for i, row in enumerate(report_data):
        background = '#F8F9FA' if i % 2 == 0 else '#E9ECEF'
        rows += f"""
        <tr style="background: {background};">
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: #F5F5F5; color: #333333;">{row['firm']}</td>
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: #F5F5F5; color: #333333;">{row['datatype']}</td>
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: #FFFFFF; color: #333333;">{row['expected']}</td>
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: #FFFFFF; color: #333333;">{row['received']}</td>
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: #00CED1; color: #333333;">{row['yet_to_receive']}</td>
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: {'#FF1493' if row['stuck_count'] > 0 else '#008080'}; color: white;">{'{} files stuck'.format(row['stuck_count']) if row['stuck_count'] > 0 else 'No files stuck'}</td>
        </tr>
        """
    return rows

def insert_count_logs(redshift_conn, report_data, count_logs_table):
    try:
        cursor = redshift_conn.cursor()
        for row in report_data:
            cursor.execute(f"""
                INSERT INTO {count_logs_table}
                (firm, datatype, trade_date, expected_count, received_count, pending_count, stuck_count, createddate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, GETDATE())
            """, (row['firm'], row['datatype'], row['trade_date'], row['expected'], row['received'], row['yet_to_receive'], row['stuck_count']))
        redshift_conn.commit()
        logging.info("Count logs inserted successfully into Redshift.")
    except Exception as e:
        logging.error(f"Error inserting count logs into Redshift: {e}")
        raise
    finally:
        cursor.close()

def send_email(smtp_credentials, subject, intro_text, report_html, end_quote, config):
    try:
        msg = MIMEMultipart()
        msg['From'] = config['frommail']  # Fetch from config table
        msg['To'] = config['tomail']  # Fetch from config table
        msg['Cc'] = config['ccmail']  # Fetch from config table
        msg['Subject'] = subject  # Fetch from config table as Recon_emailSubject

        # Combine intro, HTML report, and end quote (all fetched from config table)
        full_email_content = f"{config['email_intro_text']}\n\n{report_html}\n\n{config['email_end_quote']}"
        msg.attach(MIMEText(full_email_content, 'html'))

        with smtplib.SMTP(config['smtp_server'], int(config['smtp_port'])) as server:  # Fetch from config table
            server.starttls()
            server.login(config['frommail'], 'your_password')  # Use frommail as smtpuser, update password securely or fetch from config
            server.send_message(msg)
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error(f"Error sending email: {e}")
        raise

def s3_invoke_handler():
    redshift_conn = None
    try:
        logging.info("Lambda function started")
        # Get Redshift connection
        redshift_conn = get_redshift_connection()

        # Fetch configuration from the table (no hardcoded defaults)
        config = fetch_config(redshift_conn, config_table_name=REDSHIFT_CONFIG_TABLE)

        # Get today and yesterday dates in yymmdd format
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        today_str = today.strftime("%y%m%d").strip("[]'\"")  # e.g., '250304' for March 4, 2025
        yesterday_str = yesterday.strftime("%y%m%d").strip("[]'\"")  # e.g., '250303' for March 3, 2025

        logging.info(f"Processing files for today: {today_str} and yesterday: {yesterday_str}")

        # Update permanent recon table for today and yesterday timestamps from audit tables
        update_permanent_recon_table(redshift_conn, config, [yesterday_str, today_str])

        # Process reconciliation for today and yesterday
        firms = config['firms'].split(',')  # Fetch from config table
        datatypes = config['datatypes'].split(',')  # Fetch from config table

        report_data = []
        for trade_date in [yesterday_str, today_str]:
            logging.info(f"Processing firms: {firms}, datatypes: {datatypes}, trade_date: {trade_date}")
            for firm in firms:
                for datatype in datatypes:
                    logging.info(f"Processing firm: {firm}, datatype: {datatype}")
                    # Fetch filtered files from permanent_recon_table under main_schema (fetched from config)
                    temp_files = fetch_file_names(redshift_conn, config['main_schema'], config['permanent_recon_table'], trade_date)
                    main_files = fetch_file_names(redshift_conn, config['main_schema'], config['permanent_recon_table'], trade_date)
                    logging.info(f"Temp files for {firm}/{datatype} on {trade_date}: {temp_files}")
                    logging.info(f"Main files for {firm}/{datatype} on {trade_date}: {main_files}")

                    # Calculate expected and received counts (no bucket stuck check)
                    consolidated = calculate_counts(temp_files, main_files, config, trade_date)
                    logging.info(f"Consolidated data for {firm}/{datatype} on {trade_date}: {consolidated}")
                    report_data.extend(consolidated)

        logging.info(f"Final report_data: {report_data}")

        # Insert count logs into Redshift (table name fetched from config)
        insert_count_logs(redshift_conn, report_data, config['count_logs_table'])

        # Generate HTML report without charts (non-chart version only, showing all firms with zeros if no data)
        report_html = generate_html_report(report_data, config)

        # Send email with HTML report (all email params fetched from config)
        subject = config['Recon_emailSubject']
        intro_text = config['email_intro_text']
        end_quote = config['email_end_quote']
        smtp_credentials = {
            'smtpserver': config['smtp_server'],
            'smtpport': config['smtp_port'],
            'smtpuser': config['frommail'],  # Fetch from config
            'smtppassword': 'your_password'  # Update with a secure method or fetch from config if stored securely
        }
        send_email(smtp_credentials, subject, intro_text, report_html, end_quote, config)

        return {
            'statusCode': 200,
            'body': json.dumps('Report sent successfully')
        }
    except Exception as e:
        logging.error(f"Lambda execution failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
    finally:
        if redshift_conn:
            redshift_conn.close()
            logging.info("Redshift connection closed.")