import os
import base64
import boto3
import json
import logging
from datetime import datetime, timedelta
from time import time
import psycopg2
from Crypto.Cipher import AES

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
REDSHIFT_CONFIG_TABLE = "red_main_emir.tr_emir_filerecon_configdetails"
REDSHIFT_DATABASE = "mm-lynx"

# Initialize boto3 for Secrets Manager
secrets_client = boto3.client('secretsmanager', region_name=os.getenv('REGION', 'eu-west-1'))

def get_secret(secret_name):
    """Fetch secret from AWS Secrets Manager."""
    start_time = time()
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = response['SecretString'] if 'SecretString' in response else base64.b64decode(response['SecretBinary']).decode('utf-8')
        logging.info(f"Secret {secret_name} fetched successfully in {time() - start_time} seconds")
        return secret
    except Exception as e:
        logging.error(f"Error fetching secret {secret_name}: {e}")
        raise

def get_redshift_connection():
    """Establish a Redshift connection using credentials from Secrets Manager and environment variables."""
    try:
        # Fetch Environment from environment variables with a fallback
        environment = os.environ.get('Environment', 'lynx')  # Default to 'lynx' if not set (optional fallback, remove if you want to enforce setting)
        logging.info(f"Environment: {environment}")
        
        # Set config bucket and file based on environment (optional, can remove if not needed)
        if environment == "bobcat":
            config_bucket = 'fca-dev-mm-emr-config'
            config_file = 'SEC_Config_DEV.json'
        elif environment == "ocelot":
            config_bucket = 'fca-sit-mm-emr-config'
            config_file = 'SEC_Config_SIT.json'
        elif environment == "lynx":
            config_bucket = 'fca-pre-prod-euw-deployable'
            config_file = 'emirrefit/lambda/TR_Config_PREPROD.json'
        elif environment == "panther":
            config_bucket = 'fca-prod-euw-emr-config'
            config_file = 'SEC_Config_PROD.json'
        else:
            raise ValueError(f"Unsupported environment: {environment}")

        # Fetch Redshift secret directly from Secrets Manager (no S3 or hardcoded values needed)
        redshift_secret_name = "your-redshift-secret-name"  # Update with your actual secret name
        secret = get_secret(redshift_secret_name)
        secret_json = json.loads(secret)

        # Extract Redshift credentials
        database_password = secret_json['password']
        key = str(secret_json['Key'])
        encd_bytes = key.encode('utf-8')
        decdbase64_bytes = base64.b64decode(encd_bytes).strip()
        decdbase64_key = decdbase64_bytes.decode('utf-8')
        decryptor = EncryptDecryptCode(decdbase64_key)
        decoded_password = decryptor.decrypt(database_password)

        # Set connection parameters
        connection_params = {
            'user': secret_json['username'],
            'password': decoded_password,
            'host': secret_json['Server'],
            'port': secret_json['Port'],
            'database': REDSHIFT_DATABASE
        }

        # Create and return Redshift connection
        connection = psycopg2.connect(**connection_params)
        logging.info(f"Connected to Redshift database {REDSHIFT_DATABASE}")
        return connection

    except Exception as e:
        logging.error(f"Error in Redshift connection: {e}")
        raise

def redshift_connect(redshift_conn, query):
    """Execute a Redshift query and return results."""
    start_time = time()
    try:
        cursor = redshift_conn.cursor()
        cursor.execute(query)
        while cursor.rowcount is None:
            time.sleep(5)  # Wait for query to complete
        results = cursor.fetchall()
        logging.info(f"Query executed in {time() - start_time} seconds")
        cursor.close()
        return results
    except Exception as e:
        logging.error(f"Error executing Redshift query: {e}")
        raise

def redshift_connect_execute(redshift_conn, query):
    """Execute a Redshift query (e.g., INSERT, UPDATE, DELETE) and commit."""
    start_time = time()
    try:
        cursor = redshift_conn.cursor()
        cursor.execute(query)
        while cursor.rowcount is None:
            time.sleep(5)
        redshift_conn.commit()
        logging.info(f"Query executed and committed in {time() - start_time} seconds")
        cursor.close()
    except Exception as e:
        redshift_conn.rollback()
        logging.error(f"Error executing Redshift query: {e}")
        raise

class EncryptDecryptCode:
    def __init__(self, key):
        self.key = key.encode("utf-8")
        self.length = AES.block_size
        self.aes = AES.new(self.key, AES.MODE_ECB)
        self.unpad = lambda data: data[:-ord(data[-1])]

    def pad(self, text):
        count = len(text.encode("utf-8"))
        add = self.length - (count % self.length)
        return text + (chr(add) * add)

    def encrypt(self, encr_data):
        padded = self.pad(encr_data).encode("utf-8")
        return base64.b64encode(self.aes.encrypt(padded)).decode("utf-8")

    def decrypt(self, decr_data):
        decoded = base64.b64decode(decr_data.encode("utf-8"))
        return self.unpad(self.aes.decrypt(decoded).decode("utf-8"))

def fetch_config(redshift_conn):
    """Fetch configuration values from the Redshift config table."""
    if not redshift_conn:
        raise ValueError("Redshift connection is not established")
    
    try:
        query = f"SELECT key, value FROM {REDSHIFT_CONFIG_TABLE} WHERE active_flag=true"
        results = redshift_connect(redshift_conn, query)
        config = {row[0]: str(row[1]).strip("[]'\"") for row in results}

        logging.info(f"Loaded config from {REDSHIFT_CONFIG_TABLE}: {config}")

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
            raise ValueError(f"Missing required configuration parameters in {REDSHIFT_CONFIG_TABLE}: {missing_params}")

        return config
    except Exception as e:
        logging.error(f"Error fetching config from {REDSHIFT_CONFIG_TABLE}: {e}")
        raise

def update_permanent_recon_table(redshift_conn, config, trade_dates):
    try:
        main_schema = config['main_schema']
        permanent_table_name = config['permanent_recon_table']
        
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
        redshift_connect_execute(redshift_conn, create_table_sql)

        redshift_connect_execute(redshift_conn, f"TRUNCATE TABLE {main_schema}.{permanent_table_name}")

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        today_str = today.strftime('%Y-%m-%d')
        yesterday_str = yesterday.strftime('%Y-%m-%d')

        logging.info(f"Processing files for today: {today_str} and yesterday: {yesterday_str}")

        firms = config['firms'].split(',')
        datatypes = config['datatypes'].split(',')
        for firm in firms:
            for datatype in datatypes:
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
                FROM {config['main_schema']}.{main_table}
                WHERE file_timestamp::date IN ('{yesterday_str}', '{today_str}')
                """
                redshift_connect_execute(redshift_conn, insert_sql)
                logging.info(f"Inserted data for {firm}/{datatype} with timestamps from {yesterday_str} and {today_str}")

    except Exception as e:
        logging.error(f"Error updating permanent recon table: {e}")
        raise

def fetch_file_names(redshift_conn, schema, table, trade_date):
    try:
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        today_str = today.strftime('%Y-%m-%d')
        yesterday_str = yesterday.strftime('%Y-%m-%d')

        logging.info(f"Fetching files for trade_date: {trade_date}, checking {today_str} and {yesterday_str}")
        query = f"SELECT file_name FROM {schema}.{table} WHERE file_timestamp::date IN ('{yesterday_str}', '{today_str}')"
        results = redshift_connect(redshift_conn, query)
        files = [row[0] for row in results]
        logging.info(f"Found files: {files}")
        return files
    except Exception as e:
        logging.error(f"Error fetching file names from {schema}.{table}: {e}")
        raise

def parse_file_name(file_name):
    try:
        if not file_name.endswith('.zip'):
            logging.warning(f"Invalid file name format (not .zip): {file_name}")
            return None
        parts = file_name.rstrip('.zip').split('_')
        if len(parts) < 6:
            logging.warning(f"Invalid file name format: {file_name}")
            return None
        firm = parts[0]
        datatype = parts[1][-5:]
        trade_date = parts[4]
        part_info = parts[5].split('-')[0]
        total_parts = int(part_info[:3])
        sequence_number = int(part_info[3:])
        return {
            "firm": firm,
            "datatype": datatype,
            "trade_date": trade_date,
            "total_parts": total_parts,
            "sequence_number": sequence_number
        }
    except Exception as e:
        logging.error(f"Error parsing file name {file_name}: {e}")
        return None

def calculate_counts(temp_files, main_files, config, trade_date):
    temp_parsed = {}
    main_parsed = {}

    for file_name in temp_files:
        parsed = parse_file_name(file_name)
        if parsed:
            key = (parsed["firm"], parsed["datatype"], parsed["trade_date"])
            if key not in temp_parsed:
                temp_parsed[key] = {"expected": parsed["total_parts"], "files": set()}
            temp_parsed[key]["files"].add(parsed["sequence_number"])

    for file_name in main_files:
        parsed = parse_file_name(file_name)
        if parsed:
            key = (parsed["firm"], parsed["datatype"], parsed["trade_date"])
            if key not in main_parsed:
                main_parsed[key] = {"received": set()}
            main_parsed[key]["received"].add(parsed["sequence_number"])

    firms = config['firms'].split(',')
    datatypes = config['datatypes'].split(',')

    consolidated = []
    for firm in firms:
        for datatype in datatypes:
            key = (firm, datatype, trade_date)
            expected = temp_parsed[key]['expected'] if key in temp_parsed else 0
            received = len(main_parsed.get(key, {}).get("received", set())) if key in main_parsed else 0
            yet_to_receive = max(0, expected - received)
            stuck_count = 0  # No S3 bucket checks

            consolidated.append({
                "firm": firm,
                "datatype": datatype,
                "trade_date": trade_date,
                "expected": expected,
                "received": received,
                "yet_to_receive": yet_to_receive,
                "stuck_count": stuck_count
            })
    return consolidated

def generate_html_report(report_data, config):
    trade_date = report_data[0]['trade_date'] if report_data else "Unknown"
    has_data = any(row['expected'] > 0 or row['received'] > 0 or row['yet_to_receive'] > 0 or row['stuck_count'] > 0 for row in report_data)

    if not has_data:
        return """
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"><title>File Reconciliation Report</title></head>
        <body style="font-family: Calibri, Segoe UI, Arial, sans-serif; background-color: #f9f9f9; margin: 0; padding: 20px;">
            <table style="width: 600px; background: #ffffff; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.15); padding: 20px; margin: auto; border-collapse: collapse; border: 1px solid #D3D3D3;">
                <tr><td>
                    <h2 style="color: #FFFFFF; text-align: center; margin-bottom: 20px; background: linear-gradient(to right, #FF4040, #FF8000); padding: 20px; border-radius: 10px; border: 1px solid #D3D3D3; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 24px; font-weight: 600;">File Reconciliation Report</h2>
                    <div style="text-align: center; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 18px; color: #FF4040; padding: 20px;">No trade records available for {trade_date}</div>
                    <div style="margin-top: 20px; text-align: center; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 14px; color: #888888; background: #FFFFFF; padding: 15px; border: 1px solid #D3D3D3;">This report is generated automatically. Please contact support for any queries.</div>
                </td></tr>
            </table>
        </body>
        </html>
        """.format(trade_date=trade_date)

    html = """
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"><title>File Reconciliation Report</title></head>
    <body style="font-family: Calibri, Segoe UI, Arial, sans-serif; background-color: #f9f9f9; margin: 0; padding: 20px;">
        <table style="width: 600px; background: #ffffff; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.15); padding: 20px; margin: auto; border-collapse: collapse; border: 1px solid #D3D3D3;">
            <tr><td>
                <h2 style="color: #FFFFFF; text-align: center; margin-bottom: 20px; background: linear-gradient(to right, #FF4040, #FF8000); padding: 20px; border-radius: 10px; border: 1px solid #D3D3D3; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 24px; font-weight: 600;">File Reconciliation Report</h2>
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
                    {rows}
                </table>
                <div style="margin-top: 20px; text-align: center; font-family: Calibri, Segoe UI, Arial, sans-serif; font-size: 14px; color: #888888; background: #FFFFFF; padding: 15px; border: 1px solid #D3D3D3;">This report is generated automatically. Please contact support for any queries.</div>
            </td></tr>
        </table>
    </body>
    </html>
    """
    rows = ''.join(f"""
        <tr style="background: {'#F8F9FA' if i % 2 == 0 else '#E9ECEF'};">
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: #F5F5F5; color: #333333;">{row['firm']}</td>
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: #F5F5F5; color: #333333;">{row['datatype']}</td>
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: #FFFFFF; color: #333333;">{row['expected']}</td>
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: #FFFFFF; color: #333333;">{row['received']}</td>
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: #00CED1; color: #333333;">{row['yet_to_receive']}</td>
            <td style="padding: 15px; text-align: center; border: 1px solid #D3D3D3; background-color: {'#FF1493' if row['stuck_count'] > 0 else '#008080'}; color: white;">{'{} files stuck'.format(row['stuck_count']) if row['stuck_count'] > 0 else 'No files stuck'}</td>
        </tr>
    """ for i, row in enumerate(report_data))
    return html.format(trade_date=trade_date, rows=rows)

def insert_count_logs(redshift_conn, report_data, count_logs_table):
    try:
        cursor = redshift_conn.cursor()
        for row in report_data:
            cursor.execute("""
                INSERT INTO %s (firm, datatype, trade_date, expected_count, received_count, pending_count, stuck_count, createddate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, GETDATE())
            """, (count_logs_table, row['firm'], row['datatype'], row['trade_date'], row['expected'], row['received'], row['yet_to_receive'], row['stuck_count']))
        redshift_conn.commit()
        logging.info("Count logs inserted successfully into Redshift.")
    except Exception as e:
        redshift_conn.rollback()
        logging.error(f"Error inserting count logs into Redshift: {e}")
        raise
    finally:
        cursor.close()

def send_email(subject, intro_text, report_html, end_quote, config):
    """Send email using SMTP server and port from config, without authentication."""
    try:
        msg = MIMEMultipart()
        msg['From'] = config['frommail']
        msg['To'] = config['tomail']
        msg['Cc'] = config['ccmail']
        msg['Subject'] = subject

        full_email_content = f"{intro_text}\n\n{report_html}\n\n{end_quote}"
        msg.attach(MIMEText(full_email_content, 'html'))

        with smtplib.SMTP(config['smtp_server'], int(config['smtp_port'])) as server:
            server.starttls()  # Assuming STARTTLS is supported; adjust if not needed
            server.send_message(msg)
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error(f"Error sending email: {e}")
        raise

def s3_invoke_handler():
    redshift_conn = None
    try:
        logging.info("Lambda function started")
        redshift_conn = get_redshift_connection()
        config = fetch_config(redshift_conn)

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        today_str = today.strftime("%y%m%d")
        yesterday_str = yesterday.strftime("%y%m%d")

        logging.info(f"Processing files for today: {today_str} and yesterday: {yesterday_str}")
        update_permanent_recon_table(redshift_conn, config, [yesterday_str, today_str])

        firms = config['firms'].split(',')
        datatypes = config['datatypes'].split(',')

        report_data = []
        for trade_date in [yesterday_str, today_str]:
            for firm in firms:
                for datatype in datatypes:
                    temp_files = fetch_file_names(redshift_conn, config['main_schema'], config['permanent_recon_table'], trade_date)
                    main_files = fetch_file_names(redshift_conn, config['main_schema'], config['permanent_recon_table'], trade_date)
                    consolidated = calculate_counts(temp_files, main_files, config, trade_date)
                    report_data.extend(consolidated)

        logging.info(f"Final report_data: {report_data}")
        insert_count_logs(redshift_conn, report_data, config['count_logs_table'])

        report_html = generate_html_report(report_data, config)
        send_email(config['Recon_emailSubject'], config['email_intro_text'], report_html, config['email_end_quote'], config)

        return {'statusCode': 200, 'body': json.dumps('Report sent successfully')}
    except Exception as e:
        logging.error(f"Lambda execution failed: {e}")
        return {'statusCode': 500, 'body': json.dumps(f"Error: {str(e)}")}
    finally:
        if redshift_conn:
            redshift_conn.close()
            logging.info("Redshift connection closed.")
