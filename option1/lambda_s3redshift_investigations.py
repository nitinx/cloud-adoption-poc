import os
import json
import urllib.parse
import boto3
import logging
import psycopg2
import configparser
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

print('Loading function')

s3 = boto3.client('s3')
ses = boto3.client('ses')

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY=config.get('AWS','KEY')
SECRET= config.get('AWS','SECRET')

REDSHIFT_DATABASE= config.get("CLUSTER","DB_NAME")
REDSHIFT_USER= config.get("CLUSTER","DB_USER")
REDSHIFT_PASSWD= config.get("CLUSTER","DB_PASSWORD")
REDSHIFT_PORT = config.get("CLUSTER","DB_PORT")

REDSHIFT_ENDPOINT=config.get("CLUSTER","HOST")    
REDSHIFT_ROLE_ARN=config.get("IAM_ROLE","ARN")

# Variables for the job: 
email_from = os.environ['email_from']
email_to = os.environ['email_to']

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    #print("Function Name: " + context.function_name)

    # Get the object from the event and show its content type
    eventTime = event['Records'][0]['eventTime']
    eventName = event['Records'][0]['eventName']
    bucket = event['Records'][0]['s3']['bucket']['name']
    file_name = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    file_size = event['Records'][0]['s3']['object']['size']
    file_etag = event['Records'][0]['s3']['object']['eTag']

    response_s3 = s3.get_object(Bucket=bucket, Key=file_name)
    print("Event Time: " + eventTime)
    print("Event Name: " + eventName)
    
    print("Bucket Name: " + bucket)
    print("File Name: " + file_name)
    print("File Size: " + str(file_size))
    print("File eTag: " + file_etag)
    print("File Type: " + response_s3['ContentType'])
    
    REDSHIFT_QUERY1 = "TRUNCATE TABLE stg_investigations;"
    
    REDSHIFT_QUERY2 = """
    copy stg_investigations from 's3://cmpoc-raw/opt3/large' 
    iam_role '{}' 
    ignoreheader as 1
    delimiter ','
    region 'us-east-1';""".format(REDSHIFT_ROLE_ARN)

    REDSHIFT_QUERY3 = """
	INSERT INTO fact_detail (bus_dt, prpc_lob_seq_id, inqr_id, client_id, calc_rslv_dt, case_entr_dt, frst_rslv_dt, last_ropned_dt, ropn_cn, inqr_amt, inqr_amt_ccy, case_own_nm, first_tat, last_tat, top_client_ind)
	SELECT TO_DATE (bus_dt, 'DD/MM/YYYY') AS bus_dt,
	   CAST(prpc_lob_seq_id as smallint) AS prpc_lob_seq_id,
       inqr_id AS inqr_id,
       public.stg_investigations.client_id AS client_id,
       TO_DATE (calc_rslv_dt, 'DD/MM/YYYY') AS calc_rslv_dt,
       TO_DATE (case_entr_dt, 'DD/MM/YYYY') AS case_entr_dt,
       TO_DATE (frst_rslv_dt, 'DD/MM/YYYY') AS frst_rslv_dt,
       TO_DATE (last_ropned_dt, 'DD/MM/YYYY') AS last_ropned_dt,
       CAST(ropn_cn as smallint) AS ropn_cn,
       CAST(inqr_amt as bigint) AS inqr_amt,
       inqr_amt_ccy AS inqr_amt_ccy,
       case_own_nm AS case_own_nm,
       TO_DATE (frst_rslv_dt, 'DD/MM/YYYY') - TO_DATE (case_entr_dt, 'DD/MM/YYYY') AS first_tat,
       TO_DATE (calc_rslv_dt, 'DD/MM/YYYY') - TO_DATE (last_ropned_dt, 'DD/MM/YYYY') AS last_tat,
       top_client_ind AS top_client_ind
  FROM public.stg_investigations
  LEFT OUTER JOIN public.dim_clients_top 
  ON (public.stg_investigations.client_id = public.dim_clients_top.client_id);"""

    REDSHIFT_QUERY4 = """
	INSERT INTO fact_summ (bus_dt, client_id, total_tat, avg_tat, total_value, rslv_cnt)
	SELECT bus_dt AS bus_dt,
       client_id AS client_id,
       SUM(first_tat + last_tat) AS total_tat,
       AVG(first_tat + last_tat) AS avg_tat,
       SUM(inqr_amt) AS total_value,
       COUNT(inqr_id) AS rslv_cnt
	FROM public.fact_detail 
	GROUP BY bus_dt, client_id;"""

    try:
        conn = psycopg2.connect(
            dbname=REDSHIFT_DATABASE,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWD,
            port=REDSHIFT_PORT,
            host=REDSHIFT_ENDPOINT)
        print("Connection Status: Successful")
    except Exception as ERROR:
        print("Connection Issue: " + ERROR)
    
    try:
        cursor = conn.cursor()
        cursor.execute(REDSHIFT_QUERY1)
        conn.commit()
        cursor.execute(REDSHIFT_QUERY2)
        conn.commit()
        cursor.execute(REDSHIFT_QUERY3)
        conn.commit()
        cursor.execute(REDSHIFT_QUERY4)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as ERROR:
        #print("Execution Issue: " + ERROR)
        print(ERROR)

    #logger.info('## TRIGGERED BY EVENT: ')
    #logger.info(event['detail'])

    email_subject = 'Investigations | Feed Processed  | ' + file_name
    email_body = '[File Size: ' + str(file_size) + ']'
        
    response_ses = ses.send_email(
        Source = email_from,
        Destination={
            'ToAddresses': [
                email_to,
            ],
        },
        Message={
            'Subject': {
                'Data': email_subject
            },
            'Body': {
                'Text': {
                    'Data': email_body
                }
            }
        }
    )

    return response_ses
