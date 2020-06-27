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
    
    if file_name == 'opt3/small/clients_top.csv':
        print("Top Client | Before Query 1")
        REDSHIFT_QUERY1 = "TRUNCATE TABLE dim_clients_top;"
        
        print("Top Client | Before Query 2")
        REDSHIFT_QUERY2 = """
        copy dim_clients_top from 's3://cmpoc-raw/opt3/small/clients_top.csv' 
        iam_role '{}' 
        ignoreheader as 1
        delimiter ','
        region 'us-east-1';
        """.format(REDSHIFT_ROLE_ARN)
    else:
        print("All Client | Before Query 1")
        REDSHIFT_QUERY1 = "TRUNCATE TABLE dim_clients;"
    
        print("All Client | Before Query 2")
        REDSHIFT_QUERY2 = """
        copy dim_clients from 's3://cmpoc-raw/opt3/small/clients_all.csv' 
        iam_role '{}' 
        ignoreheader as 1
        delimiter ','
        region 'us-east-1';
        """.format(REDSHIFT_ROLE_ARN)

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
