import os
import json
import urllib.parse
import boto3
import logging
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

print('Loading function')

s3 = boto3.client('s3')
ses = boto3.client('ses')
glue = boto3.client('glue', region_name='us-east-1')

# Variables for the job: 
email_from = os.environ['email_from']
email_to = os.environ['email_to']
glue_job_name = os.environ['glue_job_name']

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

    email_subject = 'Investigations | Feed Arrived [large] | ' + file_name
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

    #logger.info('## TRIGGERED BY EVENT: ')
    #logger.info(event['detail'])
    print("Glue Job Name: " + glue_job_name)

    response_glue = glue.start_job_run(JobName = glue_job_name)
    logger.info('## STARTED GLUE JOB: ' + glue_job_name)
    logger.info('## GLUE JOB RUN ID: ' + response_glue['JobRunId'])
    return response_ses
