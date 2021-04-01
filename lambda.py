import json
import boto3
from datetime import date, datetime
import logging

print('Loading function')
today = str(date.today()) #Getting the date today in string
now = str(datetime.now()) #Getting the date and time real time in string
s3 = boto3.client('s3') #Calling the S3 service
glue = boto3.client('glue') #Calling the Glue Service
logging.basicConfig(filename='{filename}', filemode='w', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

def lambda_handler(event, context):
    logging.info("Starting function")
    #date_today = today.strftime("%y/%m/%d") Not sure but I plan to use it somewhere probably creating an S3 folder
    bucket = "{bucketname}" 
    key = "{key}"
    logging.info("Identified the bucket and file")
    try:
        logging.info("Getting the files")
        response = s3.get_object(Bucket=bucket, Key=(key))
        logging.info("CONTENT TYPE: " + response['ContentType'])
        logging.info("Running the Glue Job")
        glue.start_job_run(
            JobName = 'project-m-job',
            Arguments = {}
            )
        logging.info("Glue Job Successfully ran")
        return response['ContentType']
    except Exception as e:
        logging.exception("Exception occurred: Error getting object from {bucket}")
        #print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
