import json
import boto3
import time
from datetime import datetime


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    redshift_data = boto3.client('redshift-data')

    REDSHIFT_WORKGROUP_NAME = 'data-engineering-workgroup'  
    REDSHIFT_DATABASE = 'youtube-data'
    REDSHIFT_IAM_ROLE = 'your_arns' 
    
    sql = f""" 
            COPY "staging-tables".video_data
            FROM 's3://youtube-data203/transformed_data/videos_data/{datetime.today().date()}/'
            IAM_ROLE 'your_arns'
            CSV
            IGNOREHEADER 1
            DELIMITER ',';

            COPY "staging-tables".channel_data
            FROM 's3://youtube-data203/transformed_data/channel_data/{datetime.today().date()}/'
            IAM_ROLE 'your_arns'
            CSV
            IGNOREHEADER 1
            DELIMITER ',';

            """

    try:

        response = redshift_data.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP_NAME,
            Database=REDSHIFT_DATABASE,
            Sql=sql
        ) 

        statement_id = response['Id']

        while True:
            status_response = redshift_data.describe_statement(Id=statement_id)
            status = status_response['Status']

            if status in ['FINISHED', 'FAILED','ABORTED']:
                break
            time.sleep(5)

        if status in ['FAILED','ABORTED']:
            return{
                'statusCode': 500,
                'success':False,
                'error_details' : status_response.get('Error', 'No details available')
            }

        else:
            return{
                'statusCode': 200,
                'success':True
            }
    
    except Exception as e:
        return {
            'success':False,
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error executing SQL script',
                'error': str(e)
            })
        }
