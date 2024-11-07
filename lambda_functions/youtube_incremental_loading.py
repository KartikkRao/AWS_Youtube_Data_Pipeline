import json
import boto3
import time


def lambda_handler(event, context):

    s3 = boto3.client('s3')
    redshift_data = boto3.client('redshift-data')

    REDSHIFT_WORKGROUP_NAME = 'data-engineering-workgroup'  
    REDSHIFT_DATABASE = 'youtube-data'
    REDSHIFT_IAM_ROLE = 'your_arns'

    bucket = 'youtube-data203'
    key = 'sql_scripts/youtube_incremental_loading.sql'

    sql_file_obj = s3.get_object(Bucket=bucket, Key=key)
    sql_script = sql_file_obj['Body'].read().decode('utf-8')

    try:

        response = redshift_data.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP_NAME,
            Database=REDSHIFT_DATABASE,
            Sql=sql_script,
            WithEvent=True
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
                'loading_success':False,
                'error_details' : status_response.get('Error', 'No details available')
            }

        else:
            return{
                'statusCode': 200,
                'loading_success':True
            }
    
    except Exception as e:
        return {
            'loading_success':False,
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error executing SQL script',
                'error': str(e)
            })
        }
