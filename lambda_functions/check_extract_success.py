import json
import boto3

def lambda_handler(event, context):
    bucket = 'youtube-data203'
    key = 'raw_data/videos_data/'

    s3 = boto3.client('s3')

    res = s3.list_objects_v2(
        Bucket = bucket,
        Prefix = key 
    )
    count = 0
    if 'Contents' in res:
        for obj in res['Contents']:
            if obj['Key'].endswith('.csv'):
                count = 1
                break

    if count == 1:
        return{
            'statusCode':200,
            'file_exists': True
        }
    else:
        return{
            'statusCode':200,
            'file_exists':False
        }
