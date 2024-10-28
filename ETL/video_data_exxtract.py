import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import requests
from pyspark.sql.functions import *
from datetime import datetime 
import boto3
import json
import time

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

api_key = 'Your_key'
youtube_url = 'https://www.googleapis.com/youtube/v3/videos'
video_list = []
video_raw = []

reg_key = 's3://youtube-data203/transformed_data/region_data/2024-10-27_17-43-08-379867/'
data_code = spark.read.option("header","true").option("inferSchema","true").csv(reg_key)
country_code = data_code.select('code').rdd.flatMap(lambda x:x).collect()


for region in country_code:
    params3 = {
            'part': 'snippet,statistics',
            'chart': 'mostPopular',
            'regionCode': region,
            'maxResults': 50,
            'key': api_key
        }

    res3 = requests.get(youtube_url,params=params3)
    if res3.status_code == 200:
        video_data = res3.json()
        
        video_raw.append(video_data)
        
        i=1
        for data in video_data['items']:
            rows = (
            data['snippet'].get('publishedAt', ''),
            data['snippet'].get('channelId', ''),
            data['snippet'].get('title', ''),
            data.get('id', ''),
            data['snippet'].get('channelTitle', ''),
            int(data['snippet'].get('categoryId', '0')),
            int(data['statistics'].get('viewCount', 0)),
            int(data['statistics'].get('likeCount', 0)),
            int(data['statistics'].get('commentCount', 0)),
            region, i
            )
            i=i+1;
    
            video_list.append(rows)
            
time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
df = spark.createDataFrame(video_list,['publish_date','channel_id','video_title','video_id','channel_name','category_id','view_count','like_count','comment_count','region_code','rank'])
raw_path = f's3://youtube-data203/raw_data/videos_data/{time}'
df.write.mode("overwrite").option("header","true").format("csv").save(raw_path)


unfiltered_raw_path = f's3://youtube-data203/raw_data/unfiltered_raw/videos_data/{time}'
json_df = spark.createDataFrame(video_raw)
json_df.write.mode("overwrite").json(unfiltered_raw_path)

job.commit()
