import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import requests
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime 
import boto3
import json
import time
import math

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

api_key = 'Your_key'

read_location = f's3://youtube-data203/transformed_data/videos_data/{datetime.today().date()}/*'
data = spark.read.option("header","true").csv(read_location)
d = data.select(col("channel_id")).distinct().rdd.flatMap(lambda x:x).collect()

a = math.ceil(len(d)/50)
b=0
c=50
channel_url = 'https://www.googleapis.com/youtube/v3/channels'
channel_info =[]
channel_info_raw =[]

for i in range(a):
    if i == (a-1):
        lis = d[b:]
    else:
        lis = d[b:c]
    
    b= c
    c= c+50

    string_lis = ",".join(map(str,lis))
    params4 = {
            "part": "snippet,statistics",
            "id": string_lis ,
            "maxResults":50,
            "key":api_key
           }
    
    res4 = requests.get(channel_url,params=params4)

    if res4.status_code == 200:

        channel_data = res4.json()

        channel_info_raw.append(channel_data)
        if "items" in channel_data:
            for item in channel_data["items"]:

                row = (
                item.get("id", ''),
                item["snippet"].get("title", 'Not_found'),
                item["snippet"].get("publishedAt", '9999-12-31'),
                item["snippet"].get("country", 'Not_found'),
                int(item["statistics"].get("viewCount", 0)),
                int(item["statistics"].get("subscriberCount", 0)),
                int(item["statistics"].get("videoCount", 0)),
                datetime.today().date(),
                "9999-12-31",
                1
                )

                channel_info.append(row)
                
channel_df = spark.createDataFrame(channel_info,["channel_id","channel_name","date_created","country_of_origin","view_count","subscriber_count","video_count","start_date","end_date","is_current"])

channel_df = (
    channel_df
    .withColumn("date_created", from_utc_timestamp(col("date_created").cast("timestamp"), "Asia/Kolkata"))
    .withColumn("date_created", to_date(col("date_created")))
    .withColumn("end_date", col("end_date").cast(DateType()))
)

write_raw_location=f's3://youtube-data203/raw_data/unfiltered_raw/channel_data/{datetime.today().date()}'
write_location =f's3://youtube-data203/transformed_data/channel_data/{datetime.today().date()}'

channel_df.write.mode("overwrite").option("header","true").format("csv").save(write_location)

json_channel_raw = spark.createDataFrame(channel_info_raw)
json_channel_raw.write.mode("overwrite").json(write_raw_location)


job.commit()
