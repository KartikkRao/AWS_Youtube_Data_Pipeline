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
import time
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def delete_s3_folder(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    
    bucket.objects.filter(Prefix=prefix).delete()


def transform(df):
    default ={
        "publish_date":'9999-12-31',
        "channel_id":"None",
        "video_title":"None",
        "video_id":"None"
    }
    
    for column,value in default.items():
        df=df.fillna({column:value})
    
    df = (
        df
        .withColumn("publish_date", from_utc_timestamp(col("publish_date").cast("timestamp"), "Asia/Kolkata"))
        .withColumn("date_published", to_date(col("publish_date")))
        .withColumn("publish_time", date_format(col("publish_date"), "HH:mm:ss"))
        .withColumn("publish_time_hours", split(col("publish_time"), ":")[0].cast("int"))
        .withColumn("publish_time_minutes", split(col("publish_time"), ":")[1].cast("int"))
        .drop("publish_date", "publish_time")
    )
    
    df = df.select("date_published","publish_time_hours","publish_time_minutes","region_code","rank","is_current","category_id","channel_id","video_id","video_title","view_count","like_count","comment_count")
    
    return df


location_key = 's3://youtube-data203/raw_data/videos_data/*'
df = spark.read.option("header","true").csv(location_key)

df = transform(df)

write_location = f's3://youtube-data203/transformed_data/videos_data/{datetime.now().strftime("%Y-%m-%d")}' 
df.write.mode("overwrite").option("header","true").format("csv").save(write_location)

bucket_name = 'youtube-data203'
prefix = 'raw_data/videos_data/'
delete_s3_folder(bucket_name, prefix)

job.commit()
