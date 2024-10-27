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

region_url = 'https://www.googleapis.com/youtube/v3/i18nRegions'
params = {'part':'snippet',
          'key': api_key
          }
res = requests.get(region_url, params=params )

if res.status_code == 200:
    data = res.json()
    
    region_list = []
    for value in data['items']:
        region_data = (value['snippet']['gl'], value['snippet']['name'])
        region_list.append(region_data)
        
    df = spark.createDataFrame(region_list,['code','name'])
    region_key = f's3://youtube-data203/transformed_data/region_data/{datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")}'
    df.write.mode("overwrite").option("header","true").format("csv").save(region_key)
else:
    print(f"Failed to download {region_url}: {res.status_code}")


category_url = 'https://www.googleapis.com/youtube/v3/videoCategories'
num = ",".join(map(str,[i for i in range(1,45)]))
params1 = {'part':'snippet',
            'key':api_key,
            'id':num
        }
res1 = requests.get(category_url,params=params1)

if res1.status_code == 200:
    category_data = res1.json()
    category_list = []
    for value in category_data['items']:
        row = (value['id'],value['snippet']['title'],value['snippet']['assignable'])
        category_list.append(row)
        
    df1 = spark.createDataFrame(category_list,['id','category_name','assignable'])
    region_key1 = f's3://youtube-data203/transformed_data/category_data/{datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")}'
    df1.write.mode("overwrite").option("header","true").format("csv").save(region_key1)
else:
    print(f"download failed {category_url}:{res1.status_code}")

job.commit()
