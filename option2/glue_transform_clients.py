import os
import sys
import boto3

from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

from pyspark.sql import Row, Window, SparkSession
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import to_date, when, datediff
import pyspark.sql.functions as psf

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
#spark = glueContext.spark_session
spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "600").getOrCreate()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")

### Main data processing
input_dir = "s3://cmpoc-raw/opt2/small/"
output_dir = "s3://cmpoc-sink/opt2/output/"

## Read source datasets
df_clients_all = spark.read.csv(input_dir + 'clients_all.csv', header=True, inferSchema=True)
df_clients_top = spark.read.csv(input_dir + 'clients_top.csv', header=True, inferSchema=True)

## Write out result set to S3 in Parquet format
df_clients_all.write.mode('overwrite').parquet(output_dir + 'dim_clients/')
df_clients_top.write.mode('overwrite').parquet(output_dir + 'dim_clients_top/')

job.commit()
