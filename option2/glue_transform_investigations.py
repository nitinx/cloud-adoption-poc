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
input_dir = "s3://cmpoc-raw/opt2/large"
output_dir = "s3://cmpoc-sink/opt2/output/"
clients_top = "s3://cmpoc-sink/opt2/output/clients_top"

## Read source datasets
df_investigations = spark.read.csv(input_dir, header=True, inferSchema=True)

## Read dimensions
df_clients_top = spark.read.csv('s3://cmpoc-raw/opt2/small/clients_top.csv', header=True, inferSchema=True)
#df_clients_top = spark.read.parquet(clients_top)

## Convert string types to dates
df_investigations = df_investigations \
  .withColumn('bus_dt', to_date(df_investigations.bus_dt, 'dd-MM-yyyy')) \
  .withColumn('calc_rslv_dt', to_date(df_investigations.calc_rslv_dt, 'dd-MM-yyyy')) \
  .withColumn('case_entr_dt', to_date(df_investigations.case_entr_dt, 'dd-MM-yyyy')) \
  .withColumn('frst_rslv_dt',to_date(df_investigations.frst_rslv_dt, 'dd-MM-yyyy')) \
  .withColumn('last_ropned_dt',to_date(df_investigations.last_ropned_dt, 'dd-MM-yyyy'))

## Compute TATs
df_investigations = df_investigations \
  .withColumn('first_tat',datediff(df_investigations.frst_rslv_dt, df_investigations.case_entr_dt)) \
  .withColumn('last_tat',datediff(df_investigations.calc_rslv_dt, df_investigations.last_ropned_dt))

## Add YEAR, MONTH & DAY columns for partitioning
df_investigations = df_investigations \
  .withColumn('year', df_investigations['bus_dt'].substr(1, 4)) \
  .withColumn('month', df_investigations['bus_dt'].substr(6, 2)) \
  .withColumn('day', df_investigations['bus_dt'].substr(9, 2))

## Lookup Top Clients dataset
df_detail = df_investigations.join(df_clients_top, on=['client_id'], how='left_outer')

## Compute Aggregate Metrics
df_summ = df_detail.groupBy('bus_dt','client_id').agg( \
  psf.sum(df_detail.first_tat + df_detail.last_tat).alias('total_tat'),\
  psf.avg(df_detail.first_tat + df_detail.last_tat).alias('avg_tat'),\
  psf.sum('inqr_amt').alias('total_value'),\
  psf.count('inqr_id').alias('rslv_cnt'))

## Convert string types to dates
df_summ = df_summ.withColumn('bus_dt',to_date(df_investigations.bus_dt, 'dd-MM-yyyy'))

## Add YEAR, MONTH & DAY columns for partitioning
df_summ = df_summ \
  .withColumn('year', df_summ['bus_dt'].substr(1, 4)) \
  .withColumn('month', df_summ['bus_dt'].substr(6, 2)) \
  .withColumn('day', df_summ['bus_dt'].substr(9, 2))

## Write out result set to S3 in Parquet format
df_detail.write.partitionBy('year','month','day').mode('overwrite').parquet(output_dir + 'fact_detail/')
df_summ.write.partitionBy('year','month','day').mode('overwrite').parquet(output_dir + 'fact_summ/')

job.commit()
