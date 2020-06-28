# Project: Cloud Adoption POC

## Project Overview
An organization is looking to modernize its data analytics application and move it onto the cloud. A POC is required to demonstrate feasability, evaluate architectures and ascertain cost for a pilot set of data pipelines. 

Ask is to:
- Build a data pipelines to transform source data and populate a data warehouse / data lake to support existing on-premise analytics. 
- S3 to be staging area. 
- Minimize impact to upstream systems & downstream consumers.
- History load is not in scope.

### Data
Data would be sourced in CSV format; Reference data would be sourced on an ad-hoc basis as and when the data changes. Transactional data would be sourced three times daily basis. 

Dataset contains three categories of data:

1. **Clients Data**: List of all Clients; CSV format; Estimated volume ~5 mil records; Feed frequency: ad-hoc.
2. **Top Clients Data**: List of all Clients; CSV format; Estimated volume < 1000 records; Feed frequency: ad-hoc.
3. **Transactional Data**: Transactions at a daily grain. Estimated volume < 100k records; Feed frequency: thrice a day.


### Schema
Star Schema made up of two dimensions and two facts would be built. Details:

#### Dimension Tables
1. **clients** - clients dimension
   - client_id, client_name
2. **clients_top** - top clients dimension
   - client_id, top_client_ind

#### Fact Table
3. **fact_detail** - metrics at lowest grain
   - bus_dt, inqr_id, client_id, calc_rslv_dt, case_entr_dt, frst_rslv_dt, last_ropned_dt, ropn_cn, inqr_amt, inqr_amt_ccy, case_own_nm, first_tat, last_tat, top_client_ind
4. **fact_summ** - metrics aggregated at client grain
   - bs_dt, client_id, total_tat, avg_tat, total_value, rslv_cnt


### Architecture

Two options were evaluated as part of this POC:
1. Cloud Data Warehouse
2. Cloud Data Lake - Serverless

#### Option 1 | Cloud Data Warehouse
![Process Flow](https://github.com/nitinx/de-cloud-adoption-poc/blob/master/option1.png)

- Ingestion
	- Data sourced from existing on-premise serves and staged on S3 via DataSync
	- S3 event trigger enabled to invoke Lambda Function on upload of object
	- Lifecycle rule enable to archive objects every 1 day into S3 Glacier

- Transformation
	- Lambda Function(s)
		- Invoked on upload of source file.
		- Transforms data and loads Redshift table(s)

- Analytics
	- Downstream consumers would repoint to Redshift end point for analytics

##### Pre-requisites

An Amazon Web Services [AWS] account with access to following services: 

- [AWS Lambda](https://aws.amazon.com/lambda/)
- [AWS SES](https://aws.amazon.com/ses/)
- [AWS CloudWatch](https://aws.amazon.com/cloudwatch/)
- [AWS IAM](https://aws.amazon.com/iam/)
- [Amazon S3](https://aws.amazon.com/s3/)
- [Amazon Redshift](https://aws.amazon.com/redshift/)

Note: Code base is on **Python 3.7** and **PySpark**.

##### Code

Three Python files:

- `/option1/lambda_s3redshift_clients.py`: Lambda function to transform source data and populate Redshift.
- `/option1/lambda_s3redshift_investigations.py`: Lambda function to transform source data and populate Redshift.
- `/option1/redshift_1_CREATEs.sql`: CREATE statements for Redshift tables.
- `/option1/redshift_2_COPYs.sql`: COPY statements to load data from S3 to Redshift.
- `/option1/redshift_3_INSERTs.sql`: INSERT statement to load data from stage to fact within Redshift.
- `/option1/prototype_redshift.ipynb`: Jupyter notebook for locally prototyping code.


#### Option 2 | Cloud Data Lake - Serverless
![Process Flow](https://github.com/nitinx/de-cloud-adoption-poc/blob/master/option2.png)

- Ingestion
	- Data sourced from existing on-premise serves and staged on S3 via DataSync
	- S3 event trigger enabled to invoke Lambda Function on upload of object
	- Lifecycle rule enable to archive objects every 1 day into S3 Glacier

- Transformation
	- Lambda Function(s)
		- Invoked on upload of source file.
		- For smaller datasets, transformation would be carried out within the function
		- For larger datasets, Glue Transform job would be invoked for transformation
	- Glue ETL(s)
		- Transforms source data into partitioned parquet format.
		- Invokes Glue Crawler job

- Analytics
	- Glue Crawler would update Data Catalogue for usage by Athena
	- Downstream consumers would repoint to Athena end point for analytics

##### Pre-requisites

An Amazon Web Services [AWS] account with access to following services: 

- [AWS Lambda](https://aws.amazon.com/lambda/)
- [AWS Glue](https://aws.amazon.com/glue/)
- [AWS SES](https://aws.amazon.com/ses/)
- [AWS CloudWatch](https://aws.amazon.com/cloudwatch/)
- [AWS IAM](https://aws.amazon.com/iam/)
- [Amazon S3](https://aws.amazon.com/s3/)
- [Amazon Athena](https://aws.amazon.com/athena/)

Note: Code base is on **Python 3.7** and **PySpark**.

##### Code

Three Python files:

- `/option2/lambda_s3glue_clients.py`: Lambda function to trigger Glue Job to process source data.
- `/option2/lambda_s3glue_investigations.py`: Lambda function to trigger Glue Job to process source data.
- `/option2/glue_transform_clients.py`: Glue ETL to transform CSV into Parquets.
- `/option2/glue_transform_investigations.py`: Glue ETL to transform CSV into partitioned Parquets and invoke Glue Crawler for cataloging.
- `/option2/prototype_spark.ipynb`: Jupyter notebook for locally prototyping code.


### Cost Comparison

- Rough cost estimates/month
- Additional cost for connectivity [Direct Connect / VPN] and data transfer to be factored in

![Process Flow](https://github.com/nitinx/de-cloud-adoption-poc/blob/master/costcomparison.png)
