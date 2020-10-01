# Project: Cloud Adoption POC

## Overview
An organization is looking to modernize its data analytics application and migrate it onto the cloud. To demonstrate feasability, evaluate architectures and ascertain cost for a pilot set of data pipelines, there is a need for a POC. 

#### Scope
- Build data pipelines to transform source data and populate a data warehouse / data lake to support existing on-premise analytics. 
- S3 to be staging area. 
- Minimize impact to upstream systems & downstream consumers.
- History load is not in scope.

#### Benefits
- Reduced infrastructure cost with pay-as-you-go model
  - Elastic environments (for ex. services can be scaled up during peak analytic/reporting windows and then scaled down)
  - Pause and resume services based on usage (for ex. lower environments can be paused during weekends)
  - ETL services billed only during job execution time
- Reduced maintenance cost due to Serverless architecture / managed services. AWS takes care of OS patching, upgrades etc. for underlying infrastructure.
- Performance gain with modern tech stack (Apache Spark / Redshift’s COPY command)


## Data
Data would be sourced in CSV format; Reference data would be sourced on an ad-hoc basis as and when the data changes. Transactional data would be sourced three times daily basis. 

![Process Flow](https://github.com/nitinx/de-cloud-adoption-poc/blob/master/images/scope.png)

Dataset contains three categories of data:

1. **Clients Data**: List of all Clients; CSV format; Estimated volume ~5 mil records; Feed frequency: ad-hoc.
2. **Top Clients Data**: List of selected Top Clients; CSV format; Estimated volume < 1000 records; Feed frequency: ad-hoc.
3. **Transactional Data**: Transactions at a daily grain. Estimated volume < 100k records; Feed frequency: thrice a day.


## Schema
Star Schema made up of two dimensions and two facts would be built. Details:

#### Dimension Tables
1. **clients** - clients dimension
   - client_id, client_name
2. **clients_top** - top clients dimension
   - client_id, top_client_ind

#### Fact Tables
3. **fact_detail** - metrics at lowest grain
   - bus_dt, inqr_id, client_id, calc_rslv_dt, case_entr_dt, frst_rslv_dt, last_ropned_dt, ropn_cn, inqr_amt, inqr_amt_ccy, case_own_nm, first_tat, last_tat, top_client_ind
4. **fact_summ** - metrics aggregated at client grain
   - bs_dt, client_id, total_tat, avg_tat, total_value, rslv_cnt


## Architecture

Two options were evaluated as part of this POC:
1. Cloud Data Warehouse
2. Cloud Data Lake - Serverless

### Option 1 | Cloud Data Warehouse
![Process Flow](https://github.com/nitinx/de-cloud-adoption-poc/blob/master/images/option1.png)

- On-premise Integration / Data Migration
  - AWS DataSync agent to be installed on-premise to transfer files to cloud via AWS Direct Connect. 
  - AWS Data Migration Service could be leveraged to migrate data over from on-premise Oracle to Amazon Redshift. 

- Storage
  - Amazon S3 to host landing/staging area. Lifecycle rules setup to transition (to S3 Glacier) and expiry source feeds. Amazon Redshift to host data mart.

- Compute
  - AWS Lambda function triggered on file arrival and ELT into Amazon Redshift. 

- Analytics
  - Tableau connects natively to Amazon Redshift. JDBC & ODBC drivers also available. 

- Monitoring
  - AWS Lambda in conjunction with Amazon CloudWatch used for monitoring. 

#### Pre-requisites

An Amazon Web Services [AWS] account with access to following services: 

- [AWS Lambda](https://aws.amazon.com/lambda/)
- [AWS SES](https://aws.amazon.com/ses/)
- [AWS CloudWatch](https://aws.amazon.com/cloudwatch/)
- [AWS IAM](https://aws.amazon.com/iam/)
- [Amazon S3](https://aws.amazon.com/s3/)
- [Amazon Redshift](https://aws.amazon.com/redshift/)

Note: Code base is on **Python 3.7** and **PySpark**.

#### Code

Two Lambda Functions (Python): 
- `/option1/lambda_s3redshift_clients.py`: Lambda function to transform source data and populate Redshift.
- `/option1/lambda_s3redshift_investigations.py`: Lambda function to transform source data and populate Redshift.

Redshift SQLs: 
- `/option1/redshift_1_CREATEs.sql`: CREATE statements for Redshift tables.
- `/option1/redshift_2_COPYs.sql`: COPY statements to load data from S3 to Redshift.
- `/option1/redshift_3_INSERTs.sql`: INSERT statement to load data from stage to fact within Redshift.

Jupyter Notebook for Prototyping:
- `/option1/prototype_redshift.ipynb`: Jupyter notebook for locally prototyping code.


### Option 2 | Cloud Data Lake - Serverless
![Process Flow](https://github.com/nitinx/de-cloud-adoption-poc/blob/master/images/option2.png)

- On-premise Integration
  - AWS DataSync agent to be installed on-premise to transfer files to cloud via AWS Direct Connect. 

- Storage
  - Amazon S3 to host landing/staging area and the data lake. Lifecycle rules setup to transition (to S3 Glacier) and expire source feeds. 

- Compute
  - AWS Lambda function triggered on file arrival. Transformation of small datasets by Lambda, larger datasets by AWS Glue (leverages Apache Spark). Data transformed and stored in Parquet format. 

- Analytics
  - Glue Crawler would update Data Catalogue for usage by Athena.
  - Transformed data would be exposed via Amazon Athena for analytics. Supports standard SQL. Tableau connects natively. JDBC & ODBC drivers also available. 
  - Column-level access control can be provided by leveraging AWS Lake Formation. 

- Monitoring
  - AWS Lambda in conjunction with Amazon CloudWatch used for monitoring. 


#### Pre-requisites

An Amazon Web Services [AWS] account with access to following services: 

- [AWS Lambda](https://aws.amazon.com/lambda/)
- [AWS Glue](https://aws.amazon.com/glue/)
- [AWS SES](https://aws.amazon.com/ses/)
- [AWS CloudWatch](https://aws.amazon.com/cloudwatch/)
- [AWS IAM](https://aws.amazon.com/iam/)
- [Amazon S3](https://aws.amazon.com/s3/)
- [Amazon Athena](https://aws.amazon.com/athena/)

Note: Code base is on **Python 3.7** and **PySpark**.

#### Code

Two Lambda Functions (Python): 
- `/option2/lambda_s3glue_clients.py`: Lambda function to trigger Glue Job to process source data.
- `/option2/lambda_s3glue_investigations.py`: Lambda function to trigger Glue Job to process source data.

Two Glue Scripts (PySpark):
- `/option2/glue_transform_clients.py`: Glue ETL to transform CSV into Parquets.
- `/option2/glue_transform_investigations.py`: Glue ETL to transform CSV into partitioned Parquets and invoke Glue Crawler for cataloging.

Jupyter Notebook for Prototyping:
- `/option2/prototype_spark.ipynb`: Jupyter notebook for locally prototyping code.


## Price Comparison

- Rough annual estimate for Production environment (DR & lower environments additional)
- Data transfer cost not factored in
- Assumed data scanned annually by Athena is 120 GB

![Process Flow](https://github.com/nitinx/de-cloud-adoption-poc/blob/master/images/costcomparison.png)
