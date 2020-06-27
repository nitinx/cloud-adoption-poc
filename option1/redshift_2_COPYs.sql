    copy dim_clients from 's3://cmpoc-raw/opt3/small/clients_all.csv' 
    iam_role '{}' 
    ignoreheader as 1
    delimiter ','
    region 'us-east-1';
	

    copy dim_clients_top from 's3://cmpoc-raw/opt3/small/clients_top.csv' 
    iam_role '{}' 
    ignoreheader as 1
    delimiter ','
    region 'us-east-1';
	
	
    copy stg_investigations from 's3://cmpoc-raw/opt3/large' 
    iam_role '{}' 
    ignoreheader as 1
    delimiter ','
    region 'us-east-1';
	
