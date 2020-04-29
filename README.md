ins-data-lake 

1. launch CFN stack from cloud-formation/ins-data-lake-stack.yml or https://ins-data-lake-public-artifacts.s3.amazonaws.com/cloud-formation/ins-data-lake-stack.yml

2. data load
2.1 overnight data - drop https://ins-data-lake-public-artifacts.s3.amazonaws.com/test_data/step1-SubmissionsOvernightFeed.csv to "overnight/submissions/" folder in your raw/bronze zone bucket
2.2 watch EMR or Step Functions to complete processing
2.3 intraday data - drop https://ins-data-lake-public-artifacts.s3.amazonaws.com/test_data/step2-assign-undewriter-change-status.xml to "intraday/submissions/" folder in your raw/bronze zone bucket
2.4 watch EMR or Step Functions to complete processing
2.5 run intraday, overnight and reporting Glue crawlers created by the CFN stack

3. data consumption - you should be able to query data in raw/transformed/reporting (bronze/silver/gold) zones from Hive, Presto, Spark, Athena, QuickSight
  
4. test users - CFN stack creates 3 test users - data_lake_admin, data_engineer, data_analyst with password you provide on CFN stack creation. Also, users have different IAM and LakeFormation permissions

5. additional test data, Spark jobs jars and other artifacts can be found here https://ins-data-lake-public-artifacts.s3.amazonaws.com/