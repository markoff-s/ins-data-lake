ins-data-lake 

Provided you have a brand new AWS account, here're the configuration steps:

1. configure lake formation admin - go to lake formation and make yourself (or user you're going to run the cft stack under) a data lake admin

1. configure QuickSight - go to QS and setup an account with Athena access (this will create a QS role that is used in the cft template)

1. launch cft stack from cloud-formation/ins-data-lake-stack.yml or https://ins-data-lake-public-artifacts.s3.amazonaws.com/cloud-formation/ins-data-lake-stack.yml

1. data load
   1. overnight data - drop https://ins-data-lake-public-artifacts.s3.amazonaws.com/test_data/step1-SubmissionsOvernightFeed.csv to "overnight/submissions/" folder in your raw/bronze zone bucket
   1. watch EMR or Step Functions to complete processing
   1. intraday data - drop https://ins-data-lake-public-artifacts.s3.amazonaws.com/test_data/step2-assign-undewriter-change-status.xml to "intraday/submissions/" folder in your raw/bronze zone bucket
   1. watch EMR or Step Functions to complete processing
   1. run intraday, overnight and reporting Glue crawlers created by the CFN stack

1. data consumption - you should be able to query data in raw/transformed/reporting (bronze/silver/gold) zones from Hive, Presto, Spark, Athena, QuickSight
  
1. test users - cft stack creates 3 test users - data_lake_admin, data_engineer, data_analyst with password you provide on CFN stack creation. Also, users have different IAM and LakeFormation permissions

1. additional test data, Spark jobs jars and other artifacts can be found here https://ins-data-lake-public-artifacts.s3.amazonaws.com/