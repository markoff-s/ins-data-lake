ins-data-lake 

**1. Provided you have a brand new AWS account, here're the configuration steps:**

1. configure lake formation admin - go to lake formation and make yourself (or user you're going to run the cft stack under) a data lake admin

1. configure QuickSight - go to QS and setup an account with Athena access, this will create a QS role that is used in the cft template (this will be improved so that this role is created as part of the stack)
------------------- Deploy and run Data Lake --------------------------------------------------------------------------------------------------------------
1. launch cft stack from cloud-formation/ins-data-lake-stack.yml or https://ins-data-lake-public-artifacts.s3.amazonaws.com/cloud-formation/ins-data-lake-stack.yml

1. data load
   1. overnight data - drop https://ins-data-lake-public-artifacts.s3.amazonaws.com/test-data/overnight-submissions/overnight_submissions_new_business_sub_id_1001_cnt_10k_10d_from_now.csv to "overnight/submissions/" folder in your raw/bronze zone bucket
   1. watch EMR or Step Functions to complete processing
   1. intraday data - drop https://ins-data-lake-public-artifacts.s3.amazonaws.com/test-data/step2-assign-undewriter-change-status.xml to "intraday/submissions/" folder in your raw/bronze zone bucket
   1. watch EMR or Step Functions to complete processing
   1. intraday, overnight and reporting Glue crawlers will be run automatically after completion of overnight data import. otherwise, run crawlers automatically

1. data consumption - you should be able to query data in raw/transformed/reporting (bronze/silver/gold) zones from Hive, Presto, Spark, Athena, QuickSight
   1. Note: Grand AWS Lake Formation Permissions to CrowlerRole and Quicksight role on table hudi_submissions table, this is not covered by CF template
   1. Note: Grand AWS Lake Formation Permissions to CrowlerRole and Quicksight role on table sentiment table, this is not covered by CF template 
  
1. test users - cft stack creates 3 test users - data_lake_admin, data_engineer, data_analyst with password you provide on CFN stack creation. Also, users have different IAM and LakeFormation permissions

1. additional test data, Spark jobs jars and other artifacts can be found here https://ins-data-lake-public-artifacts.s3.amazonaws.com/

------------------ Deploy and Run Sentiment analysis on the top of Data Lake ---------------------------------------------------------------------------------
1. launch cft stack from audio-processor/cloud-formation https://github.com/markoff-s/ins-data-lake/blob/develop/audio-processor/cloud-formation/audio-recog.yml
1. upload audio files from audio-processor/incoming files folder https://github.com/markoff-s/ins-data-lake/tree/develop/audio-processor/incoming-files/audio
to S3 AudioSourceBucket (drag & drop all folder "audio")
1. drom any of json files from https://github.com/markoff-s/ins-data-lake/tree/develop/audio-processor/incoming-files to AudioSourceBucket
1. wait for audio processing step function completion
1. run sentiment crawler and access data from Athena, Hive, Presto, QuickSight etc.


**2. Test data generation - use /load-tests/submissions-generator/submissions-generator.py**

1.	Download & extract NAD_r3.txt and save to the new directory: https://www.transportation.gov/content/national-address-database-disclaimer
1. Change the path to source file on line 36
1.	Change the value on line 22 to match the # of records you wish to create
1.	The first time you run the python code you may get errors about missing libraries (install them with pip as needed)
1.	A file called submissions.csv will be created in the new directory
