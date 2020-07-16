import json
import boto3
import datetime
import os
import csv

def lambda_handler(event, context):
    # event = {'Bucket': bucket_name, 'RootFolder': os.environ['TRANSCRIBE_ROOT_FOLDER'], 'CustomerId': customer, 'ClaimId': claim, 'SessionId': session}
    
    print('GetSentiment function has been executed with parameters {0} at {1}'.format(str(event), datetime.datetime.now()))

    bucket = event["Bucket"]
    customer = event["CustomerId"]
    claim = event["ClaimId"]
    base_key = event["SessionId"]

    file_name = event['RootFolder'] + '/' + str(customer) + '/' + str(claim) + '/' + base_key + '/' + base_key + '.json'
    
    print('Source file name is {0}'.format(file_name))

    target_folder = os.environ['COMPREHEND_OUTPUT'] + '/Entities/' + str(customer) + '/' + str(claim) + '/' + base_key + '/'

    target_file =  target_folder + base_key + '.csv'
    
    s3 = boto3.client('s3')
    
    file = s3.get_object(Bucket = bucket, Key = file_name)
    transcribe_result = json.loads(file["Body"].read())
    
    comprehend = boto3.client('comprehend')
    
    tmp_file = '/' + target_file.replace(target_folder, 'tmp/')
        
    print('Temp csv file has been created: {0}'.format(tmp_file))
    print('Target file is {0}'.format(target_file))
        
    with open(tmp_file, 'w') as outfile:
        fieldnames = ['speaker_label', 'score', 'text', 'type', 'begin_offset', 'end_offset']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
    
        for channel in transcribe_result['Channels']:
            response = comprehend.detect_entities(Text = channel['Text'], LanguageCode = 'en')
            
            for entity in response['Entities']:
                writer.writerow({   'speaker_label': channel['SpeakerLabel'], 
                                    'score': entity['Score'], 
                                    'text': entity['Text'], 
                                    'type': entity['Type'], 
                                    'begin_offset':  entity['BeginOffset'], 
                                    'end_offset':  entity['EndOffset']})

    s3.upload_file(tmp_file, bucket, target_file)
    
    #tmp_parquet = tmp_file.replace('csv', 'parquet')
    #df = pd.read_csv(tmp_file)
    #df.to_parquet(tmp_parquet)
    
    #gold_zone_bucket = os.environ['GOLD_ZONE_BUCKET']

    #s3.upload_file(tmp_parquet, gold_zone_bucket, tmp_parquet.split('/')[1])
    
    #glue = boto3.client('glue')
    #glue.start_job_run(JobName = 'Entities2Parquet', Arguments = {})

    return {"Status": 200}
