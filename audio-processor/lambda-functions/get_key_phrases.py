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

    target_folder = os.environ['COMPREHEND_OUTPUT'] + '/KeyPhrases/' + str(customer) + '/' + str(claim) + '/' + base_key + '/'

    target_file =  target_folder + base_key + '.csv'
    
    s3 = boto3.client('s3')
    
    file = s3.get_object(Bucket = bucket, Key = file_name)
    transcribe_result = json.loads(file["Body"].read())
    
    comprehend = boto3.client('comprehend')
    
    tmp_file = '/' + target_file.replace(target_folder, 'tmp/')
        
    print('Temp csv file has been created: {0}'.format(tmp_file))
    print('Target file is {0}'.format(target_file))
        
    with open(tmp_file, 'w') as outfile:
        fieldnames = ['speaker_label', 'text', 'score', 'begin_offset', 'end_offset']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
    
        for channel in transcribe_result['Channels']:
            response = comprehend.detect_key_phrases(Text = channel['Text'], LanguageCode = 'en')
            
            for key_phrase in response['KeyPhrases']:
                writer.writerow({   'speaker_label': channel['SpeakerLabel'], 
                                    'text': key_phrase['Text'], 
                                    'score': key_phrase['Score'], 
                                    'begin_offset':  key_phrase['BeginOffset'], 
                                    'end_offset':  key_phrase['EndOffset']})

    s3.upload_file(tmp_file, bucket, target_file)
    
    #glue = boto3.client('glue')
    #glue.start_job_run(JobName = 'KeyPhrases2Parquet', Arguments = {})

    return {"Status": 200}
