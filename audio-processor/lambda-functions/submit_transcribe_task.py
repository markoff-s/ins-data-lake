import json
import time
import datetime
import boto3
from urllib.request import urlopen

def lambda_handler(event, context):
#
#event = {'BucketName': bucket_name, 'FileName': audio_file, 'CustomerId': customer, 'ClaimId': claim, 'SessionId': session}
#
    # log parameters
    print('SubmitTranscribeTask function parameters: {0}'.format(str(event)))
    
    transcribe=boto3.client("transcribe")

    bucket_name = event["BucketName"]
    file_name = event["FileName"]

    file_url="s3://"+bucket_name+"/"+file_name
    file_type=file_name.split(".")[1]
    
    session=event["SessionId"]
    
    print('Job {0} for file {1} in bucket {2} is started at {3}.'.format(session, file_name, bucket_name, datetime.datetime.now()))
    
    response = transcribe.start_transcription_job(  TranscriptionJobName=session,
                                                Media={'MediaFileUri':file_url},
                                                MediaFormat=file_type,
                                                LanguageCode='en-US',
                                                Settings =  {
                                                                'ShowSpeakerLabels': True,
                                                                'MaxSpeakerLabels': 9   
                                                            })

    status = response["TranscriptionJob"]["TranscriptionJobStatus"]
    print('Job {0} has been submitted with status {1}.'.format(session, status))
    
    response = {
        'Status': status,
        'CustomerId': event['CustomerId'],
        'ClaimId': event['ClaimId'],
        'SessionId': session
    }
    
    #log response
    print('SubmitTranscribeTask function response: {0}'.format(str(response)))
    
    return response
    
# end function