import json
import datetime
import time
import boto3

def lambda_handler(event, context):
#
#event = {'TranscribeJobName': 'JobName', 'WaitTime': 20}
#
    #log parameters
    print('Get TranscribeJobStatus function parameters: {0}'.format(str(event)))

    transcribe_client=boto3.client("transcribe")
    
    print('Parameters: {0}'.format(str(event)))
    
    job_name = event["SessionId"]
    
    transcribe_response = transcribe_client.get_transcription_job(TranscriptionJobName = job_name)
    
    status = transcribe_response["TranscriptionJob"]["TranscriptionJobStatus"]
    
    print('Job {0} has status {1} at {2}.'.format(job_name, status, datetime.datetime.now()))
    
    if status in ['COMPLETED', 'FAILED']:
        result_url = transcribe_response["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
    else:
        result_url = ''

    response = {
        'ResultUri': result_url,
        'Status': status,
        'CustomerId': event['CustomerId'],
        'ClaimId': event['ClaimId'],
        'SessionId': job_name
    
    }

    #log response
    print('Get TranscribeJobStatus function response: {0}'.format(str(response)))

    return response
# end function