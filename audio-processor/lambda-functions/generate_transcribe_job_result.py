import json
import time
import datetime
import boto3
import os
from urllib.request import urlopen

def lambda_handler(event, context):
#
#event = {'ResultUri': 'https://...', 'Status': status}
#
    #log parameters
    print('Get GenerateTranscribeJobResults function parameters: {0}'.format(str(event)))

    s3 = boto3.client("s3")
    
    result_url = event["ResultUri"]
    customer = event['CustomerId']
    claim = event['ClaimId']
    session = event['SessionId']
    
    bucket_name = os.environ['SILVER_BUCKET']
    out_suffix = os.environ['TRANSCRIBE_ROOT_FOLDER'] + '/' + str(customer) + '/' + str(claim) + '/' + str(session) + '/'
    
    print('Transcription result URL: {}'.format(result_url))
    print('Target path on bucket {0} is {1}'.format(bucket_name, out_suffix))
    
    transcription_result = json.load(urlopen(result_url))
    
    print('Transcription result:{0}'.format(str(transcription_result)))

    speakers = list()
    speaker_count = transcription_result['results']['speaker_labels']['speakers']
    
    segments = transcription_result['results']['speaker_labels']['segments']
    items = transcription_result['results']['items']
    
    for item in items:
        next_segment_flag = False
        for segment in segments:
            for segment_item in segment['items']:
                if item['type'] == 'pronunciation' and segment_item['end_time'] == item['end_time'] and segment_item['start_time'] == item['start_time']:
                    next_index = items.index(item) + 1
    
                    if next_index < len(items) and items[items.index(item) + 1]['type'] == 'punctuation':
                        punctuation = items[items.index(item) + 1]['alternatives'][0]['content']
                    else:
                        punctuation = ''
                    speakers.append({
                        'speaker_label': segment_item['speaker_label'],
                        'content': item['alternatives'][0]['content'] + punctuation,
                        'start_time': item['start_time'],
                        'end_time': item['end_time']})
                    next_segment_flag = True
                    break
            if next_segment_flag:
                break
    
    str_response = '{"Channels": [{"SpeakerLabel": "All","Text": "' + transcription_result['results']['transcripts'][0]['transcript'] + '"},'
    speaker_range = range(speaker_count)
    for speaker_id in speaker_range:
        str_response += '{"SpeakerLabel": "Speaker_' + str(speaker_id) + '", "Text": "' + ' '.join([speaker['content'] for speaker in speakers if speaker['speaker_label'] == 'spk_'+ str(speaker_id)]).strip() + '"}'
        if speaker_id < (speaker_count - 1):
            str_response += ','
    
    str_response += ']}'
    
    json_response = json.loads(str_response)

    #
    s3.put_object(Bucket=bucket_name, Key='{0}{1}.json'.format(out_suffix,session), Body=json.dumps(json_response))

    response = {'Bucket': bucket_name, 'RootFolder': os.environ['TRANSCRIBE_ROOT_FOLDER'], 'CustomerId': customer, 'ClaimId': claim, 'SessionId': session}
    
    #log response
    print('Get GenerateTranscribeJobResults function response: {0}'.format(str(response)))

    #
    return response
# end function