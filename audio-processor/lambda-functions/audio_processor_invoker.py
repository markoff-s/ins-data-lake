import json
import datetime
import boto3
import os

def lambda_handler(event, context):
    print('AudioProcessorInvoker started at {}'.format(datetime.datetime.now()))
    if event:
        metadata_obj = event["Records"][0]
        metadata_file = str(metadata_obj["s3"]["object"]["key"])
        print('AudioProcessorInvoker: file name is {}'.format(metadata_file))
        
        if metadata_file.split('.')[1] == 'json':
            bucket_name=str(metadata_obj["s3"]["bucket"]["name"])
            
            s3 = boto3.client('s3')

            s3_object = s3.get_object(Bucket = bucket_name, Key = metadata_file)
            s3_clientdata = s3_object['Body'].read().decode('utf-8')
            metadata = json.loads(s3_clientdata)
            
            audio_file = metadata['RecordPath']
            
            if audio_file.split('.')[1] in ['mp3', 'mp4', 'wav', 'flac']:
                customer = metadata['CustomerId']
                claim = metadata['ClaimId']
                print('AudioProcessorInvoker: audio file name {0} is linked to claim {1} of customer {2}'.format(audio_file, claim, customer))
        
                step_functions=boto3.client("stepfunctions")
        
                step_functions_instance_name = context.aws_request_id
                
                input = {'BucketName': bucket_name, 'FileName': audio_file, 'CustomerId': customer, 'ClaimId': claim, 'SessionId': step_functions_instance_name}
                
                step_function_arn = os.environ['STEP_FUNCTION_ARN']
                
                response = step_functions.start_execution(   stateMachineArn = step_function_arn,
                                                    name = step_functions_instance_name,
                                                    input = json.dumps(input))
                
                timestamp = datetime.datetime.now()
                if response['executionArn']:
                    print('Step Function: {0} has been run for {1} in bucket{2} at {3}'.format(step_functions_instance_name, audio_file, bucket_name, timestamp))
                else:
                    print('Step Function: {0} has failed at {1}'.format(step_functions_instance_name, timestamp))
            else:
                print('Incompatible file type: {0}'.format(audio_file))
        else:
            print('No metadata file.')

    return  {
                "statuscode": "200"
            }