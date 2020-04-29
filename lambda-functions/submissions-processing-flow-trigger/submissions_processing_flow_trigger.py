import os
import sys
import json
import boto3
from datetime import datetime

stepFunctionsClient = boto3.client('stepfunctions')


def main():
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
        f = open(file_name)
        event = json.load(f)

        lambda_handler(event, None)


def lambda_handler(event, context):
    print("------- EVENT ---------")
    print(event)
    print("------- END of EVENT -------")

    dest_bucket, overnight_flow_step_function_arn, intraday_flow_step_function_arn, emr_cluster_id = get_params()

    path_to_dest_bucket = "s3://" + dest_bucket + "/"
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        path_to_source_file = "s3://" + bucket + "/" + key
        print(f"--- ClusterId = {emr_cluster_id} | PathToSourceFile = {path_to_source_file} "
              f"| PathToDestBucket = {path_to_dest_bucket} ---")

        step_function_arn = overnight_flow_step_function_arn \
            if key.endswith(".csv") \
            else intraday_flow_step_function_arn
        trigger_step_function(emr_cluster_id, step_function_arn, path_to_source_file, path_to_dest_bucket)


def trigger_step_function(emr_cluster_id, step_function_arn, path_to_source_file, path_to_dest_bucket):
    response = stepFunctionsClient.start_execution(
        stateMachineArn=step_function_arn,
        name="submissions_flow_at__" + datetime.now().strftime("%m_%d_%Y__%H_%M_%S"),
        input=f'{{'
              f'"ClusterId": "{emr_cluster_id}", '
              f'"PathToSourceFile": "{path_to_source_file}", '
              f'"PathToDestBucket": "{path_to_dest_bucket}"}}'
    )

    print("--- step function response ---")
    print(response)


def get_params():
    if len(sys.argv) > 2:
        dest_bucket = sys.argv[2]
    else:
        dest_bucket = os.environ["TRANSFORMED_ZONE_BUCKET"]

    if len(sys.argv) > 3:
        overnight_flow_step_function_arn = sys.argv[3]
    else:
        overnight_flow_step_function_arn = os.environ["OVERNIGHT_FLOW_STEP_FUNCTION_ARN"]

    if len(sys.argv) > 4:
        intraday_flow_step_function_arn = sys.argv[4]
    else:
        intraday_flow_step_function_arn = os.environ["INTRADAY_FLOW_STEP_FUNCTION_ARN"]

    if len(sys.argv) > 5:
        emr_cluster_id = sys.argv[5]
    else:
        emr_cluster_id = os.environ["EMR_CLUSTER_ID"]

    return dest_bucket, overnight_flow_step_function_arn, intraday_flow_step_function_arn, emr_cluster_id


if __name__ == '__main__':
    main()
