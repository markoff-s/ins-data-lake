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

    raw_zone_bucket, \
    token_map_bucket, \
    transformed_zone_bucket, \
    overnight_flow_step_function_arn, \
    intraday_flow_step_function_arn, \
    emr_cluster_id = get_params()

    timestamp = datetime.now().strftime("%m_%d_%Y__%H_%M_%S")
    path_to_raw_zone_bucket = "s3://" + raw_zone_bucket + "/" + timestamp + "/"
    path_to_transformed_zone_bucket = "s3://" + transformed_zone_bucket + "/"
    path_to_token_map_bucket = "s3://" + token_map_bucket + "/" + timestamp + "/"

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        path_to_source_file = "s3://" + bucket + "/" + key

        step_function_arn = overnight_flow_step_function_arn \
            if key.endswith(".csv") \
            else intraday_flow_step_function_arn

        step_function_params = f'{{ "ClusterId": "{emr_cluster_id}", ' \
                               f'"PathToSourceFile": "{path_to_source_file}", ' \
                               f'"PathToRawZoneBucket": "{path_to_raw_zone_bucket}", ' \
                               f'"PathToTransformedZoneBucket": "{path_to_transformed_zone_bucket}", ' \
                               f'"PathToTokenMapBucket": "{path_to_token_map_bucket}" }}'
        print(f"params: {step_function_params}")

        response = stepFunctionsClient.start_execution(
            stateMachineArn=step_function_arn,
            name="submissions_flow_at__" + timestamp,
            input=step_function_params
        )

    print("--- step function response ---")
    print(response)


def get_params():
    raw_zone_bucket = os.environ["RAW_ZONE_BUCKET"]
    token_map_bucket = os.environ["TOKEN_MAP_BUCKET"]
    transformed_zone_bucket = os.environ["TRANSFORMED_ZONE_BUCKET"]
    overnight_flow_step_function_arn = os.environ["OVERNIGHT_FLOW_STEP_FUNCTION_ARN"]
    intraday_flow_step_function_arn = os.environ["INTRADAY_FLOW_STEP_FUNCTION_ARN"]
    emr_cluster_id = os.environ["EMR_CLUSTER_ID"]

    return raw_zone_bucket, \
           token_map_bucket, \
           transformed_zone_bucket, \
           overnight_flow_step_function_arn, \
           intraday_flow_step_function_arn, \
           emr_cluster_id


if __name__ == '__main__':
    main()
