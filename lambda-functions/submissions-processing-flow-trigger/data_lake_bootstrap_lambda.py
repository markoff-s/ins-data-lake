import boto3
import cfnresponse

s3 = boto3.resource('s3')


def handler(event, context):
    print('EVENT:[{}]'.format(event))

    try:
        if event['RequestType'] == 'Delete':
            # if delete, clean up data folders
            buckets_to_clean = ["${RawZoneBucket}", "${TransformedZoneBucket}", "${ReportingZoneBucket}"]
            for bucket_to_clean in buckets_to_clean:
                bucket = s3.Bucket(bucket_to_clean)
                objects_to_delete = []
                for obj in bucket.objects.filter():
                    objects_to_delete.append({'Key': obj.key})

                bucket.delete_objects(Delete={'Objects': objects_to_delete})
        else:
            # create folders for raw overnight and intraday submissions
            raw_bucket_name = "${RawZoneBucket}"
            keys = ["overnight/submissions/", "intraday/submissions/"]
            for key in keys:
                obj = s3.Object(raw_bucket_name, key)
                obj.put()

        cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
    except Exception as e:
        print('Exception:[{}]'.format(e))
        cfnresponse.send(event, context, cfnresponse.FAILED, {})
