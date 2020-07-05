import boto3
import cfnresponse

s3 = boto3.resource('s3')


def handler(event, context):
    print('EVENT:[{}]'.format(event))

    try:
        if event['RequestType'] == 'Delete':
            # if delete, clean up data folders
            print('Cleaning up buckets...')
            buckets_to_clean = ["${QuarantineZoneBucket}", "${RawZoneBucket}", "${TransformedZoneBucket}", "${ReportingZoneBucket}"]
            for bucket_to_clean in buckets_to_clean:
                bucket = s3.Bucket(bucket_to_clean)
                objects_to_delete = []
                for obj in bucket.objects.filter():
                    objects_to_delete.append({'Key': obj.key})

                try:
                    # this method throws an exception if the bucket is empty
                    bucket.delete_objects(
                        Delete={'Objects': objects_to_delete})
                except Exception as e:
                    print('Exception on cleaning up bucket "{}" :[{}]'.format(
                        bucket.name, e))
        else:
            # create folders for raw overnight and intraday submissions
            print('Initializing buckets...')
            buckets_to_init = ["${QuarantineZoneBucket}", "${RawZoneBucket}"]
            keys = ["overnight/submissions/", "intraday/submissions/"]
            for bucket in buckets_to_init:
                for key in keys:
                    obj = s3.Object(bucket, key)
                    obj.put()

        cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
    except Exception as e:
        print('Exception:[{}]'.format(e))
        cfnresponse.send(event, context, cfnresponse.FAILED, {})
