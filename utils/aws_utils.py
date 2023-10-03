import boto3

def write_to_bucket(s, bucket_name, path_file):
    cli = boto3.resource('s3')
    bucket = cli.Bucket(bucket_name)
    bucket.put_object(Key=path_file, Body=s)

