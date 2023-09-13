import json

import boto3
from prefect.blocks.core import Block
from prefect.blocks.fields import SecretDict


class AwsBaseBlock(Block):
    """Block used for AWS deploy."""

    aws: SecretDict
    clima_tempo: SecretDict

    def get_secrets(self, secret_dict):
        """Returns a dictionary with all secrets."""

        d = getattr(self, secret_dict)
        return d.get_secret_value()

    def get_secret(self, secret_dict, secret):
        """Return a single secret."""

        d = getattr(self, secret_dict)
        return d.get_secret_value()[secret]

    def list_secrets(self, secret_dict):
        """List all secrets for a secret dict."""

        d = getattr(self, secret_dict)
        return d.get_secret_value().keys()

    def s3_get_cli(self):
        cli = boto3.resource(
            's3',
            aws_access_key_id=self.get_secret('aws', 'AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=self.get_secret('aws', 'AWS_SECRET_ACCESS_KEY'),
            region_name=self.get_secret('aws', 'AWS_DEFAULT_REGION'),
        )
        return cli

    def s3_write(self, s, bucket_name, path_file):
        cli = self.s3_get_cli()
        bucket = cli.Bucket(bucket_name)
        bucket.put_object(Key=path_file, Body=s)

    def s3_delete(self, bucket_name, path_file):
        cli = self.s3_get_cli()
        cli.Object(bucket_name, path_file).delete()

    def s3_read(self, bucket_name, path_file):
        cli = self.s3_get_cli()
        obj = cli.Object(bucket_name, path_file)
        return obj.get()['Body'].read()

    def invoke_lambda(self, name: str, payload: str | dict):
        """Invoke a lambda function end returns its result."""

        if isinstance(payload, dict):
            payload = json.dumps(payload)

        cli = boto3.client(
            'lambda',
            aws_access_key_id=self.get_secret('aws', 'AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=self.get_secret('aws', 'AWS_SECRET_ACCESS_KEY'),
            region_name=self.get_secret('aws', 'AWS_DEFAULT_REGION'),
        )
        r = cli.invoke(
            FunctionName=name,
            Payload=payload
        )
        try:
            r['json'] = json.load(r['Payload'])
        except Exception as e:
            r['json'] = str(e)

        return r


def test_s3_invoke():
    """Calls lambda function and write/read/delete."""

    bucket_name = 'ahow-delta-lake'
    file_path = 'raw/test.csv'

    block = AwsBaseBlock.load('base-aws')
    r = block.invoke_lambda(
        'multiply', {'array': [1, 2, 15]}
    )
    s = json.dumps(r['json'])

    block.s3_write(s, bucket_name, file_path)
    ss = block.s3_read(bucket_name, file_path).decode()
    block.s3_delete(bucket_name, file_path)

    assert s == ss


if __name__ == '__main__':
    # Deploy the AWS base block with AWS keys.

    import os
    from dotenv import load_dotenv
    load_dotenv(".github/workflows/aws_deploy/.env")

    if os.environ['AWS_ACCESS_KEY_ID'] is None or \
        os.environ['AWS_ACCESS_KEY_ID'] == '':
        raise Exception("AWS_ACCESS_KEY_ID is '' or None")

    block = AwsBaseBlock(
        aws={
            'AWS_ACCESS_KEY_ID': os.environ['AWS_ACCESS_KEY_ID'],
            'AWS_SECRET_ACCESS_KEY': os.environ['AWS_SECRET_ACCESS_KEY'],
            'AWS_DEFAULT_REGION': os.environ['AWS_DEFAULT_REGION'],
            'AWS_LAMBDA_ROLE': os.environ['AWS_LAMBDA_ROLE'],
            'AWS_EMR_BUCKET': os.environ['AWS_EMR_BUCKET'],
        },
        clima_tempo={
            "TOKEN": os.environ['CLIMATEMPO_TOKEN']
        }
    ).save('base-aws', overwrite=True)

    # Prefect has problems with PYTHONPATH, hence the test is done here.
    # test_s3_invoke()

