from botocore.config import Config

my_config = Config(
    region_name='us-east-1',
    signature_version='v4',
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }

)

# client = boto3.client('kinesis', config=my_config)
