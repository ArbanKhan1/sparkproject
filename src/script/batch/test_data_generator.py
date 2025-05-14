import boto3
import json
import random
import string

# Configurations
s3_bucket = 'retail-dataset1'
s3_prefix = 'source_files'
kinesis_stream_name = 'retail_dataset_source_stream'
region_name = 'eu-north-1'  # e.g., 'us-east-1'

# Initialize clients
s3_client = boto3.client('s3', region_name=region_name)
kinesis_client = boto3.client('kinesis', region_name=region_name)

json_files = []
# Step 1: List JSON files
def list_json_files(bucket, prefix):

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    print(response)

    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.json'):
            json_files.append(key)
    print(json_files)
    return json_files

# Step 2: Read JSON content from S3
def read_json_from_s3(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    print("get object response ",response)
    content = response['Body'].read().decode('utf-8')
    print("content_type",content, type(content))
    print("json_loads_type",json.loads(content),type(json.loads(content)))
    try:
        return json.loads(content)
    
    except json.JSONDecodeError:
        # Handle line-delimited JSON (one JSON object per line)
        return [json.loads(line) for line in content.strip().split('\n') if line]

# Step 3: Post to Kinesis stream
def generate_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def post_to_kinesis(stream_name, records):
    for record in records:
        kinesis_response=kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(record),
            PartitionKey=generate_random_string(10))
        print(record)
        print(kinesis_response)
    

def main():
    json_files = list_json_files(s3_bucket, s3_prefix)
    for key in json_files:
        print(f"Processing file: {key}")
        records = read_json_from_s3(s3_bucket, key)
        post_to_kinesis(kinesis_stream_name, records)
        print(f"Finished sending records from {key}")



if __name__ == "__main__":
    main()
