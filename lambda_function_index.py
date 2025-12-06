import json
import boto3
import os
from datetime import datetime
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')

ES_ENDPOINT = os.environ.get('ES_ENDPOINT')
ES_INDEX = os.environ.get('ES_INDEX', 'photos')
REGION = 'us-east-1'

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, 'es', session_token=credentials.token)

es_client = OpenSearch(
    hosts=[{'host': ES_ENDPOINT, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def lambda_handler(event, context):
    print(f"Event: {json.dumps(event)}")
    
    for record in event.get('Records', []):
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        print(f"Processing: {key}")
        
        # Rekognition
        response = rekognition.detect_labels(
            Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MaxLabels=10,
            MinConfidence=70.0
        )
        labels = [l['Name'].lower() for l in response['Labels']]
        print(f"Rekognition: {labels}")
        
        # Custom labels
        metadata = s3.head_object(Bucket=bucket, Key=key).get('Metadata', {})
        custom = metadata.get('customlabels', '')
        if custom:
            labels.extend([l.strip().lower() for l in custom.split(',') if l.strip()])
        
        print(f"Final labels: {labels}")
        
        # Index
        doc = {
            'objectKey': key,
            'bucket': bucket,
            'createdTimestamp': datetime.utcnow().isoformat(),
            'labels': list(set(labels))
        }
        
        es_client.index(index=ES_INDEX, id=key, body=doc, refresh=True)
        print(f"✅ Indexed {key}")
    
    return {'statusCode': 200}
