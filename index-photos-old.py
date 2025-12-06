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
REGION = os.environ.get('AWS_REGION', 'us-east-1')

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    'es',
    session_token=credentials.token
)

es_client = OpenSearch(
    hosts=[{'host': ES_ENDPOINT, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event, indent=2)}")
    
    processed = 0
    errors = 0
    
    for record in event.get('Records', []):
        try:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            print(f"Processing: s3://{bucket}/{key}")
            
            rekognition_response = rekognition.detect_labels(
                Image={'S3Object': {'Bucket': bucket, 'Name': key}},
                MaxLabels=10,
                MinConfidence=70.0
            )
            
            detected_labels = [label['Name'].lower() for label in rekognition_response['Labels']]
            print(f"Rekognition detected {len(detected_labels)} labels: {detected_labels}")
            
            metadata_response = s3.head_object(Bucket=bucket, Key=key)
            metadata = metadata_response.get('Metadata', {})
            custom_labels_str = metadata.get('customlabels', '')
            
            if custom_labels_str:
                custom_labels = [label.strip().lower() for label in custom_labels_str.split(',') if label.strip()]
            else:
                custom_labels = []
            
            all_labels = list(set(detected_labels + custom_labels))
            print(f"Final combined labels ({len(all_labels)}): {all_labels}")
            
            document = {
                "objectKey": key,
                "bucket": bucket,
                "createdTimestamp": datetime.utcnow().isoformat(),
                "labels": all_labels
            }
            
            response = es_client.index(
                index=ES_INDEX,
                body=document,
                id=key,
                refresh=True
            )
            
            print(f"✅ Successfully indexed")
            processed += 1
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            errors += 1
    
    print(f"Processing complete: {processed} succeeded, {errors} failed")
    return {'statusCode': 200}
