import json
import boto3
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

es_endpoint = os.environ.get('ES_ENDPOINT', 'search-photos-ef4nehxeanhqk4ourwtldsail4.us-east-1.es.amazonaws.com')
es_index = os.environ.get('ES_INDEX', 'photos')

opensearch = OpenSearch(
    hosts=[{'host': es_endpoint, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    query = event.get('queryStringParameters', {}).get('q', '') if event.get('queryStringParameters') else ''
    
    if not query:
        return {
            'statusCode': 400,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': 'Missing query parameter'})
        }
    
    keywords = [word.strip() for word in query.lower().split() if len(word.strip()) > 2]
    results = search_photos_in_elasticsearch(keywords)
    
    photo_results = []
    for hit in results:
        source = hit['_source']
        photo_url = generate_presigned_url(source['bucket'], source['objectKey'])
        photo_results.append({'url': photo_url, 'labels': source.get('labels', [])})
    
    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({'results': photo_results, 'count': len(photo_results), 'query': query, 'keywords': keywords})
    }

def search_photos_in_elasticsearch(keywords):
    should_clauses = []
    for keyword in keywords:
        should_clauses.append({"match": {"labels": {"query": keyword, "fuzziness": "AUTO"}}})
        if keyword.endswith('s') and len(keyword) > 2:
            should_clauses.append({"match": {"labels": keyword[:-1]}})
    
    query_body = {"query": {"bool": {"should": should_clauses, "minimum_should_match": 1}}}
    response = opensearch.search(index=es_index, body=query_body)
    return response['hits']['hits']

def generate_presigned_url(bucket, key, expiration=3600):
    try:
        return s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=expiration)
    except Exception as e:
        return f"https://{bucket}.s3.amazonaws.com/{key}"
