#####  DynamoDB to ElasticSearch full upload ######

import boto3
import json
import os
import requests
import hashlib
from pprint import pprint

# Dependency: Following environment variables are required
# ES_URL
# ES_INDEX
# ES_APIKEY
# DB_REGION_NAME
# DB_TABLE_NAME

result = {}
created = 0
updated = 0

ES_URL = os.environ.get("ES_URL")
ES_INDEX = os.environ.get("ES_INDEX")
ES_APIKEY = os.environ.get("ES_APIKEY")
DB_REGION_NAME = os.environ.get("DB_REGION_NAME")
DB_TABLE_NAME = os.environ.get("DB_TABLE_NAME")

def lambda_handler(event, context):
    global result

    pull_records_from_dynamodb()
    
    return json.dumps(result)
    
def pull_records_from_dynamodb():
    partition_key_value = "NEWS"
    dynamodb = boto3.resource('dynamodb', region_name=DB_REGION_NAME)
    table = dynamodb.Table(DB_TABLE_NAME)

    #projection_expression = 'SK, author, content, enrichment, headline, NK, tags' # add source, status, url - later these are keywords for some reason

    response = table.query(
        KeyConditionExpression='PK = :pkval',
        ExpressionAttributeValues={
            ':pkval': partition_key_value
        }
        #,ProjectionExpression=projection_expression
    )
    items = response['Items']

    while 'LastEvaluatedKey' in response: # set false
        response = table.query(
            KeyConditionExpression='PK = :pkval',
            ExpressionAttributeValues={
                ':pkval': partition_key_value
            },
            #ProjectionExpression=projection_expression,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])
        if len(items) > 900:
            index(items)
            items.clear()
    if len(items) > 0:
        index(items)

def get_hash(value):
    value = value if value is not None else "null"
    hash_object = hashlib.sha256()
    hash_object.update(value.encode('utf-8'))
    hash_hex = hash_object.hexdigest()
    return hash_hex[:40]

def index(records):
    documents = transform(records)
    bulk_index_documents(documents)

def update_stats(response):
    global created, updated, result
    if response.content:
        res = json.loads(response.content)
        created += len([1 for item in res['items'] if 'index' in item and 'result' in item.get('index') and  item['index']['result'] == 'created'])
        updated += len([1 for item in res['items'] if 'index' in item and 'result' in item.get('index') and  item['index']['result'] == 'updated'])
        print( f"Created: {created}, Updated: {updated}")
        result = {'created': created, 'updated': updated}

def transform(items):
    documents = []
    for item in items:
        document = {}
        document['SK'] = item.get("SK")
        document['author'] = item.get("author")
        document['content'] = item.get("content")
        document['summary'] = item.get("enrichment").get("summary") if "enrichment" in item else None
        document['headline'] = item.get("headline")
        document['source'] = item.get("source")
        document['NK'] = int(item.get("NK"))
        document['tags'] = list(set([d.get("data") for d in item.get("tags") if "data" in d])) if "tags" in item else None
        document['status'] = item.get("status")
        document['url'] = item.get("url")
        documents.append(document)

    return documents

def bulk_index_documents(data):
    # Prepare bulk request payload
    bulk_data = ''
    for item in data:
        bulk_data += json.dumps({'index': { '_index': ES_INDEX, '_id': get_hash(item['SK'])}}) + '\n'
        bulk_data += json.dumps(item) + '\n'

    # Send bulk request to Elasticsearch
    headers = {'Content-Type': 'application/x-ndjson', 'Authorization': f'ApiKey {ES_APIKEY}'}
    response = requests.post(ES_URL+ "/_bulk", headers=headers, data=bulk_data)

    # Check response status code and content
    update_stats(response)

if __name__ == "__main__":
    pull_records_from_dynamodb()