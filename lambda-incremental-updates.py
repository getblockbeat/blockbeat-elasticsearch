#####  DynamoDB to ElasticSearch incremental updates ######
#### function is triggered by the new batch of updates on DynamoDB stream ####
#### Function updates news and newstags indices #####

import os
import json
import hashlib
import requests
from dynamodb_json import json_util as djson

ES_URL = os.environ.get("ES_URL")
ES_APIKEY = os.environ.get("ES_APIKEY")

created = 0
updated = 0
deleted = 0
NEWS_INDEX_NAME = "news"
TAGS_INDEX_NAME = "newstags"
TAGS = {}

# This function mush be set in lambda to get triggered 
def lambda_handler(events, context):
    if events.get("Records"):
        print(f'Number of unfiltered records:{len(events.get("Records"))}')

    events = djson.loads(events)
    events = get_news_events(events)
    if events:
        print(f'Number of news records:{len(events)}')

    if len(events) > 0:
        index_news(events)
        index_tags()

    print({
        'statusCode': 200,
        'body': json.dumps(f'Processed {len(events)} news records'),
        'result': result
    })

    return {
        'statusCode': 200,
        'body': json.dumps(f'Processed {len(events)} news records'),
        'result': result
    }

# Function stores different tags for newstags index
def add_tags(tag, tag_type):
    if tag and tag_type and len(tag) > 0 and len(tag_type) > 0:
        tag_type = tag_type.upper()
        if tag_type not in TAGS:
            TAGS[tag_type] = set([tag])
        else:
            TAGS[tag_type].add(tag)


# Filters NEWS events and drops remaining events
def get_news_events(events):
    if events is None or "Records" not in events or len(events.get('Records')) == 0:
        return []
    news = []
    for rec in events.get("Records"):
        if rec.get("dynamodb") and rec.get("dynamodb").get("Keys"):
            if rec["dynamodb"]['Keys'].get("PK") == "NEWS":
                news.append(rec)
                
    return news


# Transform the news event to ES document
def transform_news(events):
    documents = []

    for event in events:
        if event.get("eventName") and event.get("dynamodb"):
            document = {}
            document['eventName'] = event.get("eventName")

            if document['eventName'] == "DELETE":
                if event['dynamodb'].get("Keys"):
                    document['SK'] = event['dynamodb']['Keys'].get("SK")
                    documents.append(document)
                continue

            if event["dynamodb"].get("NewImage"):
                item = event["dynamodb"].get("NewImage")

                document['SK'] = item.get("SK")
                document['author'] = item.get("author")
                document['content'] = item.get("content")
                document['summary'] = item.get("enrichment").get("summary") if "enrichment" in item else None
                document['headline'] = item.get("headline")
                document['source'] = item.get("source")
                document['NK'] = int(item.get("NK"))
                document['tags'] = extract_tags(item.get('tags'), item.get('assets'))
                document['status'] = item.get("status")
                document['url'] = item.get("url")
                
                documents.append(document)
                add_tags(document['source'], "SOURCE")

    return documents


# Creates documents for newstags index
def index_tags():
    documents = []
    for key in TAGS:
        for val in TAGS[key]:
            documents.append({"tag":val, "tagType": key})

    if len(documents) > 0:
        bulk_index_documents(TAGS_INDEX_NAME, documents)


# Generates hash based on given input, same input generates same tags
def get_hash(value):
    value = value if value is not None else "null"
    hash_object = hashlib.sha256()
    hash_object.update(value.encode('utf-8'))
    hash_hex = hash_object.hexdigest()
    return hash_hex[:40]


# Index news documents
def index_news(records):
    documents = transform_news(records)
    bulk_index_documents(NEWS_INDEX_NAME, documents)


# emits stats for indexed documents
def update_stats(response):
    global created, updated, deleted, result
    if response.content:
        res = json.loads(response.content)
        created += len([1 for item in res['items'] if 'index' in item and 'result' in item.get('index') and  item['index']['result'] == 'created'])
        updated += len([1 for item in res['items'] if 'index' in item and 'result' in item.get('index') and  item['index']['result'] == 'updated'])
        deleted += len([1 for item in res['items'] if 'index' in item and 'result' in item.get('index') and  item['index']['result'] == 'deleted'])
        print( f"Created: {created}, Updated: {updated}, Deleted: {deleted}")
        result = {'created': created, 'updated': updated, 'deleted': deleted}

# Extracts tags from attributes from DB attributes
def extract_tags(tag_object, asset_object):
    tag_list = []

    if tag_object:
        tag_list = [d["data"] for d in tag_object if "data" in d] if tag_object else []
        _ = [add_tags(d["data"], d['type']) for d in tag_object if "data" in d and "type" in d]

    if asset_object:
        tag_list.extend( [d["name"] for d in asset_object if "name" in d] if asset_object else [] )
        _ = [add_tags(d["symbol"], "symbol") for d in asset_object if "symbol" in d]
        _ = [add_tags(d["name"], "name") for d in asset_object if "name" in d]

    return list(set(tag_list))

# Generates hash based on document type
def get_hashed_id(index_name, data):
    
    if index_name == "newstags":
        return get_hash(f"{data['tag']}_{data['tagType']}")
    elif index_name == "news":
        return get_hash(data.get('SK'))
    else:
        return get_hash(None)


# Generates payload for bulk indexing API and indexes documents
def bulk_index_documents(index_name, data):
    # Prepare bulk request payload
    bulk_data = ''
    counter = 0
    headers = {'Content-Type': 'application/x-ndjson', 'Authorization': f'ApiKey {ES_APIKEY}'}

    for item in data:
        counter += 1

        eventName = None
        if item.get("eventName"):
            eventName = item.get("eventName")
            del item['eventName']

        if eventName == "DELETE":
            bulk_data += json.dumps({'delete': { '_index': index_name, '_id': get_hashed_id(index_name, item)}}) + '\n'
        else:
            bulk_data += json.dumps({'index': { '_index': index_name, '_id': get_hashed_id(index_name, item)}}) + '\n'
            bulk_data += json.dumps(item) + '\n'

        if counter % 1000 == 0:
            # Send bulk request to Elasticsearch
            response = requests.post(ES_URL+ "/_bulk", headers=headers, data=bulk_data)
            update_stats(response)
            bulk_data = ''

    if counter % 1000 != 0:
        response = requests.post(ES_URL+ "/_bulk", headers=headers, data=bulk_data)
        update_stats(response)