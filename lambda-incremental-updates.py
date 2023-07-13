#####  DynamoDB to ElasticSearch incremental updates ######
#### function is triggered by the new batch of updates on DynamoDB stream ####
#### Function updates news and newstags indices #####

import os
import json
import hashlib
import requests
from decimal import Decimal
from dynamodb_json import json_util as djson

ES_URL = os.environ.get("ES_URL")
ES_APIKEY = os.environ.get("ES_APIKEY")

created = 0
updated = 0
deleted = 0
NEWS_INDEX_NAME = "news"
TAGS_INDEX_NAME = "newstags"
TAGS = {}
result = ""


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        return super(DecimalEncoder, self).default(obj)



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
        tag['tagType'] = tag_type

        if tag_type not in TAGS:
            TAGS[tag_type] = {tag['tag']:tag}
        else:
            TAGS[tag_type][tag['tag']] = tag


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
                document['tags'] = extract_tags(item)
                document['status'] = item.get("status")
                document['url'] = item.get("url")
                document['iconType'] = item.get("iconType")
                document['upVote'] = int(item.get("upVote")) if item.get("upVote") else 0
                document['downVote'] = int(item.get("downVote")) if item.get("downVote") else 0
                document['sentiment'] = float(item.get('enrichment').get('sentiment')) if item.get('enrichment') and item.get('enrichment').get('sentiment') else None
                document['hot'] = item.get('ai').get('hot') if item.get('ai') and item.get('ai').get('hot') else False
                document['assets'] = extract_assets(item.get('assets'))
                document['enrichment'] = { "sentiment" : document['sentiment']}
                document['ai'] = { "hot" : document['hot']}

                documents.append(document)
                add_tags({'display':document['source'], 'tag':strip_source_prefix(document['source']), "iconType":item.get("iconType")}, "SOURCE")

    return documents


#extract name and symbol from assets
def extract_assets(assets):
    asset_list = []

    if not assets:
        return asset_list

    for asset in assets:
        element = {}
        element['name'] = asset.get("name")
        element['symbol'] = asset.get("symbol")

        if element['name'] or element['symbol']:
            asset_list.append(element)

    return asset_list


# Creates documents for newstags index
def index_tags():
    documents = []
    for key in TAGS:
        for inner_key in TAGS[key]:
            documents.append(TAGS[key][inner_key])

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
def extract_tags(item):
    tag_object = item.get('tags')
    assets = item.get('assets')
    tag_list = []

    if tag_object:
        tag_list = [d["data"] for d in tag_object if "data" in d] if tag_object else []
        _ = [add_tags({'display':d["data"], 'tag':d["data"]}, d['type']) for d in tag_object if "data" in d and "type" in d and d["data"]]

    if assets:
        tag_list.extend( [d["name"] for d in assets if "name" in d] if assets else [] )
        for asset in assets:
            tag = {}
            name = asset.get("name")
            symbol = asset.get("symbol")
            asset_id = asset.get("id")

            if symbol and symbol != name:
                tag['display'] = f'{name} {symbol}'

            if name or symbol:
                tag['tag'] = name if name else symbol
                tag['symbol'] = symbol
                tag['assetId'] = None if asset_id is None else int(asset_id)
                cap = extract_market_cap(asset)
                tag['marketCap'] = None if cap is None else float(cap)
                add_tags(tag, "asset")

            # if name and symbol and name != symbol:
            #     another_tag = {}
            #     another_tag['display'] = f'{name} {symbol}'
            #     another_tag['tag'] = symbol
            #     add_tags(another_tag, "asset")

    return list(set(tag_list))


# Removes 'r/' from 'r/bitguy'
def strip_source_prefix(str):
    return str[2:] if str is not None and str.startswith("r/") else str


# Extract market map
def extract_market_cap(asset):
    if ( asset.get("quote")
         and asset.get("quote").get("USD") ):

        return asset.get("quote").get("USD").get("market_cap")

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
            bulk_data += json.dumps(item, cls=DecimalEncoder) + '\n'

        if counter % 1000 == 0:
            # Send bulk request to Elasticsearch
            response = requests.post(ES_URL+ "/_bulk", headers=headers, data=bulk_data)
            update_stats(response)
            bulk_data = ''

    if counter % 1000 != 0:
        response = requests.post(ES_URL+ "/_bulk", headers=headers, data=bulk_data)
        update_stats(response)