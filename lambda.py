#####  DynamoDB to ElasticSearch full upload ######

import boto3
import json
import os
import requests
import hashlib
import time

# Dependency: Following environment variables are required
# ES_URL
# ES_APIKEY
# DB_REGION_NAME
# DB_TABLE_NAME

result = {}
created = 0
updated = 0

ES_URL = os.environ.get("ES_URL")
ES_APIKEY = os.environ.get("ES_APIKEY")
DB_REGION_NAME = os.environ.get("DB_REGION_NAME")
DB_TABLE_NAME = os.environ.get("DB_TABLE_NAME")

NEWS_ALIAS = "news"
TAGS_ALIAS = "newstags"
NEWS_INDEX_NAME = ''
TAGS_INDEX_NAME = ''
TAGS = {}


def lambda_handler(event, context):
    global result
    pull_records_from_dynamodb()
    return json.dumps(result)


# Function stores different tags for newstags index
def add_tags(tag, tag_type):
    if tag and tag_type and len(tag) > 0 and len(tag_type) > 0:
        tag_type = tag_type.upper()
        tag['tagType'] = tag_type

        if tag_type not in TAGS:
            TAGS[tag_type] = {tag['tag']:tag}
        else:
            TAGS[tag_type][tag['tag']] = tag

# Pulls all data from NEWS partition
# Creates indices for news and newstags
# Switches alias at the end
def pull_records_from_dynamodb():
    global NEWS_INDEX_NAME, TAGS_INDEX_NAME
    NEWS_INDEX_NAME = get_new_index_name("news")
    TAGS_INDEX_NAME = get_new_index_name("newstags")
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
    create_new_index(NEWS_INDEX_NAME)
    create_new_index(TAGS_INDEX_NAME)

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
            index_news(items)
            items.clear()

    if len(items) > 0:
        index_news(items)

    index_tags()
    switch_alias("news")
    switch_alias("newstags")


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
    global created, updated, result
    if response.content:
        res = json.loads(response.content)
        created += len([1 for item in res['items'] if 'index' in item and 'result' in item.get('index') and  item['index']['result'] == 'created'])
        updated += len([1 for item in res['items'] if 'index' in item and 'result' in item.get('index') and  item['index']['result'] == 'updated'])
        print( f"Created: {created}, Updated: {updated}")
        result = {'created': created, 'updated': updated}


# Extracts tags from attributes from DB attributes
def extract_tags(tag_object, assets):
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

            if symbol and symbol != name:
                tag['display'] = f'{name} {symbol}'

            if name or symbol:
                tag['tag'] = name if name else symbol
                add_tags(tag, "asset")

            if name and symbol and name != symbol:
                another_tag = {}
                another_tag['display'] = f'{name} {symbol}'
                another_tag['tag'] = symbol
                add_tags(another_tag, "asset")

    return list(set(tag_list))


# Converts DB document to ES documents
def transform_news(items):
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
        document['tags'] = extract_tags(item.get('tags'), item.get('assets'))
        document['status'] = item.get("status")
        document['url'] = item.get("url")
        
        documents.append(document)
        add_tags({'display':document['source'], 'tag':document['source']}, "SOURCE")

    return documents


# Generates hash based on document type
def get_hashed_id(index_name, data):
    if index_name.startswith("news_"):
        return get_hash(data.get('SK'))
    elif index_name.startswith("newstags_"):
        return get_hash(f"{data['tag']}_{data['tagType']}")
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

# Invokes create alias API to create new alias
def create_new_alias(index_name, alias_name):
    # Create the new alias and point it to the new index
    alias_endpoint = f"{ES_URL}/_alias"
    alias_update = {
        "actions": [
            {
                "add": {
                    "index": index_name,
                    "alias": alias_name
                }
            }
        ]
    }
    headers = {'Content-Type': 'application/json', 'Authorization': f'ApiKey {ES_APIKEY}'}
    alias_update_response = requests.put(alias_endpoint, json=alias_update, headers=headers)
    if alias_update_response.status_code == 200:
        print(f"Success: Created new alias '{alias_name}' pointing to '{index_name}'")
    else:
        print(f"Error: An error occurred while creating the new alias: {alias_update_response.json()}")


# Invokes create alias API to switch index in alias
def switch_alias_to_new_index(index_name, alias_name):
    update_alias_endpoint = f"{ES_URL}/_aliases"
    alias_update = {
        "actions": [
            {
                "remove": {
                    "index": "*",
                    "alias": alias_name
                }
            },
            {
                "add": {
                    "index": index_name,
                    "alias": alias_name
                }
            }
        ]
    }
    headers = {'Content-Type': 'application/json', 'Authorization': f'ApiKey {ES_APIKEY}'}
    alias_update_response = requests.post(update_alias_endpoint, json=alias_update, headers=headers)
    if alias_update_response.status_code == 200:
        print(f"Success: Alias '{alias_name}' is now pointing to '{index_name}'")
    else:
        print(f"Error: An error occurred while switching indices: {alias_update_response.json()}")


# Retrieve names of indices associated with given alias
def get_index_used_by_alias(alias_name):
    old_index = None
    # Check if the alias already exists
    alias_endpoint = f"{ES_URL}/_alias"
    headers = {'Content-Type': 'application/json', 'Authorization': f'ApiKey {ES_APIKEY}'}
    alias_response = requests.get(f"{alias_endpoint}/{alias_name}", headers=headers)

    if alias_response.status_code == 200:
        old_index = list(alias_response.json().keys())

    return old_index


# Delete index
def delete_old_index(old_index_name):
    # Check if the alias already exists
    delete_endpoint = f"{ES_URL}/{old_index_name}"
    headers = {'Content-Type': 'application/json', 'Authorization': f'ApiKey {ES_APIKEY}'}
    response = requests.delete(delete_endpoint, headers=headers)

    if response.status_code == 200:
        print(f"Deleted old index: {old_index_name}")


# Orchestrates to switch alias
# It contains a threshold logic to avoid switching alias if the difference in documents is too big
def switch_alias(alias_name):

    new_index = NEWS_INDEX_NAME if "news" == alias_name else TAGS_INDEX_NAME
    count_endpoint = f"{ES_URL}/{new_index}/_count"
    old_index_names = get_index_used_by_alias(alias_name)

    if old_index_names is None:
        # here alias is not associated to old index therefore point alias to new index and exit
        create_new_alias(new_index, alias_name)
        return
    elif new_index in old_index_names:
        #no change is required:
        print(f"Success: no change is required, Alias '{alias_name}' is already pointing to '{new_index}'")

    old_index_name = old_index_names[0]

    switch_alias_to_new_index(new_index, alias_name)
    for old_index_name in old_index_names:
        delete_old_index(old_index_name)


# ES schema for news index
def get_news_mappings():
    return {
        "mappings": {
            "properties": {
                "sk": {
                    "type": "keyword"
                },
                "author": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "summary": {
                    "type": "text"
                },
                "headline": {
                    "type": "text"
                },
                "content": {
                    "type": "text"
                },
                "tags": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "nk": {
                    "type": "long"
                },
                "source": {
                    "type": "keyword"
                },
                "url": {
                    "type": "keyword"
                }
            }
        }
    }

# ES schema for newstags index
def get_tags_mappings():
    return {
        "mappings": {
            "properties": {
                "tag": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
                "tagType": {
                    "type": "keyword"
                },
                "display": {
                    "type": "keyword"
                }
            }
        }
    }

# Defines new index in ES
def create_new_index(index_name):
    mapping = {}

    if index_name.startswith("news_"):
        mapping = get_news_mappings()
    elif index_name.startswith("newstags_"):
        mapping = get_tags_mappings()

    # Create the index
    url = f"{ES_URL}/{index_name}"
    headers = {"Content-Type": "application/json", 'Authorization': f'ApiKey {ES_APIKEY}'}
    response = requests.put(url, json=mapping, headers=headers)

    # Check if the index creation was successful
    if response.status_code == 200:
        print(f"Index '{index_name}' created successfully.")
    else:
        print(f"Failed to create index '{index_name}'. Error: {response.content}")

# generate new name for index using time
def get_new_index_name(name):
    epoch = int(time.time())
    return f"{name}_{epoch}"


if __name__ == "__main__":
    pull_records_from_dynamodb()