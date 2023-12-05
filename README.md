# ES Index & Fields

**Index name**: news

**Fields**

- **SK** : Unique Key
- **author** : name of author
- **content** : content of article
- **summary** : summary of article
- **headline** : title of the article
- **source** : text - source of the article
- **NK** : number- Epoch of the article post date
- **tags** : text - tags of the article
- **status** : text - status of the article
- **url** : keyword - URL of the article
- **iconType** : text - tags of the article
- **upVote** : long - upvotes for the article
- **downVote** : long: downvotes for the article

Sample news Document:

```json
{
  "SK": "\"anything-on-the-table:\"-coinbase-ceo-mulls-moving-headquarters-outside-us-amid-crypto-crackdown",
  "author": "Tyler Durden",
  "content": "Coinbase CEO Brian Armstrong is considering relocating the company's headquarters outside the US due to increasing regulatory pressure from state and federal regulators, including Sen. Elizabeth Warren's 'Anti-crypto Army.' The relocation risk comes after the Securities and Exchange Commission issued a Wells notice to Coinbase, warning of potential securities violations. Armstrong cited regulatory uncertainty in the US and mentioned the UK as the company's second-largest market globally by revenue. Coinbase, with around 100 million verified users, plans to expand across Europe.",
  "summary": "Coinbase CEO Brian Armstrong is considering relocating the company's headquarters outside the US due to increasing regulatory pressure from state and federal regulators, including Sen. Elizabeth Warren's 'Anti-crypto Army.' The relocation risk comes after the Securities and Exchange Commission issued a Wells notice to Coinbase, warning of potential securities violations. Armstrong cited regulatory uncertainty in the US and mentioned the UK as the company's second-largest market globally by revenue. Coinbase, with around 100 million verified users, plans to expand across Europe.",
  "headline": "\"Anything On The Table:\" Coinbase CEO Mulls Moving Headquarters Outside US Amid Crypto Crackdown",
  "source": "Zerohedge",
  "NK": 1681836600000,
  "tags": ["Coinbase", "regulation"],
  "status": "New",
  "url": "https://www.zerohedge.com/crypto/anything-table-coinbase-ceo-mulls-moving-headquarters-outside-us-amid-crypto-crackdown",
  "iconType": "NEWS",
  "upVote": 10,
  "downVote": 15
}
```

Sample newstags Document:

```json
{
  "tag": "DAI",
  "tagType": "SYMBOL"
}
```

ElasticSearch Schema

Index schema is defined in news_schema.json file.

```json
PUT news_{epoch}
<JSON from news_schema.json>
```

Sample ES Query are in es_queries.txt file

APIKey generation Step from DevTools at elastic.co: use “encoded” attribute from response as value of **ES_APIKEY** configuration.

```bash
post _security/api_key
{
  "name": "Some-Key-Name-For-Reference",
  "role_descriptors": {
    "my_role": {
      "cluster": ["all"],
      "index": [
        {
          "names": ["news*"],
          "privileges": ["all"]
        }
      ]
    }
  }
}
```

Reindex data from one index to another index in same cluster.
Reindex will copy all the documents and progress can be monitored using \_count command on destination index.
Size attribute defines the batch size.

```json
POST _reindex
{
  "source": {
    "index": "<SOURCE_LOCAL_INDEX>",
    "size": 200
  },
  "dest": {
    "index": "<DESTINATION_LOCAL_INDEX>"
  }
}
```

Reindex data from remote index to another index in different cluster.
Reindex will copy all the documents and progress can be monitored using \_count command on destination index.
Size attribute defines the batch size.

```json
POST _reindex
{
  "source": {
    "remote": {
      "host": "https://<REMOTE_ES_HOST>:443",
      "headers": {
        "Authorization": "ApiKey <ENCODED_AUTH_TOKEN>"
      }
    },
    "index": "<SOURCE_REMOTE_INDEX>",
    "size": 200
  },
  "dest": {
    "index": "<DESTINATION_LOCAL_INDEX>"
  }
}
```

Monitor the change in count in destination index

```json
GET <DESTINATION_LOCAL_INDEX>/_count
```

Remove all documents from an index

```json
POST <INDEX_NAME>/_delete_by_query
{
  "query": {
    "match_all" : {}
  }
}
```

Delete an index

```json
DELETE <INDEX_NAME>
```

Get disk usage by elasticsearch index

```json
GET _cat/allocation?v
```

Get shards allocation, document count and size of an index

```json
GET _cat/shards/<INDEX_NAME>?v
```

**Additional configurations used by lamda function**

- **ES_URL** - URL to access ElasticSearch, it contains protocol, ES host and port number.
- **ES_APIKEY** - Authentication key to access ‘documents’ index and related APIs in ES.
- **DB_TABLE_NAME** - DynamoDB host name
- **DB_REGION_NAME** - DynamoDB region name

# steps to create/setup lambda package for function to incremental sync news from DynamoDB to elasticsearch.

python3 -m venv sync_lambda
cd sync_lambda
source bin/activate
pip3 install --upgrade requests==2.29.0 -t .
pip3 install dynamodb_json -t .
cp <project_root>/lambda-incremental-updates.py .
zip -r lambda-incremental-updates.zip \*

<!-- zip -r lambda-incremental-updates.zip * -->
<!-- this is the correct one. There is a loose slash there that autosave keeps adding -->

lambda-incremental-updates.zip file can be uploaded as code for lamdba function, with trigger function as lambda-incremental-updates.lambda_handler

following environmental variables are also required.

- **ES_URL** - URL to access ElasticSearch, it contains protocol, ES host and port number.
- **ES_APIKEY** - Authentication key to access ‘documents’ index and related APIs in ES.

# steps to create/setup lambda package for function for fresh index creation for News from DynamoDB to elasticsearch.

following environmental variables are required.

- **ES_URL** - URL to access ElasticSearch, it contains protocol, ES host and port number.
- **ES_APIKEY** - Authentication key to access ‘documents’ index and related APIs in ES.
- **DB_TABLE_NAME** - DynamoDB host name
- **DB_REGION_NAME** - DynamoDB region name

pip3 insyall -r requirements.txt
python3 full_indexer.py

Important:
full_indexer.py was designed for lambda function but it takes longer than 15 minutes beyond which lambda function timesout.
This program can be executed in cloudshell or from a VM that has access to dynamoDB tables.
