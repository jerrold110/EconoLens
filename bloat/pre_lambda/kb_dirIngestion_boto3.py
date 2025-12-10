import boto3
from botocore.exceptions import ClientError

from dotenv import load_dotenv

import os
from os.path import join, dirname
import json


dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

knowlegebase_id = os.environ.get("BEDROCK_KB_ID")
knowledgebase_source_id = os.environ.get("BEDROCK_KB_DATASOURCE_ID")

client = boto3.client('bedrock-agent')

response = client.ingest_knowledge_base_documents(
    knowledgeBaseId=knowlegebase_id,
    dataSourceId=knowledgebase_source_id,
    documents=[
        {
            
            'content': {
                'dataSourceType': 'S3',
                's3': {
                    's3Location': {
                        'uri': "s3://econolens-data-enriched/2025-08-01/original/consumer_behavior/Tariffs?_In_This_Economy?_Good_Luck_With_That.txt"
                    }
                }
            },
            'metadata': {
                'type': 'S3_LOCATION',
                's3Location': {
                    'uri': "s3://econolens-data-enriched/2025-08-01/original/consumer_behavior/Tariffs?_In_This_Economy?_Good_Luck_With_That.txt.metadata.json",
                    'bucketOwnerAccountId': '975373241930'
                }
            },
            # 'metadata': {
            #     'type': 'IN_LINE_ATTRIBUTE',
            #     'inlineAttributes': [
            #         {
            #             'key': 'title',
            #             'value': {
            #                 'type': 'STRING',
            #                 'stringValue': 'thisIsMyTitle'
            #             }
            #         }
            #     ]
            # }
            
        }
    ]
)
#print(response['HTTPStatusCode'])
print(response)