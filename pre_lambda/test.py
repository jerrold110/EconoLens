import boto3

client = boto3.client('bedrock-agent')

response = client.ingest_knowledge_base_documents(
    knowledgeBaseId='your-knowledge-base-id',
    dataSourceId='your-data-source-id',
    documents=[
        {
            'content': {
                'dataSourceType': 'S3',
                's3': {
                    's3Location': {'uri': 's3://your-bucket/path/to/file.txt'}
                }
            },
            'metadata': {
                'inlineAttributes': [
                    {
                        'key': 'category',
                        'value': {'stringValue': 'finance', 'type': 'STRING'}
                    }
                ]
            }
        }
    ]
)

print(response)
===========================================================================================







import boto3
from datetime import datetime

client = boto3.client("bedrock-agent")

def to_epoch(date_str):
    return int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())

response = client.ingest_knowledge_base_documents(
    knowledgeBaseId="kb-xxxxxxx",
    dataSourceId="ds-xxxxxxx",
    documents=[
        {
            "content": {
                "dataSourceType": "S3",
                "s3": {"s3Location": {"uri": "s3://my-data/quarterly_reports.csv"}}
            },
            "metadata": {
                "inlineAttributes": [
                    {
                        "key": "date",
                        "value": {"numberValue": to_epoch("2024-04-30"), "type": "NUMBER"}
                    }
                ]
            }
        }
    ]
)
