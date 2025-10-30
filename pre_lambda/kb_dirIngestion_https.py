import os
import json
import uuid
import boto3
import requests
from dotenv import load_dotenv
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# Load environment variables
load_dotenv()

region = "us-east-1"
service = "bedrock"  # not bedrock-agent
knowledge_base_id = os.getenv("BEDROCK_KB_ID")
data_source_id = os.getenv("BEDROCK_KB_DATASOURCE_ID")

host = f"bedrock-agent.{region}.amazonaws.com"
url = f"https://{host}/knowledgebases/{knowledge_base_id}/datasources/{data_source_id}/documents"

payload = {
    "clientToken": str(uuid.uuid4()),
    "documents": [
        {
            "content": {
                "dataSourceType": "S3",
                "s3": {
                    "s3Location": {
                        "uri": "s3://econolens-data-enriched/2025-08-01/original/consumer_behavior/Tariffs?_In_This_Economy?_Good_Luck_With_That.txt"
                    }
                }
            },
            "metadata": {
                "type": "S3_LOCATION",
                "s3Location": {
                    "bucketOwnerAccountId": "975373241930",
                    "uri": "s3://econolens-data-enriched/2025-08-01/original/consumer_behavior/Tariffs?_In_This_Economy?_Good_Luck_With_That.txt.metadata.json"
                }
            }
        }
    ]
}

# Get AWS credentials
session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()

aws_request = AWSRequest(
    url=url,
    method="PUT",
    headers={
        "Host": host,
        "Content-Type": "application/json",
    },
    data=json.dumps(payload)
)

SigV4Auth(credentials, service, region).add_auth(aws_request)

response = requests.put(
    url,
    headers=dict(aws_request.headers),
    data=aws_request.body
)

print("Status Code:", response.status_code)
print("Response Body:", response.text)
