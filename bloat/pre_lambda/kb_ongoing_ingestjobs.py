import boto3
import json

client = boto3.client("bedrock-agent", region_name="us-east-1")

# response = client.list_ingestion_jobs(
#     knowledgeBaseId="GETBPVLI55",
#     dataSourceId="DT0NM2KJX3"
# )

# print(json.dumps(response, indent=2))

docs = client.list_knowledge_base_documents(
    knowledgeBaseId="UBOWHZSNBG",
    dataSourceId="FPFIQDISSE"
)

print(docs)