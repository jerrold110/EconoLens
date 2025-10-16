import os
import json
import requests
from dotenv import load_dotenv

import boto3
import time

# Load environment variables
load_dotenv()

def sync_data_source():

    knowledge_base_id = os.getenv("BEDROCK_KB_ID")
    data_source_id = os.getenv("BEDROCK_KB_DATASOURCE_ID")
    bedrock_apikey = os.getenv("BEDROCK_TOKEN")

    bedrock_agent_client = boto3.client('bedrock-agent')

    response = bedrock_agent_client.start_ingestion_job(
        knowledgeBaseId=knowledge_base_id,
        dataSourceId=data_source_id)

    bedrock_job_id = response['ingestionJob']['ingestionJobId']

    print("A new BKB ingestion job started with ID:", bedrock_job_id)

    # Wait until ingestion job completes
    print("Waiting until BKB ingestion job completes: ", end='')
    start_time = time.time()
    while True:
        response = bedrock_agent_client.get_ingestion_job(
            knowledgeBaseId = knowledge_base_id,
            dataSourceId = data_source_id,
            ingestionJobId = bedrock_job_id)
        if response['ingestionJob']['status'] == 'COMPLETE':
            print(" done.")
            end_time = time.time()
            break
        print('█', end='', flush=True)
        time.sleep(5)

    print("The BKB ingestion job finished:", json.dumps(response['ingestionJob'], indent=2, default=str))

    elapsed_time = end_time - start_time  
    print(f"Time taken: {elapsed_time} seconds")

sync_data_source()
