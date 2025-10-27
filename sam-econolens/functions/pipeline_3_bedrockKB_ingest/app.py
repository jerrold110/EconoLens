import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError, BotoCoreError

import json
import time
import os

knowledge_base_id = os.getenv("BEDROCK_KB_ID")
data_source_id = os.getenv("BEDROCK_KB_DATASOURCE_ID")
# bedrock_apikey = os.getenv("BEDROCK_TOKEN")

def sync_data_source():
    """
    start_ingestion_job is an asynchronous process, hence lambda timeout at 15 minutes is not an issue

    Response documentation
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bedrock-agent/client/start_ingestion_job.html
    """

    bedrock_agent_client = boto3.client('bedrock-agent')

    try:
        response = bedrock_agent_client.start_ingestion_job(
            knowledgeBaseId=knowledge_base_id,
            dataSourceId=data_source_id)

        bedrock_job_id = response['ingestionJob']['ingestionJobId']

        print(f"A new BKB ingestion job started with ID: {bedrock_job_id} request has been sent")
        print(f"status: {response['ingestionJob']['status']}")
        print(f"statistics: {response['ingestionJob']['statistics']}")
        # If failure encountered
        if 'failureReasons' in response['ingestionJob'].keys():
            print(f"failureReasons: {response['ingestionJob']['failureReasons']}")

        # Wait until ingestion job completes
        # print("Waiting until BKB ingestion job completes: ", end='')
        # start_time = time.time()
        # while True:
        #     response = bedrock_agent_client.get_ingestion_job(
        #         knowledgeBaseId = knowledge_base_id,
        #         dataSourceId = data_source_id,
        #         ingestionJobId = bedrock_job_id)
        #     if response['ingestionJob']['status'] == 'COMPLETE':
        #         print(" done.")
        #         end_time = time.time()
        #         break
        #     print('█', end='', flush=True)
        #     time.sleep(5)

        # print("The BKB ingestion job finished:", json.dumps(response['ingestionJob'], indent=2, default=str))

        # elapsed_time = end_time - start_time  
        # print(f"Time taken: {elapsed_time} seconds")
    
    except BotoCoreError as e:
        print(f"Low-level Boto3/Botocore error: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


def lambda_handler(event, context):

    sync_data_source()

    return {
        "statusCode": 200,
        "batch_date": event['batch_date'],
        "body": json.dumps({
            "message": f"Function sync_data_source() call finished",
        }),
    }


