import boto3
from datetime import datetime, timedelta
import json

bedrock_agent_runtime = boto3.client('bedrock-agent-runtime')

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bedrock-agent-runtime/client/retrieve.html#
# https://docs.aws.amazon.com/bedrock/latest/userguide/kb-test-config.html
response = bedrock_agent_runtime.retrieve(
    knowledgeBaseId="QS2RHC3IUW",
    retrievalQuery={
        'text': query
    },
    retrievalConfiguration={
        'vectorSearchConfiguration': {
            'numberOfResults': n_chunks,
            'overrideSearchType': 'HYBRID',
            'filter': {
                'andAll': [
                    # Date filters
                    {
                        'greaterThanOrEquals': {
                            'key': 'unix_time',
                            'value': start_unixtime
                        }
                    },
                    {
                        'lessThanOrEquals': {
                            'key': 'unix_time',
                            'value': end_unixtime
                        }
                    },
                    # Topic filters
                    # {
                    #     'stringContains': {
                    #         'key': 'topic',
                    #         'value': topic
                    #     }
                    # },
                    # Keyword filters
                    {
                        'listContains': {
                            'key': 'persons',
                            'value': person.lower()
                        }
                    },
                    # {
                    #     'listContains': {
                    #         'key': 'organizations',
                    #         'value': instutition.lower()
                    #     }
                    # }
                ]
            },
            # aws bedrock get-foundation-model --model-identifier cohere.rerank-v3-5:0
            'rerankingConfiguration': {
                'type': 'BEDROCK_RERANKING_MODEL',
                'bedrockRerankingConfiguration': {
                    'numberOfRerankedResults': 80,
                    'modelConfiguration': {
                        'modelArn': 'arn:aws:bedrock:us-east-1::foundation-model/cohere.rerank-v3-5:0',
                    },
                    
                }
            }
        }
    }
)
print(response)
for i in response['retrievalResults']:
    print(i, "\n\n")

sd = '2025-08-01'
ed = '2025-08-31'
topic = 'inflation'
person = 'Jerome Powell'
query = "what did jerome do in August 2025"

kb_id = "QS2RHC3IUW"

query_date_topic(sd, ed)

def lambda_handler(event, context):
    """
    
    """

    result = query_date_topic(event['start_date_str'],
                     event['end_date_str'],
                     event['topic'])

    return {
        "statusCode": 200,
        "batch_date": event['batch_date'],
        "body": json.dumps({
            "message": f"Function process_date with argument {event['batch_date']} finished",
        }),
    }
