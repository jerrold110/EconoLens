import boto3
from datetime import datetime, timedelta
import json

def date_to_unix(date_str:str, is_end:bool=False) -> int:
    """
    Input string is in the format yyyy-mm-dd. Returns unix timestamp as an integer.
    """
    try:
        #print(f"Function input: {date_str}")
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        if is_end == True:
            dt = dt + timedelta(days=1)

    except ValueError:
        raise AssertionError(f"Date string '{date_str}' does not follow %Y-%m-%d format.")
    
    unix = dt.timestamp()
    # print(unix)
    return unix

def days_difference(start_date_str, end_date_str) -> int:
    """
    Logic is inclusive
    """
    sd = datetime.strptime(start_date_str, '%Y-%m-%d')
    ed = datetime.strptime(end_date_str, '%Y-%m-%d')

    difference = ed - sd
    number_of_days = difference.days

    return number_of_days + 1


def query_date_topic(start_date_str, end_date_str, topic, chunks_per_day=4):
    """
    Query data based on prompt and metadata filters
    Rerank data

    Roughly 4 chunks per day per topic. chunks are about 500 tokens.
    """
    start_unixtime = date_to_unix(start_date_str)
    end_unixtime = date_to_unix(end_date_str)
    
    assert topic in ["consumer_behavior",
                     "corporate",
                     "economy_general",
                     "economy_long_term",
                     "government_and_policy",
                     "inflation",
                     "labor_market"]

    day_diff = days_difference(start_date_str, end_date_str)
    n_chunks = day_diff * chunks_per_day

    bedrock_agent_runtime = boto3.client('bedrock-agent-runtime')

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bedrock-agent-runtime/client/retrieve.html#
    # https://docs.aws.amazon.com/bedrock/latest/userguide/kb-test-config.html
    response = bedrock_agent_runtime.retrieve(
        knowledgeBaseId='LWVHC1VCWF',
        retrievalQuery={
            'text': 'economy'
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
                        {
                            'stringContains': {
                                'key': 'topic',
                                'value': "economy_long_term"
                            }
                        },
                        # Keyword filters
                        {
                            'listContains': {
                                'key': 'persons',
                                'value': "biden"
                            }
                        },
                        {
                            'listContains': {
                                'key': 'organizations',
                                'value': "white house"
                            }
                        }

                    ]
                },
                # aws bedrock get-foundation-model --model-identifier cohere.rerank-v3-5:0
                'rerankingConfiguration': {
                    'type': 'BEDROCK_RERANKING_MODEL',
                    'bedrockRerankingConfiguration': {
                        'modelConfiguration': {
                            'modelArn': 'arn:aws:bedrock:us-east-1::foundation-model/cohere.rerank-v3-5:0',
                        },
                        
                    }
                }
            }
        }
    )
#    print(response)
    for i in response['retrievalResults']:
        print(i, "\n\n")

sd = '2025-08-01'
ed = '2025-08-02'
topic = 'corporate'
query_date_topic(sd, ed, topic)

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
