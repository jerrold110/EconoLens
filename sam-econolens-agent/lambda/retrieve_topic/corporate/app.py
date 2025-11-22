import boto3
from datetime import datetime, timedelta
import json
import os
import random, time
from botocore.exceptions import ClientError

kb_id = os.environ.get("BEDROCK_KNOWLEDGE_BASE_ID")
day_chunk = int(os.environ.get("RETRIEVE_CHUNK_PER_DAY_COUNT"))
rerank_chunk_return = int(os.environ.get("RERANK_CHUNK_COUNT"))
query = 'Everything related to corporate news including mergers, acquisitions, earnings, layoffs, and finance'

def with_backoff(func):
    """
    Decorator that retries a function with random backoff when a ThrottlingException (or other retryable error) occurs.
    """
    def wrapper(*args, **kwargs):
        max_retries = 5
        max_backoff = 10

        for attempt in range(max_retries):
            try:
                # Call the wrapped function
                sleep = random.uniform(0, max_backoff)
                print(f"Initial call. Sleeping {sleep:.2f}s")
                return func(*args, **kwargs)

            except ClientError as e:
                code = e.response["Error"]["Code"]

                # Only retry on throttling
                if code != "ThrottlingException":
                    raise

                # Compute backoff 
                sleep = random.uniform(1, max_backoff)
                print(f"[retry {attempt+1}] Throttled. Sleeping {sleep:.2f}s")
                time.sleep(sleep)

        raise RuntimeError("Exceeded retry limit due to ThrottlingException")
    return wrapper

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
    print(unix)
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

@with_backoff
def query_topic_corporate(start_date_str, end_date_str, query, chunks_per_day=day_chunk, rerank_chunk_return=rerank_chunk_return):
    """
    Query data based on prompt and metadata filters
    Rerank data

    Roughly 5 chunks per day per topic. chunks are about ... tokens.
    """
    start_unixtime = date_to_unix(date_str=start_date_str)
    end_unixtime = date_to_unix(date_str=end_date_str, is_end=True)

    day_diff = days_difference(start_date_str, end_date_str)
    # max numberOfResults allowed is 100 chunks
    # max numberOfResults allowed is 100 chunks
    n_chunks_retrieve = min(day_diff * chunks_per_day, 100)
    n_chunks_return = min(rerank_chunk_return, 100)

    bedrock_agent_runtime = boto3.client('bedrock-agent-runtime')

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/bedrock-agent-runtime/client/retrieve.html#
    # https://docs.aws.amazon.com/bedrock/latest/userguide/kb-test-config.html
    response = bedrock_agent_runtime.retrieve(
        knowledgeBaseId=kb_id,
        retrievalQuery={
            'text': query
        },
        retrievalConfiguration={
            'vectorSearchConfiguration': {
                'numberOfResults': n_chunks_retrieve,
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
                            'lessThan': {
                                'key': 'unix_time',
                                'value': end_unixtime
                            }
                        },
                        # Topic filters
                        {
                            'stringContains': {
                                'key': 'topic',
                                'value': "corporate"
                            }
                        },
                    ]
                },
                # aws bedrock get-foundation-model --model-identifier cohere.rerank-v3-5:0
                'rerankingConfiguration': {
                    'type': 'BEDROCK_RERANKING_MODEL',
                    'bedrockRerankingConfiguration': {
                        'numberOfRerankedResults': n_chunks_return,
                        'modelConfiguration': {
                            'modelArn': 'arn:aws:bedrock:us-east-1::foundation-model/cohere.rerank-v3-5:0',
                        },
                        
                    }
                }
            }
        }
    )
#    print(response)
    payload = {'topic':"corporate",
               'chunks': n_chunks_return,
               'context':[]}
    
    for i in response['retrievalResults']:
        payload['context'].append(i['content']['text'])

    #print(payload)
    return payload

def lambda_handler(event, context):
    """
    
    """
    

    params = event.get('parameters')
    print("Event:")
    print(event)
    print("Input parameters:")
    print(params)

    start_date_str = next(d['value'] for d in params if d['name'] == 'start_date_str')
    end_date_str = next(d['value'] for d in params if d['name'] == 'end_date_str')

    payload = query_topic_corporate(start_date_str,
                                       end_date_str,
                                       query)
    agent = event['agent']
    actionGroup = event['actionGroup']
    function = event['function']
    parameters = event.get('parameters', [])

    response_body = {
        'TEXT': {
            'body': json.dumps(payload)##{"context":["The unemployment rate is at 10%"]}
        }
    }

    function_response = {
        'actionGroup': event['actionGroup'],
        'function': event['function'],
        'functionResponse': {
            'responseBody': response_body
        }
    }

    session_attributes = event['sessionAttributes']
    prompt_session_attributes = event['promptSessionAttributes']
    
    action_response = {
        'messageVersion': '1.0', 
        'response': function_response,
        'sessionAttributes': session_attributes,
        'promptSessionAttributes': prompt_session_attributes
    }
    print('Returning the following: ')
    print(action_response)
    return action_response


# print(query_topic_corporate("2025-08-01",
#                             "2025-08-02",
#                             'Mergers, acquisitions, earnings, corporate events, layoffs'
#                             ))