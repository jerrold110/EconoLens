import boto3
from datetime import datetime, timedelta
import json

kb_id = "QPJDJ2SLQT"

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


def query_topic(start_date_str, end_date_str, topic, query, chunks_per_day=5):
    """
    Query data based on prompt and metadata filters
    Rerank data

    Roughly 5 chunks per day per topic. chunks are about ... tokens.
    """
    start_unixtime = date_to_unix(date_str=start_date_str)
    end_unixtime = date_to_unix(date_str=end_date_str, is_end=True)
    
    assert topic in ["consumer_behavior",
                     "corporate",
                     "economy_general",
                     "economy_long_term",
                     "government_and_policy",
                     "inflation",
                     "labor_market"]

    day_diff = days_difference(start_date_str, end_date_str)
    n_chunks = day_diff * chunks_per_day
    n_chunks_return = min(n_chunks, 50) # max 50 chunks returned

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
                                'value': topic
                            }
                        },
                        # # Keyword filters
                        # {
                        #     'listContains': {
                        #         'key': 'persons',
                        #         'value': "biden"
                        #     }
                        # },
                        # {
                        #     'listContains': {
                        #         'key': 'organizations',
                        #         'value': "white house"
                        #     }
                        # }

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
    payload = {'topic':topic,
                'chunks': n_chunks_return,
               'context':[]}
    
    for i in response['retrievalResults']:
        payload['context'].append(i['content']['text'])


    #print(payload)
    return payload

def lambda_handler(event, context):
    """
    
    """
    topic_to_query = {'labor_market': 'Events about labor markets, employment, unemployment and their effects on the economy'}

    params = event.get('parameters')
    print(params)
    abc
    start_date_str = event.get('start_date_str')
    end_date_str = event.get('end_date_str')
    topic = event.get('topic')

    payload = query_topic(event['start_date_str'],
                          event['end_date_str'],
                          event['topic'],
                          topic_to_query[event['topic']])
    
    agent = event['agent']
    actionGroup = event['actionGroup']
    function = event['function']
    parameters = event.get('parameters', [])

    response_body = {
        'TEXT': {
            'body': payload
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

    return action_response