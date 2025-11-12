import boto3
from datetime import datetime, timedelta
import json
import os

kb_id = os.environ.get("BEDROCK_KNOWLEDGE_BASE")

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


def query_topic_labor_market(start_date_str, end_date_str, query, chunks_per_day=5):
    """
    Query data based on prompt and metadata filters
    Rerank data

    Roughly 5 chunks per day per topic. chunks are about ... tokens.
    """
    print('unix start and end')
    start_unixtime = date_to_unix(date_str=start_date_str)
    end_unixtime = date_to_unix(date_str=end_date_str, is_end=True)
    
    # assert topic in ["consumer_behavior",
    #                  "corporate",
    #                  "economy_general",
    #                  "economy_long_term",
    #                  "government_and_policy",
    #                  "inflation",
    #                  "labor_market"]

    day_diff = days_difference(start_date_str, end_date_str)
    # max numberOfResults allowed is 100 chunks
    n_chunks = 2 #min(day_diff * chunks_per_day, 100)
    n_chunks_return = 2 #min(n_chunks, 50) # 50 chunks returned

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
                                'value': "labor_market"
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
    payload = {'topic':"labor_market",
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
    print("input parameters:")
    print(params)

    start_date_str = next(d['value'] for d in params if d['name'] == 'start_date_str')
    end_date_str = next(d['value'] for d in params if d['name'] == 'end_date_str')
    
    query = 'Events about labor markets, employment, unemployment and their effects on the economy'
    payload = query_topic_labor_market(start_date_str,
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

# print(query_topic_labor_market("2025-08-01",
#                             "2025-08-02",
#                             'Events about labor markets, employment, unemployment and their effects on the economy'
#                             ))