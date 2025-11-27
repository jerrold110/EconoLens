import boto3
from botocore.exceptions import ClientError

import requests

import json
from datetime import datetime, timedelta
import os

bucket_name = os.environ.get("S3_STAGE")

# API key for GNews from AWS secrets
def get_gnews_api_key():

    secret_name = "gnews-api-key-3"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        print("Secret successfully retrieved from secrets")
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])
    return secret['GNEWS_API_KEY'] # GNEWS_API_KEY is the key of the secret

def store_article(folder_name, article, topic, s3_client):
    
    article_title = article['title'].replace(" ", "_")
    object_key = f"{folder_name}/{article_title}.json"

    data = {
        'title': article['title'],
        'description': article['description'],
        'publishedAt': article['publishedAt'],
        'topic': topic,
        'content': article['content']
    }
    json_data = json.dumps(data, indent=4)
    print(f'Processing: {article_title} on {folder_name}')

    try:
        # print(bucket_name)
        # print(object_key)
        # print(json_data)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json_data,
            ContentType="application/json"
        )
        
        print(f"Successfully uploaded: {object_key} to {bucket_name}")
        return True
    except Exception as e:
        print(f"Upload failed: {e}")
        return False

def process_topic(start_date_str:str, topic_str:str, apikey:str):
    """
    date_prefix is in the format yyyy-mm-dd
    """

    # search keywords for each topic
    keywords = {
        'economy_general': '(Tax) OR (Tariff)',
        'economy_long_term': '((American OR US) AND Economy) OR (National output) OR (National income)',
        'labor_market': '(Labor market) OR (jobless) OR (unemployment)',
        'inflation': '(Inflation)',
        'consumer_behavior': '(Retail sales) OR (consumer spending) OR (disposable income) OR (household spending)',
        'government_and_policy': '(Federal Reserve) OR (Fed policy) OR (Interest rate) OR (rate cuts) OR (Treasury)',
        'corporate': '(merger) OR (acquisition) OR (corporate earning)'
    }
    topic_keywords = keywords[topic_str]

    url = f"https://gnews.io/api/v4/search?q=example&apikey={apikey}"

    # Parse start date and compute end date (next day)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = start_date + timedelta(days=1)
    
    # Format dates in ISO 8601 format for API
    from_time = start_date.strftime("%Y-%m-%dT00:00:00.000Z")
    to_time = end_date.strftime("%Y-%m-%dT00:00:00.000Z")

    folder_name = f'{start_date_str}/{topic_str}'

    # 10 articles per topic
    params = {
        'q': topic_keywords,
        'lang': 'en',
        'country': 'us',
        'in': 'title,description', # do not search content for keywords as it hampers search query results
        'nullable': 'image',
        'max': '10',
        'from': from_time,
        'to': to_time,
        'sortby': 'relevance',
        'expand': 'content' # content, or None
    }
    print(f'------------ Start topic {topic_str} on {start_date_str} ------------')
    response = requests.get(url, params=params)
    # for debugging
    #print(json.dumps(response.json(), indent=4))
    article_count, ingested_count = 0, 0
    if response.status_code == 200:
        
        response = response.json()
        if response: # check if response is empty
            articles = response['articles']

            s3_client = boto3.client("s3")
            
            for a in articles:
                status = store_article(folder_name, a, topic_str, s3_client)
                article_count += 1
                if status == True:
                    ingested_count += 1
    else:
        print(f"Error: {response.status_code}")
    print(f"Ingested {ingested_count} out of {article_count} articles")
    print(f'------------ End topic {topic_str} on {start_date_str} ------------ \n')

def process_date(input_datetime_str:str):
    """
    Process the data on previous day's date. Input comes from eventbridge context object in ISO 8601 format.
    https://docs.aws.amazon.com/scheduler/latest/UserGuide/managing-schedule-context-attributes.html
    """
    try:
        datetime.strptime(input_datetime_str, '%Y-%m-%dT%H:%M:%SZ') # throws an error if input format is different
        previous_date = datetime.strptime(input_datetime_str, '%Y-%m-%dT%H:%M:%SZ') - timedelta(days=1) # move start_date_str back one day
        ystd_date_str = previous_date.strftime("%Y-%m-%d")
        print(f"->input_datetime_str,: {input_datetime_str,}\n->ystd_date_str: {ystd_date_str}")

    except ValueError:
        raise AssertionError(f"Date string '{input_datetime_str},' does not follow %Y-%m-%dT00:00:00.000Z format.")

    api_key = get_gnews_api_key()
    topics = ['economy_general', 'economy_long_term', 'labor_market', 'inflation', 'consumer_behavior', 'government_and_policy', 'corporate']

    for topic in topics:
        process_topic(ystd_date_str, topic, api_key)


def lambda_handler(event, context):
    """
    
    """

    process_date(event['batch_date'])

    return {
        "statusCode": 200,
        "batch_date": event['batch_date'],
        "body": json.dumps({
            "message": f"Function process_date with argument {event['batch_date']} finished",
        }),
    }


