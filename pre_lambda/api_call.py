import boto3
from botocore.exceptions import ClientError

import requests
from dotenv import load_dotenv

import json
import re
from datetime import datetime, timedelta

# API key for GNews
def get_gnews_api_key():

    secret_name = "Gnews-api-key"
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
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])
    return secret['GNEWS_API_KEY']

def store_article(folder_name, article, topic, s3_client):

    bucket_name = "econolens-staging-area"

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
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json_data,
            ContentType="application/json"
        )
        print(f"Successfully uploaded: {object_key} to {bucket_name}")
    except Exception as e:
        print(f"Upload failed: {e}")

def process_topic(start_date_str:str, topic_str:str, apikey:str):
    """
    date_prefix is in the format yyyy-mm-dd
    """

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
    print(json.dumps(response.json(), indent=4))

    if response.status_code == 200:
        
        response = response.json()
        # check if response is empty
        if response:
            articles = response['articles']

            s3_client = boto3.client("s3")
            for a in articles:
                store_article(folder_name, a, topic_str, s3_client)

    else:
        print(f"Error: {response.status_code}")

    print(f'------------ End topic {topic_str} on {start_date_str} ------------ \n')

def process_date(start_date_str:str):
    """
    
    """
    pattern = r"^\d{4}-\d{2}-\d{2}$"
    assert (re.match(pattern, start_date_str))

    api_key = get_gnews_api_key()
    topics = ['economy_general', 'economy_long_term', 'labor_market', 'inflation', 'consumer_behavior', 'government_and_policy', 'corporate']

    for topic in topics:
        process_topic(start_date_str, topic, api_key)

# process_date('2025-08-04')
# process_date('2025-08-05')
# process_date('2025-08-06')
# process_date('2025-08-07')
# process_date('2025-08-08')
process_date('2025-09-01')
