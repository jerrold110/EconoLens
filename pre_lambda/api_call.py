import boto3
from botocore.exceptions import ClientError

import requests
from dotenv import load_dotenv

import os
import json
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

def store_article(folder_name, article, s3_client):

    bucket_name = "econolens-staging-area"

    article_title = article['title'].replace(" ", "_")
    object_key = f"{folder_name}/{article_title}.json"

    data = {
        'title': article['title'],
        'description': article['description'],
        'publishedAt': article['publishedAt'],
        'content': article['content']
    }
    json_data = json.dumps(data, indent=4)
    print(f'Processing article {article_title} on {object_key}')

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json_data,
            ContentType="application/json"
        )
        print(f"✅ Successfully uploaded {object_key} to {bucket_name}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")


def process_topic(start_date_str:str, topic_keywords_str:str, apikey:str):
    """
    date_prefix is in the format yyyy-mm-dd
    """
    url = f"https://gnews.io/api/v4/search?q=example&apikey={apikey}"

    # Parse start date and compute end date (next day)
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = start_date + timedelta(days=1)
    
    # Format dates in ISO 8601 format for API
    from_time = start_date.strftime("%Y-%m-%dT00:00:00.000Z")
    to_time = end_date.strftime("%Y-%m-%dT00:00:00.000Z")

    folder_name = f'{start_date_str}/{topic_keywords_str}'

    # 10 articles per topic
    params = {
        'q': topic_keywords,
        'lang': 'en',
        'country': 'us',
        'in': 'title,description,content',
        'nullable': 'image',
        'max': '10',
        'from': from_time,
        'to': to_time,
        'sortby': 'relevance',
        'expand': 'content'
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        response = response.json()
        # check if response is empty
        if response:
            articles = response['articles']

            s3_client = boto3.client("s3")
            for a in articles:
                store_article(folder_name, a, s3_client)

    else:
        print(f"Error: {response.status_code}")




    
search_keywords = {
        'economy_general': '((Economy) OR (Economic growth) OR (economic slowdown) OR (Recession) OR (Economic downturn)) AND (USA OR America)',
        'economy_long-term': '((Economy) OR (National output) OR (National income)) AND (USA OR America)',
        'labor_market': '((Economy) OR (Labor market) OR (jobless) OR (unemployment)) AND (USA OR America)',
        'inflation': '(Inflation) AND (USA OR America)',
        'consumer behavior': '((Economy) OR (Retail sales) OR (consumer spending)) AND (USA OR America)',
        'government and policy': '((Federal Reserve) OR (Fed policy) OR (Interest rate) OR (rate cuts) OR (Treasury)) AND (USA OR America)',
        'corporate': '((merger) OR (acquisition) OR (corporate earning)) AND (USA OR America)'
    }


api_key = get_gnews_api_key()

topic_keywords = search_keywords['economy_general']

process_topic('2025-09-01', topic_keywords, api_key)
