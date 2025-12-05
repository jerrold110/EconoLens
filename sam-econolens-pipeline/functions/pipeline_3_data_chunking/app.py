"""
The chunking is not perfect, but still decent, and far better than Naive chunking

Metadata includes title of article for tracing RAG sources
Metadata persons/orgs tags are lower-cased

If function times out, break up topics into 2 or 3 lambdas instead of processing all at once and add to DAG
"""

import boto3
from botocore.exceptions import ClientError

import json, os, time, random
from datetime import datetime, timedelta
from os.path import join, dirname

# -------------------------------
# Helpers functions
# -------------------------------

from langchain_experimental.text_splitter import SemanticChunker
from langchain_aws import BedrockEmbeddings
# amazon.titan-embed-text-v1
# https://huggingface.co/amazon/Titan-text-embeddings-v2  lightweight and effective  
# https://us-east-1.console.aws.amazon.com/bedrock/home?region=us-east-1#/model-catalog/serverless/amazon.titan-embed-text-v2:0

# Summarize functions

def with_backoff(func):
    """
    Decorator that retries a function with random backoff when a ThrottlingException (or other retryable error) occurs.
    """
    def wrapper(*args, **kwargs):
        max_retries = 3
        max_backoff = 5

        for attempt in range(max_retries):
            try:
                # Call the wrapped function
                sleep = random.uniform(1, max_backoff)
                print(f"Initial call. Sleeping {sleep:.2f}s")
                return func(*args, **kwargs)

            except ClientError as e:
                code = e.response["Error"]["Code"]

                # Only retry on throttling
                if code != "ThrottlingException":
                    raise

                # Compute backoff 
                sleep = random.uniform(2, max_backoff)
                print(f"[retry {attempt+1}] Throttled. Sleeping {sleep:.2f}s")
                time.sleep(sleep)

        raise RuntimeError("Exceeded retry limit due to ThrottlingException")
    return wrapper

def summarize_chunk(text):
    """
    Function for summarizing text with nova-lita into <800 tokens
    """

    system = [{ "text": "You are a text summarizer for news articles. Summarize text as a coherent piece of writing while preserving important information into less than 600 words" }]

    messages = [
        {"role": "user", "content": [{"text": f"{text}"}]},
    ]

    inf_params = {"maxTokens": 850, "topP": 0.3, "temperature": 0.7}

    try:
        model_response = bedrock_client.converse(
            modelId="us.amazon.nova-lite-v1:0", 
            messages=messages, 
            system=system, 
            inferenceConfig=inf_params,
        )

        http_code = json.dumps(model_response['ResponseMetadata']['HTTPStatusCode'], indent=2)
        print(f"Response Http code: {http_code}")

        return model_response["output"]["message"]["content"][0]["text"]
    
    except Exception as e:
        print(f"An error occurred: {e} \n Text: \n {text[:200]}")
        return ""


_embeddings = BedrockEmbeddings(model_id="amazon.titan-embed-text-v2:0") 
_Semchunker = SemanticChunker(_embeddings, breakpoint_threshold_type='percentile', breakpoint_threshold_amount=96) # 1% difference from default

def _chunk_text(document_text:str, key:str) -> list:
    """
    Semantic chunking with an AWS text embedding model, requires authentication to AWS services. Uses text embeddings v1 instead of v2 because of better performance at
    higher compute cost
    https://pypi.org/project/langchain-aws/

    Tested with:
    Langchain-experimental: 0.3.4
    Langchain-aws: 0.2.35

    https://api.python.langchain.com/en/latest/text_splitter/langchain_experimental.text_splitter.SemanticChunker.html

    """
    print(f"\t\tStarting chunking of document: {key}")
    start_time = time.time()

    chunks = _Semchunker.split_text(document_text)
    end_time = time.time()
    print(f"\t\tChunking completed into: {len(chunks)} chunks")

    elapsed_time = end_time - start_time
    print(f"\t\tTime taken: {elapsed_time:.2f} seconds")

    return chunks

def copy_metadata_for_chunk(client,
                          source_bucket, 
                          dest_bucket, 
                          document_key,
                          chunk_doc_key):
    
    metadata_key = document_key.replace(".txt", ".txt.metadata.json")
    chunk_metadata_key = chunk_doc_key.replace(".txt", ".txt.metadata.json")

    response = client.get_object(Bucket=source_bucket, Key=metadata_key)

    try:
        metadata_obj = json.loads(response["Body"].read().decode("utf-8")) # It was stored as utf-8

        json_bytes = json.dumps(metadata_obj, ensure_ascii=False).encode("utf-8") # Encode in utf-8 again
        client.put_object(
            Bucket=dest_bucket,
            Key=chunk_metadata_key,
            Body=json_bytes,
            ContentType="application/json; charset=utf-8"
        )
        print(f"✅ \tProcessed chunk metadata {chunk_metadata_key}")
    
    except Exception as e:
        raise Exception(f"Error processing metadata for chunk {chunk_metadata_key}: {e}")

    

def chunk_and_copy(client, 
                    source_bucket, 
                    dest_bucket, 
                    doc_key):
    """
    Each chunk .txt file must have a corresponding Metadata file 
    since the plan is to ingest the same metadata for every document's chunk
    
    """
    print(f"🔹 Processing text document: {doc_key}")
    try:
        response = client.get_object(Bucket=source_bucket, Key=doc_key)
        document_str = response["Body"].read().decode("utf-8") # It was stored as utf-8
        
        chunks = _chunk_text(document_str, doc_key)

        # Prepare prefix and suffix
        path = doc_key.rsplit('.', 1)[0]
        ext = doc_key.rsplit('.', 1)[1]

        for i, chunk_text in enumerate(chunks, start=1):
            # If chunk_text is over 800 tokens (600 words), invoke Nova-lite with boto3 to summarize it
            if len(chunk_text.split()) > 600:
                print("⚠️  Chunk is over 800 tokens. Calling bedrock fm to summarize")
                chunk_text = summarize_chunk(chunk_text)
            else:
                print("✅ Chunk is less than 800 tokens")

            destination_path = f"{path}_chunk_{i}_.{ext}" # eg: 2025-10-11/economy_general/filename_chunk_1.txt

            client.put_object(Bucket=dest_bucket,
                            Key=destination_path,
                            Body=chunk_text.encode("utf-8"), # Encode as utf-8 again
                            ContentType="text/plain; charset=utf-8"
                            )
            print(f"✅  \tProcessed doc chunk {destination_path}")
            copy_metadata_for_chunk(client,
                                    source_bucket,
                                    dest_bucket,
                                    doc_key,
                                    destination_path)
            #print(f"🔹 \tProcessed chunk metadata {destination_path}")
            # message is inside above function
    except Exception as e:
        print(f"❌ Error processing text file {doc_key}: {e}")
    except json.JSONDecodeError:
        print(f"❌ Error opening JSON metadata: {e}")

def loop_doc_and_metadata(date_prefix):
    """
    Loops over text documents, chunks them, copies them to destination bucket with their associated metadata

    Transformations:
      - source: 2025-10-11/economy_general/filename.json
      - source: 2025-10-11/economy_general/filename.txt.metadata.json
        → dest: 2025-10-11/economy_general/filename_chunk_1.txt
        → dest: 2025-10-11/economy_general/filename_chunk_2.txt
        ....
        → dest: 2025-10-11/economy_general/filename.txt.metadata.json
        ....

    """
    s3 = boto3.client("s3", region_name="us-east-1")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=source_bucket, Prefix=date_prefix)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            print(key)
            # Process metadata file
            if key.endswith(".json"):
                pass
                
            # Chunk and process text file (document)
            elif key.endswith(".txt"):
                chunk_and_copy(s3, source_bucket, dest_bucket, key)
                print(f"✅✅ Processed {key}")

# -------------------------------
# Main function
# -------------------------------

source_bucket = os.environ.get("S3_ENRICH")
dest_bucket = os.environ.get("S3_CHUNK")
print(source_bucket, dest_bucket)

# Client for model inference call
bedrock_client = boto3.client("bedrock-runtime")

def chunk_doc_and_metadata(datetime_input:str):
    try:
        datetime.strptime(datetime_input, '%Y-%m-%dT%H:%M:%SZ')
        # Parse string, then back into string with new format
        date_prefix = datetime.strptime(datetime_input, '%Y-%m-%dT%H:%M:%SZ') - timedelta(days=1) # move start_date_str back one day
        date_prefix = date_prefix.strftime("%Y-%m-%d")
        print(f"Function input: {date_prefix}")
    except ValueError:
        raise AssertionError(f"Date string '{datetime_input}' does not follow %Y-%m-%dT%H:%M:%SZ format.")
    
    print("-----Begin data chunking-----")
    overall_start_time = time.time()
    loop_doc_and_metadata(date_prefix)
    print("-----End data chunking-----\n")
    overall_end_time = time.time()
    overall_elapsed_time = overall_end_time - overall_start_time
    print(f"\t\tTime taken: {overall_elapsed_time:.2f} seconds")

def lambda_handler(event, context):

    chunk_doc_and_metadata(event['batch_date'])

    return {
        "statusCode": 200,
        "batch_date": event['batch_date'],
        "body": json.dumps({
            "message": f"Function chunk_doc_and_metadata with argument {event['batch_date']} finished",
        }),
    }


