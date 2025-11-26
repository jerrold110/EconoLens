"""
The chunking is not perfect, but still decent, and far better than Naive chunking

Metadata includes title of article for tracing RAG sources
Metadata persons/orgs tags are lower-cased
"""

import boto3
from botocore.exceptions import ClientError

from dotenv import load_dotenv

import json, os, time
from datetime import datetime
from os.path import join, dirname
from dotenv import load_dotenv

# -------------------------------
# Helper functions
# -------------------------------

from langchain_experimental.text_splitter import SemanticChunker
from langchain_aws import BedrockEmbeddings
# amazon.titan-embed-text-v1
# https://huggingface.co/amazon/Titan-text-embeddings-v2 
# https://us-east-1.console.aws.amazon.com/bedrock/home?region=us-east-1#/model-catalog/serverless/amazon.titan-embed-text-v2:0
_embeddings = BedrockEmbeddings(model_id="amazon.titan-embed-text-v2:0") # lightweight and effective  
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
    print(f"\t\tChunking completed into {len(chunks)} chunks: {key}")

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

# -------------------------------
# Main function
# -------------------------------

def process_doc_and_metadata(date_prefix, source_bucket, dest_bucket):
    """
    Copies JSON files from `source_bucket` whose keys start with `date_prefix`
    to `dest_bucket`, extracting content and metadata separately

    Transformations:
      - source: 2025-10-11/economy_general/filename.json
      - source: 2025-10-11/economy_general/filename.txt.metadata.json
        → dest: 2025-10-11/economy_general/filename_chunk_1.txt
        → dest: 2025-10-11/economy_general/filename_chunk_2.txt
        ....
        → dest: 2025-10-11/economy_general/filename.txt.metadata.json

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
                


def action(date_prefix):
    try:
        datetime.strptime(date_prefix, '%Y-%m-%d')
    except ValueError:
        raise AssertionError(f"Date string '{date_prefix}' does not follow YYYY-MM-DD format.")
    
    print("-----Begin process_doc_and_metadata-----")
    process_doc_and_metadata(date_prefix, source_bucket, dest_bucket)
    print("-----End process_doc_and_metadata-----\n")



dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
source_bucket = os.environ.get("S3_DESTINATION")
dest_bucket = os.environ.get("S3_DESTINATION_1")

action('2025-08-01')
#action('2025-08-02')
#summarize_and_copy('2025-08-03')
#summarize_and_copy('2025-08-04')

# summarize_and_copy('2025-09-01')
# summarize_and_copy('2025-09-03')
# summarize_and_copy('2025-09-02')
# summarize_and_copy('2025-09-02')
# summarize_and_copy('2025-09-02')

