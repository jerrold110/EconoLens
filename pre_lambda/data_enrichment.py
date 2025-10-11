"""
In SAM environment variables are declared in template.yaml

Using client-side interation as there only around 50 objects at each run
"""

import boto3
from botocore.exceptions import ClientError
from transformers import AutoTokenizer

from dotenv import load_dotenv
import json

def copy_json_content_and_metadata(source_bucket, dest_bucket, date_prefix):
    """
    Copies JSON files from `source_bucket` whose keys start with `date_prefix`
    to `dest_bucket`, extracting content and metadata separately.

    Transformations:
      - source: 2025-10-11/economy_general/filename.json
        → dest: 2025-10-11/original/economy_general/filename.txt
      - metadata file: 
        → dest: 2025-10-11/original/economy_general/filename_metadata.json

    Extracts from each JSON file:
      - 'content' → saved as .txt file
      - 'publishedAt' and 'topic' → saved as _metadata.json file
    """
    s3 = boto3.client("s3", region_name="us-east-1")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=source_bucket, Prefix=date_prefix)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # Only process JSON files
            if not key.endswith(".json"):
                continue

            try:
                # Download the JSON object
                print(f"Processing: {key}")
                response = s3.get_object(Bucket=source_bucket, Key=key)
                data = json.loads(response["Body"].read().decode("utf-8"))

                # Extract fields
                content = data.get("content")
                published_at = data.get("publishedAt")
                topic = data.get("topic")

                # Build destination path components
                # Example:
                # 2025-10-11/economy_general/filename.json
                # → 2025-10-11/original/economy_general/filename.txt
                parts = key.split("/", 1)
                if len(parts) < 2:
                    print(f"!!! Skipping {key}: unexpected key format.")
                    continue

                date_prefix_dir = parts[0]              # e.g., "2025-10-11"
                sub_path = parts[1]                     # e.g., "economy_general/filename.json"
                sub_path_txt = sub_path.replace(".json", ".txt")
                sub_path_metadata = sub_path.replace(".json", "_metadata.json")

                # Build destination keys
                dest_txt_key = f"{date_prefix_dir}/original/{sub_path_txt}"
                dest_metadata_key = f"{date_prefix_dir}/original/{sub_path_metadata}"

                # Upload text file
                s3.put_object(
                    Bucket=dest_bucket,
                    Key=dest_txt_key,
                    Body=content.encode("utf-8"),
                    ContentType="text/plain"
                )

                # Build and upload metadata JSON
                metadata_obj = {
                    "publishedAt": published_at,
                    "topic": topic
                }

                s3.put_object(
                    Bucket=dest_bucket,
                    Key=dest_metadata_key,
                    Body=json.dumps(metadata_obj, ensure_ascii=False, indent=2).encode("utf-8"),
                    ContentType="application/json"
                )

                print(f"Processed {key}")
                print(f"   → {dest_txt_key}")
                print(f"   → {dest_metadata_key}")

            except json.JSONDecodeError:
                print(f"Skipping {key}: invalid JSON.")
            except Exception as e:
                print(f"Error processing {key}: {e}")




import boto3
import json
from transformers import AutoTokenizer

# -------------------------------
# Summarization Helpers
# -------------------------------

import os
from os.path import join, dirname
from dotenv import load_dotenv

def get_endpoint():
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)
    return os.environ.get("SAGE_TS_ENDPOINT")

endpoint_name = get_endpoint()

def query_endpoint(encoded_text):
    client = boto3.client('runtime.sagemaker')
    response = client.invoke_endpoint(
        EndpointName=endpoint_name,
        ContentType='application/x-text',
        Body=encoded_text
    )
    return response

def parse_response(response):
    model_predictions = json.loads(response['Body'].read())
    return model_predictions['summary_text']

def get_summary(input_text):
    try:
        query_response = query_endpoint(input_text.encode('utf-8'))
    except Exception as e:
        if hasattr(e, "response") and e.response['Error']['Code'] == 'ModelError':
            raise Exception(f"To use this notebook, please launch the endpoint again. Error: {e}.")
        else:
            raise
            
    try:
        summary_text = parse_response(query_response)
    except (TypeError, KeyError) as e:
        raise Exception(e)

    return summary_text


# -------------------------------
# Main Function
# -------------------------------

def summarize_json_files_from_s3(
    source_bucket,
    dest_bucket,
    date_prefix,
    context_window=1024,
    overlap=100
):
    """
    Reads JSON files from `source_bucket` (e.g. 2025-10-11/economy_general/filename.json),
    extracts 'content', tokenizes and chunks if needed, summarizes each chunk via SageMaker,
    and uploads summarized text and metadata to `dest_bucket` under:
    2025-10-11/summarized/economy_general/filename.txt
    and corresponding metadata file.

    Args:
        source_bucket (str): S3 bucket containing JSON files.
        dest_bucket (str): Destination S3 bucket.
        date_prefix (str): Prefix like '2025-10-11/'.
        context_window (int): Token limit per chunk.
        overlap (int): Overlap between chunks.
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=source_bucket, Prefix=date_prefix)

    tokenizer = AutoTokenizer.from_pretrained("sshleifer/distilbart-cnn-12-6")

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # Only process .json files, skip summarized or metadata
            if not key.endswith(".json") or "/summarized/" in key:
                continue

            print(f"🔹 Processing {key}")

            try:
                # -------------------------------
                # Read JSON file
                # -------------------------------
                response = s3.get_object(Bucket=source_bucket, Key=key)
                data = json.loads(response["Body"].read().decode("utf-8"))

                content = data.get("content")
                published_at = data.get("publishedAt")
                topic = data.get("topic")

                if not content:
                    print(f"⚠️ Skipping {key}: missing 'content' field.")
                    continue

                # -------------------------------
                # Tokenize and chunk if needed
                # -------------------------------
                tokens = tokenizer(content, return_offsets_mapping=True, truncation=False)
                input_ids = tokens["input_ids"]

                if len(input_ids) > context_window:
                    print(f"✂️ Text exceeds {context_window} tokens; chunking required.")
                    chunks = []
                    start = 0
                    while start < len(input_ids):
                        end = start + context_window
                        chunk_tokens = input_ids[start:end]
                        chunk_text = tokenizer.decode(chunk_tokens)
                        chunks.append(chunk_text)
                        start += context_window - overlap
                else:
                    chunks = [content]

                # -------------------------------
                # Summarize and upload each chunk
                # -------------------------------
                for i, chunk_text in enumerate(chunks, start=1):
                    summarized_text = 'bobobo' #get_summary(chunk_text)

                    # Derive destination keys
                    # e.g. 2025-10-11/economy_general/filename.json ->
                    #      2025-10-11/summarized/economy_general/filename.txt
                    parts = key.split("/", 1)
                    date_dir, sub_path = parts
                    summarized_sub_path = f"summarized/{sub_path}"
                    base = summarized_sub_path.rsplit(".", 1)[0]

                    if len(chunks) > 1:
                        txt_key = f"{date_dir}/{base}_{i}.txt"
                        meta_key = f"{date_dir}/{base}_{i}_metadata.json"
                    else:
                        txt_key = f"{date_dir}/{base}.txt"
                        meta_key = f"{date_dir}/{base}_metadata.json"

                    # Metadata file
                    metadata = {
                        "publishedAt": published_at,
                        "topic": topic
                    }

                    # Upload summarized text
                    s3.put_object(
                        Bucket=dest_bucket,
                        Key=txt_key,
                        Body=summarized_text.encode("utf-8"),
                        ContentType="text/plain"
                    )

                    # Upload metadata
                    s3.put_object(
                        Bucket=dest_bucket,
                        Key=meta_key,
                        Body=json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8"),
                        ContentType="application/json"
                    )

                    print(f"✅ Uploaded {txt_key}")
                    print(f"✅ Uploaded {meta_key}")

            except json.JSONDecodeError:
                print(f"⚠️ Skipping {key}: invalid JSON format.")
            except Exception as e:
                print(f"❌ Error processing {key}: {e}")


source_bucket = 'econolens-staging-area'
dest_bucket = 'econolens-data-enriched'
date_prefix = '2025-09-01'

summarize_json_files_from_s3(source_bucket, dest_bucket, date_prefix)
#copy_json_content_and_metadata(source_bucket, dest_bucket, date_prefix)