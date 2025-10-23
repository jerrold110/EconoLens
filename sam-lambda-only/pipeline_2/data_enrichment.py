"""
In SAM environment variables are declared in template.yaml

Using S3 client-side interaction as there only around 50 original article objects at each run

Keywords prioritise relevent information over volume. It is better to have no data than irrelevant data.

Metadata includes title of article for tracing RAG sources
Metadata persons/orgs tags are lower-cased
"""

import boto3
from botocore.exceptions import ClientError

import spacy
import nltk
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

import json
from datetime import datetime
import os
from os.path import join, dirname

# -------------------------------
# Helpers functions
# -------------------------------

# Declare spacy and nltk variables once
nlp = spacy.load('en_core_web_sm')
stop_words = set(stopwords.words('english'))

def remove_non_ascii_encode_decode(text):
    """
    Remove non ascii characters from a string
    """
    return text.encode('ascii', 'ignore').decode('ascii')

def extract_persons_and_orgs(text):
    """
    Extract the person and organisation entities from a body of text 
    and return them as lowercase unique lists
    """
    doc = nlp(text)
    persons, orgs = set(), set()

    def clean(phrase):
        tokens = word_tokenize(phrase.lower())
        filtered = [w for w in tokens if w not in stop_words]
        return ' '.join(filtered)

    for ent in doc.ents:
        if ent.label_ == 'PERSON':
            persons.add(clean(ent.text))
        elif ent.label_ == 'ORG':
            orgs.add(clean(ent.text))

    return list(persons), list(orgs)

# -------------------------------
# Main Function
# -------------------------------

def copy_json_files_from_s3(date_prefix):
    """
    Copies JSON files from `source_bucket` whose keys start with `date_prefix`
    to `dest_bucket`, extracting content and metadata separately.

    Transformations:
      - source: 2025-10-11/economy_general/filename.json
        → dest: 2025-10-11/original/economy_general/filename.txt
      - metadata file: 
        → dest: 2025-10-11/original/economy_general/filename.metadata.json

    """
    s3 = boto3.client("s3", region_name="us-east-1")
    source_bucket = os.environ.get("S3_SOURCE")
    dest_bucket = os.environ.get("S3_DESTINATION")

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
                print(f"🔹 Processing: {key}")
                response = s3.get_object(Bucket=source_bucket, Key=key)
                data = json.loads(response["Body"].read().decode("utf-8"))

                # Extract fields
                content = remove_non_ascii_encode_decode(data.get("content"))
                persons_metadata, orgs_metadata = extract_persons_and_orgs(content)
                published_at = data.get("publishedAt")
                topic = data.get("topic")
                title = data.get("title")
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                unix_time = int(dt.timestamp())

                # Build destination path components
                # Example:
                # 2025-10-11/economy_general/filename.json
                # → 2025-10-11/original/economy_general/filename.txt
                parts = key.split("/", 1)
                if len(parts) < 2:
                    print(f"⚠️ Skipping {key}: unexpected key format.")
                    continue

                date_prefix_dir = parts[0]              # e.g., "2025-10-11"
                sub_path = parts[1]                     # e.g., "economy_general/filename.json"
                sub_path_txt = sub_path.replace(".json", ".txt")
                sub_path_metadata = sub_path.replace(".json", ".metadata.json")

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
                    "title": title,
                    "topic": topic,
                    "publishedAt": published_at,
                    "unix_time": unix_time,
                    "summary": 'no',
                    "persons": persons_metadata,
                    "organizations": orgs_metadata
                }

                s3.put_object(
                    Bucket=dest_bucket,
                    Key=dest_metadata_key,
                    Body=json.dumps(metadata_obj, ensure_ascii=False, indent=2).encode("utf-8"),
                    ContentType="application/json"
                )

                print(f"✅ Processed {key}")
                print(f"   → {dest_txt_key}")
                print(f"   → {dest_metadata_key}")

            except json.JSONDecodeError:
                print(f"⚠️ Skipping {key}: invalid JSON.")
            except Exception as e:
                print(f"❌ Error processing {key}: {e}")


# -------------------------------
# Main Function
# -------------------------------

def summarize_and_copy(date_prefix):
    try:
        datetime.strptime(date_prefix, '%Y-%m-%d')
    except ValueError:
        raise AssertionError(f"Date string '{date_prefix}' does not follow YYYY-MM-DD format.")
    
    print("-----Begin data copy-----")
    copy_json_files_from_s3(date_prefix)
    print("-----End data copy-----\n")

def lambda_handler(event, context):

    summarize_and_copy(event['batch_date'])

    return {
        "statusCode": 200,
        "batch_date": event['batch_date'],
        "body": json.dumps({
            "message": f"Function summarize_and_copy with argument {event['batch_date']} finished",
        }),
    }

