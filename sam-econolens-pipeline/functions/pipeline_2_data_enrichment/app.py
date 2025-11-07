"""
Using S3 client-side interaction as there only around 50 original article objects at each run

Keywords prioritise relevent information over volume. It is better to have no data than irrelevant data.
Metadata includes title of article for tracing RAG sources
Metadata persons/orgs tags are lower-cased
"""
import boto3
from botocore.exceptions import ClientError

import spacy

import json
from datetime import datetime, timedelta
import os
from os.path import join, dirname
import re
import unicodedata

# -------------------------------
# Helpers functions
# -------------------------------

# Declare spacy and nltk variables once
print("executing line > spacy.load('en_core_web_sm')")
nlp = spacy.load('en_core_web_sm')
print("spacy.load('en_core_web_sm') complete ")
stop_words = nlp.Defaults.stop_words  # spaCy's built-in stopwords set

# def remove_non_ascii_encode_decode(text):
#     """
#     Remove non ascii characters from a string. Cleans text in articles before extraction, and ingestion into bedrock knowledge base
#     """
#     return text.encode('ascii', 'ignore').decode('ascii')

def clean_text_for_ingestion(text: str) -> str:
    """
    Steps:
      1. Normalize Unicode characters (smart quotes, accented chars)
      2. Remove non-printable control characters
      3. Replace escape sequences with spaces
      4. Normalize whitespace and newlines
      5. Strip leading/trailing spaces
    """
    if not isinstance(text, str):
        text = str(text)

    # 1️⃣ Normalize Unicode (e.g., smart quotes → normal quotes)
    text = unicodedata.normalize("NFKC", text)

    # 2️⃣ Remove control characters (ASCII 0–31, except \n and \t)
    text = re.sub(r"[\x00-\x09\x0B\x0C\x0E-\x1F\x7F-\x9F]", " ", text)

    # 3️⃣ Replace visible escape sequences (\n, \t, etc.) with spaces
    text = re.sub(
        r"\\[abfnrtv'\"\\]|\\x[0-9A-Fa-f]{2}|\\u[0-9A-Fa-f]{4}|\\U[0-9A-Fa-f]{8}",
        " ",
        text,
    )

    # 4️⃣ Normalize whitespace (collapse multiple spaces or newlines)
    text = re.sub(r"[ \t]+", " ", text)     # collapse spaces/tabs
    text = re.sub(r"\s*\n\s*", "\n", text)  # tidy up newlines
    text = re.sub(r"\n{3,}", "\n\n", text)  # limit multiple blank lines

    # 5️⃣ Strip leading/trailing whitespace
    text = text.strip()

    return text

def clean(phrase):
        # Use spaCy tokenization and filter stopwords
        tokens = [token.text.lower() for token in nlp(phrase) if token.text.lower() not in stop_words]
        return ' '.join(tokens)

def extract_persons_and_orgs(text):
    """
    Extract person and organization entities from text,
    return them as lowercase unique lists with stopwords removed.
    """
    doc = nlp(text)
    persons, orgs = set(), set()

    for ent in doc.ents:
        if ent.label_ == 'PERSON':
            persons.add(clean(ent.text))
        elif ent.label_ == 'ORG':
            orgs.add(clean(ent.text))

    return list(persons), list(orgs)

# -------------------------------
# Main Function
# -------------------------------
source_bucket = os.environ.get("S3_SOURCE")
dest_bucket = os.environ.get("S3_DESTINATION")

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
    
    print("client, source_bucket, dest_bucket: ")
    print(s3, source_bucket, dest_bucket)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=source_bucket, Prefix=date_prefix)

    for page in pages:
        #print("loop start")
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

                # Extract fields and clean data
                content = clean_text_for_ingestion(data.get("content"))
                persons_metadata, orgs_metadata = extract_persons_and_orgs(content)
                published_at = data.get("publishedAt")
                topic = data.get("topic")
                title = data.get("title")
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                unix_time = int(dt.timestamp())

                # Build destination path components
                # Example:
                # 2025-10-11/economy_general/filename.json
                # → 2025-10-11/economy_general/filename.txt
                parts = key.split("/", 1)
                if len(parts) < 2:
                    print(f"⚠️ Skipping {key}: unexpected key format.")
                    continue

                date_prefix_dir = parts[0]              # e.g., "2025-10-11"
                sub_path = parts[1]                     # e.g., "economy_general/filename.json"
                sub_path_txt = sub_path.replace(".json", ".txt")
                sub_path_metadata = sub_path.replace(".json", ".metadata.json")

                # Build destination keys
                dest_txt_key = f"{date_prefix_dir}/{sub_path_txt}"
                dest_metadata_key = f"{date_prefix_dir}/{sub_path_metadata}"

                # Upload text file
                s3.put_object(
                    Bucket=dest_bucket,
                    Key=dest_txt_key,
                    Body=content.encode("utf-8"),
                    ContentType="text/plain"
                )

                # Build and upload metadata JSON
                metadata_obj = {
                    "metadataAttributes": {
                        "title": title,
                        "topic": topic,
                        "publishedAt": published_at,
                        "unix_time": unix_time,
                        "summary": 'no',
                        "persons": persons_metadata,
                        "organizations": orgs_metadata
                    }
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

def summarize_and_copy(datetime_input:str):
    try:
        datetime.strptime(datetime_input, '%Y-%m-%dT%H:%M:%SZ')
        # Parse string, then back into string with new format
        date_prefix = datetime.strptime(datetime_input, '%Y-%m-%dT%H:%M:%SZ') - timedelta(days=1) # move start_date_str back one day
        date_prefix = date_prefix.strftime("%Y-%m-%d")
        print(f"Function input: {date_prefix}")
    except ValueError:
        raise AssertionError(f"Date string '{datetime_input}' does not follow %Y-%m-%dT%H:%M:%SZ format.")
    
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

