"""
Using S3 client-side interaction as there only around 50-100 original article objects at each run

Keywords prioritise relevent information over volume. It is better to have no data than irrelevant data.

There can be duplicate articles from different newsssources that will cause error during ingestion

Metadata includes title of article for tracing RAG sources
Metadata persons/orgs tags are lower-cased
"""

import boto3
from botocore.exceptions import ClientError
#from transformers import AutoTokenizer
import spacy
import re
# import nltk
# nltk.download('punkt', quiet=True)
# nltk.download('stopwords', quiet=True)
# from nltk.corpus import stopwords
# from nltk.tokenize import word_tokenize

from dotenv import load_dotenv

import json
from datetime import datetime
import os
from os.path import join, dirname
from dotenv import load_dotenv

import re
import unicodedata

# -------------------------------
# Helpers functions
# -------------------------------

# Declare spacy and nltk variables once
nlp = spacy.load('en_core_web_md')
stop_words = nlp.Defaults.stop_words  # spaCy's built-in stopwords set


def extract_persons_and_orgs(text:str):
    """
    Extract person and organization entities from text,
    return them as lowercase unique lists with stopwords removed.
    """
    doc = nlp(text)
    persons, orgs = set(), set()

    def clean(phrase):
        # Use spaCy tokenization and filter stopwords
        tokens = [token.text.lower().strip() for token in nlp(phrase) if token.text.lower() not in stop_words]
        return ' '.join(tokens)
    
    def is_valid(item: str):
        """
        Only allow values with alphabets or spaces
        """
        if not item:
            return False
        if len(item) < 2:  # remove single-character items
            return False
        if any(not (c.isalpha() or c.isspace()) for c in item):
            return False
        return True

    for ent in doc.ents:
        if ent.label_ == 'PERSON':
            cleaned = clean(ent.text)
            if is_valid(cleaned):
                persons.add(cleaned)
        elif ent.label_ == 'ORG':
            cleaned = clean(ent.text)
            if is_valid(cleaned):
                orgs.add(cleaned)

    return list(persons), list(orgs)




def clean_text_for_ingestion(text: str) -> str:
    """
    Clean a string of text to make it safe for ingestion into
    semantic search or knowledge base systems (e.g., Bedrock + OpenSearch).
    
    Steps:
      1. Normalize Unicode characters (smart quotes, accented chars)
      2. Remove non-printable control characters
      3. Replace escape sequences with spaces
      4. Normalize whitespace and newlines
      5. Strip leading/trailing spaces
    """
    if not isinstance(text, str):
        text = str(text)

    # Normalize Unicode (e.g., smart quotes → normal quotes)
    text = unicodedata.normalize("NFKC", text)

    # Remove control characters (ASCII 0–31, except \n and \t)
    text = re.sub(r"[\x00-\x09\x0B\x0C\x0E-\x1F\x7F-\x9F]", " ", text)

    # Replace visible escape sequences (\n, \t, etc.) with spaces
    text = re.sub(
        r"\\[abfnrtv'\"\\]|\\x[0-9A-Fa-f]{2}|\\u[0-9A-Fa-f]{4}|\\U[0-9A-Fa-f]{8}",
        " ",
        text,
    )

    # Normalize whitespace (collapse multiple spaces or newlines)
    text = re.sub(r"[ \t]+", " ", text)     # collapse spaces/tabs
    text = re.sub(r"\s*\n\s*", "\n", text)  # tidy up newlines
    text = re.sub(r"\n{3,}", "\n\n", text)  # limit multiple blank lines

    # Strip leading/trailing whitespace
    text = text.strip()

    return text

# -------------------------------
# Main Function
# -------------------------------

def copy_json_files_from_s3(date_prefix):
    """
    Copies JSON files from `source_bucket` whose keys start with `date_prefix`
    to `dest_bucket`, extracting content and metadata separately. All objects are encoded into utf-8 and encoding is declared in upload

    Transformations:
      - source: 2025-10-11/economy_general/filename.json
        → dest: 2025-10-11/economy_general/filename.txt
        → dest: 2025-10-11/economy_general/filename.txt.metadata.json

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
                print(f"🔹 Processing: {key}")
                response = s3.get_object(Bucket=source_bucket, Key=key)
                data = json.loads(response["Body"].read()) # Unknown byte encoding right now
                
                # Extract fields and clean data
                content = clean_text_for_ingestion(data.get("content"))
                persons_metadata, orgs_metadata = extract_persons_and_orgs(content)
                published_at = data.get("publishedAt")
                topic = data.get("topic") # topic only consists of _ and a-z
                title = data.get("title")
                title = re.sub(r'[^a-zA-Z0-9\s]', '', title) # symbols screw the metadata
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                unix_time = int(dt.timestamp())

                if len(persons_metadata) > 20 or len(orgs_metadata) > 20:
                    print(f"⚠️ Skipping {key}: Too many entities in metadata")
                    continue

                # Build destination path components
                # Example:
                # 2025-10-11/economy_general/filename.json
                # → 2025-10-11/original/economy_general/filename.txt
                parts = key.split("/", 2)
                if len(parts) < 2:
                    print(f"⚠️ Skipping {key}: unexpected key format.")
                    continue

                date_prefix_dir = parts[0] # e.g., "2025-10-11"
                
                sub_path = parts[2] # e.g., "news_article_title.json"
                sub_path = parts[1] + "/" + sub_path.replace("/", "") # some paths(titles) have / causing nested folders
                # Remove symbols from sub_path
                sub_path_txt = sub_path.replace(".json", ".txt")
                sub_path_metadata = sub_path.replace(".json", ".txt.metadata.json")

                # Build destination keys
                dest_txt_key = f"{date_prefix_dir}/{sub_path_txt}"
                dest_metadata_key = f"{date_prefix_dir}/{sub_path_metadata}"

                # Upload String as a text file
                # Encode as UTF8 so that S3 receives actual UTF-8 bytes https://www.rfc-editor.org/rfc/rfc9110.html#name-content-type
                # Declare the endoding in metadata
                s3.put_object(
                    Bucket=dest_bucket,
                    Key=dest_txt_key,
                    Body=content.encode("utf-8"),
                    ContentType="text/plain; charset=utf-8"
                )

                # Upload metadata JSON
                metadata_obj = {
                    "metadataAttributes": {
                        "title": title,
                        "topic": topic,
                        "publishedAt": published_at,
                        "unix_time": unix_time,
                        "persons": persons_metadata,
                        "organizations": orgs_metadata
                    }
                }
                json_bytes = json.dumps(metadata_obj, ensure_ascii=False).encode("utf-8")
                s3.put_object(
                    Bucket=dest_bucket,
                    Key=dest_metadata_key,
                    Body=json_bytes,
                    ContentType="application/json; charset=utf-8"
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

# def summarize_json_files_from_s3(
#     date_prefix,
#     context_window=1000, # reduce context_window slightly from 1024 to reduce errors
#     overlap=100
# ):
#     """
#     Reads JSON files from `source_bucket` (e.g. 2025-10-11/economy_general/filename.json),
#     extracts 'content', tokenizes and chunks if needed, summarizes each chunk via SageMaker,
#     and uploads summarized text and metadata to `dest_bucket` under:
#     2025-10-11/summarized/economy_general/filename.txt
#     and corresponding metadata file.

#     Args:
#         date_prefix (str): Prefix like '2025-10-11/'.
#         context_window (int): Token limit per chunk.
#         overlap (int): Overlap between chunks.
#     """
#     s3 = boto3.client("s3", region_name="us-east-1")
#     paginator = s3.get_paginator("list_objects_v2")
#     pages = paginator.paginate(Bucket=source_bucket, Prefix=date_prefix)

#     # tokenizer for the model in the summarisation endpoint
#     tokenizer = AutoTokenizer.from_pretrained("sshleifer/distilbart-cnn-12-6")

#     for page in pages:
#         for obj in page.get("Contents", []):
#             key = obj["Key"]

#             # Only process .json files, skip summarized or metadata
#             if not key.endswith(".json") or "/summarized/" in key:
#                 continue

#             print(f"🔹 Processing {key}")

#             try:
#                 # -------------------------------
#                 # Read JSON file
#                 # -------------------------------
#                 response = s3.get_object(Bucket=source_bucket, Key=key)
#                 data = json.loads(response["Body"].read().decode("utf-8"))

#                 content = remove_non_ascii_encode_decode(data.get("content"))
#                 published_at = data.get("publishedAt")
#                 topic = data.get("topic")
#                 title = data.get("title")
#                 dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
#                 unix_time = int(dt.timestamp())

#                 if not content:
#                     print(f"⚠️ Skipping {key}: missing 'content' field.")
#                     continue

#                 # -------------------------------
#                 # Tokenize and chunk if needed
#                 # -------------------------------
#                 tokens = tokenizer(content, return_offsets_mapping=True, truncation=False)
#                 input_ids = tokens["input_ids"]
#                 print('Length of sequence:',len(input_ids))
#                 if len(input_ids) > context_window:
#                     print(f"✂️ Text exceeds {context_window} tokens; chunking required.")
#                     chunks = []
#                     start = 0
#                     while start < len(input_ids):
#                         end = start + context_window
#                         chunk_tokens = input_ids[start:end]
#                         chunk_text = tokenizer.decode(chunk_tokens).lstrip('<s>').rstrip('</s>')
#                         chunks.append(chunk_text)
#                         start += context_window - overlap
#                 else:
#                     chunks = [content]

#                 # print('==============Print token length of each chunk================')
                
#                 # for chunk in chunks:
#                 #     tokens = tokenizer(chunk, return_offsets_mapping=True, truncation=False)
#                 #     input_ids = tokens["input_ids"]
#                 #     print(f"length of chunk text: {len(input_ids)}")
#                 # print('==============================')

#                 # -------------------------------
#                 # Summarize and upload each chunk
#                 # -------------------------------
#                 for i, chunk_text in enumerate(chunks, start=1):
#                     tokens = tokenizer(chunk_text, return_offsets_mapping=True, truncation=False)
#                     input_ids = tokens["input_ids"]

#                     summarized_text = remove_non_ascii_encode_decode(get_summary(chunk_text))
#                     persons_metadata, orgs_metadata = extract_persons_and_orgs(summarized_text)

#                     # Derive destination keys
#                     # e.g. 2025-10-11/economy_general/filename.json ->
#                     #      2025-10-11/summarized/economy_general/filename.txt
#                     #      2025-10-11/summarized/economy_general/filename_metadata.txt
#                     parts = key.split("/", 1)
#                     date_dir, sub_path = parts
#                     summarized_sub_path = f"summarized/{sub_path}"
#                     base = summarized_sub_path.rsplit(".", 1)[0]

#                     if len(chunks) > 1:
#                         txt_key = f"{date_dir}/{base}_{i}.txt"
#                         meta_key = f"{date_dir}/{base}_{i}.metadata.json"
#                     else:
#                         txt_key = f"{date_dir}/{base}.txt"
#                         meta_key = f"{date_dir}/{base}.metadata.json"

#                     # Metadata file
#                     metadata = {
#                         "title": title,
#                         "topic": topic,
#                         "publishedAt": published_at,
#                         "unix_time": unix_time,
#                         "summary": 'yes',
#                         "persons": persons_metadata,
#                         "organizations": orgs_metadata
#                     }

#                     # Upload summarized text
#                     s3.put_object(
#                         Bucket=dest_bucket,
#                         Key=txt_key,
#                         Body=summarized_text.encode("utf-8"),
#                         ContentType="text/plain"
#                     )

#                     # Upload metadata
#                     s3.put_object(
#                         Bucket=dest_bucket,
#                         Key=meta_key,
#                         Body=json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8"),
#                         ContentType="application/json"
#                     )

#                     #print(f"✅ Uploaded {txt_key}")
#                     print(f"✅ Uploaded {meta_key}")

#             except json.JSONDecodeError:
#                 print(f"⚠️ Skipping {key}: invalid JSON format.")
#             except Exception as e:
#                 print(f"❌ Error processing {key}: {e}")


def summarize_and_copy(date_prefix):
    try:
        datetime.strptime(date_prefix, '%Y-%m-%d')
    except ValueError:
        raise AssertionError(f"Date string '{date_prefix}' does not follow YYYY-MM-DD format.")
    
    print("-----Begin data copy-----")
    copy_json_files_from_s3(date_prefix)
    print("-----End data copy-----\n")

    # print("-----Begin data summarise-----")
    # summarize_json_files_from_s3(date_prefix)
    # print("-----End data summarise-----\n")


dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
source_bucket = os.environ.get("S3_SOURCE")
dest_bucket = os.environ.get("S3_DESTINATION")
endpoint_name = os.environ.get("SAGE_TS_ENDPOINT")

summarize_and_copy('2025-08-01')
#summarize_and_copy('2025-08-02')
#summarize_and_copy('2025-08-03')
#summarize_and_copy('2025-08-04')

# summarize_and_copy('2025-09-01')
# summarize_and_copy('2025-09-03')
# summarize_and_copy('2025-09-02')
# summarize_and_copy('2025-09-02')
# summarize_and_copy('2025-09-02')

#summarize_json_files_from_s3(source_bucket, dest_bucket, date_prefix)
#copy_json_content_and_metadata(source_bucket, dest_bucket, date_prefix)

# print("\n" + get_summary("""
                         
# Paris is the capital and most populous city of France, with an estimated population of 2,175,601 residents as of 2018, 
# in an area of more than 105 square kilometres (41 square miles). The City of Paris is the centre and seat of government of the region and province of Île-de-France, 
# or Paris Region, which has an estimated population of 12,174,880, or about 18 percent of the population of France as of 2017.
                         
# """))