# **EconoLens Data Pipeline**

A fully serverless, container-based data pipeline deployed with Infrastucture as Code using **AWS SAM**, designed to ingest, clean, enrich, chunk, summarize, and ingest economic news articles into an **Amazon Bedrock Knowledge Base**.

This pipeline extracts articles from the **GNews API**, stores and processes them through multiple ETL stages running in **Lambda container images**, and triggers a Bedrock ingestion job for semantic search and RAG workflows.

---

## **Table of Contents**

* [Architecture Overview](#architecture-overview)
* [Pipeline Stages](#pipeline-stages)
* [Infrastructure Components](#infrastructure-components)
* [State Machine Orchestration](#state-machine-orchestration)
* [Build & Deployment](#build--deployment)
* [Environment Variables](#environment-variables)
* [Folder Structure](#folder-structure)
* [IAM & Security](#iam--security)
* [Extending the Pipeline](#extending-the-pipeline)

---

# **Architecture Overview**

The pipeline orchestrates multiple Lambda container functions via **AWS Step Functions**.
Data flows through several S3 buckets representing each stage of processing:

```
GNews API → S3 (staging) → cleaning → metadata + body separation → entity extraction → chunking + summarisation → Bedrock KB ingestion
```

All files are **UTF-8 encoded before writing** and **decoded when reading** across stages.

### **High-Level Data Flow**

1. **Extract** economic news across categories
2. **Store raw JSON** articles in S3 staging bucket
3. **Clean and normalize** article body
4. **Separate metadata from text** into JSON and TXT files
5. **Extract entities** with **spaCy** (en_core_web_md)
6. **Semantic chunking + summarization** using

   * **Amazon Nova Micro (11B)** for summarizing >800-token chunks
7. **Ingest** processed chunks into an **Amazon Bedrock Knowledge Base**

### **Daily Automation**

An **EventBridge Scheduler** triggers the pipeline **every day at 10 AM Pacific Time**. 

Cloudwatch logs store the logs from each lambda during operation at each schedule.

---

# **Pipeline Stages**

## **1. API Extraction (pipeline_1_api_call)**

* Calls **GNews API** across specified categories
* UTF-8 encodes article payloads
* Stores raw JSON files in **S3_STAGE (econolens-staging-area)**
* Runs in a containerized Lambda (Python 3.11)

## **2. Data Enrichment (pipeline_2_data_enrichment)**

* Cleans article body and normalizes text
* Runs entity extraction with **spaCy (en_core_web_md)**
* Splits metadata and content
* Writes enriched output to **S3_ENRICH (econolens-data-enriched)**
* Requires **2 GB memory** to load models

## **3. Semantic Chunking (pipeline_3_data_chunking)**

* Embeds text with Titan-text-embeddings-v2 and separates into semantically meaningful chunks
* Summarizes chunks >800 tokens using **Amazon Nova Micro (11B)**
* Uploads chunked files to **S3_CHUNK (econolens-data-chunked)**
* Memory: **3008MB**, Timeout: **900s**

## **4. Bedrock Knowledge Base Ingestion (pipeline_4_bedrockKB_ingest)**

* Reads chunked outputs
* Starts an ingestion job using:

  * **BEDROCK_KB_ID**
  * **BEDROCK_KB_DATASOURCE_ID**
* Waits for job completion
* Uses IAM permissions aligned with Bedrock documentation

---

# **Infrastructure Components**

### **AWS SAM Resources**

| Resource                                         | Description                                            |
| ------------------------------------------------ | ------------------------------------------------------ |
| **AWS::Serverless::Function (container images)** | Each pipeline stage is a containerized Lambda          |
| **AWS::Serverless::StateMachine**                | Orchestrates all stages sequentially                   |
| **IAM Roles**                                    | Custom roles with S3, Bedrock, and logging permissions |
| **EventBridge Scheduler**                        | Triggers pipeline daily at 10 AM PT                    |
| **S3 Buckets**                                   | Staging → enriched → chunked                           |

The SAM template uses `DefinitionSubstitutions` to inject Lambda ARNs into the Step Functions ASL file.

---

# **State Machine Orchestration**

The Step Functions workflow (`statemachine/news_pipeline.asl.json`) coordinates:

1. **Invoke APICallFunction**
2. **Invoke DataEnrichmentFunction**
3. **Invoke DataChunkFunction**
4. **Invoke BedrockKBIngestFunction**

This ensures each stage receives the files produced by the previous stage.

![State machine](assets/stepfunctions_graph.png)

The scheduler passes the timestamp via:

```json
{ "batch_date": "<aws.scheduler.scheduled-time>" }
```

Retry logic with backoff is incorporated at each task
---

# **Build & Deployment**

### **Prerequisites**

* AWS CLI
* AWS SAM CLI
* Docker (required for building Lambda container images)

### **Build all container images**

```bash
sam build --use-container --parallel
```

### **Deploy the pipeline**

```bash
sam deploy --guided
```

The guided deploy will prompt you to set parameters and create the necessary roles and resources.

---

# **Environment Variables**

Configured under `Globals → Function` in the SAM template:

| Variable                     | Purpose                      |
| ---------------------------- | ---------------------------- |
| **S3_STAGE**                 | Raw staging area bucket      |
| **S3_ENRICH**                | Enriched data bucket         |
| **S3_CHUNK**                 | Chunked data bucket          |
| **BEDROCK_KB_ID**            | Knowledge Base ID            |
| **BEDROCK_KB_DATASOURCE_ID** | DS ID associated with the KB |

These propagate automatically into all functions.

---

# **Folder Structure**

```
.
├── template.yaml                # SAM template
├── samconfig.toml               # SAM config
├── statemachine/
│   └── news_pipeline.asl.json   # Step Functions definition
├── functions/
│   ├── pipeline_1_api_call/
│   ├── pipeline_2_data_enrichment/
│   ├── pipeline_3_data_chunking/
│   └── pipeline_4_bedrockKB_ingest/
└── README.md
```

Each function folder contains:

* `Dockerfile`
* handler code
* requirements

All Lambda functions run from **container images**, built using the local Docker context.

---

# **IAM & Security**

### IAM roles include:

* **AWSLambdaBasicExecutionRole**
* **AmazonS3FullAccess** (can be scoped down in production)
* **AmazonBedrockFullAccess** (chunking stage)
* **Custom Bedrock KB ingestion permissions** (final stage)

These were configured to meet the minimum necessary capabilities for each stage.

---

# **Extending the Pipeline**

Here are ideas for future improvements:

* **Add CloudWatch dashboards** for throughput metrics
* **Integrate OpenSearch Serverless** as a secondary search index
* **Add automated unit testing with sam local invoke**
* **Add S3 object lifecycle policies** for cost optimization

---

# **License**

MIT

---
