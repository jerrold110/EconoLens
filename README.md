# **Econolens documentation**
#### Agentic RAG System for U.S. Economic Analysts

## Introduction
The goal of this project is to develop an Agentic RAG chatbot that has a repository of years of publicly available American economic information (events, reports, numbers, official statements, corporate, etc...) that can generate economic reports or answer questions like an economic analyst. A data pipeline downloads, cleans, chunks, and ingests news data from a paid API daily to ensure that the knowledge base is up-to-date. The entirety of this application uses serverless services from the AWS stack using Infrastructure-as-code and can be deployed in multiple environments through the SAM console. 

*Use-cases might include: assisting an economic analyst in understanding the current/past economic conditions, understanding the drivers behind economic events and their effects, or getting an in-depth analysis about a particular sector of the economy or a specific corporation.*

Some of the features in this system include:
* A full RAG agent ecosystem: Bedrock Agent, Alias, Guardrails, Lambda tools, Opensearch Vector database
* RAG pipeline with hybrid retrieval (term-based search, embedding-based search) and Reranking 
* Batch unstructured data pipeline where data records are kept: S3, GNews API, Data cleaning/enriching/chunking/ingestion into Opensearch, SpaCy for entity extraction
* A multi-environment **CI/CD pipeline** using GitHub Actions with smoke and unit tests involving semantic similarity
* Offline multi-turn RAG Agent evaluation using an open-source framework (Ragas, LLM-as-a-judge, Langfuse)
* Research notebooks for semantic chunking and text summarization experiments
* Observation with Cloudwatch Metrics and Cloudwatch Logs
* Infrastructure-as-Code (IaC) using **AWS SAM and CloudFormation**

## The main goal of this project
Vector database RAG systems scale notoriously poorly when the number of chunks reaches a certain magnitude because search-time increases, this problem worsens as more chunks are added, making response generation slow down to a crawl. This RAG project is unique because that it stores a very large amount of news data over multiple economic topics and ingests new data every day. 

This project required me to designed a unique workflow for the RAG agent to narrow down the number of chunks needed to be retrieved based on the user's requests, and way to efficiently search for the chunks based on certain metadata filters that are extracted from the user's request. Utilised together, this unique RAG architecture allows the storage of extremely large amounts of data in a vector database (100,000s of chunks) while minimising retrieval latency. This is RAG chatbot not impeded by the low scalability of traditional RAG architecture and has access to a detailed archive of years of economic news data.

Refer to `sam-econolens-agent-doc.md` and `sam-econolens-pipeline-docs.md` for technical documentation.

---
# **Repository Structure**

```
/
├── sam-econolens-pipeline/        # Serverless data pipeline (cleaning + entity extraction + chunk enrichment + semantic chunking)
|   ├── template.yaml              # Full IaC for data pipeline
│   └── README.md
│
├── sam-econolens-agent/           # Main Agent Infra (Bedrock Agent + Lambdas)
│   ├── template.yaml              # Full IaC for agent ecosystem
│   └── README.md
│
├── evaluation/                    # Offline evaluation with open-source framework using Langfuse/Ragas/LLM-judge
│   └──  sample_responses.md       # Sample responses to prompt use-cases
│
├── tests/                         # Test suite executed locally & in CI
│   ├── unit/                      # Unit tests for Lambda handlers & utils
│   └── smoke/                     # Integration-level sanity checks
│
├── notebook/                      # Research & prototyping notebooks
│   ├── semantic_chunking.ipynb
│   └── summary_eval.ipynb         # Text summarisation model evaluation study  
│
├── .github/workflows/             # GitHub Actions CI/CD
│   ├── run-tests.yml
│   └── deploy.yml
│
└── README.md                      # You are here
```


## Data product management
At the heart of this RAG system lies is an indepth understanding of the data around which all design desicions are made. We have to know what data we need for it to function as an economic analyst chatbot to generate expert-level responses. Understanding the data also allows us to create a structure out of all the data we download to enrich data chunks and reduce search-space thus reducing data retrieval latency. In this article, I explore the various aspects of economic news that an analyst needs, review possible data sources, and design the main data workflow in this system from News API search to Retrieval reranking.

https://jerroldsworkshopandsymposium.substack.com/p/econolens-part-2-data-product-management


This image shows the metadata of a single news article that will be attached to its text chunks:

![Data](/assets/11a.png "Title")

This diagram shows the resulting user interaction workflow that narrows the chunks to retrieve based on:
- Date range
- Economic topic
- Person entities
- Institutional entities

![Data](/assets/agent_workflow.png "Agent")


# **Overall System Architecture**

![Data](/assets/architecture.png "Architecture")


### **1. RAG Agent**

* Amazon Nova Pro as the Agent's reasoning engine
* Guardrails preventing non-U.S. economic content
* Agent Alias for multi-environment routing
* Multiple tools for retrieving data based on queries and filters

### **2. Knowledge Base**

* Bedrock vector store populated with processed economic news
* Hybrid retrieval of BM25/Cosine similarity
* Enriched chunks with multiple fields for metadata searching
* Ingests data from S3

### **3. Data pipeline**

* Data pipeline that cleans, stores, and processes data in several stages
* Ingests data into Amazon Opensearch vector database
* Scheduling and orchestration with Step Functions and Eventbridge
* Logging at each step in Cloudwatch logs and error handling
* Semantic chunking

### **4. Observability**

* CloudWatch metrics: Agent-level metrics
* S3 stores invocation logs (large bedrock agent logs)
* Custom CloudFormation resources to activate or deactivate Bedrock logging

### **5. CI/CD Pipeline**

* GitHub Actions triggers unit tests and smoke tests upon creation of Pull Request
* samconfig.toml defines defaults for dev/stag environments
* Lightweight embedding model (<100mb) for semantic similarity comparison against reference answers involving a range of multi-turn test cases in unit testing with minimum thresholds

### **6. Sample responses**

Samples prompt and response conversations can be found in this [location](https://github.com/jerrold110/EconoLens/blob/main/evaluation/sample_responses.md) 

---

# **Environments**

The platform supports multiple environments:

| Environment | Purpose                            |
| ----------- | ---------------------------------- |
| **dev**     | Developer testing, experimentation |
| **stag**    | Pre-production validation          |
| **prod**    | Production Econolens environment   |

All resources are **environment-prefixed** via the `Environment` parameter (`dev`, `stag`, `prod`).

Example:

```
EconolensAgent_dev
RetrieveTopicCorporate-stag
econolens-invocation-logs-prod
```

---

# **Deployment Instructions**

Both stacks (`sam-econolens-pipeline` and `sam-econolens-agent`) should be deployed sequentially with SAM. Refer to documentation within folder for further details. 

Deploy Vector database and Data pipeline Object buckets before that.

![Data](/assets/deployment.png "Deployment")

---

# **CI/CD Pipeline**

GitHub Actions performs:

### **1. On Pull Request**

* Runs unit tests
* Runs smoke tests
* Static checks (optional)
* Lints Python code
* Blocks merges on failure

### **2. On Merge to Main**

* Re-runs tests
* Deploys SAM pipeline (if modified)
* Deploys agent stack to **dev**
* Optional: automatic promote to **stag**

### **3. Manual Approvals**

Production deployments require manual review within AWS CodePipeline or GitHub Actions (depending on your pipeline design).

---

# **Evaluation Framework**

The `/evaluation` directory provides a reproducible framework for scoring and analyzing model behaviors via:

### **Ragas**

* Context precision
* Context recall
* Faithfulness
* Answer relevancy

### **LLM-as-a-Judge (Claude)**

* Tracks quality, reasoning, factuality
* Used for agent regression testing

### **Langfuse**

* Structured dataset logging
* Evaluation workflows
* Experiment versioning

### **Use cases**

* Benchmarking chunking strategies
* Regression testing after prompt changes
* Evaluating tool retrieval quality
* Comparing models (e.g., Nova Pro vs alternatives)

---

# **Testing**

The `/tests` directory contains the files for the unit and smoke tests, run with pytest.
