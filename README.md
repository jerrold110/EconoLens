# **Econolens documentation**
### -> Agentic RAG System for U.S. Economic Analysts

## Project introduction
The goal of this project is to develop an Agentic RAG report generator and chatbot that has a repository of years of publicly available U.S. economic information (events, reports, numbers, official statements, corporate, etc...) that can generate economic reports or answer questions like an economic analyst. A data pipeline downloads, cleans, chunks, and ingests news data from a paid API daily to ensure that the knowledge base is up-to-date. RAG applications with vector databases are highly unscalable for vast quantities of data, which would normally be a problem, but this application uses a combination of NLP, ingestion/retrieval engineering, and an Agent to create a scalable RAG architecture. *Use-cases might include: assisting an economic analyst in understanding the current/past economic conditions, understanding the drivers behind economic events and their effects, or getting an in-depth analysis about a particular sector of the economy or a specific corporation.*

The entirety of this application uses AWS serverless services and has a CI/CD pipeline spanning multiple environments on AWS using SAM and Cloudformation (Infra-as-code). This allows code and infrastructure to be deployed in multiple environments through a few commands the SAM console greatly speeding up development. 


## RAG architecture
Vector database RAG systems scale poorly when the number of chunks reaches a certain magnitude because retrieval time increases, this problem worsens as more chunks are added, making response generation slow down to a crawl. This RAG project is unique because that it stores a very large amount of news data over multiple economic topics and the amount of data is stored increases every day. 

I have designed a unique RAG archiecture using NLP (entity extraction), ingestion/retrieval engineering, an Agent, hybrid retrieval (AWS abstracted, but probably a combination of BM25 and cosine similarity), and reranking to narrow down the number of chunks retrieved based on the user's request. It efficiently searches for the chunks based on `metadata filters` that are extracted from the user's request, for documents that have been chunked using semantic chunking and LLM summarisation, then reranks them and selects the top N chunks. This unique RAG architecture allows the storage of extremely large amounts of data in a vector database (100,000s of chunks) while minimising retrieval latency. 

The agent understands historical time ranges, persons, institutions, economic topics and uses these fields to improve retrieval.

Refer to `sam-econolens-agent-doc.md` and `sam-econolens-pipeline-docs.md` for documentation on their specific implementation.


## Components in this application
This system includes:
* A RAG agent ecosystem: `Bedrock Agent, Alias, Guardrails, Lambda functions as agent tools`
* `Opensearch` Vector database with daily retrieval from API, processing, ingestion, and data archiving at each stage: S3, GNews API, Data cleaning/enriching/chunking/ingestion into Opensearch, `SpaCy` for entity extraction
* Hybrid retrieval (term-based search, embedding-based search) and Reranking with `Cohere rerank 3.5`
* A multi-environment **CI/CD pipeline** using GitHub Actions with smoke and unit tests involving semantic similarity based on embeddings by `all-MiniLM-L6`
* Offline evaluation for multi-turn RAG Agent using an open-source framework (Ragas, LLM-as-a-judge, Langfuse)
* Observability with Cloudwatch Metrics and Cloudwatch Logs
* Research notebooks for semantic chunking and text summarization experiments
* Infrastructure-as-Code setup using **AWS SAM and CloudFormation**
* *To Do*: Integrate client app with Langfuse https://langfuse.com/integrations/model-providers/amazon-bedrock

![Data](/assets/architecture.png "Architecture")


## Data product management and Agent
The heart of this RAG system is in understanding the data we work with. We have to know what data we need for it to function as an economic analyst chatbot to generate expert-level responses. Understanding the data also allows us to create a structure out of all the data being ingested to apply retrieval engineering and an Agent to create relevant and fast context retrieval. This is the agent's workflow

![Data](/assets/agent_workflow.png "Agent")

-> This is an article I wrote on substack where I explore the various aspects of economic news that an analyst needs, review possible data sources, and design the chunk metadata and data workflow. 

https://jerroldsworkshopandsymposium.substack.com/p/econolens-part-2-data-product-management


This image shows the metadata fields of a single news article that will be attached to its chunks:

- Date range
- Economic topic
- Person entities
- Institutional entities


![Data](/assets/11a.png "Title")


---
## Details of the system

### **1. RAG Agent**

* Amazon Nova Pro as the Agent's reasoning engine
* Guardrails preventing non-U.S. economic content
* Agent Alias for multi-environment routing
* Multiple tools for retrieving data based on queries and filters

### **2. Opensearch serverless (vector database)**

* Bedrock vector store populated with processed economic news
* Hybrid retrieval of BM25/Cosine similarity
* Enriched chunks with multiple fields for metadata searching
* Ingests data from S3

### **3. Data pipeline**

* Data pipeline that cleans, stores, and processes data in several stages. Data is stored for pipeline debugging.
* Ingests data into Amazon Opensearch vector database
* Scheduling and orchestration with Step Functions and Eventbridge
* Semantic chunking with context compression for extremely large chunks from Amazon bedrock Nova-lite (fast, low-cost)
* Entity extraction for metadata enrichment of chunks
* Logging at each step in Cloudwatch logs and error handling
* Opensearch dashboard for monitoring vector database on AWS management UI

### **4. Observability**

* CloudWatch metrics: Agent-level metrics
* S3 stores invocation logs (large bedrock agent logs)
* Custom CloudFormation resources to activate or deactivate Bedrock logging
* Client app will be integrated with Langfuse for in-depth tracing (latency, agent steps, token count and cost)

### **5. CI/CD Pipeline**

* GitHub Actions triggers unit tests and smoke tests upon creation of Pull Request
* samconfig.toml defines defaults for dev/stag environments
* Lightweight embedding model (<100mb) for semantic similarity comparison against reference answers involving a range of multi-turn test cases in unit testing with minimum thresholds

### **6. Sample responses**

Samples prompt and response conversations can be found in this [location](https://github.com/jerrold110/EconoLens/blob/main/evaluation/sample_responses.md) 

### *7. Offline evaluation*
* Ragas
* LLM Judge
---

# **Offline Evaluation**

The `/evaluation` directory provides a reproducible framework for scoring and analyzing model behaviors via:

### **Ragas**

* Context precision
* Context recall
* Faithfulness
* Answer relevancy

### **LLM-as-a-Judge (Claude from Bedrock)**

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

# **CI/CD with SAM**

The SAM (serverless application model) cli reads the `template.yaml and samconfig.toml` files when a sequence of SAM commands are run. It creates a Cloudformation manifest file for a specific development enviroment (the current default is dev in samconfig.toml), and cloudformation creates or updates these resources. The resources created are prefixed with the name of the environment which helps in identifying a resource's environment. Region in specified. And Lint checks the `template.yaml` for errors. IAM policies/roles are also defined in the IaC manifest file.

The `/tests` directory contains the files for the unit and smoke tests, run with pytest during CI/CD.

Example:

```
EconolensAgent_dev
RetrieveTopicCorporate-stag
econolens-invocation-logs-prod
```

All resources are **environment-prefixed** via the `Environment` parameter (`dev`, `stag`, `prod`). These are also branches on GitHub

| Environment | Purpose                            |
| ----------- | ---------------------------------- |
| **dev-x**   | Developer testing, experimentation |
| **stag**    | Pre-production validation          |
| **prod**    | Production Econolens environment   |


---

# **Deployment**

Both stacks (`sam-econolens-pipeline` and `sam-econolens-agent`) should be deployed sequentially with SAM. Refer to documentation `sam-econolens-agent-docs.md` and `sam-econolens-pipeline-docs.md` for further details. I have omitted Opensearch (the vector database) from the IaC manifest (even though) because it takes 30 mins to start this service.

During build, it builds docker containers for the lambda functions because some of them hold large spacy models.

Example command workflow https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-cli-command-reference-sam-init.html
```
sam init
sam build... 
sam deploy
```

![Data](/assets/deployment.png "Deployment")

---

# **Github actions**

GitHub is used for version control. Github actions is used for automated testing upon merging a pull request from the dev branch with the staging/UAT/Prod branch

### **1. On Pull Request**

* Runs unit tests
* Runs smoke tests
* Blocks merges on failure

### **2. On Merge to Stage**

* Runs tests
* Deploys agent stack to **stag**

### **3. Manual Approvals**

Production deployments require manual review within AWS CodePipeline or GitHub Actions (depending on your pipeline design).

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
├── README.md                      # You are here
├── sam-econolens-agent-docs.md         # In-depth docs
└── same-econolens-pipeline-docs.md     # In-depth docs
```
