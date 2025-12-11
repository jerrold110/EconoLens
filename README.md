# **Econolens AI Platform**
*Time-sensitive Agentic RAG System for U.S. Economic Analysis*

Econolens is an economic report generating and questions answering AI with a **unique Agentic RAG architecture** that enables up-to-date responses and intelligently retrieves data vector database with a number of documents in the magnitude of 100,000s through enriched chunks and metadata filtering. It ingests a large number of news articles from the world wide web on a daily basis into a vector database and breaks them down into enriched chunks with an **NLP data pipeline**. 

Through the Agent's nuanced workflow (check diagrams), it can engage the user in multi-turn conversaitons to generate economic reports and answers questions related to the U.S. economy over specific periods of time. From a large catalogue of articles for specific economic topics,persons, institutions.
Use-cases might include: assisting an economist in understanding the current/past economic conditions, understanding the drivers behind economic events and their effects, or getting a in-depth analysis about a particular sector of the economy/corporation.

Check DIAGRAMS.md for agent/architecture diagrams

This repository contains:

* Infrastructure-as-Code (IaC) using **AWS SAM**
* A multi-environment **CI/CD pipeline**
* Offline evaluation tools (Langfuse, Ragas, LLM-as-a-judge)
* Automated unit + smoke testing
* Research notebooks for chunking and summarization experiments
* A full agent ecosystem: Bedrock Agent, Alias, Guardrails, Lambda tools, logging, and monitoring

---

# **Table of Contents**

1. [Repository Structure](#repository-structure)
2. [High-Level System Architecture](#high-level-system-architecture)
3. [Environments](#environments)
4. [Deployment Instructions](#deployment-instructions)
5. [CI/CD Pipeline](#cicd-pipeline)
6. [Evaluation Framework](#evaluation-framework)
---

# **Repository Structure**

```
/
├── sam-econolens-pipeline/        # CI/CD SAM stack (deployment pipeline)
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
│   └── summary_eval.ipynb
│
├── .github/workflows/             # GitHub Actions CI/CD
│   ├── run-tests.yml
│   └── deploy.yml
│
└── README.md                      # You are here
```

---

# **High-Level System Architecture**

The Econolens agent system consists of:

### **1. Bedrock Agent Core**

* Amazon Nova Pro as the foundation model
* Custom orchestration prompt (prompt override)
* Guardrails preventing non-U.S. economic content
* Agent Alias for multi-environment routing
* Memory disabled (stateless current-context behavior)

### **2. Action Groups (Tools)**

Each tool is backed by a Lambda function responsible for retrieving vector-based economic news content:

**Topic retrieval tools**

* Consumer Behavior
* Corporate
* Economy General
* Economy Long-Term
* Government & Policy
* Inflation
* Labor Market

**Entity retrieval tools**

* Institution-level retrieval
* Person-level retrieval

**General tool**

* Query-only retrieval

These Lambdas retrieve embeddings from a **Bedrock Knowledge Base**, search for chunks with a hybrid of BM25/Cosine-similarity, and rerank the chunks before returning them to the LLM as context.

### **3. Knowledge Base**

* Bedrock vector store populated with processed economic news
* Index used for retrieval + reranking
* Enriched chunks with multiple fields for metadata searching

### **4. Observability**

* CloudWatch metrics: Agent-level metrics
* S3 model invocation logging (large bedrock agent logs)
* Custom CloudFormation resource to activate logging

### **5. CI/CD Pipeline**

* GitHub Actions triggers unit tests and smoke tests
* SAM pipeline handles cross-account deployment
* samconfig.toml defines defaults for dev/stag environments

## Architecture
![Data](/assets/architecture.png "Architecture")

## Agent

![Data](/assets/agent_workflow.jpg "Agent")

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

### **Langfuse**

* Structured dataset logging
* Evaluation workflows
* Experiment versioning

### **LLM-as-a-Judge (Claude)**

* Tracks quality, reasoning, factuality
* Used for agent regression testing

### **Use cases**

* Benchmarking chunking strategies
* Regression testing after prompt changes
* Evaluating tool retrieval quality
* Comparing models (e.g., Nova Pro vs alternatives)

---

# **Testing**

The `/tests` directory contains the files for the unit and smoke tests, run with pytest.
