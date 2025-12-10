# **Econolens Agent – Infrastructure-as-Code (CloudFormation and SAM)**

Infrastructure deployment for the **Econolens Agent**, an agentic RAG-based economic analysis system built on **Amazon Bedrock Agents**, **Lambda**, **OpenSearch**, and **CloudWatch/S3** for observability.
This SAM template deploys the full Bedrock Agent ecosystem—including the agent, alias, guardrails, action groups, Lambda tools, IAM roles, and logging infrastructure—across multiple environments. Through version control on Github, all the components especially the Agent are versioned and Github Actions is used for CI/CD.

---

## **Table of Contents**

* [Overview](#overview)
* [Architecture](#architecture)

  * [Bedrock Agent](#bedrock-agent)
  * [Action Groups & Lambda Tools](#action-groups--lambda-tools)
  * [Knowledge Base Integration](#knowledge-base-integration)
  * [Guardrails](#guardrails)
  * [Logging & Observability](#logging--observability)
  * [IAM Role Structure](#iam-role-structure)
* [RAG Workflow](#rag-workflow)
* [Agent Workflow](#agent-workflow)
* [Environment Management](#environment-management)
* [Deployment](#deployment)

  * [Prerequisites](#prerequisites)
  * [Deploying to Dev/Stage/Prod](#deploying-to-devstagprod)
  * [samconfig.toml](#samconfigtoml)
* [Project Structure](#project-structure)
* [Outputs](#outputs)
* [Operational Notes & Caveats](#operational-notes--caveats)

---

# **Overview**

This project defines the **Econolens Bedrock RAG Agent**, a system that:

* Generates detailed **economic reports**.
* Answers questions about **U.S. economic events, institutions, people, and indicators**.
* Retrieves up-to-date economic data via **agent action groups calling AWS Lambda functions**, which query an **OpenSearch vector database**.
* Engages the user in multi-turn conversations to understand user's intention
* Uses **Bedrock Knowledge Base** embeddings, **Chunk metadata filtering**, and **Reranking** to support high-quality and **low-latency** RAG.
* Implements strong **guardrails** to restrict the agent to U.S. economic content.
* Sends detailed **agent invocation logs to S3** due to their large size.
* Publishes operational metrics to **CloudWatch Metrics**.

This SAM template is designed for **multi-environment deployments**, with all resource names prefixed by the environment (`dev`, `stag`, or `prod`).

---

# **Architecture**

### **Bedrock Agent**

The central component is the Bedrock Agent:

* Uses **amazon.nova-pro-v1** (80B params) as the foundational model.
* Defines a full **instruction set**, including:

  * Tool usage rules
  * Report generation workflow
  * Question answering workflow
  * Required date handling
  * Step-by-step reasoning inside `<thinking>` tags
* Uses **PromptOverrideConfiguration** to tightly control inference agent behavior (temperature, top-p, stop sequences, etc.) and facilitate ReAct/CoT prompting.
* Connected to all action groups and tools defined in this stack.
* Uses a **GuardrailConfiguration** to restrict economic queries to U.S. topics only.
* Sample interactions with user for few-shot learning

Each deployed environment gets its own Agent and Alias:

```
EconolensAgent_dev
EconolensAgent_stag
EconolensAgent_prod
```

### **Action Groups & Lambda Tools**

Each economic topic, entity type, or general query is backed by a **Lambda-based action group**, including:

#### **Topic-Based Tools**

* GetTopicConsumerBehavior
* GetTopicCorporate
* GetTopicEconomyGeneral
* GetTopicEconomyLongTerm
* GetTopicGovernmentAndPolicy
* GetTopicInflation
* GetTopicLaborMarket

#### **Entity-Based Tools**

* GetEntityInstitution
* GetEntityPerson

#### **General Tool**

* GetQueryOnly (broad unclassified queries)

Each action group maps to a Lambda function that:

1. Accepts metadata parameters (`start_date_str`, `end_date_str`, `query`, etc.) for efficient retrieval from **Amazon Opensearch Serverless**
2. Retrieves relevant chunks from OpenSearch
3. Applies reranking with **Cohere Rerank 3.5**
4. Returns payload with chunk data and metadata for the agent to use 

All Lambda functions:

* Use consistent environment variables
* Are prefixed by environment
* Run Python 3.11
* Have IAM permissions via the `AgentFunctionsRole`

### **Knowledge Base Integration**

While the Agent itself does not query the KB directly, **Lambda functions use the Knowledge Base vector embeddings** for retrieval.
The following is configured globally:

```
BEDROCK_KNOWLEDGE_BASE_ID
RETRIEVE_CHUNK_PER_DAY_COUNT
RERANK_CHUNK_COUNT
```

These environment variables control retrieval parameters for all tools.

### **Guardrails**

A Bedrock Guardrail is deployed with:

* The **BlockEconomyOutsideUSA** Topic Policy
* Input blocking for non-U.S. economic requests
* Attached to the agent through `GuardrailConfiguration`

A version resource (`GuardrailVersion`) is created to stabilize the guardrail configuration.

### **Logging & Observability**

#### **S3-based Agent Invocation Logging**

Bedrock invocation logs can be extremely large, so this stack:

* Creates **two S3 buckets**:

  * `econolens-server-access-logs-<env>` – server access logs
  * `econolens-invocation-logs-<env>` – Bedrock invocation logs
* Uses a **custom resource Lambda** to:

  * Call `PutModelInvocationLoggingConfiguration`
  * Enable model invocation logging for Bedrock Agents
* Restricts access so that only Bedrock can write to invocation logs.

#### **CloudWatch Metrics**

IAM policy permits the agent to publish to:

```
AWS/Bedrock/Agents
```

This enables metric dashboards tracking:

* Invocation count
* Latency
* Failures
* Guardrail activations
* Token usage

### **IAM Role Structure**
#### **AgentFunctionsRole**

* Assumed by Lambda functions
* Grants:

  * AWSLambdaBasicExecutionRole
  * AmazonBedrockFullAccess

#### **BedrockAgentExecutionRole**

* Assumed by Bedrock Agent
* Allows:

  * Invoking specific Lambdas
  * Running Bedrock APIs
  * Applying guardrails
  * Writing CloudWatch metrics

#### **Logging Execution Role**

* For custom resource Lambda handling log configuration

---

# **RAG Workflow**
```
┌────────────────┐
│     USER        │
│  Query / Prompt │
└───────┬────────┘
        │
        ▼
┌───────────────────────────────┐
│        BEDROCK AGENT          │
│  • Determine task type        │
│  • Validate dates             │
│  • Identify categories        │
│  • Decide tool(s) to use      │
└───────────┬───────────────────┘
            │
            ▼
┌───────────────────────────────┐
│     ACTION GROUP SELECTOR     │
│  Select one or more tools:    │
│   • Topic Tools               │
│   • Entity Tools              │
│   • Query-Only Tool           │
└───────────┬───────────────────┘
            │
            ▼
      ┌───────────────┐
      │  LAMBDA TOOL  │
      │ (per category)│
      └───────┬───────┘
              │
              ▼
   ┌──────────────────────────┐
   │   OPENSEARCH VECTOR DB   │
   │  • Embed query           │
   │  • Retrieve chunks       │
   │  • Metadata filtering    │
   └───────────┬──────────────┘
               │
               ▼
      ┌──────────────────────┐
      │     RERANKING        │
      │  Reduce results to   │
      │  most relevant N     │
      └──────────┬───────────┘
                 │
                 ▼
       ┌──────────────────────┐
       │    BEDROCK AGENT     │
       │ • Synthesize answer  │
       │ • OR generate report │
       └──────────┬───────────┘
                  │
                  ▼
          ┌──────────────┐
          │    USER       │
          │ Final Output  │
          └──────────────┘

```

---

# **Agent Workflow**

The agent follows a deterministic workflow:

### **1. Determine task type**

* **Report generation** (phrases like “economic report”, “overview of”)
* **Question answering** (queries starting with *What/Why/How* etc.)

### **2. Validate and extract required inputs**

* Requires `start_date_str` and `end_date_str`
* Prompts the user if dates or categories are missing
* Identifies categories:

  * Topics
  * People
  * Institutions
  * Ambiguous → falls back to `GetQueryOnly`

### **3. Create retrieval parameters**

* Build metadata queries for each Lambda tool
* Pass date ranges, query strings, and optional categories

### **4. Retrieve and rerank**

* Vector DB returns candidate chunks
* Lambda functions rerank down to configurable chunk counts
* Agent uses strictly retrieved material ― **never past conversation content**

### **5. Generate final output**

* For reports → structured 7-section economic report + Executive Summary
* For Q&A → synthesized answer from tool responses

---

# **Environment Management**

All resources include the `Environment` parameter (`dev`, `stag`, `prod`) in their names and metadata:

* AgentName = `EconolensAgent_<env>`
* Lambda functions = `RetrieveTopicConsumerBehavior-<env>`
* Buckets = `econolens-invocation-logs-<env>`

This ensures clean separation of environments.

### **samconfig.toml**

Currently includes configuration for:

* `dev`
* `stag`

To add `prod`, create another TOML section:

```toml
[prod.deploy.parameters]
stack_name = "econolens-agent-prod"
parameter_overrides = "Environment=prod"
capabilities = "CAPABILITY_NAMED_IAM"
resolve_s3 = true
```

---

# **Deployment**

## **Prerequisites**

* AWS CLI configured
* AWS SAM CLI installed
* Docker installed
* Permissions to deploy IAM roles, Bedrock Agents, Lambda, and S3
* OpenSearch Vector DB already populated and accessible by Lambda

---

## **Deploying to Dev**

```bash
sam validate
sam build
sam local invoke RetrieveEntityPerson --event event/agent_invoke_entityPersons_test.json 
sam deploy --config-env dev
```

## **Deploying to Stage**

```bash
sam deploy --config-env stag
```

## **Deploying to Prod**

If you add a prod config:

```bash
sam deploy --config-env prod
```

---

# **Project Structure**

Example structure expected by the template:

```
.
├── template.yaml
├── lambda/
│   ├── retrieve_topic/
│   │   ├── consumer_behavior/app.py
│   │   ├── corporate/app.py
│   │   ├── economy_general/app.py
│   │   ├── economy_long_term/app.py
│   │   ├── government_and_policy/app.py
│   │   ├── inflation/app.py
│   │   └── labor_market/app.py
│   ├── retrieve_entity/
│   │   ├── institution/app.py
│   │   └── person/app.py
│   ├── retrieve_query_only/app.py
│
└── samconfig.toml
```

---

# **Outputs**

After deployment, SAM outputs:

### **AgentID**

The Bedrock Agent ID used for invoking the agent programmatically.

### **AgentAliasID**

The Alias ID (always referencing Version 1 due to Bedrock versioning constraints).

---

# **Operational Notes & Caveats**

### **Bedrock Agent Versioning**

AWS currently has known issues with:

* Alias → version mapping
* Draft version creation
* Version update propagation

Therefore, the stack forces:

* A single stable version
* Alias always pointing to version 1

See comments in template for references to AWS issues.

### **S3 Buckets are Retained**

Both buckets use:

```
DeletionPolicy: Retain
UpdateReplacePolicy: Retain
```

You **must manually empty the buckets** before deletion.

### **Guardrails Block Non-U.S. Economics**

Any question involving foreign economies will be blocked.

### **Lambda Invocation Permissions**

Permissions use `bedrock.amazonaws.com` as the principal and must reference the agent ARN.

### **Log Size**

Invocation logs can be extremely large, so S3 logging is mandatory and CloudWatch logging is intentionally not used.

---

# **License**

MIT.

