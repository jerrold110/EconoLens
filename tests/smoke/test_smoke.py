import time
import uuid
import os
import boto3
from botocore.exceptions import ClientError



# ---- CONFIG ----
agentId="SSFZ7SY1DW"
agentAliasId="NH4NWLPABD"
sessionId=str(uuid.uuid4())

bedrock_agent_runtime_client = boto3.client("bedrock-agent-runtime", region_name="us-east-1")

def request_agent(prompt, client=bedrock_agent_runtime_client, agent_id=agentId, agent_alias_id=agentAliasId, session_id=sessionId):

    try:
        response = client.invoke_agent(
            agentId=agent_id,
            agentAliasId=agent_alias_id,
            sessionId=session_id,
            inputText=prompt,
            
        )

        statusCode = response['ResponseMetadata']['HTTPStatusCode'] 

        if statusCode != 200:
            print(response['ResponseMetadata'])
            raise Exception(f"The HTTP status code returned was {statusCode}")

        output = ""
        for event in response.get("completion"):
            chunk = event["chunk"]
            output = output + chunk["bytes"].decode()

    except ClientError as e:
        raise ClientError(f"Error during invocation of client. {e}")

    return output

# -------------------------------------------------------
# 1. SERVICE HEALTH CHECK
# -------------------------------------------------------
def test_service_health():
    request_agent("Hello")

# -------------------------------------------------------
# 2. RESPONSE GENERATION TEST
# -------------------------------------------------------
def test_response_generation():
    text = request_agent("Hello")

    assert len(text) > 0, "Empty response from agent"

# -------------------------------------------------------
# 3. LATENCY TEST
# -------------------------------------------------------
def test_latency():
    MAX_LATENCY_SECONDS = 8.0  # adjust based on model/context

    start = time.time()
    text = request_agent("Say hello.")
    end = time.time()
    latency = end - start

    assert latency < MAX_LATENCY_SECONDS, \
        f"Latency too high: {latency:.2f}s (limit {MAX_LATENCY_SECONDS}s)"
    
# -------------------------------------------------------
# 4. LOGIC TEST
# -------------------------------------------------------
def test_logic():
    text = request_agent("What is 2+2")

    assert '4' in text, f"Expected '4' in response, got: {text}"

