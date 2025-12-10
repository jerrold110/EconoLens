import time
import uuid
import os
import boto3
from botocore.exceptions import ClientError


agentId = os.getenv("AGENT_ID")
agentAliasId = os.getenv("AGENT_ALIAS_ID")
assert agentId is not None; assert agentAliasId is not None

bedrock_agent_runtime_client = boto3.client("bedrock-agent-runtime", region_name="us-east-1")

def request_agent(prompt, session_id, client=bedrock_agent_runtime_client, agent_id=agentId, agent_alias_id=agentAliasId):

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
    """
    Throws error if response is not 200
    """
    request_agent("Hello", str(uuid.uuid4()))

# -------------------------------------------------------
# 2. RESPONSE GENERATION TEST
# -------------------------------------------------------
def test_response_generation():
    text = request_agent("Hello", str(uuid.uuid4()))

    assert len(text) > 0, "Empty response from agent"

# -------------------------------------------------------
# 3. LATENCY TEST
# -------------------------------------------------------
def test_latency():
    MAX_LATENCY_SECONDS = 8.0  # adjust based on model/context

    start = time.time()
    text = request_agent("Say hello.", str(uuid.uuid4()))
    end = time.time()
    latency = end - start

    assert latency < MAX_LATENCY_SECONDS, \
        f"Latency too high: {latency:.2f}s (limit {MAX_LATENCY_SECONDS}s)"
    
# -------------------------------------------------------
# 4. LOGIC TEST
# -------------------------------------------------------
def test_logic():
    """
    The agent refuses to respond to mathematical questions at times. So ask a question about its function, it should include the word 'report'
    """
    text = request_agent("What can you do?", str(uuid.uuid4()))

    assert 'report' in text, f"Expected 'report' in response, got: {text}"

