import time
import uuid
import os
import boto3
from botocore.exceptions import ClientError

from sentence_transformers import SentenceTransformer, util

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

# https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2
# https://huggingface.co/sentence-transformers
# https://huggingface.co/tasks/sentence-similarity
model = SentenceTransformer('all-MiniLM-L6-v2')

def compare_cosine_sim(inference, reference):
    """
    Sentence embedding cosine similarity using cosine similarity
    """
    emb1 = model.encode(inference, convert_to_tensor=True)
    emb2 = model.encode(reference, convert_to_tensor=True)

    # returns a torch tensor([[1.]])
    # shape torch.Size([1, 1])
    cos_sim = util.pytorch_cos_sim(emb1, emb2)
    
    return float(cos_sim)


# -------------------------------------------------------
# 1. GUARDRAIL TEST
# -------------------------------------------------------
def test_guardrail():
    prompt = "Generate a report for the EU economy in 2025"
    reference = "The input contains blocked content"
    inference = request_agent(prompt)

    cos_sim = compare_cosine_sim(inference, reference)

    assert cos_sim > 0.7, f"Similarity between texts is {float(cos_sim):.4}, below 0.7\nReference: {reference}\nInference: {inference}"


# -------------------------------------------------------
# 2. OUTSIDE DATABASE DATE RANGE TEST
# -------------------------------------------------------
def test_outside_date_range():
    prompt = "What were the consumer behavior trends in 2020?"
    reference = "I don't have enough information to answer that."
    inference = request_agent(prompt)

    cos_sim = compare_cosine_sim(inference, reference)

    assert cos_sim > 0.9, f"Similarity between texts is {float(cos_sim):.4}, below 0.9\nReference: {reference}\nInference: {inference}"

# -------------------------------------------------------
# 3. QUALITY TEST - QUESTION
# -------------------------------------------------------
def test_quality_question():
    prompt = "How did tariffs affect the economy in August 2025?"
    reference = """
    Based on the retrieved information, tariffs were exerting only a modest influence on the economy as of August 2025. Federal Reserve Governors Christopher Waller and Michelle Bowman indicated that tariffs were having a minimal effect on inflation and were likely to remain a minor factor. They noted that inflation would likely be nearer to the Feds 2% goal if the tariffs were not in place. Still, there are worries that postponing interest-rate adjustments could weaken the labor market and further slow economic growth. President Donald Trump has criticized the Fed for holding rates steady, urging significant rate cuts instead. The Feds decision to maintain current rates reflects its intention to continue assessing how tariffs are affecting inflation and overall economic conditions.
    """
    inference = request_agent(prompt)

    cos_sim = compare_cosine_sim(inference, reference)

    assert cos_sim > 0.7, f"Similarity between texts is {float(cos_sim):.4}, below 0.7\n\nReference: {reference}\n\nInference: {inference}"


# -------------------------------------------------------
# 4. QUALITY TEST - REPORT
# -------------------------------------------------------
def test_quality_report():
    prompt = "Generate an economic report for August 2025"
    reference = """
    Economic Report for August 2025 

    **Consumer Behavior**
    Consumer behavior in August 2025 is characterized by a shift towards prioritizing experiences over material goods. Higher-earning consumers are spending significantly more on foreign travel, specialty foods, and entertainment. Mastercard research indicates that two-thirds of consumers consider checking off bucket-list experiences a top priority. This trend is more pronounced among higher-earning demographics, who are six times more likely to pursue elevated dining and unique travel experiences. Financial institutions are responding by offering tailored products and services, such as Mastercard's The Mastercard Collection, which provides access to exclusive experiences. However, consumer spending has shown signs of slowing, particularly in categories like apparel and entertainment, indicating a potential local economic slowdown.
    
    **Corporate** 
    Corporate activity in August 2025 is marked by significant mergers and acquisitions, as well as strategic investments. Paramount Global and Skydance Media completed their $8.4 billion merger, forming Paramount Skydance Corp. Coinbase raised $2 billion in convertible notes for acquisitions and general corporate purposes, despite a 25% revenue drop in Q2. Block Inc. added 108 Bitcoin to its corporate treasury, bringing total holdings to 8,692 BTC worth approximately $1 billion. Joby Aviation announced plans to acquire Blade Air Mobility's passenger business for up to $125 million, aiming to expedite its journey to commercialization. Berkshire Hathaway reported a profit decline due to a $3.76 billion writedown on its Kraft Heinz stake, reflecting challenges in the food industry. 
    
    **Economy General**
    The general economy in August 2025 is experiencing mixed signals. GDP rebounded more robustly than expected in the second quarter, but core inflation accelerated to 2.8%, above the Fed's 2% target. Consumer spending rose less than expected in June, and construction spending continued to decline. The Institute for Supply Management's manufacturing activity index dipped, indicating a quicker contraction in the sector. The Atlanta Fed's GDP tracker points to continued growth but expects a deceleration to 2.1% in the third quarter from 3% in the second quarter. The unemployment rate has remained stable between 4% and 4.2%, but the labor force has stagnated due to immigration policy changes. 
    
    **Economy Long-Term** 
    Long-term economic indicators for August 2025 suggest a weakening outlook. Inflation-adjusted consumer spending ticked up by 0.1% in June, but aggregate weekly payrolls were 5.3% higher in July compared to the previous year, representing an improvement from June. However, the economic outlook is gloomier than a week ago, with Trump's trade policies nudging the US toward stagflation. Many sectors are responding to rising import costs by shedding payroll, and the Federal Reserve may face an impasse if both inflation and slow job growth persist. The Trump administration's tariffs and immigration policies are expected to lower annual GDP growth by 0.8 percentage points. 
    
    **Government and Policy**
    Government and policy actions in August 2025 are dominated by Trump's trade policies and tariff impositions. The US has imposed tariffs on goods from India, Canada, and other countries, leading to increased costs for American businesses and consumers. The Federal Reserve has maintained high interest rates, despite calls from President Trump for rate cuts. The Bank of England cut its main interest rate by a quarter percentage point to 4%, aiming to bolster the sluggish UK economy. The US Treasury released a policy roadmap for digital financial technology, emphasizing the need for a national strategy to align public and private sector innovation. 
    
    **Inflation** 
    Inflation in August 2025 is influenced by Trump's tariffs, which have added upward pressure on prices. The Personal Consumption Expenditures price index showed a 0.3% increase in June, boosting the annual inflation rate to 2.6%. The New York Federal Reserve reported that Americans' longer-term inflation outlook deteriorated in July, with expected inflation five years from now rising to 2.9%. The European Central Bank expects inflation in the euro zone to hold below 2%, contrasting with the Bank of England's forecast of 4% inflation in September. 
    
    **Labor Market**
    The labor market in August 2025 is showing signs of weakness. The US added 73,000 jobs in July, less than expected, and hiring in May and June was revised down by 258,000 jobs. The unemployment rate edged up to 4.2%, and the number of Americans receiving unemployment benefits rose to the highest level since November 2021. The labor market slowdown is attributed to Trump's tariffs, federal spending cuts, and aggressive immigration restrictions. The Federal Reserve is increasingly concerned about the labor market, with several officials signaling openness to a rate cut in September. 
    
    **Executive Summary** 
    The economic report for August 2025 highlights a mixed economic landscape. Consumer behavior is shifting towards experiences, while corporate activity is marked by significant mergers and acquisitions. The general economy shows signs of slowing, with core inflation rising and consumer spending less robust than expected. Long-term economic indicators suggest a weakening outlook, influenced by Trump's trade policies and immigration restrictions. Government and policy actions are focused on tariff impositions and interest rate decisions, with the Federal Reserve maintaining high rates despite calls for cuts. Inflation is rising due to tariffs, and the labor market is showing signs of weakness, with job growth slowing and unemployment edging up. The Federal Reserve is increasingly concerned about the labor market, signaling a potential rate cut in September.
    """
        
    inference = request_agent(prompt)

    cos_sim = compare_cosine_sim(inference, reference)

    assert cos_sim > 0.5, f"Similarity between texts is {float(cos_sim):.4}, below 0.5\n\nReference: {reference}\n\nInference: {inference}"







