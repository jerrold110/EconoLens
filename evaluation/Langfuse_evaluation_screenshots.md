# Evaluation screenshots
*RAG evaluator was used for all of the test cases. The only other options are SQL-to-Text evaluation, and Custom evaluation*

The left pane shows the RAG and Chain-of-thought scores. The right pane shows the Input/Output/Ground Truth, or the Jugde LLM's commentary on the quality of the agent's CoT observation and reasoning process which is used to calculate the CoT scores on the left.

# **Simple response evaluations**

# Simple question on Agent's function
![Data](assets/1.png "")
![Data](assets/2.png "")


# Prompts that activate guardrails
![Data](assets/3.png "")

# Request triggers agent to request for further information
![Data](assets/4.png "")
![Data](assets/5.png "")

# Data not in database correct response
![Data](assets/6.png "")

# **Responses to question prompts**
## Multi-category question that should invoke two tools, not one
### *The answer is spot-on, but CoT scores are lower than usual 0.5-0.7 because Ragas expects an argument proving the context by which the response is generated. This is not possible with Bedrock because it is a closed-system (unlike Langchain/langgraph)*
![Data](assets/12.png "")
![Data](assets/13.png "")
![Data](assets/14.png "")

# **Responses to Repose generation  -error in Ragas package**
![Data](assets/15.png "")
![Data](assets/10.png "")
![Data](assets/11.png "")
