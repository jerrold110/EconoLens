## Evaluation
This directory is for offline evaluation that analyse Agent and RAG quality with qualitative and quantitative metrics. 
* LLM-as-a-judge (CoT)
* RAGAS

It is based on the open-source repository https://github.com/aws-samples/open-source-bedrock-agent-evaluation and has been slightly modified

Using RAGAS to evaluate Agentic RAG with context is not possible without manual effort of copying context from logs, because Bedrock agents acts as a closed platform that does not expose the context retrieved from vector database.

RAGAS scoring does not seem to be working with reports because of the length of the output for some reason, but CoT scoring works. This open-source framework is not bug-free.

At the time evaluation was performed. Data for the first 10 days of Jul/Aug/Sep 2025 were loaded into the vectot database amounting to over 1000 documents.

Further reading
https://www.confident-ai.com/blog/llm-evaluation-metrics-everything-you-need-for-llm-evaluation#ai-agent-metrics

https://medium.com/@AlignX_AI/agents-testing-is-now-more-important-than-ever-before-ad468392fc3b

https://www.datacamp.com/blog/llm-evaluation
