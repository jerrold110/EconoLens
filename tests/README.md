## Unit tests
This folder is for a series of tests that are automatically run on a push or pull request made to the stage branch
This folder is for automated tests with a prompt and model answer dataset. The metrics are quantitative to automatically determine whether the tests pass or fail.

LLM-as-a-judge is done during offline model evaluation, but might be possible to incorporate into an CI/CD pipeline.

### Smoke tests:
* Health check
* Response generation
* Latency
* Agent instructions logic

### Unit tests:
* Cosine similarity due to lightweight nature of calculation with lightweight model (<100mb): Questions, Report, Guardrails, multi-turn, invalid date range