# AI SDK Webinar Demo

This repository contains the demo for the [From Zero to Production: Orchestrating LLM workflows with the Airflow AI SDK webinar](https://www.astronomer.io/events/webinars/from-zero-to-production-orchestrating-llm-workflows-with-the-airflow-ai-sdk-video/), showcasing how to use the [Airflow AI SDK package](https://github.com/astronomer/airflow-ai-sdk) to add AI steps in your DAGs.

## Repository Content

### Syntax Examples (`dags/syntax/`)

* **`example_syntax_task_embed.py`** - Demonstrates use of `@task.embed` to create embeddings for vector comparison
* **`example_syntax_task_llm.py`** - Shows how to use `@task.llm` to make a call to the OpenAI API
* **`example_syntax_task_llm_branch.py`** - Illustrates conditional branching based on LLM responses with `@task.llm_branch`
* **`example_syntax_task_agent.py`** - Shows how to use `@task.agent` to add agentic tasks to your DAG.

### Use Cases (`dags/use_cases/`)

* **`write_exec_blog_post.py`** - DAG that orchestrates an AI agent that researches topics and writes executive-focused blog posts
* **`personalized_betting.py`** - Personalized recommendation system for betting scenarios using `@task.lmm` and `@task.llm_branch` in both a batch inference and inference execution (ad-hoc) pattern.
* **`webtrigger_sqs.py`** - Helper DAG Web-triggered workflows with SQS integration

## Getting Started

## Run the Demo Locally

1. Fork this repo and clone this branch (`ai-sdk-webinar`) to your local machine.

2. Make sure you have the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed and are at least on version 1.34.0 to run Airflow 3.

3. Copy the `.env_example` file to a new file called `.env` and add your information. Note that for the syntax examples only your `OPENAI_API_KEY` is needed. The other values are optional and only needed for the use case examples.

3. Start the Airflow project with 
   ```bash
   astro dev start
   ```
   The API server with the Airflow UI will be available at `localhost:8080`.

4. Run the DAGs and experiment with the prompts.
