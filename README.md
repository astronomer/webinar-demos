# AstroTrips: Airflow for Production-ready AI

Demo for the Astronomer webinar **"Airflow for Production-ready AI"**, showing the Apache Airflow® **Common AI provider** (`apache-airflow-providers-common-ai`).

Everyone has access to the same models. The differentiation is the orchestration layer: retries, observability, human oversight, and giving agents real, governed capabilities. This demo shows the same warehouse question answered **two ways**, so you can see when to keep a human in control and when to hand an agent the keys.

## The scenario

**AstroTrips** is a fictional interplanetary travel agency (trips to the Moon, Mars, and Europa). The Snowflake warehouse holds bookings, payments, promo codes, cancellations, and a per-planet daily revenue report. A story is planted in the data: an early-2026 **Europa launch window slips**, six Europa trips are cancelled (`LAUNCH_DELAY`, fully refunded), and Europa's realized revenue collapses from **$546k → $180k** while Moon and Mars stay steady. That is the mystery the agent demo uncovers.

## What's in here

| Dag | What it shows |
|-----|---------------|
| `dags/00-setup.py` | Deterministic Snowflake schema + seed data (run this first). |
| `dags/01-ask_astrotrips.py` | **Governed NL analytics gateway**: the model proposes, a human approves, Airflow executes. |
| `dags/02-agent_investigation.py` | **Autonomous agent**: a durable agent with a read-only SQL toolset investigates on its own. |
| `dags/03-retry_policy_demo.py` | **LLM-powered retries**: errors classified as retryable recover; unrecoverable ones fail fast. |
| `plugins/token_console/` | **Token Console**: a plugin that visualizes LLM token usage and cost. |
| `plugins/schema_console/` | **Schema**: a plugin rendering an ER diagram of the warehouse tables. |

### 1. Ask AstroTrips

Trigger it with a natural-language `question` param. The pipeline:

```
understand_question   @task.llm      -> a structured QuerySpec (and: is this answerable?)
      |
    gate              @task.branch   -> cheap exit if not answerable (no SQL, no cost)
      |
generate_sql          @task.llm_sql  -> writes SQL; a human approves or edits it (HITL)
      |
run_sql               SQLExecuteQueryOperator   -> deterministic execution on Snowflake
      |
compose_answer        @task.llm      -> a plain-language Answer, grounded in the rows
```

Hardened with an `LLMRetryPolicy` and `UsageLimits`. Try an answerable question ("How did net revenue break down by planet in 2025, and which planet performed best?") and an unanswerable one ("What is our average customer satisfaction score?") to see the cheap exit.

### 2. Agent investigation

A single `@task.agent` gets a read-only, table-allow-listed `SQLToolset` over the same warehouse and investigates *"why did Europa revenue fall at the start of 2026?"* on its own: listing tables, inspecting schemas, running its own queries, and returning a structured `Finding`. `durable=True` means a retry resumes from cached model/tool steps instead of re-paying for them. This is the opposite trust model to demo 1: same question, agent decides.

### 3. Retry policy

Three tasks raise representative errors. `LLMRetryPolicy` asks an LLM to classify each one: an expired API key (auth) and a bad column type (data) **fail fast** (don't burn paid retries), while a rate-limit is classified retryable, backs off, and **recovers on the third attempt**.

## Run it locally

**Prerequisites:** [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview), Docker, a Snowflake account, and an OpenAI API key.

1. **Configure connections.** Copy `.env.dist` to `.env` and fill in:
   - `AIRFLOW_CONN_SNOWFLAKE_ASTROTRIPS`: Snowflake (key-pair auth).
   - `AIRFLOW_CONN_PYDANTICAI_DEFAULT`: your OpenAI API key (model `openai:gpt-5-mini`).
   - `AIRFLOW__COMMON_AI__DURABLE_CACHE_PATH` and `TOKEN_CONSOLE_*` are pre-filled for local use.
2. **Start Airflow:** `astro dev start`
3. **Seed the warehouse:** trigger the `setup` Dag once.
4. **Run the demos:** trigger `ask_astrotrips` (approve the SQL when it pauses), then `agent_investigation`, then `retry_policy_demo`.
5. **Watch the cost:** open the **Token** page in the left nav.

## Demo flow

**1. Seed the warehouse.** Trigger `setup` once. Mention the planted story: Europa revenue climbs through late 2025, then collapses at the new year.

**2. Ask AstroTrips, the answerable question.** Trigger `ask_astrotrips` with:

> *How did net revenue break down by planet in 2025, and which planet performed best?*

Walk the tasks: `understand_question` produces a structured `QuerySpec`; the branch decides it is answerable; `generate_sql` **pauses for approval**, show the generated SQL, optionally edit it, then approve; `run_sql` executes on Snowflake; `compose_answer` returns the answer. Expected result: **Mars had the highest net revenue (~$273k)**, and Europa's gross is huge but its net collapses to ~$120k after refunds, a teaser for demo 4.

Other answerable questions to try:

> *How much did we refund by planet and reason in 2025?*

Surfaces the story directly: **Europa `LAUNCH_DELAY` $486,000**, Mars `MEDICAL` $15,000, Moon `CUSTOMER_REQUEST` $6,000. A clean lead-in to the agent demo.

> *Compare passengers and net revenue for Mars vs Europa by month in 2025.*

Returns a month-by-month table plus year totals: **Mars; 10 passengers, $273k net; Europa; 11 passengers, $120k net.** Europa's cancelled months net to $0 (the launch-delay refunds showing through). Note: the seed data spans Jul 2025–Jan 2026, so Jan–Jun read as empty.

> *Which promo code was used most, and how much discount did it give away?*

**ASTRO10 was used most (7 bookings, ~$11,600 in discounts)**, but **ASTRO20 (5 bookings) gave away far more (~$69,200)** because it lands on pricier Mars/Europa trips; a nice "most-used isn't most-expensive" insight.

**3. Ask AstroTrips, the unanswerable question.** Trigger it again with:

> *What is our average customer satisfaction score?*

The intake step marks it `is_answerable = false` and the branch takes the **fast exit**.

**4. Agent investigation.** Trigger `agent_investigation`. The agent explores the warehouse on its own and returns a `Finding` naming the Europa `LAUNCH_DELAY` cancellations as the driver.

**5. Retry policy.** Trigger `retry_policy_demo`. Auth and data errors fail fast; the rate-limit task is classified retryable, backs off, and **recovers green on the third attempt**.

**6. Token console.** Open the **Token** nav item. Show per-task input/output tokens and cost (priced via genai-prices), the answerable run vs the near-zero unanswerable run, and the autonomous agent's spend visibly dwarfing the constrained gateway, the cost of autonomy.
