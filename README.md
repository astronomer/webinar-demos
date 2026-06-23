# Intro to Airflow

Companion project for the _Intro to Airflow: From basics to agentic_ webinar. It uses the **AstroTrips** scenario, a fictional company selling space trips with a Snowflake-backed schema.

![AstroTrips schema](doc/astrotrips-base-tables.png)

## Stack

- Astro Runtime 3.2 (Airflow 3)
- Snowflake for the AstroTrips data, PydanticAI connection for the agent demo, PokeAPI as a public HTTP example
- Providers: `common-sql`, `snowflake`, `http`, `common-ai`

## Getting started

```bash
astro dev start
```

The Airflow UI is then available at `localhost:8080`. Connections are defined in `include/connections.yaml` and `.env` (`snowflake_astrotrips`, `pydanticai_default`, `pokeapi`).

## The Dags

Each Dag is one building block from the talk, going from the basics to an agentic pipeline.

| Dag | What it shows | Slide topic |
| --- | --- | --- |
| `demo1` | `@task.bash` and `@task`, dependency inference, XCom | Define tasks, Dependencies |
| `demo2` | `HttpOperator` wired into a `@task` via `.output` | HttpOperator, XCom |
| `demo3` | Asset-aware scheduling: a producer materializes `Asset("data")`, a consumer runs on it | Scheduling |
| `demo4` | Data quality with `SQLValueCheckOperator` and `SQLColumnCheckOperator` grouped in a task group, feeding a query task | SQL check operators |
| `demo5` | `@task.agent` with a `SQLToolset`, an LLM analyst that queries Snowflake and returns a report | Airflow for AI |
| `demo6` | Built live during the webinar (read planets, print the result) | Let's build |

## Resources

- Demo repo: https://github.com/astronomer/webinar-demos/tree/intro-to-airflow
- Registry: https://airflow.apache.org/registry
- Learn guides: https://www.astronomer.io/docs/learn/overview
