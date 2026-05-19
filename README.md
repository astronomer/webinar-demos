# Local data engineering in the agentic era

Companion project for the _local data engineering in the agentic era_ webinar. It uses the **AstroTrips** scenario, a fictional company selling space trips with a Snowflake-backed schema.

![AstroTrips schema](doc/astrotrips-base-tables.png)

The point of this repository is not the code itself. It is the workflow around the code: how a data engineer can run a real Airflow stack locally, drive it with agents, and stay in control while the agents do the typing.

## What you can take away from this

The sections below are organized as **learnings**, not a script. Each one describes a capability or pattern, why it matters for data engineering, and how to explore it in this repo. Pick whichever ones are new to you and try them.

## 1. Run a real Airflow locally before you reach for the cloud

The fastest feedback loop for an Airflow developer is a local environment that behaves like production. The Astro CLI gives you exactly that: an Airflow stack running in Docker, identical Runtime image, identical providers.

What is worth learning here:

- `astro dev start` boots the full stack. `astro dev kill && astro dev start` is your reset button when state gets weird.
- `astro dev ps` shows the containers; `astro dev bash --scheduler` drops you inside the scheduler so you can `ps auxfw` and see how the `LocalExecutor` actually spawns tasks. This demystifies what is otherwise a black box.
- `astro dev logs --dag-processor -f` (piped through `tspin` or similar) is far more useful than tailing files inside the container.
- `astro dev run` is a passthrough to the Airflow CLI. Try `astro dev run dags list-import-errors` and `astro dev run cheat-sheet`. Anything the UI does, the CLI does, which means anything an agent can do too.

**Best practice:** if you cannot reproduce a pipeline issue locally, you do not understand it yet. Build the muscle of running the real stack on your machine before debugging in production.

## 2. Treat agent tooling as a first-class part of your data stack

Astronomer publishes open-source agent tooling at [astronomer/agents](https://github.com/astronomer/agents). Installing the `astronomer-data` plugin adds skills, an MCP server, and the `af` CLI in one step:

```sh
claude plugin marketplace add astronomer/agents
claude plugin install astronomer-data@astronomer
```

The takeaway is not the install command. It is the layered model of the **agent harness**. Anthropic's own [guidance for large codebases](https://claude.com/blog/how-claude-code-works-in-large-codebases-best-practices-and-where-to-start) recommends building it up in this order:

1. **`AGENTS.md`** for project conventions, loaded every session.
2. **Hooks** to automate consistency and capture learnings.
3. **Skills** that load on demand for specific tasks (e.g. `/managing-astro-local-env`, `/authoring-dags`, `/airflow`).
4. **Plugins** to bundle and distribute working setups across a team.
5. **MCP servers** to connect to internal tools, APIs, and data sources (Airflow API, warehouses).

The `af` CLI is one such MCP entrypoint, also runnable standalone with `uvx --from astro-airflow-mcp af`.

Once installed, a single prompt can replace a sequence of manual steps:

> Using /managing-astro-local-env run the setup Dag in the local environment and summarize the log output of its tasks.

**Try it:** install the plugin, then ask the agent to trigger and summarize a Dag run. Notice what it does that you would normally do by hand, and what it does differently.

## 3. Understand the difference between the harness and the model

Claude Code is a **harness**: it owns the tools, the context window, the skill loading, the file edits. The model is the engine inside. Swap the model and the harness still works.

To make this concrete, you can run Claude Code against a local Ollama model:

```sh
OLLAMA_CONTEXT_LENGTH=131072 ollama serve
ollama pull gemma4:e2b
ollama launch claude --model gemma4:e2b -- --mcp-config <(echo '{"mcpServers":{}}') --strict-mcp-config
```

Then ask it to summarize a file in this repo. Watch GPU utilization with `btop` in another tab.

What this teaches:

- **Context window matters more than you think.** Smaller open models often truncate prompts silently. This is why `AGENTS.md` should be lean.
- **The harness is portable across models.** Your workflow is not locked to one provider.
- **Local models are a real option** for code summarization, doc lookups, and small-scoped edits, especially when data sensitivity rules out cloud calls.

## 4. Dev Containers make "works on my machine" go away

This repo ships a `.devcontainer/devcontainer.json` ([spec](https://containers.dev/implementors/spec/)). Reopening the project in a container gives every contributor (and every agent) the same Python version, the same `astro` CLI, the same extensions.

Things to try inside the container:

- Run `dags/daily_report.py` directly. The `if __name__ == "__main__":` block uses `dag.test()` with a connections YAML, so you can step through tasks under a debugger.
- Set a breakpoint inside `print_report` and step over it. You get a real REPL into your pipeline without spinning up the full stack.

**Caveat:** Dev Containers add overhead. For heavy local Airflow runs you may still want to drop back to native mode. Use the container for portability and onboarding, not necessarily for everyday work.

## 5. Make `AGENTS.md` your single source of truth

Every agent wants its own config file. Claude reads `CLAUDE.md`, Cursor reads `.cursorrules`, Copilot reads `.github/copilot-instructions.md`. Maintaining all of them is a tax.

Two patterns that scale:

- **Symlink approach:** keep `AGENTS.md` as the source, symlink `CLAUDE.md` to it. One file, multiple readers.
  ```sh
  mv CLAUDE.md AGENTS.md
  ln -s AGENTS.md CLAUDE.md
  ```
- **Ruler approach:** for teams using many agents, [intellectronica/ruler](https://github.com/intellectronica/ruler) generates all of them from one config.

Reference: [agents.md](https://agents.md/) and the [Apache Airflow AGENTS.md](https://github.com/apache/airflow/blob/main/AGENTS.md) are good examples to study.

**Go hierarchical.** A single root `AGENTS.md` does not scale. Put the big picture at the repo root and add local files under subdirectories (`dags/AGENTS.md`, `include/sql/AGENTS.md`) for conventions that only apply there. The agent walks up the tree and loads them additively, so you can start a session inside a subdirectory and the relevant context comes along automatically.

**Best practices:**

- Keep agent instructions short. Long preambles burn context window before the agent has read any of your code.
- Review your `AGENTS.md` files every few months. Rules written for older models can constrain newer ones, and good conventions drift out of date as the codebase evolves.
- Use `.claude/settings.json` and `.gitignore` as exclusion files. Anything the agent does not need to read (generated SQL, build artifacts, large fixtures) is wasted context if it lands in the prompt.

## 6. Agentic data engineering: spec, hooks, worktrees

The most interesting part of the workflow is what an agent can build when you give it a real spec and a real stack to test against.

**Worktrees** let you run an agent in parallel with your normal work:

```sh
claude --dangerously-skip-permissions --worktree promo-code-report-dag
```

The agent gets its own branch and working directory. You keep editing on main.

**Hooks** in `.claude/settings.json` make the agent's environment observable and self-improving. Examples in this repo:

- `.claude/hook-activity.log` captures every tool invocation.
- `.claude/dag-changelog.md` is appended to whenever a Dag file changes.
- A `Stop` hook can suggest `AGENTS.md` updates based on what went wrong during the session, turning each run into an improvement to your conventions.

Combined with the Astro CLI and the `/airflow` skill, an agent can implement a new Dag, restart the local Airflow environment, trigger a logical-date run, wait for downstream Dags, and read back the task logs, all from one prompt.

**Subagents for parallel exploration.** While the main agent edits code, spin up read-only subagents to map unfamiliar parts of the codebase or check related modules. They return findings without polluting the main context window. This is especially powerful on large Airflow projects with many Dag files and shared task groups.

**Try it:** look at `.claude/settings.json` and `implementation-notes.md` (generated during a previous run) to see what a successful agentic implementation looks like. Then write your own spec and a hook that runs `astro dev parse` on every Python edit.

## 7. Use Astronomer's data engineering agent for root cause analysis on remote deployments

Astronomer ships its own data engineering agent, invoked with `astro otto`. The difference from a general-purpose coding agent is that it already knows about Airflow deployments, Dag runs, and warehouse schemas.

A realistic RCA flow:

> Check the local data engineering webinar deployment for failing Dag runs, if you find any, perform a root cause analysis and propose a code fix.

The agent can read the failed run, inspect logs, pull warehouse schemas (via `~/.astro/agents/warehouse.yml`, see `/warehouse-init`), correlate symptoms with code, and propose a diff. You stay in the loop for the actual fix.

**Best practice:** RCA is where agents shine, because the search space is bounded (one failed run, one stack trace) and the cost of a wrong guess is low (you read the diff before applying).

## 8. Spec-driven development is worth a look

[GitHub Spec Kit](https://github.com/github/spec-kit) formalizes the "agent + spec" workflow. The flow is:

```
/speckit-constitution   - project principles
/speckit-specify        - baseline spec
/speckit-clarify        - structured questions to de-risk ambiguity
/speckit-plan           - implementation plan
/speckit-tasks          - actionable task list
/speckit-implement      - execute
```

The constitution file (`.specify/memory/constitution.md`) is the interesting artifact. It encodes the conventions every spec must follow, things like "every reporting Dag follows the aggregate/fetch/print shape" or "all SQL lives in `include/sql/` and is referenced through `template_searchpath`".

You do not need Spec Kit to get the benefit. The lesson is: **agents implement what you specify**. A precise spec produces a precise diff. A vague spec produces a refactor you did not ask for.

## 9. LSP gives agents symbol-level navigation

By default, an agent searches your codebase with `grep`. That is pattern matching, which trips over two identically-named functions in different files, shadowed variables, and re-exports. LSP (the Language Server Protocol used by every modern editor) gives the agent real go-to-definition, find-references, and type information.

To wire Pyright into Claude Code:

```sh
npm install -g pyright
# install plugin
# /plugin install pyright-lsp@claude-plugins-official
```

Then enable it in `.claude/settings.json`. Now a prompt like:

> Using LSP, find the definition of the `print_report` function.

returns the exact symbol, not a list of `grep` hits.

**Why it matters for data engineers:** large Airflow projects have shared task groups, factory functions, and operator subclasses across many files. LSP-powered agents follow symbols correctly. Pattern-based agents miss or confuse them.

## Further reading

- [Claude Code in large codebases](https://claude.com/blog/how-claude-code-works-in-large-codebases-best-practices-and-where-to-start)
- [agents.md](https://agents.md/)
- [intellectronica/ruler](https://github.com/intellectronica/ruler) (multi-agent config sync)
- [github/spec-kit](https://github.com/github/spec-kit) (spec-driven development)
- [thedotmack/claude-mem](https://github.com/thedotmack/claude-mem) (persistent agent memory)
- [Agent evals](https://cameronrwolfe.substack.com/p/agent-evals) — once your agent workflows go beyond prototypes, you need a way to measure them. The short version: prefer outcome-based grading (did the Dag actually run and produce the right table?) over trajectory grading (did the agent call the tools you expected?), since multiple paths can be valid. `Pass@K` measures capability across K trials, `Pass^K` measures reliability (success in **all** K). The latter is what matters in production.

## Repo layout

- `dags/` — Airflow Dags including `daily_report.py`.
- `include/sql/` — SQL files referenced by Dags through `template_searchpath`.
- `include/` — Python utilities used by Dag tasks.
- `.devcontainer/` — Dev Container definition.
- `.claude/` — agent settings, hooks, and generated activity logs.
- `doc/` — schema diagrams and reference material.
