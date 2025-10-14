# Airflow 3.1 Demo

- Home view
  - Colors
  - Dark / light mode
  - i18n
  - Pool Slots
- Dags view
  - Basic flow of `ai_support_ticket_system`
  - Code of `ai_support_ticket_system`
  - Run `ai_support_ticket_system` with approval
  - Run `ai_support_ticket_system` with escalate and manual response (stop at entry)
  - Show different integrations of HITL required actions
    - Individual dag view
    - Home view
    - Dags view
    - Browse → Required Actions (with filter)
    - Browse → Audit Log (with filter)
  - Enter manual response for `ai_support_ticket_system`
- API showcase
- Gantt chart
- Open task instance, show log options
- Calendar view
- Shortcut g to switch between graph and grid view
- Plugins
    - Macro (format confidence)
    - External view (learn)
    - React app (toggle)
    - Game

## API showcase

The following flow shows how to interact with HITL via API as well as the new inference execution endpoint.

```shell
# trigger dag run
dag_id=ai_support_ticket_system

curl -X POST "localhost:8080/api/v2/dags/$dag_id/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{"logical_date": null}' | jq .

curl -X GET "localhost:8080/api/v2/dags/$dag_id/dagRuns" | jq .

# copy dag_run_id from output
dag_run_id=manual__2025-10-14T01:32:39.465322+00:00_ezNMD11d

# different window, wait for completion with 2-second polling interval
curl -X GET "http://localhost:8080/api/v2/dags/$dag_id/dagRuns/$dag_run_id/wait?interval=2" \
  -H "Accept: application/json"

# get HITL details
curl -X GET "localhost:8080/api/v2/dags/$dag_id/dagRuns/$dag_run_id/hitlDetails" | jq .

# copy task_id and map_index from output
task_id=review_ai_response
map_index=-1

# update HITL details
curl -X PATCH "localhost:8080/api/v2/dags/$dag_id/dagRuns/$dag_run_id/taskInstances/$task_id/$map_index/hitlDetails" \
  -H "Content-Type: application/json" \
  -d '{
    "chosen_options": ["Approve AI Response"],
    "params_input": {}
  }' | jq .
```
