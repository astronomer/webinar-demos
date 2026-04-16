#!/usr/bin/env bash
set -euo pipefail

echo "=== Airflow Config Lint: Check AF2 config compatibility with AF3 ==="

echo ""
echo "Step 1: Exporting current Airflow configuration..."
astro dev run config list | awk '/^\[core\]/ {found=1} found' > airflow.cfg
echo "  -> airflow.cfg written ($(wc -l < airflow.cfg) lines)"

echo ""
echo "Step 2: Finding scheduler container..."
SCHEDULER_ID=$(docker ps --filter "name=scheduler" --format "{{.ID}}" | head -n1)
if [[ -z "$SCHEDULER_ID" ]]; then
  echo "ERROR: No running scheduler container found. Is 'astro dev start' running?" >&2
  rm -f airflow.cfg
  exit 1
fi
echo "  -> Scheduler container: $SCHEDULER_ID"

echo ""
echo "Step 3: Copying airflow.cfg into scheduler container..."
docker cp airflow.cfg "$SCHEDULER_ID":/usr/local/airflow/airflow.cfg

echo ""
echo "Step 4: Running 'airflow config lint' inside the container..."
echo "=========================================="
docker exec "$SCHEDULER_ID" airflow config lint
echo "=========================================="

rm -f airflow.cfg
echo ""
echo "Done. Review the output above for any configuration you are manually"
echo "overriding as an environment variable and make the necessary changes."
echo "You can ignore flagged variables that you are NOT manually overriding —"
echo "those will be updated automatically."
