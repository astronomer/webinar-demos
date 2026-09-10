FROM astrocrpublic.azurecr.io/runtime:3.3-7

# pyspark bundles spark-submit under its package dir but not on PATH. The SparkSubmitHook
# invokes a bare "spark-submit", so wrap the bundled one (with SPARK_HOME set) onto PATH.
USER root
RUN SPARK_HOME="$(python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')" && \
    printf '#!/usr/bin/env bash\nexport SPARK_HOME=%s\nexec "$SPARK_HOME/bin/spark-submit" "$@"\n' "$SPARK_HOME" \
      > /usr/local/bin/spark-submit && \
    chmod +x /usr/local/bin/spark-submit
USER astro
