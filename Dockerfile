FROM astrocrpublic.azurecr.io/runtime:3.3-11
RUN pip install --no-cache-dir --uploaded-prior-to=2026-10-19T00:00:00Z apache-airflow-providers-amazon==9.37.0 apache-airflow-providers-ssh==7.0.0
