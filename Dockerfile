# Astro Runtime includes the following pre-installed providers packages: https://www.astronomer.io/docs/astro/runtime-image-architecture#provider-packages
FROM astrocrpublic.azurecr.io/runtime:3.2-3

USER root

COPY plugins/cluster_policies plugins/cluster_policies
RUN pip install --no-cache-dir ./plugins/cluster_policies

USER astro
