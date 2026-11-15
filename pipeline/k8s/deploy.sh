#!/bin/sh
# Publishes the pipeline code as the ConfigMap mta-pipeline-code and applies the CronJob.
set -eu
cd "$(dirname "$0")/.."
kubectl -n mta-pipeline create configmap mta-pipeline-code \
  --from-file=src/mta_pipeline/ --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/batch-elt.yaml
