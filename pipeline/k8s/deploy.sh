#!/bin/sh
# Publishes the code as ConfigMaps and applies the batch CronJob, the weekly training CronJob, and the app.
# The app needs the Secret mta-app-db (read-only PostgreSQL account) to exist first.
set -eu
cd "$(dirname "$0")/.."
find src -name __pycache__ -prune -exec rm -rf {} +
kubectl -n mta-pipeline create configmap mta-pipeline-code \
  --from-file=src/mta_pipeline/ --dry-run=client -o yaml | kubectl apply -f -
kubectl -n mta-pipeline create configmap mta-app-code \
  --from-file=app/streamlit_app.py --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/batch-elt.yaml -f k8s/train.yaml
if kubectl -n mta-pipeline get secret mta-app-db >/dev/null 2>&1; then
  kubectl apply -f k8s/app.yaml
  kubectl -n mta-pipeline rollout restart deployment/mta-app  # pick up a changed ConfigMap
else
  echo "Secret mta-app-db not found; skipping the app" >&2
fi
