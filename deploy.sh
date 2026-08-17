#!/bin/bash

echo "Cloud Run Job dağıtımı başlatılıyor: santral-data-artvin-job..."

gcloud run jobs deploy santral-data-artvin-job \
  --source . \
  --region us-central1 \
  --max-retries 3 \
  --memory 512Mi \
  --cpu 1 \
  --task-timeout 10m

echo "Dağıtım işlemi tamamlandı!"
