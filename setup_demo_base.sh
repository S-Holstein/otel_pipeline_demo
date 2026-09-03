#!/bin/bash

set -e

echo "Suche nach laufenden Python-Workern und beende diese..."
# '|| true' verhindert, dass das Skript abbricht, falls keine Worker liefen.
pkill -f "python3 -m src.worker.worker" || true
pkill -f "python3 -m src.worker.legacy_worker" || true
pkill -f "python3 -m src.worker.item_worker" || true
sleep 1

########################################################

echo "Starte Docker-Container neu..."
sudo docker compose down --volumes --remove-orphans
sudo docker volume prune -f
sudo docker compose up -d

echo "Warte auf die Operaton Engine (http://localhost:8080)..."
set +e
until $(curl --output /dev/null --silent --head --fail http://localhost:8080/engine-rest/version); do
    printf '.'
    sleep 2
done
set -e
echo -e "\nEngine ist bereit!"

#############################################################

echo "Führe Deployment aus..."
curl -w "\n" \
     -H "Accept: application/json" \
     -F "deployment-name=vorteil-otel-demo" \
     -F "enable-duplicate-filtering=true" \
     -F "deploy-changed-only=true" \
     -F "process.bpmn=@./workflow/process.bpmn" \
     http://localhost:8080/engine-rest/deployment/create

sleep 1

echo "Starte 50 Prozessinstanzen..."
for i in {1..50}; do
    curl -s -X POST http://localhost:8080/engine-rest/process-definition/key/vorteil-otel-demo-process/start \
         -H "Content-Type: application/json" \
         -d "{}"
    echo " -> Instanz $i gestartet"
done

#############################################################

echo "Starte Operaton Python Worker..."

python3 -m src.worker.worker > ./logs/worker.log 2>&1 &
echo " -> worker.py im Hintergrund gestartet (Logs in worker.log)"

python3 -m src.worker.legacy_worker > ./logs/legacy_worker.log 2>&1 &
echo " -> legacy_worker.py im Hintergrund gestartet (Logs in legacy_worker.log)"

python3 -m src.worker.item_worker > ./logs/item_worker.log 2>&1 &
echo " -> item_worker.py im Hintergrund gestartet (Logs in item_worker.log)"

sleep 2

echo "Done!"