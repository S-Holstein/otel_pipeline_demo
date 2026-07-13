import time
# Wichtig: Die Imports ändern sich komplett für den Operaton-Client
from operaton.external_task.external_task import ExternalTask, TaskResult
from operaton.external_task.external_task_worker import ExternalTaskWorker

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# ==========================================
# 1. OpenTelemetry & Collector Verbindung
# ==========================================
exporter = OTLPMetricExporter(endpoint="http://localhost:4317", insecure=True)
reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
provider = MeterProvider(metric_readers=[reader])
metrics.set_meter_provider(provider)

meter = metrics.get_meter("operaton.worker.demo")
task_counter = meter.create_counter(
    "processed_tasks_total",
    description="Gesamtanzahl der verarbeiteten Operaton-Tasks",
    unit="1"
)

# ==========================================
# 2. Operaton External Worker Logik
# ==========================================
# Konfiguration für den Worker (URL zeigt standardmäßig auf die Engine-REST)
config = {
    "baseUrl": "http://localhost:8080/engine-rest",
    "maxTasks": 1,
    "lockDuration": 10000,
    "asyncResponseTimeout": 5000,
    "sleepSeconds": 2  # Polling-Frequenz bei Leerlauf
}


def handle_task(task: ExternalTask) -> TaskResult:
    task_id = task.get_task_id()
    print(f"[WORKER] Hole Task ab: {task_id}")

    # Simulierte Rechnungsverarbeitung
    time.sleep(0.5)

    # OpenTelemetry Metrik inkrementieren
    task_counter.add(1, {
        "task_topic": "invoice-processing",
        "status": "success"
    })

    print(f"[WORKER] Task {task_id} erfolgreich verarbeitet und Metrik erfasst!")

    # Rückgabewert muss hier ein valides TaskResult (complete) sein
    return task.complete()


# Initialisierung und Start des Polling-Workers
if __name__ == "__main__":
    print("[WORKER] Starte Operaton Python-Worker. Lausche auf 'invoice-processing'...")

    # Erstellt den Worker und abonniert direkt das Topic
    ExternalTaskWorker(worker_id="python-otel-worker-1", config=config).subscribe(
        "invoice-processing",
        handle_task
    )
