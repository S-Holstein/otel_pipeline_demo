import time
# Wichtig: Die Imports ändern sich komplett für den Operaton-Client
from operaton.external_task.external_task import ExternalTask, TaskResult
from operaton.external_task.external_task_worker import ExternalTaskWorker

from opentelemetry import metrics, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# NEU: Imports für das Tracing Setup
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter


# 1. Erstellen Sie eine eindeutige Beschreibung für Ihre App
app_resource = Resource.create({"service.name": "operaton-python-worker"})


# ==========================================
# 1. OpenTelemetry & Collector Verbindung
# ==========================================
exporter = OTLPMetricExporter(endpoint="http://localhost:4317", insecure=True)
reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
provider = MeterProvider(metric_readers=[reader], resource=app_resource)
metrics.set_meter_provider(provider)

meter = metrics.get_meter("operaton.worker.demo")
task_counter = meter.create_counter(
    "processed_tasks_total",
    description="Gesamtanzahl der verarbeiteten Operaton-Tasks",
    unit="1"
)

# ==========================================
# 2. NEU: OpenTelemetry TRACING Setup
# ==========================================

trace_exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
trace_provider = TracerProvider(resource=app_resource)
trace_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
trace.set_tracer_provider(trace_provider)

# Tracer erstellen, um "Spans" (Zeitabschnitte) zu dokumentieren
tracer = trace.get_tracer("operaton.worker.demo")

# ==========================================
# 3. Operaton External Worker Logik
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
    activity_id = task.get_activity_id()  # Für besseren Kontext im Trace
    print(f"[WORKER] Hole Task ab: {task_id}")

    # NEU: Wir umschließen die Code-Ausführung mit einem OTel-Trace-Span
    with tracer.start_as_current_span("Execute-Operaton-Task") as main_span:
        # Metadaten an den Trace heften für die Root-Cause-Analysis
        main_span.set_attribute("operaton.task_id", str(task_id))
        main_span.set_attribute("operaton.activity_id", str(activity_id))

        # --- SCHRITT 1: PDF GENERIEREN ---
        # Wir starten einen Unter-Span (Sub-Span) auf dem Zeitstrahl
        with tracer.start_as_current_span("Generate-PDF"):
            time.sleep(0.1) # Simuliert schnelle lokale Arbeit

        # --- SCHRITT 2: DATENBANK ABFRAGE ---
        with tracer.start_as_current_span("Fetch-Database-Records"):
            time.sleep(0.4) # Simuliert normale DB-Laufzeit

        # --- SCHRITT 3: E-MAIL SENDEN (Hier bauen wir das Problem ein) ---
        with tracer.start_as_current_span("Send-Invoice-Email") as email_span:
            try:
                # Simulierter Fehler genau in diesem Schritt
                if int(time.time()) % 2 == 0:
                    raise ConnectionError("SMTP Server antwortet nicht (Timeout nach 1.5s)!")

                    time.sleep(0.2)  # Normaler Mail-Versand
            except Exception as e:
                email_span.record_exception(e)
                email_span.set_status(trace.StatusCode.ERROR)
                #email_span.set_attribute("error", True)

                # Wir leiten den Fehler nach oben weiter, damit der Haupt-Span auch fehlschlägt
                # main_span.set_status(trace.StatusCode.ERROR)

                task_counter.add(1, {"task_topic": "invoice-processing", "status": "error"})
                return task.failure(
                    error_message=str(e),
                    error_details="Fehler beim SMTP-Versand.",
                    max_retries=0, retry_timeout=0
                )

        # Erfolgsfall
        task_counter.add(1, {"task_topic": "invoice-processing", "status": "success"})
        main_span.set_status(trace.StatusCode.OK)
        print(f"[WORKER] Task {task_id} erfolgreich verarbeitet!")
        return task.complete()


        # except Exception as e:
        #     # WICHTIG: OTel erfasst den Fehler UND den Stacktrace automatisch im aktuellen Span
        #     span.record_exception(e)
        #     span.set_status(trace.StatusCode.ERROR, str(e))
        #
        #     # 3. EXTRA-BOOST FÜR JAEGER: Ein explizites Error-Attribut setzen
        #     # Das zwingt Jaeger visuell in den Fehlermodus!
        #     span.set_attribute("error", True)
        #
        #     # Fehler-Metrik für Prometheus hochzählen
        #     task_counter.add(1, {"task_topic": "invoice-processing", "status": "error"})
        #     print(f"[WORKER] Task {task_id} fehlgeschlagen: {str(e)}")
        #
        #     # Task in Operaton als Fehlschlag melden
        #     return task.failure(
        #         error_message=str(e),
        #         error_details="Details im OTel Trace auffindbar.",
        #         max_retries=0,  # 0 Wiederholungen, damit direkt ein Incident in Operaton entsteht
        #         retry_timeout=0  # Wartezeit bis zum Wiederholungsversuch in Millisekunden
        #     )


# Initialisierung und Start des Polling-Workers
if __name__ == "__main__":
    print("[WORKER] Starte Operaton Python-Worker. Lausche auf 'invoice-processing'...")

    # Erstellt den Worker und abonniert direkt das Topic
    ExternalTaskWorker(worker_id="python-otel-worker-1", config=config).subscribe(
        "invoice-processing",
        handle_task
    )
