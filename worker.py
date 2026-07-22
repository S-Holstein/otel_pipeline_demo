import time
import logging

from operaton.external_task.external_task import ExternalTask, TaskResult
from operaton.external_task.external_task_worker import ExternalTaskWorker

from opentelemetry import metrics, trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.context import get_current

from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

# ==========================================
# SETUP: OpenTelemetry Logging & Ressourcen
# ==========================================
app_resource = Resource.create({"service.name": "operaton-python-worker"})

logger_provider = LoggerProvider(resource=app_resource)
set_logger_provider(logger_provider)

log_exporter = OTLPLogExporter(endpoint="http://localhost:4317", insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

handler = LoggingHandler(logger_provider=logger_provider)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)

# ==========================================
# SETUP: OpenTelemetry Metriken & Tracing
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

trace_exporter = OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
trace_provider = TracerProvider(resource=app_resource)
trace_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer("operaton.worker.demo")

# Worker Konfiguration
config = {
    "baseUrl": "http://localhost:8080/engine-rest",
    "maxTasks": 1,
    "lockDuration": 10000,
    "asyncResponseTimeout": 5000,
    "sleepSeconds": 2
}


# ==========================================
# WORKER LOGIK
# ==========================================
def handle_task(task: ExternalTask) -> TaskResult:
    task_id = task.get_task_id()
    activity_id = task.get_activity_id()
    # process_instance_id hilft enorm, um Logs einer bestimmten BPMN-Instanz zu filtern
    instance_id = task.get_process_instance_id()

    # Log 1: System-Ebene (außerhalb des Spans, zeigt das Polling-Ergebnis)
    logger.info(
        "Task erfolgreich vom Operaton-Server abgeholt",
        extra={"operaton.task_id": task_id, "operaton.process_instance_id": instance_id}
    )

    with tracer.start_as_current_span("Execute-Operaton-Task") as main_span:
        main_span.set_attribute("operaton.task_id", str(task_id))
        main_span.set_attribute("operaton.activity_id", str(activity_id))
        main_span.set_attribute("operaton.process_instance_id", str(instance_id))

        # --- SCHRITT 1: PDF GENERIEREN ---
        with tracer.start_as_current_span("Generate-PDF"):
            # Log 2: Fortschritts-Log (wird automatisch mit dem aktuellen Trace verknüpft)
            logger.info("Starte PDF-Generierung für die Rechnung.")
            time.sleep(0.1)

            # --- SCHRITT 2: DATENBANK ABFRAGE ---
        with tracer.start_as_current_span("Fetch-Database-Records"):
            logger.info("Rufe Kundendaten aus der PostgreSQL-Datenbank ab.")
            time.sleep(0.4)

            # --- SCHRITT 3: E-MAIL SENDEN ---
        with tracer.start_as_current_span("Send-Invoice-Email") as email_span:
            try:
                logger.info("Verbinde mit SMTP-Server für den E-Mail-Versand...")

                if int(time.time()) % 2 == 0:
                    raise ConnectionError("SMTP Server antwortet nicht (Timeout nach 1.5s)!")
                time.sleep(0.2)

                # Erfolgsfall
                task_counter.add(1, {"task_topic": "invoice-processing", "status": "success"})
                main_span.set_status(trace.StatusCode.OK)

                # Log 3: Erfolgs-Log mit Metadaten für Business-Metriken in Loki
                logger.info(
                    "Task erfolgreich verarbeitet und E-Mail versendet.",
                    extra={"operaton.status": "success", "invoice.type": "standard"}
                )
                return task.complete()

            except Exception as e:
                # Fehlerfall im Trace dokumentieren
                email_span.record_exception(e)
                email_span.set_status(trace.StatusCode.ERROR)

                task_counter.add(1,
                                 {"task_topic": "invoice-processing", "status": "error"},
                                 context=get_current()
                                 )

                # Log 4: Das wertvollste Log für Loki!
                # logger.exception fängt automatisch den kompletten Stacktrace ab.
                # 'extra' liefert Suchfilter für Loki, ohne den Index zu sprengen.
                logger.exception(
                    "Verarbeitung des Tasks abgebrochen wegen eines SMTP-Fehlers",
                    extra={
                        "operaton.status": "error",
                        "operaton.task_id": task_id,
                        "operaton.process_instance_id": instance_id,
                        "error.type": type(e).__name__
                    }
                )

                return task.failure(
                    error_message=str(e),
                    error_details="Fehler beim SMTP-Versand. Details im OTel Trace.",
                    max_retries=0, retry_timeout=0
                )


if __name__ == "__main__":
    # Log 5: Lifecycle-Log (Wichtig um Neustarts oder Crashes des gesamten Containers in Loki zu sehen)
    logger.info(
        "Starte Operaton Python-Worker. Abonniere Topic: 'invoice-processing'",
        extra={"worker.config.baseUrl": config["baseUrl"], "worker.maxTasks": config["maxTasks"]}
    )

    ExternalTaskWorker(worker_id="python-otel-worker-1", config=config).subscribe(
        "invoice-processing",
        handle_task
    )
