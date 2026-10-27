import time
import logging

# WICHTIG: Telemetry sofort als Erstes initialisieren!
from src.otel_setup.telemetry import init_telemetry
SERVICE_NAME="operaton-python-worker"
init_telemetry(service_name=SERVICE_NAME)

# Operaton SDK Imports
from operaton.external_task.external_task import ExternalTask, TaskResult
from operaton.external_task.external_task_worker import ExternalTaskWorker

# OTel API Imports
from opentelemetry import metrics, trace
from opentelemetry.propagate import inject
from opentelemetry.trace import set_span_in_context
from src.test_module.pdf_service import generate_pdf_document

# Logger, Meter und Tracer lokal für dieses Modul holen
logger = logging.getLogger(__name__)
meter = metrics.get_meter("operaton.worker.demo")
tracer = trace.get_tracer("operaton.worker.demo")

task_counter = meter.create_counter(
    "processed_tasks_total",
    description="Gesamtanzahl der verarbeiteten Operaton-Tasks",
    unit="1"
)


# Worker Konfiguration
config = {
    # "baseUrl": "http://localhost:8080/engine-rest",
    "baseUrl": "http://docker.internal",
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
    instance_id = task.get_process_instance_id()
    print(task_id)
    logger.info(
        "Task erfolgreich vom Operaton-Server abgeholt",
        extra={"operaton.task_id": task_id, "operaton.process_instance_id": instance_id}
    )

    with tracer.start_as_current_span("Execute-Operaton-Task") as main_span:
        main_span.set_attribute("operaton.task_id", str(task_id))
        main_span.set_attribute("operaton.activity_id", str(activity_id))
        main_span.set_attribute("operaton.process_instance_id", str(instance_id))

        # Trace-Context des Haupt-Spans in einen Carrier
        # injizieren, damit der nachgelagerte Multi-Instance-Worker
        # (invoice-item-processing) seine Spans als Kinder dieses Spans
        # anlegen kann. Ohne das würde jeder Worker-Prozess seine eigene,
        # isolierte Trace-ID erzeugen.
        trace_carrier = {}
        inject(trace_carrier, context=set_span_in_context(main_span))
        trace_parent = trace_carrier.get("traceparent")

        generate_pdf_document()

        with tracer.start_as_current_span("Fetch-Database-Records"):
            logger.info("Rufe Kundendaten aus der PostgreSQL-Datenbank ab.")
            time.sleep(0.4)

        with tracer.start_as_current_span("Send-Invoice-Email") as email_span:
            try:
                logger.info("Verbinde mit SMTP-Server für den E-Mail-Versand...")

                if int(time.time()) % 2 == 0:
                    raise ConnectionError("SMTP Server antwortet nicht (Timeout nach 1.5s)!")
                time.sleep(0.2)

                # Metrik
                task_counter.add(1, {"task_topic": "invoice-processing", "status": "success"})
                main_span.set_status(trace.StatusCode.OK)

                # Liste von Rechnungspositionen ermitteln, die im
                # nachfolgenden Multi-Instance-Task ("invoice-item-processing")
                # jeweils einzeln verarbeitet werden sollen.
                invoice_items = fetch_invoice_items(instance_id)
                main_span.set_attribute("invoice.item_count", len(invoice_items))

                logger.info(
                    "Task erfolgreich verarbeitet und E-Mail versendet.",
                    extra={
                        "operaton.status": "success",
                        "invoice.type": "standard",
                        "invoice.item_count": len(invoice_items)
                    }
                )

                return task.complete(global_variables={
                    "invoiceItems": invoice_items,
                    "otelTraceContext": trace_parent            # hier trace id für parent-child Verbindung übergeben
                })

            except Exception as e:
                email_span.record_exception(e)
                email_span.set_status(trace.StatusCode.ERROR)

                # Metrik
                task_counter.add(1,
                                 {"task_topic": "invoice-processing", "status": "error"},
                                 # context=get_current()
                                 )

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


def fetch_invoice_items(instance_id) -> list:
    """
    Platzhalter: liefert die Liste der Rechnungspositionen, die im
    nachgelagerten Multi-Instance-Task einzeln abgearbeitet werden.
    """
    return [f"INV-{instance_id}-{i:03d}" for i in range(1, 4)]


if __name__ == "__main__":
    logger.info(
        "Starte Operaton Python-Worker. Abonniere Topic: 'invoice-processing'",
        extra={"worker.config.baseUrl": config["baseUrl"], "worker.maxTasks": config["maxTasks"]}
    )

    ExternalTaskWorker(worker_id="python-otel-worker-1", config=config).subscribe(
        "invoice-processing",
        handle_task
    )