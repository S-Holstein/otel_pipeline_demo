import time
import logging

# WICHTIG: Telemetry sofort als Erstes initialisieren!
from src.otel_setup.telemetry import init_telemetry
SERVICE_NAME="operaton-invoice-item-worker"
init_telemetry(service_name=SERVICE_NAME)

# Operaton SDK Imports
from operaton.external_task.external_task import ExternalTask, TaskResult
from operaton.external_task.external_task_worker import ExternalTaskWorker

# OTel API Imports
from opentelemetry import metrics, trace
from opentelemetry.propagate import extract

# Logger, Meter und Tracer lokal für dieses Modul holen
logger = logging.getLogger(__name__)
meter = metrics.get_meter("operaton.worker.demo")
tracer = trace.get_tracer("operaton.worker.demo")

task_counter = meter.create_counter(
    "processed_invoice_items_total",
    description="Gesamtanzahl der verarbeiteten Rechnungspositionen (Loop-Instanzen)",
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
    # Die Multi-Instance-Loop-Charakteristik im BPMN setzt pro Schleifen-
    # durchlauf die Element-Variable "invoiceItem" (camunda:elementVariable).
    invoice_item = task.get_variable("invoiceItem")

    logger.info(
        "Loop-Task für Rechnungsposition abgeholt",
        extra={
            "operaton.task_id": task_id,
            "operaton.process_instance_id": instance_id,
            "invoice.item": invoice_item
        }
    )

    # Trace-Context, den Worker 1 über die Prozessvariable "otelTraceContext"
    # mitgegeben hat, extrahieren. Ohne diesen Schritt würde jeder Aufruf
    # von handle_task eine eigene, isolierte Trace-ID erzeugen und nicht
    # unter dem Trace von Worker 1 in Jaeger auftauchen.
    trace_parent = task.get_variable("otelTraceContext")
    trace_carrier = {"traceparent": trace_parent} if trace_parent else {}
    parent_context = extract(trace_carrier)

    with tracer.start_as_current_span("Execute-Invoice-Item-Task", context=parent_context) as main_span:
        main_span.set_attribute("operaton.task_id", str(task_id))
        main_span.set_attribute("operaton.activity_id", str(activity_id))
        main_span.set_attribute("operaton.process_instance_id", str(instance_id))
        main_span.set_attribute("invoice.item", str(invoice_item))

        try:
            with tracer.start_as_current_span("Process-Invoice-Item"):
                logger.info("Verarbeite Rechnungsposition %s ...", invoice_item)
                time.sleep(0.2)

            task_counter.add(1, {"task_topic": "invoice-item-processing", "status": "success"})
            main_span.set_status(trace.StatusCode.OK)

            logger.info(
                "Rechnungsposition erfolgreich verarbeitet.",
                extra={"operaton.status": "success", "invoice.item": invoice_item}
            )
            return task.complete()

        except Exception as e:
            main_span.record_exception(e)
            main_span.set_status(trace.StatusCode.ERROR)

            task_counter.add(1, {"task_topic": "invoice-item-processing", "status": "error"})

            logger.exception(
                "Verarbeitung der Rechnungsposition fehlgeschlagen",
                extra={
                    "operaton.status": "error",
                    "operaton.task_id": task_id,
                    "invoice.item": invoice_item,
                    "error.type": type(e).__name__
                }
            )

            return task.failure(
                error_message=str(e),
                error_details="Fehler bei der Verarbeitung einer Rechnungsposition. Details im OTel Trace.",
                max_retries=0, retry_timeout=0
            )


if __name__ == "__main__":
    logger.info(
        "Starte Operaton Python-Worker. Abonniere Topic: 'invoice-item-processing'",
        extra={"worker.config.baseUrl": config["baseUrl"], "worker.maxTasks": config["maxTasks"]}
    )

    ExternalTaskWorker(worker_id="python-otel-worker-2", config=config).subscribe(
        "invoice-item-processing",
        handle_task
    )