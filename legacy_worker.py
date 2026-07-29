import time
import logging
# WICHTIG: Das native Prometheus SDK importieren
from prometheus_client import start_http_server, Counter

# Operaton SDK Imports (Identisch!)
from operaton.external_task.external_task import ExternalTask, TaskResult
from operaton.external_task.external_task_worker import ExternalTaskWorker
from pdf_service import generate_pdf_document

# Standard-Logging (Normales stdout/stderr, keine OTel-Verknüpfung)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==========================================
# PROMETHEUS SETUP (Der Legacy-Weg)
# ==========================================
# 1. Startet den /metrics HTTP-Endpunkt auf Port 9100 im Hintergrund
start_http_server(9100)

# 2. Metrik über das Prometheus SDK definieren
task_counter = Counter(
    "processed_tasks_total",  # Gleicher Name wie im OTel-Worker!
    "Gesamtanzahl der verarbeiteten Operaton-Tasks",
    ["task_topic", "status"]  # Labels werden hier als Liste definiert
)

config = {
    # "baseUrl": "http://localhost:8080/engine-rest",
    "baseUrl": "http://docker.internal",
    "maxTasks": 1,
    "lockDuration": 10000,
    "asyncResponseTimeout": 5000,
    "sleepSeconds": 2
}


# ==========================================
# WORKER LOGIK (Identischer Ablauf, kein Tracing!)
# ==========================================
def handle_task(task: ExternalTask) -> TaskResult:
    task_id = task.get_task_id()
    instance_id = task.get_process_instance_id()

    # Normales Logging ohne OTel-Kontext (Trace-IDs fehlen hier im Log-Output!)
    logger.info(f"Task {task_id} erfolgreich vom Operaton-Server abgeholt.")

    # KEIN Tracer-Span hier!
    generate_pdf_document()

    logger.info("Rufe Kundendaten aus der PostgreSQL-Datenbank ab.")
    time.sleep(0.4)

    try:
        logger.info("Verbinde mit SMTP-Server für den E-Mail-Versand...")

        if int(time.time()) % 2 == 0:
            raise ConnectionError("SMTP Server antwortet nicht (Timeout nach 1.5s)!")
        time.sleep(0.2)

        # Prometheus SDK Syntax zum Hochzählen mit Labels
        task_counter.labels(task_topic="invoice-processing", status="success").inc()

        logger.info("Task erfolgreich verarbeitet und E-Mail versendet.")
        return task.complete()

    except Exception as e:
        # Prometheus SDK Syntax für den Fehlerfall
        task_counter.labels(task_topic="invoice-processing", status="error").inc()

        logger.exception("Verarbeitung des Tasks abgebrochen wegen eines SMTP-Fehlers")
        return task.failure(
            error_message=str(e),
            error_details="Fehler beim SMTP-Versand. (Kein OTel Trace verfügbar!)",
            max_retries=0, retry_timeout=0
        )


if __name__ == "__main__":
    logger.info("Starte LEGACY Operaton Python-Worker auf Topic: 'invoice-processing'")

    # Eindeutige ID für den zweiten Worker, damit sie sich im Operaton-Server nicht beißen
    ExternalTaskWorker(worker_id="python-legacy-worker-2", config=config).subscribe(
        "invoice-processing",
        handle_task
    )
