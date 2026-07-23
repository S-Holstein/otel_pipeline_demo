# pdf_service.py (Ausgelagerte Logik)
import time
import logging
from opentelemetry import trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("operaton.worker.pdf")

def generate_pdf_document():
    with tracer.start_as_current_span("Generate-PDF-Internal"):
        logger.info("Starte PDF-Generierung für die Rechnung in ausgelagerter Funktion...")
        time.sleep(0.2)
