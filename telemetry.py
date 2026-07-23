import logging
from opentelemetry import metrics, trace
from opentelemetry.sdk.resources import Resource

# Metrics Komponenten
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# Tracing Komponenten
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Logging Komponenten
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter


def init_telemetry(service_name: str, endpoint: str = "http://localhost:4317") -> None:
    """Initialisiert das komplette OpenTelemetry Setup (Logs, Traces, Metrics)."""

    # Gemeinsame Ressource definieren
    app_resource = Resource.create({"service.name": service_name})

    # 1. SETUP: OpenTelemetry Logging
    logger_provider = LoggerProvider(resource=app_resource)
    set_logger_provider(logger_provider)

    log_exporter = OTLPLogExporter(endpoint=endpoint, insecure=True)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

    # Python Standard-Logging mit OTel verknüpfen
    handler = LoggingHandler(logger_provider=logger_provider)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)

    # 2. SETUP: OpenTelemetry Metriken
    metric_exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
    reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=1000)
    meter_provider = MeterProvider(metric_readers=[reader], resource=app_resource)
    metrics.set_meter_provider(meter_provider)

    # 3. SETUP: OpenTelemetry Tracing
    trace_exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    trace_provider = TracerProvider(resource=app_resource)
    trace_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
    trace.set_tracer_provider(trace_provider)
