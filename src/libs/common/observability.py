"""
OpenTelemetry observability setup for distributed tracing and metrics.
"""
import os
from typing import Optional
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.instrumentation.grpc import GrpcAioInstrumentorServer, GrpcAioInstrumentorClient
import structlog

logger = structlog.get_logger(__name__)


class ObservabilityManager:
    """Manages OpenTelemetry setup for the platform."""
    
    _initialized = False
    _tracer_provider: Optional[TracerProvider] = None
    _meter_provider: Optional[MeterProvider] = None
    
    @classmethod
    def initialize(
        cls,
        service_name: str,
        otlp_endpoint: Optional[str] = None,
    ) -> None:
        """Initialize OpenTelemetry instrumentation."""
        if cls._initialized:
            return
        
        otlp_endpoint = otlp_endpoint or os.getenv(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "http://localhost:4317",
        )
        
        # Create resource
        resource = Resource.create({
            SERVICE_NAME: service_name,
            "environment": os.getenv("ENVIRONMENT", "development"),
        })
        
        # Setup tracing
        trace_exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
        span_processor = BatchSpanProcessor(trace_exporter)
        
        cls._tracer_provider = TracerProvider(resource=resource)
        cls._tracer_provider.add_span_processor(span_processor)
        trace.set_tracer_provider(cls._tracer_provider)
        
        # Setup metrics
        metric_exporter = OTLPMetricExporter(endpoint=otlp_endpoint)
        metric_reader = PeriodicExportingMetricReader(metric_exporter)
        
        cls._meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader],
        )
        metrics.set_meter_provider(cls._meter_provider)
        
        # Instrument gRPC
        GrpcAioInstrumentorServer().instrument()
        GrpcAioInstrumentorClient().instrument()
        
        cls._initialized = True
        logger.info(
            "observability.initialized",
            service_name=service_name,
            endpoint=otlp_endpoint,
        )
    
    @classmethod
    def get_tracer(cls, name: str) -> trace.Tracer:
        """Get a tracer instance."""
        if not cls._initialized:
            cls.initialize(name)
        return trace.get_tracer(name)
    
    @classmethod
    def get_meter(cls, name: str) -> metrics.Meter:
        """Get a meter instance."""
        if not cls._initialized:
            cls.initialize(name)
        return metrics.get_meter(name)
    
    @classmethod
    def shutdown(cls) -> None:
        """Shutdown observability providers."""
        if cls._tracer_provider:
            cls._tracer_provider.shutdown()
        if cls._meter_provider:
            cls._meter_provider.shutdown()
        cls._initialized = False
        logger.info("observability.shutdown")