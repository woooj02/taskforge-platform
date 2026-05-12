"""
Task Service entry point.
Initializes database, event store, gRPC server, and Kafka consumers.
"""
import asyncio
import os
import signal
from typing import Optional
import click
import structlog
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text

from libs.common.event_bus import EventPublisher, EventSubscriber
from libs.common.observability import ObservabilityManager
from .domain.events import (
    TaskCreatedEvent, TaskUpdatedEvent, TaskAssignedEvent,
    TaskStatusChangedEvent, TaskPriorityChangedEvent, TaskDeletedEvent,
)
from .infrastructure.event_store import PostgresEventStore, EVENT_STORE_SCHEMA
from .infrastructure.repository import TaskRepository
from .infrastructure.grpc_server import TaskServiceServicer, TaskGrpcServer

logger = structlog.get_logger(__name__)

# Event registry - maps event types to classes
EVENT_REGISTRY = {
    "task.created": TaskCreatedEvent,
    "task.updated": TaskUpdatedEvent,
    "task.assigned": TaskAssignedEvent,
    "task.status_changed": TaskStatusChangedEvent,
    "task.priority_changed": TaskPriorityChangedEvent,
    "task.deleted": TaskDeletedEvent,
}


class TaskServiceApp:
    """Main Task Service application."""
    
    def __init__(self):
        self.db_url = os.getenv(
            "DATABASE_URL",
            "postgresql+asyncpg://taskforge:taskforge_secret@localhost:5432/taskforge",
        )
        self.kafka_brokers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.grpc_port = int(os.getenv("TASK_SERVICE_PORT", "50051"))
        
        self._engine = None
        self._session_factory = None
        self._event_store: Optional[PostgresEventStore] = None
        self._event_publisher: Optional[EventPublisher] = None
        self._grpc_server: Optional[TaskGrpcServer] = None
        self._running = False
    
    async def initialize(self) -> None:
        """Initialize all service components."""
        logger.info("task_service.initializing")
        
        # Initialize observability
        ObservabilityManager.initialize(
            service_name="task-service",
        )
        
        # Initialize database
        self._engine = create_async_engine(
            self.db_url,
            echo=False,
            pool_size=20,
            max_overflow=10,
        )
        
        # Create session factory
        self._session_factory = async_sessionmaker(
            self._engine,
            expire_on_commit=False,
        )
        
        # Create event store schema
        await self._create_schema()
        
        # Initialize event publisher
        self._event_publisher = EventPublisher(
            bootstrap_servers=self.kafka_brokers,
            client_id="task-service-publisher",
        )
        await self._event_publisher.start()
        
        # Initialize gRPC server
        async with self._session_factory() as session:
            event_store = PostgresEventStore(session, EVENT_REGISTRY)
            servicer = TaskServiceServicer(event_store, self._event_publisher)
            self._grpc_server = TaskGrpcServer(
                servicer=servicer,
                port=self.grpc_port,
            )
        
        logger.info("task_service.initialized")
    
    async def _create_schema(self) -> None:
        """Create database schema if not exists."""
        async with self._engine.begin() as conn:
            await conn.execute(text(EVENT_STORE_SCHEMA))
        logger.info("task_service.schema_created")
    
    async def start(self) -> None:
        """Start the service."""
        await self.initialize()
        
        self._running = True
        
        # Start gRPC server
        grpc_task = asyncio.create_task(self._grpc_server.serve_forever())
        
        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(self.shutdown())
            )
        
        logger.info(
            "task_service.started",
            grpc_port=self.grpc_port,
        )
        
        try:
            await grpc_task
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()
    
    async def shutdown(self) -> None:
        """Graceful shutdown."""
        if not self._running:
            return
        
        logger.info("task_service.shutting_down")
        self._running = False
        
        # Stop gRPC server
        if self._grpc_server:
            await self._grpc_server.stop()
        
        # Stop event publisher
        if self._event_publisher:
            await self._event_publisher.stop()
        
        # Close database connections
        if self._engine:
            await self._engine.dispose()
        
        # Shutdown observability
        ObservabilityManager.shutdown()
        
        logger.info("task_service.stopped")


@click.group()
def cli():
    """Task Service CLI."""
    pass


@cli.command()
@click.option("--port", default=50051, help="gRPC server port")
def serve(port: int):
    """Start Task Service."""
    os.environ["TASK_SERVICE_PORT"] = str(port)
    app = TaskServiceApp()
    asyncio.run(app.start())


@cli.command()
def migrate():
    """Create database schema."""
    async def _migrate():
        engine = create_async_engine(
            os.getenv("DATABASE_URL", "postgresql+asyncpg://taskforge:taskforge_secret@localhost:5432/taskforge"),
        )
        async with engine.begin() as conn:
            await conn.execute(text(EVENT_STORE_SCHEMA))
        await engine.dispose()
        logger.info("Migration complete")
    
    asyncio.run(_migrate())


if __name__ == "__main__":
    cli()