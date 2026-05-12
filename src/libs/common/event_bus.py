"""
Event Bus implementation using Kafka.
Supports both publishing and subscribing with async patterns,
dead letter queues, and retry logic.
"""
import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    Any, Callable, Dict, List, Optional, Type, TypeVar,
    Awaitable, AsyncIterator, Generic,
)
from abc import ABC, abstractmethod
import structlog
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential

logger = structlog.get_logger(__name__)

T = TypeVar('T')


@dataclass(frozen=True)
class DomainEvent(ABC):
    """Base class for all domain events."""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    aggregate_id: str = ""
    event_type: str = ""
    version: int = 0
    occurred_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, str] = field(default_factory=dict)
    user_id: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize event to dictionary."""
        return {
            "event_id": self.event_id,
            "aggregate_id": self.aggregate_id,
            "event_type": self.event_type,
            "version": self.version,
            "occurred_at": self.occurred_at.isoformat(),
            "metadata": self.metadata,
            "user_id": self.user_id,
            "data": self._serialize_data(),
        }
    
    @abstractmethod
    def _serialize_data(self) -> Dict[str, Any]:
        """Serialize event-specific data."""
        pass
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DomainEvent':
        """Deserialize event from dictionary."""
        pass


class EventPublisher:
    """Publishes domain events to Kafka topics."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        client_id: str = "taskforge-publisher",
        acks: str = "all",
        compression_type: str = "snappy",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.acks = acks
        self.compression_type = compression_type
        self._producer: Optional[AIOKafkaProducer] = None
        self._lock = asyncio.Lock()
        self._metrics = {
            "events_published": 0,
            "publish_errors": 0,
            "last_publish_time": None,
        }
    
    async def start(self) -> None:
        """Initialize Kafka producer connection."""
        async with self._lock:
            if self._producer is not None:
                return
            
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                acks=self.acks,
                compression_type=self.compression_type,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                max_in_flight_requests_per_connection=5,
                retry_backoff_ms=100,
                enable_idempotence=True,
            )
            await self._producer.start()
            logger.info("event_publisher.started", client_id=self.client_id)
    
    async def stop(self) -> None:
        """Gracefully stop the producer."""
        if self._producer:
            await self._producer.stop()
            self._producer = None
            logger.info("event_publisher.stopped")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def publish(
        self,
        topic: str,
        event: DomainEvent,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Publish a domain event to a Kafka topic.
        
        Args:
            topic: Kafka topic name
            event: Domain event to publish
            key: Partition key (typically aggregate_id)
            headers: Optional Kafka headers
        
        Returns:
            True if published successfully
        """
        if self._producer is None:
            raise RuntimeError("Publisher not started. Call start() first.")
        
        try:
            event_data = event.to_dict()
            kafka_key = key or event.aggregate_id
            
            # Add trace headers for observability
            kafka_headers = [
                ("event_type", event.event_type.encode()),
                ("event_version", str(event.version).encode()),
                ("correlation_id", str(uuid.uuid4()).encode()),
            ]
            if headers:
                kafka_headers.extend([(k, v.encode()) for k, v in headers.items()])
            
            future = await self._producer.send(
                topic=topic,
                value=event_data,
                key=kafka_key,
                headers=kafka_headers,
            )
            
            # Wait for confirmation
            record_metadata = await future
            
            self._metrics["events_published"] += 1
            self._metrics["last_publish_time"] = datetime.utcnow()
            
            logger.debug(
                "event.published",
                topic=topic,
                event_type=event.event_type,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
            )
            
            return True
            
        except KafkaError as e:
            self._metrics["publish_errors"] += 1
            logger.error("event.publish_failed", topic=topic, error=str(e))
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get publisher metrics."""
        return self._metrics.copy()


class EventSubscriber(Generic[T]):
    """Subscribes to domain events from Kafka topics."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        event_class: Type[T],
        handler: Callable[[T], Awaitable[None]],
        auto_offset_reset: str = "earliest",
        max_poll_records: int = 100,
        enable_auto_commit: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.event_class = event_class
        self.handler = handler
        self.auto_offset_reset = auto_offset_reset
        self.max_poll_records = max_poll_records
        self.enable_auto_commit = enable_auto_commit
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._dlq_producer: Optional[AIOKafkaProducer] = None
        self._metrics = {
            "events_consumed": 0,
            "handler_errors": 0,
            "dlq_events": 0,
        }
    
    async def start(self) -> None:
        """Start consuming events."""
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            max_poll_records=self.max_poll_records,
            enable_auto_commit=self.enable_auto_commit,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        )
        
        # DLQ producer for failed events
        self._dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        
        await self._consumer.start()
        await self._dlq_producer.start()
        
        self._running = True
        logger.info(
            "event_subscriber.started",
            topic=self.topic,
            group_id=self.group_id,
        )
    
    async def stop(self) -> None:
        """Stop consuming events."""
        self._running = False
        if self._consumer:
            await self._consumer.stop()
        if self._dlq_producer:
            await self._dlq_producer.stop()
        logger.info("event_subscriber.stopped", topic=self.topic)
    
    async def consume(self) -> None:
        """Main consumption loop with error handling."""
        if not self._consumer:
            raise RuntimeError("Subscriber not started. Call start() first.")
        
        async for message in self._consumer:
            if not self._running:
                break
            
            try:
                event_data = message.value
                event = self.event_class.from_dict(event_data)
                
                await self.handler(event)
                self._metrics["events_consumed"] += 1
                
                # Manual commit after successful processing
                if not self.enable_auto_commit:
                    await self._consumer.commit()
                
            except Exception as e:
                self._metrics["handler_errors"] += 1
                logger.error(
                    "event.handler_error",
                    topic=self.topic,
                    error=str(e),
                    offset=message.offset,
                )
                
                # Send to dead letter queue
                await self._send_to_dlq(message.value, str(e))
    
    async def _send_to_dlq(self, event_data: Dict[str, Any], error: str) -> None:
        """Send failed event to dead letter queue."""
        try:
            dlq_event = {
                "original_event": event_data,
                "error": error,
                "failed_at": datetime.utcnow().isoformat(),
                "topic": self.topic,
                "consumer_group": self.group_id,
            }
            
            await self._dlq_producer.send(
                topic=f"{self.topic}.dlq",
                value=dlq_event,
            )
            self._metrics["dlq_events"] += 1
            
        except Exception as dlq_error:
            logger.error("dlq.send_failed", error=str(dlq_error))
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get subscriber metrics."""
        return self._metrics.copy()