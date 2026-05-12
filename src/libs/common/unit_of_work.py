"""
Unit of Work pattern for managing transactions across repositories.
Ensures atomicity for event-sourced aggregates.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Type, AsyncContextManager
import asyncio
import structlog

from .event_bus import DomainEvent, EventPublisher

logger = structlog.get_logger(__name__)


class AggregateRoot(ABC):
    """Base class for event-sourced aggregate roots."""
    
    def __init__(self, aggregate_id: str):
        self.aggregate_id = aggregate_id
        self._uncommitted_events: List[DomainEvent] = []
        self.version: int = 0
    
    @abstractmethod
    def apply_event(self, event: DomainEvent) -> None:
        """Apply an event to mutate state."""
        pass
    
    def raise_event(self, event: DomainEvent) -> None:
        """Record a new domain event."""
        event.aggregate_id = self.aggregate_id
        event.version = self.version + 1
        self._uncommitted_events.append(event)
        self.apply_event(event)
        self.version += 1
    
    def get_uncommitted_events(self) -> List[DomainEvent]:
        """Get all uncommitted events."""
        return self._uncommitted_events.copy()
    
    def mark_events_committed(self) -> None:
        """Mark all events as committed."""
        self._uncommitted_events.clear()
    
    @classmethod
    @abstractmethod
    def from_events(cls, events: List[DomainEvent]) -> 'AggregateRoot':
        """Reconstitute aggregate from event stream."""
        pass


class EventStore(ABC):
    """Abstract event store interface."""
    
    @abstractmethod
    async def save_events(
        self,
        aggregate_id: str,
        events: List[DomainEvent],
        expected_version: int,
    ) -> None:
        """Save events with optimistic concurrency control."""
        pass
    
    @abstractmethod
    async def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0,
    ) -> List[DomainEvent]:
        """Retrieve events for an aggregate."""
        pass
    
    @abstractmethod
    async def get_all_events(
        self,
        from_event_id: int = 0,
        limit: int = 100,
    ) -> List[DomainEvent]:
        """Retrieve all events (for projections)."""
        pass


class Repository(ABC, Generic[AggregateRoot]):
    """Abstract repository for aggregate roots."""
    
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
    
    @abstractmethod
    async def get_by_id(self, aggregate_id: str) -> Optional[AggregateRoot]:
        """Retrieve aggregate by ID."""
        pass
    
    @abstractmethod
    async def save(
        self,
        aggregate: AggregateRoot,
        expected_version: Optional[int] = None,
    ) -> None:
        """Save aggregate and publish events."""
        pass


class UnitOfWork(AsyncContextManager):
    """
    Unit of Work for managing transactions.
    Tracks changes across repositories and publishes events atomically.
    """
    
    def __init__(
        self,
        event_store: EventStore,
        event_publisher: EventPublisher,
    ):
        self.event_store = event_store
        self.event_publisher = event_publisher
        self._tracked_aggregates: Dict[str, AggregateRoot] = {}
        self._events_to_publish: List[DomainEvent] = []
        self._committed = False
        self._rolled_back = False
    
    def track_aggregate(self, aggregate: AggregateRoot) -> None:
        """Track an aggregate for changes."""
        self._tracked_aggregates[aggregate.aggregate_id] = aggregate
    
    async def commit(self) -> None:
        """
        Commit all tracked changes.
        Saves events to event store and publishes to event bus.
        """
        if self._committed or self._rolled_back:
            raise RuntimeError("Unit of Work already completed")
        
        try:
            # Save all aggregate events to event store
            for aggregate_id, aggregate in self._tracked_aggregates.items():
                events = aggregate.get_uncommitted_events()
                if events:
                    await self.event_store.save_events(
                        aggregate_id=aggregate_id,
                        events=events,
                        expected_version=aggregate.version - len(events),
                    )
                    self._events_to_publish.extend(events)
            
            # Publish all events (fire-and-forget with error handling)
            publish_tasks = []
            for event in self._events_to_publish:
                topic = self._get_topic_for_event(event)
                task = asyncio.create_task(
                    self._publish_with_retry(event, topic)
                )
                publish_tasks.append(task)
            
            # Wait for all publishes (with timeout)
            if publish_tasks:
                done, pending = await asyncio.wait(
                    publish_tasks,
                    timeout=10.0,
                )
                
                if pending:
                    logger.warning(
                        "uow.publish_timeout",
                        pending_count=len(pending),
                    )
                    for task in pending:
                        task.cancel()
            
            # Mark aggregates as committed
            for aggregate in self._tracked_aggregates.values():
                aggregate.mark_events_committed()
            
            self._committed = True
            logger.info(
                "uow.committed",
                aggregates=len(self._tracked_aggregates),
                events=len(self._events_to_publish),
            )
            
        except Exception as e:
            await self.rollback()
            raise
    
    async def rollback(self) -> None:
        """Rollback all tracked changes."""
        self._tracked_aggregates.clear()
        self._events_to_publish.clear()
        self._rolled_back = True
        logger.warning("uow.rolled_back")
    
    async def __aenter__(self) -> 'UnitOfWork':
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            await self.rollback()
        elif not self._committed and not self._rolled_back:
            await self.commit()
    
    async def _publish_with_retry(
        self,
        event: DomainEvent,
        topic: str,
        max_retries: int = 3,
    ) -> None:
        """Publish an event with retry logic."""
        for attempt in range(max_retries):
            try:
                await self.event_publisher.publish(
                    topic=topic,
                    event=event,
                )
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(
                        "uow.publish_failed",
                        event_id=event.event_id,
                        error=str(e),
                    )
                    raise
                await asyncio.sleep(2 ** attempt)
    
    def _get_topic_for_event(self, event: DomainEvent) -> str:
        """Determine topic from event type."""
        event_type = event.event_type
        
        if event_type.startswith("task."):
            return "task.events"
        elif event_type.startswith("workflow."):
            return "workflow.events"
        elif event_type.startswith("notification."):
            return "notifications"
        else:
            return "default.events"