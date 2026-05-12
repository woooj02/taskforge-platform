"""
PostgreSQL-backed event store implementation.
Supports optimistic concurrency and event streaming.
"""
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Type
import asyncpg
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import structlog

from libs.common.unit_of_work import EventStore, DomainEvent

logger = structlog.get_logger(__name__)


class PostgresEventStore(EventStore):
    """
    Event store backed by PostgreSQL.
    Uses append-only event storage with version checking.
    """
    
    def __init__(self, session: AsyncSession, event_registry: Dict[str, Type[DomainEvent]]):
        self.session = session
        self.event_registry = event_registry
    
    async def save_events(
        self,
        aggregate_id: str,
        events: List[DomainEvent],
        expected_version: int,
    ) -> None:
        """
        Save events with optimistic concurrency control.
        Uses transaction to ensure atomicity.
        """
        if not events:
            return
        
        # Check current version (optimistic concurrency)
        current_version = await self._get_current_version(aggregate_id)
        
        if current_version != expected_version:
            raise ValueError(
                f"Version conflict for aggregate '{aggregate_id}': "
                f"expected {expected_version}, got {current_version}"
            )
        
        # Insert all events in a single transaction
        for event in events:
            await self._insert_event(aggregate_id, event)
        
        logger.info(
            "events.saved",
            aggregate_id=aggregate_id,
            count=len(events),
            version_from=expected_version + 1,
            version_to=expected_version + len(events),
        )
    
    async def _get_current_version(self, aggregate_id: str) -> int:
        """Get current version of an aggregate."""
        query = text("""
            SELECT COALESCE(MAX(version), 0) as current_version
            FROM event_store
            WHERE aggregate_id = :aggregate_id
        """)
        
        result = await self.session.execute(
            query,
            {"aggregate_id": aggregate_id},
        )
        row = result.fetchone()
        return row.current_version if row else 0
    
    async def _insert_event(self, aggregate_id: str, event: DomainEvent) -> None:
        """Insert a single event into the store."""
        query = text("""
            INSERT INTO event_store (
                event_id, aggregate_id, event_type, event_data,
                version, user_id, occurred_at, metadata
            ) VALUES (
                :event_id, :aggregate_id, :event_type, :event_data,
                :version, :user_id, :occurred_at, :metadata
            )
        """)
        
        await self.session.execute(
            query,
            {
                "event_id": event.event_id,
                "aggregate_id": aggregate_id,
                "event_type": event.event_type,
                "event_data": json.dumps(event.to_dict()),
                "version": event.version,
                "user_id": event.user_id or "system",
                "occurred_at": event.occurred_at,
                "metadata": json.dumps(event.metadata),
            },
        )
    
    async def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0,
    ) -> List[DomainEvent]:
        """Retrieve events for an aggregate."""
        query = text("""
            SELECT event_id, aggregate_id, event_type, event_data,
                   version, user_id, occurred_at, metadata
            FROM event_store
            WHERE aggregate_id = :aggregate_id
              AND version > :from_version
            ORDER BY version ASC
        """)
        
        result = await self.session.execute(
            query,
            {"aggregate_id": aggregate_id, "from_version": from_version},
        )
        
        events = []
        for row in result:
            event = self._deserialize_event(dict(row._mapping))
            if event:
                events.append(event)
        
        return events
    
    async def get_all_events(
        self,
        from_event_id: int = 0,
        limit: int = 100,
    ) -> List[DomainEvent]:
        """Retrieve all events for building projections."""
        query = text("""
            SELECT event_id, aggregate_id, event_type, event_data,
                   version, user_id, occurred_at, metadata
            FROM event_store
            WHERE id > :from_event_id
            ORDER BY id ASC
            LIMIT :limit
        """)
        
        result = await self.session.execute(
            query,
            {"from_event_id": from_event_id, "limit": limit},
        )
        
        events = []
        for row in result:
            event = self._deserialize_event(dict(row._mapping))
            if event:
                events.append(event)
        
        return events
    
    def _deserialize_event(self, row: Dict[str, Any]) -> Optional[DomainEvent]:
        """Deserialize event from database row."""
        event_type = row.get("event_type", "")
        event_class = self.event_registry.get(event_type)
        
        if not event_class:
            logger.warning(
                "event.no_handler",
                event_type=event_type,
                event_id=row.get("event_id"),
            )
            return None
        
        try:
            event_data = row.get("event_data", "{}")
            if isinstance(event_data, str):
                event_data = json.loads(event_data)
            
            return event_class.from_dict(event_data)
        
        except Exception as e:
            logger.error(
                "event.deserialize_error",
                event_type=event_type,
                error=str(e),
            )
            return None


# SQL to create the event store table
EVENT_STORE_SCHEMA = """
CREATE TABLE IF NOT EXISTS event_store (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB NOT NULL,
    version INTEGER NOT NULL,
    user_id VARCHAR(100) NOT NULL DEFAULT 'system',
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_event_store_aggregate_id 
    ON event_store(aggregate_id);
CREATE INDEX IF NOT EXISTS idx_event_store_aggregate_version 
    ON event_store(aggregate_id, version);
CREATE INDEX IF NOT EXISTS idx_event_store_event_type 
    ON event_store(event_type);
CREATE INDEX IF NOT EXISTS idx_event_store_occurred_at 
    ON event_store(occurred_at);

-- Unique constraint to prevent version conflicts
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_store_aggregate_version_unique 
    ON event_store(aggregate_id, version);
"""