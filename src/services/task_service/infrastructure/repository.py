"""
Task Repository implementation with event sourcing.
"""
from typing import Optional, List
import structlog

from libs.common.unit_of_work import Repository, EventStore
from libs.common.event_bus import DomainEvent
from ..domain.aggregates import TaskAggregate
from ..domain.exceptions import TaskNotFoundError, TaskVersionConflictError

logger = structlog.get_logger(__name__)


class TaskRepository(Repository[TaskAggregate]):
    """Repository for Task aggregates using event sourcing."""
    
    def __init__(self, event_store: EventStore):
        super().__init__(event_store)
        self._cache: dict = {}  # Simple in-memory cache
    
    async def get_by_id(self, aggregate_id: str) -> Optional[TaskAggregate]:
        """
        Retrieve task aggregate by ID.
        Reconstructs from event stream with caching.
        """
        # Check cache first
        if aggregate_id in self._cache:
            logger.debug("repository.cache_hit", task_id=aggregate_id)
            return self._cache[aggregate_id]
        
        # Load events from event store
        events = await self.event_store.get_events(aggregate_id)
        
        if not events:
            return None
        
        # Reconstruct aggregate from events
        try:
            task = TaskAggregate.from_events(events)
            
            # Check if deleted
            if task.is_deleted:
                return None
            
            # Cache for performance
            self._cache[aggregate_id] = task
            
            logger.debug(
                "repository.task_loaded",
                task_id=aggregate_id,
                events_count=len(events),
                version=task.version,
            )
            
            return task
            
        except Exception as e:
            logger.error(
                "repository.reconstruction_error",
                task_id=aggregate_id,
                error=str(e),
            )
            raise
    
    async def save(
        self,
        aggregate: TaskAggregate,
        expected_version: Optional[int] = None,
    ) -> None:
        """
        Save task aggregate changes.
        Uses optimistic concurrency control.
        """
        events = aggregate.get_uncommitted_events()
        
        if not events:
            return
        
        # Calculate expected version
        if expected_version is None:
            expected_version = aggregate.version - len(events)
        
        try:
            # Save to event store
            await self.event_store.save_events(
                aggregate_id=aggregate.aggregate_id,
                events=events,
                expected_version=expected_version,
            )
            
            # Update cache with latest state
            self._cache[aggregate.aggregate_id] = aggregate
            
            logger.info(
                "repository.task_saved",
                task_id=aggregate.aggregate_id,
                events_count=len(events),
                new_version=aggregate.version,
            )
            
        except ValueError as e:
            if "Version conflict" in str(e):
                raise TaskVersionConflictError(
                    task_id=aggregate.aggregate_id,
                    expected=expected_version,
                    actual=expected_version,  # Would need to fetch actual
                )
            raise
    
    async def list_tasks(
        self,
        status: Optional[str] = None,
        assignee_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[TaskAggregate]:
        """
        List tasks with filtering.
        In a real implementation, this would use a read model/projection.
        For event sourcing, we scan events to find active aggregates.
        """
        # This is a simplified implementation
        # In production, you'd use a separate read model (CQRS)
        all_events = await self.event_store.get_all_events(
            from_event_id=offset,
            limit=limit * 10,  # Need more events since we filter
        )
        
        # Group events by aggregate
        aggregates: dict = {}
        for event in all_events:
            if event.aggregate_id not in aggregates:
                aggregates[event.aggregate_id] = []
            aggregates[event.aggregate_id].append(event)
        
        # Reconstruct each aggregate
        tasks = []
        for agg_id, events in aggregates.items():
            try:
                task = TaskAggregate.from_events(events)
                
                # Apply filters
                if task.is_deleted:
                    continue
                if status and task.status.value != status:
                    continue
                if assignee_id and (
                    not task.assignee_id or str(task.assignee_id) != assignee_id
                ):
                    continue
                
                tasks.append(task)
                
                if len(tasks) >= limit:
                    break
                    
            except Exception as e:
                logger.warning(
                    "repository.list_reconstruction_error",
                    aggregate_id=agg_id,
                    error=str(e),
                )
                continue
        
        return tasks
    
    def invalidate_cache(self, aggregate_id: str = None) -> None:
        """Invalidate cache entries."""
        if aggregate_id:
            self._cache.pop(aggregate_id, None)
        else:
            self._cache.clear()