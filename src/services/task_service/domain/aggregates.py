"""
Task Aggregate Root - implements event sourcing pattern.
All state changes go through domain events.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from libs.common.unit_of_work import AggregateRoot
from libs.common.event_bus import DomainEvent

from .value_objects import (
    TaskId, UserId, Label, TaskDescription, TaskTitle,
    TaskPriority, TaskStatus, DueDate, TaskMetadata,
)
from .events import (
    TaskCreatedEvent, TaskUpdatedEvent, TaskAssignedEvent,
    TaskStatusChangedEvent, TaskPriorityChangedEvent, TaskDeletedEvent,
)
from .commands import (
    CreateTaskCommand, UpdateTaskCommand, AssignTaskCommand, DeleteTaskCommand,
)


class TaskAggregate(AggregateRoot):
    """
    Task aggregate root with event sourcing.
    Maintains full task state and enforces business invariants.
    """
    
    def __init__(
        self,
        aggregate_id: str,
        title: TaskTitle = None,
        description: TaskDescription = None,
        priority: TaskPriority = TaskPriority.MEDIUM,
        status: TaskStatus = TaskStatus.BACKLOG,
        assignee_id: Optional[UserId] = None,
        created_by: Optional[UserId] = None,
        labels: List[Label] = None,
        due_date: DueDate = None,
        metadata: TaskMetadata = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        is_deleted: bool = False,
    ):
        super().__init__(aggregate_id)
        self.title = title or TaskTitle("Untitled")
        self.description = description or TaskDescription("")
        self.priority = priority
        self.status = status
        self.assignee_id = assignee_id
        self.created_by = created_by
        self.labels = labels or []
        self.due_date = due_date or DueDate()
        self.metadata = metadata or TaskMetadata()
        self.created_at = created_at
        self.updated_at = updated_at
        self.is_deleted = is_deleted
    
    @classmethod
    def create(cls, command: CreateTaskCommand) -> 'TaskAggregate':
        """Factory method to create a new task."""
        command.validate()
        
        task_id = str(TaskId())
        aggregate = cls(aggregate_id=task_id)
        
        event = TaskCreatedEvent(
            aggregate_id=task_id,
            title=command.title,
            description=command.description,
            priority=command.priority,
            assignee_id=command.assignee_id,
            created_by=command.created_by,
            labels=command.labels,
            due_date=command.due_date.isoformat() if command.due_date else None,
        )
        
        aggregate.raise_event(event)
        return aggregate
    
    def update(self, command: UpdateTaskCommand) -> None:
        """Update task fields."""
        command.validate()
        self._validate_not_deleted()
        
        field_changes = {}
        
        if command.title is not None and command.title != str(self.title):
            new_title = TaskTitle(command.title)
            field_changes["title"] = {"old": str(self.title), "new": str(new_title)}
        
        if command.description is not None and command.description != str(self.description):
            new_desc = TaskDescription(command.description)
            field_changes["description"] = {"old": str(self.description), "new": str(new_desc)}
        
        if command.status is not None:
            new_status = TaskStatus(command.status)
            if new_status != self.status:
                self._validate_status_transition(self.status, new_status)
                field_changes["status"] = {"old": self.status.value, "new": new_status.value}
        
        if command.priority is not None:
            new_priority = TaskPriority(command.priority)
            if new_priority != self.priority:
                field_changes["priority"] = {"old": self.priority.value, "new": new_priority.value}
        
        if command.labels is not None:
            new_labels = [Label(l) for l in command.labels]
            field_changes["labels"] = {
                "old": [str(l) for l in self.labels],
                "new": [str(l) for l in new_labels],
            }
        
        if field_changes:
            event = TaskUpdatedEvent(
                aggregate_id=self.aggregate_id,
                field_changes=field_changes,
                updated_by=command.updated_by,
            )
            self.raise_event(event)
    
    def assign(self, command: AssignTaskCommand) -> None:
        """Assign task to a user."""
        command.validate()
        self._validate_not_deleted()
        
        new_assignee = UserId(command.new_assignee_id)
        previous_assignee = self.assignee_id
        
        if previous_assignee and str(previous_assignee) == str(new_assignee):
            return  # No change
        
        event = TaskAssignedEvent(
            aggregate_id=self.aggregate_id,
            previous_assignee_id=str(previous_assignee) if previous_assignee else None,
            new_assignee_id=str(new_assignee),
            assigned_by=command.assigned_by,
        )
        self.raise_event(event)
    
    def change_status(self, new_status: TaskStatus, changed_by: str, reason: Optional[str] = None) -> None:
        """Change task status."""
        self._validate_not_deleted()
        self._validate_status_transition(self.status, new_status)
        
        if new_status == self.status:
            return
        
        event = TaskStatusChangedEvent(
            aggregate_id=self.aggregate_id,
            previous_status=self.status.value,
            new_status=new_status.value,
            changed_by=changed_by,
            reason=reason,
        )
        self.raise_event(event)
    
    def change_priority(self, new_priority: TaskPriority, changed_by: str) -> None:
        """Change task priority."""
        self._validate_not_deleted()
        
        if new_priority == self.priority:
            return
        
        event = TaskPriorityChangedEvent(
            aggregate_id=self.aggregate_id,
            previous_priority=self.priority.value,
            new_priority=new_priority.value,
            changed_by=changed_by,
        )
        self.raise_event(event)
    
    def delete(self, command: DeleteTaskCommand) -> None:
        """Soft delete the task."""
        command.validate()
        self._validate_not_deleted()
        
        event = TaskDeletedEvent(
            aggregate_id=self.aggregate_id,
            deleted_by=command.deleted_by,
            reason=command.reason,
        )
        self.raise_event(event)
    
    def apply_event(self, event: DomainEvent) -> None:
        """Apply an event to mutate aggregate state."""
        if isinstance(event, TaskCreatedEvent):
            self._apply_created(event)
        elif isinstance(event, TaskUpdatedEvent):
            self._apply_updated(event)
        elif isinstance(event, TaskAssignedEvent):
            self._apply_assigned(event)
        elif isinstance(event, TaskStatusChangedEvent):
            self._apply_status_changed(event)
        elif isinstance(event, TaskPriorityChangedEvent):
            self._apply_priority_changed(event)
        elif isinstance(event, TaskDeletedEvent):
            self._apply_deleted(event)
    
    def _apply_created(self, event: TaskCreatedEvent) -> None:
        """Apply task created event."""
        self.title = TaskTitle(event.title)
        self.description = TaskDescription(event.description)
        self.priority = TaskPriority(event.priority)
        self.status = TaskStatus.BACKLOG
        self.assignee_id = UserId(event.assignee_id) if event.assignee_id else None
        self.created_by = UserId(event.created_by)
        self.labels = [Label(l) for l in event.labels]
        self.due_date = DueDate(
            datetime.fromisoformat(event.due_date)
        ) if event.due_date else DueDate()
        self.created_at = event.occurred_at
        self.updated_at = event.occurred_at
        self.is_deleted = False
    
    def _apply_updated(self, event: TaskUpdatedEvent) -> None:
        """Apply task updated event."""
        changes = event.field_changes
        
        if "title" in changes:
            self.title = TaskTitle(changes["title"]["new"])
        if "description" in changes:
            self.description = TaskDescription(changes["description"]["new"])
        if "status" in changes:
            self.status = TaskStatus(changes["status"]["new"])
        if "priority" in changes:
            self.priority = TaskPriority(changes["priority"]["new"])
            self.metadata = self.metadata.increment_priority_changes()
        if "labels" in changes:
            self.labels = [Label(l) for l in changes["labels"]["new"]]
        
        self.updated_at = event.occurred_at
    
    def _apply_assigned(self, event: TaskAssignedEvent) -> None:
        """Apply task assigned event."""
        self.assignee_id = UserId(event.new_assignee_id)
        self.metadata = self.metadata.increment_assignee_changes()
        self.updated_at = event.occurred_at
    
    def _apply_status_changed(self, event: TaskStatusChangedEvent) -> None:
        """Apply status changed event."""
        self.status = TaskStatus(event.new_status)
        self.updated_at = event.occurred_at
    
    def _apply_priority_changed(self, event: TaskPriorityChangedEvent) -> None:
        """Apply priority changed event."""
        self.priority = TaskPriority(event.new_priority)
        self.metadata = self.metadata.increment_priority_changes()
        self.updated_at = event.occurred_at
    
    def _apply_deleted(self, event: TaskDeletedEvent) -> None:
        """Apply task deleted event."""
        self.is_deleted = True
        self.updated_at = event.occurred_at
    
    def _validate_not_deleted(self) -> None:
        """Ensure task is not deleted."""
        if self.is_deleted:
            raise ValueError(f"Task {self.aggregate_id} has been deleted")
    
    def _validate_status_transition(self, from_status: TaskStatus, to_status: TaskStatus) -> None:
        """Validate status transition is allowed."""
        if not from_status.can_transition_to(to_status):
            raise ValueError(
                f"Cannot transition from {from_status.value} to {to_status.value}"
            )
    
    @classmethod
    def from_events(cls, events: List[DomainEvent]) -> 'TaskAggregate':
        """Reconstitute aggregate from event stream."""
        if not events:
            raise ValueError("Cannot reconstitute from empty event stream")
        
        first_event = events[0]
        aggregate = cls(aggregate_id=first_event.aggregate_id)
        
        for event in sorted(events, key=lambda e: e.version):
            aggregate.apply_event(event)
            aggregate.version = event.version
        
        return aggregate
    
    def to_dict(self) -> dict:
        """Serialize aggregate to dictionary for projections."""
        return {
            "id": self.aggregate_id,
            "title": str(self.title),
            "description": str(self.description),
            "priority": self.priority.value,
            "status": self.status.value,
            "assignee_id": str(self.assignee_id) if self.assignee_id else None,
            "created_by": str(self.created_by) if self.created_by else None,
            "labels": [str(l) for l in self.labels],
            "due_date": str(self.due_date) if self.due_date.date else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "is_deleted": self.is_deleted,
            "version": self.version,
        }