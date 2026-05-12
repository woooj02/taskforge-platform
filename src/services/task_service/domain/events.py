"""
Domain events for the Task aggregate.
Each event represents a state change in the task lifecycle.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List

from libs.common.event_bus import DomainEvent


@dataclass(frozen=True)
class TaskCreatedEvent(DomainEvent):
    """Emitted when a new task is created."""
    event_type: str = "task.created"
    title: str = ""
    description: str = ""
    priority: str = "medium"
    assignee_id: Optional[str] = None
    created_by: str = ""
    labels: List[str] = field(default_factory=list)
    due_date: Optional[str] = None
    
    def _serialize_data(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "description": self.description,
            "priority": self.priority,
            "assignee_id": self.assignee_id,
            "created_by": self.created_by,
            "labels": self.labels,
            "due_date": self.due_date,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskCreatedEvent':
        event_data = data.get("data", {})
        return cls(
            event_id=data.get("event_id", ""),
            aggregate_id=data.get("aggregate_id", ""),
            version=data.get("version", 0),
            occurred_at=datetime.fromisoformat(data.get("occurred_at", datetime.utcnow().isoformat())),
            title=event_data.get("title", ""),
            description=event_data.get("description", ""),
            priority=event_data.get("priority", "medium"),
            assignee_id=event_data.get("assignee_id"),
            created_by=event_data.get("created_by", ""),
            labels=event_data.get("labels", []),
            due_date=event_data.get("due_date"),
        )


@dataclass(frozen=True)
class TaskUpdatedEvent(DomainEvent):
    """Emitted when task details are modified."""
    event_type: str = "task.updated"
    field_changes: Dict[str, Any] = field(default_factory=dict)
    updated_by: str = ""
    
    def _serialize_data(self) -> Dict[str, Any]:
        return {
            "field_changes": self.field_changes,
            "updated_by": self.updated_by,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskUpdatedEvent':
        event_data = data.get("data", {})
        return cls(
            event_id=data.get("event_id", ""),
            aggregate_id=data.get("aggregate_id", ""),
            version=data.get("version", 0),
            field_changes=event_data.get("field_changes", {}),
            updated_by=event_data.get("updated_by", ""),
        )


@dataclass(frozen=True)
class TaskAssignedEvent(DomainEvent):
    """Emitted when task is assigned to a different user."""
    event_type: str = "task.assigned"
    previous_assignee_id: Optional[str] = None
    new_assignee_id: str = ""
    assigned_by: str = ""
    
    def _serialize_data(self) -> Dict[str, Any]:
        return {
            "previous_assignee_id": self.previous_assignee_id,
            "new_assignee_id": self.new_assignee_id,
            "assigned_by": self.assigned_by,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskAssignedEvent':
        event_data = data.get("data", {})
        return cls(
            event_id=data.get("event_id", ""),
            aggregate_id=data.get("aggregate_id", ""),
            version=data.get("version", 0),
            previous_assignee_id=event_data.get("previous_assignee_id"),
            new_assignee_id=event_data.get("new_assignee_id", ""),
            assigned_by=event_data.get("assigned_by", ""),
        )


@dataclass(frozen=True)
class TaskStatusChangedEvent(DomainEvent):
    """Emitted when task status changes."""
    event_type: str = "task.status_changed"
    previous_status: str = ""
    new_status: str = ""
    changed_by: str = ""
    reason: Optional[str] = None
    
    def _serialize_data(self) -> Dict[str, Any]:
        return {
            "previous_status": self.previous_status,
            "new_status": self.new_status,
            "changed_by": self.changed_by,
            "reason": self.reason,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskStatusChangedEvent':
        event_data = data.get("data", {})
        return cls(
            event_id=data.get("event_id", ""),
            aggregate_id=data.get("aggregate_id", ""),
            version=data.get("version", 0),
            previous_status=event_data.get("previous_status", ""),
            new_status=event_data.get("new_status", ""),
            changed_by=event_data.get("changed_by", ""),
            reason=event_data.get("reason"),
        )


@dataclass(frozen=True)
class TaskPriorityChangedEvent(DomainEvent):
    """Emitted when task priority is changed."""
    event_type: str = "task.priority_changed"
    previous_priority: str = ""
    new_priority: str = ""
    changed_by: str = ""
    
    def _serialize_data(self) -> Dict[str, Any]:
        return {
            "previous_priority": self.previous_priority,
            "new_priority": self.new_priority,
            "changed_by": self.changed_by,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskPriorityChangedEvent':
        event_data = data.get("data", {})
        return cls(
            event_id=data.get("event_id", ""),
            aggregate_id=data.get("aggregate_id", ""),
            version=data.get("version", 0),
            previous_priority=event_data.get("previous_priority", ""),
            new_priority=event_data.get("new_priority", ""),
            changed_by=event_data.get("changed_by", ""),
        )


@dataclass(frozen=True)
class TaskDeletedEvent(DomainEvent):
    """Emitted when a task is deleted."""
    event_type: str = "task.deleted"
    deleted_by: str = ""
    reason: Optional[str] = None
    
    def _serialize_data(self) -> Dict[str, Any]:
        return {
            "deleted_by": self.deleted_by,
            "reason": self.reason,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskDeletedEvent':
        event_data = data.get("data", {})
        return cls(
            event_id=data.get("event_id", ""),
            aggregate_id=data.get("aggregate_id", ""),
            version=data.get("version", 0),
            deleted_by=event_data.get("deleted_by", ""),
            reason=event_data.get("reason"),
        )