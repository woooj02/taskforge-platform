"""
Value objects for the Task domain.
Immutable objects that represent domain concepts without identity.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List
import uuid


class TaskPriority(str, Enum):
    """Task priority levels with ordering."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    
    @property
    def numeric_value(self) -> int:
        """Get numeric priority for comparison."""
        mapping = {
            TaskPriority.LOW: 0,
            TaskPriority.MEDIUM: 1,
            TaskPriority.HIGH: 2,
            TaskPriority.CRITICAL: 3,
        }
        return mapping[self]
    
    def __lt__(self, other: 'TaskPriority') -> bool:
        return self.numeric_value < other.numeric_value
    
    def __gt__(self, other: 'TaskPriority') -> bool:
        return self.numeric_value > other.numeric_value


class TaskStatus(str, Enum):
    """Task lifecycle status."""
    BACKLOG = "backlog"
    TODO = "todo"
    IN_PROGRESS = "in_progress"
    IN_REVIEW = "in_review"
    DONE = "done"
    ARCHIVED = "archived"
    
    def can_transition_to(self, target: 'TaskStatus') -> bool:
        """Check if transition from current status to target is valid."""
        valid_transitions = {
            TaskStatus.BACKLOG: {TaskStatus.TODO, TaskStatus.ARCHIVED},
            TaskStatus.TODO: {TaskStatus.IN_PROGRESS, TaskStatus.BACKLOG, TaskStatus.ARCHIVED},
            TaskStatus.IN_PROGRESS: {TaskStatus.IN_REVIEW, TaskStatus.TODO, TaskStatus.DONE},
            TaskStatus.IN_REVIEW: {TaskStatus.DONE, TaskStatus.IN_PROGRESS, TaskStatus.TODO},
            TaskStatus.DONE: {TaskStatus.ARCHIVED, TaskStatus.TODO},
            TaskStatus.ARCHIVED: {TaskStatus.BACKLOG},
        }
        return target in valid_transitions.get(self, set())


@dataclass(frozen=True)
class TaskId:
    """Strongly-typed task identifier."""
    value: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def __str__(self) -> str:
        return self.value
    
    def __eq__(self, other: object) -> bool:
        if isinstance(other, TaskId):
            return self.value == other.value
        return False


@dataclass(frozen=True)
class UserId:
    """Strongly-typed user identifier."""
    value: str
    
    def __str__(self) -> str:
        return self.value
    
    def __eq__(self, other: object) -> bool:
        if isinstance(other, UserId):
            return self.value == other.value
        return False


@dataclass(frozen=True)
class Label:
    """Task label/tag value object."""
    name: str
    
    def __post_init__(self):
        if not self.name or len(self.name) > 50:
            raise ValueError("Label name must be between 1 and 50 characters")
        if not self.name.replace('-', '').replace('_', '').isalnum():
            raise ValueError("Label can only contain alphanumeric characters, hyphens, and underscores")
    
    def __str__(self) -> str:
        return self.name.lower()


@dataclass(frozen=True)
class TaskDescription:
    """Task description with validation."""
    text: str
    
    def __post_init__(self):
        if len(self.text) > 5000:
            raise ValueError("Description cannot exceed 5000 characters")
    
    @property
    def excerpt(self) -> str:
        """Get first 200 characters as excerpt."""
        return self.text[:200] + "..." if len(self.text) > 200 else self.text
    
    def __str__(self) -> str:
        return self.text


@dataclass(frozen=True)
class TaskTitle:
    """Task title with validation."""
    text: str
    
    def __post_init__(self):
        if not self.text or len(self.text) < 3:
            raise ValueError("Title must be at least 3 characters")
        if len(self.text) > 200:
            raise ValueError("Title cannot exceed 200 characters")
        if self.text.isspace():
            raise ValueError("Title cannot be only whitespace")
    
    def __str__(self) -> str:
        return self.text.strip()


@dataclass(frozen=True)
class DueDate:
    """Task due date value object."""
    date: Optional[datetime] = None
    
    def __post_init__(self):
        if self.date is not None and self.date.tzinfo is None:
            object.__setattr__(self, 'date', self.date.replace(tzinfo=timezone.utc))
    
    @property
    def is_overdue(self) -> bool:
        """Check if due date has passed."""
        if self.date is None:
            return False
        return datetime.now(timezone.utc) > self.date
    
    @property
    def is_approaching(self) -> bool:
        """Check if due within 24 hours."""
        if self.date is None:
            return False
        now = datetime.now(timezone.utc)
        return now < self.date and (self.date - now).total_seconds() < 86400
    
    def __str__(self) -> str:
        return self.date.isoformat() if self.date else "No due date"


@dataclass(frozen=True)
class TaskMetadata:
    """Additional task metadata."""
    source: str = "api"
    priority_changed_count: int = 0
    assignee_changed_count: int = 0
    tags: List[str] = field(default_factory=list)
    custom_fields: dict = field(default_factory=dict)
    
    def increment_priority_changes(self) -> 'TaskMetadata':
        """Create new metadata with incremented priority change count."""
        return TaskMetadata(
            source=self.source,
            priority_changed_count=self.priority_changed_count + 1,
            assignee_changed_count=self.assignee_changed_count,
            tags=self.tags.copy(),
            custom_fields=self.custom_fields.copy(),
        )
    
    def increment_assignee_changes(self) -> 'TaskMetadata':
        """Create new metadata with incremented assignee change count."""
        return TaskMetadata(
            source=self.source,
            priority_changed_count=self.priority_changed_count,
            assignee_changed_count=self.assignee_changed_count + 1,
            tags=self.tags.copy(),
            custom_fields=self.custom_fields.copy(),
        )