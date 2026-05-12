"""
Command objects for the Task service.
Commands represent user intentions to modify the system.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
import uuid

from .value_objects import TaskPriority, TaskStatus


@dataclass(frozen=True)
class CreateTaskCommand:
    """Command to create a new task."""
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    title: str = ""
    description: str = ""
    priority: str = TaskPriority.MEDIUM.value
    assignee_id: Optional[str] = None
    created_by: str = ""
    labels: List[str] = field(default_factory=list)
    due_date: Optional[datetime] = None
    
    def validate(self) -> None:
        """Validate command data."""
        errors = []
        
        if not self.title or len(self.title) < 3:
            errors.append("Title must be at least 3 characters")
        if len(self.title) > 200:
            errors.append("Title cannot exceed 200 characters")
        if len(self.description) > 5000:
            errors.append("Description cannot exceed 5000 characters")
        if not self.created_by:
            errors.append("Created by user ID is required")
        if self.priority not in [p.value for p in TaskPriority]:
            errors.append(f"Invalid priority: {self.priority}")
        if self.due_date and self.due_date.tzinfo is None:
            errors.append("Due date must be timezone-aware")
        
        if errors:
            raise ValueError("; ".join(errors))


@dataclass(frozen=True)
class UpdateTaskCommand:
    """Command to update an existing task."""
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_id: str = ""
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    priority: Optional[str] = None
    assignee_id: Optional[str] = None
    labels: Optional[List[str]] = None
    due_date: Optional[datetime] = None
    updated_by: str = ""
    expected_version: int = 0
    
    def validate(self) -> None:
        """Validate command data."""
        errors = []
        
        if not self.task_id:
            errors.append("Task ID is required")
        if not self.updated_by:
            errors.append("Updated by user ID is required")
        if self.title is not None and (len(self.title) < 3 or len(self.title) > 200):
            errors.append("Title must be between 3 and 200 characters")
        if self.description is not None and len(self.description) > 5000:
            errors.append("Description cannot exceed 5000 characters")
        if self.status and self.status not in [s.value for s in TaskStatus]:
            errors.append(f"Invalid status: {self.status}")
        if self.priority and self.priority not in [p.value for p in TaskPriority]:
            errors.append(f"Invalid priority: {self.priority}")
        
        if errors:
            raise ValueError("; ".join(errors))


@dataclass(frozen=True)
class AssignTaskCommand:
    """Command to assign a task to a user."""
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_id: str = ""
    new_assignee_id: str = ""
    assigned_by: str = ""
    expected_version: int = 0
    
    def validate(self) -> None:
        errors = []
        if not self.task_id:
            errors.append("Task ID is required")
        if not self.new_assignee_id:
            errors.append("New assignee ID is required")
        if not self.assigned_by:
            errors.append("Assigned by user ID is required")
        if errors:
            raise ValueError("; ".join(errors))


@dataclass(frozen=True)
class DeleteTaskCommand:
    """Command to delete a task."""
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_id: str = ""
    deleted_by: str = ""
    reason: Optional[str] = None
    expected_version: int = 0
    
    def validate(self) -> None:
        errors = []
        if not self.task_id:
            errors.append("Task ID is required")
        if not self.deleted_by:
            errors.append("Deleted by user ID is required")
        if errors:
            raise ValueError("; ".join(errors))