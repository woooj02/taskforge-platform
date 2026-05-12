import asyncio
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable
from enum import Enum


class TaskStatus(str, Enum):
    BACKLOG = "backlog"
    TODO = "todo"
    IN_PROGRESS = "in_progress"
    IN_REVIEW = "in_review"
    DONE = "done"
    ARCHIVED = "archived"

    def can_transition_to(self, target: 'TaskStatus') -> bool:
        valid = {
            TaskStatus.BACKLOG: {TaskStatus.TODO, TaskStatus.ARCHIVED},
            TaskStatus.TODO: {TaskStatus.IN_PROGRESS, TaskStatus.BACKLOG},
            TaskStatus.IN_PROGRESS: {TaskStatus.IN_REVIEW, TaskStatus.DONE, TaskStatus.TODO},
            TaskStatus.IN_REVIEW: {TaskStatus.DONE, TaskStatus.IN_PROGRESS, TaskStatus.TODO},
            TaskStatus.DONE: {TaskStatus.ARCHIVED, TaskStatus.TODO},
            TaskStatus.ARCHIVED: {TaskStatus.BACKLOG},
        }
        return target in valid.get(self, set())    

class TaskPriority(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SagaStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


@dataclass
class DomainEvent:
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    aggregate_id: str = ""
    event_type: str = ""
    version: int = 0
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    data: Dict[str, Any] = field(default_factory=dict)


class EventStore:
    def __init__(self):
        self._events: Dict[str, List[DomainEvent]] = {}
    
    def save_events(self, aggregate_id, events, expected_version):
        current = len(self._events.get(aggregate_id, []))
        if current != expected_version:
            raise ValueError(f"VERSION CONFLICT: expected v{expected_version}, current v{current}")
        if aggregate_id not in self._events:
            self._events[aggregate_id] = []
        self._events[aggregate_id].extend(events)


class TaskAggregate:
    def __init__(self, aggregate_id):
        self.aggregate_id = aggregate_id
        self.title = ""
        self.priority = TaskPriority.MEDIUM
        self.status = TaskStatus.BACKLOG
        self.assignee_id = None
        self.version = 0
        self._uncommitted = []
    
    def create(self, title, description, priority, created_by):
        e = DomainEvent(aggregate_id=self.aggregate_id, event_type="task.created",
                        version=self.version+1, data={"title": title, "priority": priority})
        self._apply(e)
        self._uncommitted.append(e)
        return self
    
    def assign(self, new_assignee):
        if self.assignee_id == new_assignee:
            return self
        e = DomainEvent(aggregate_id=self.aggregate_id, event_type="task.assigned",
                        version=self.version+1, data={"new": new_assignee})
        self._apply(e)
        self._uncommitted.append(e)
        return self
    
    def change_status(self, new_status):
        target = TaskStatus(new_status)
        if not self.status.can_transition_to(target):
            raise ValueError(f"Cannot transition {self.status.value} to {target.value}")
        e = DomainEvent(aggregate_id=self.aggregate_id, event_type="task.status_changed",
                        version=self.version+1, data={"to": target.value})
        self._apply(e)
        self._uncommitted.append(e)
        return self
    
    def _apply(self, event):
        if event.event_type == "task.created":
            self.title = event.data["title"]
            self.priority = TaskPriority(event.data["priority"])
        elif event.event_type == "task.assigned":
            self.assignee_id = event.data["new"]
        elif event.event_type == "task.status_changed":
            self.status = TaskStatus(event.data["to"])
        self.version = event.version
    
    def get_uncommitted(self):
        return self._uncommitted.copy()
    
    def mark_committed(self):
        self._uncommitted.clear()


async def demo():
    print("\n" + "="*50)
    print("  TASKFORGE PLATFORM - Demo")
    print("="*50)
    
    store = EventStore()
    
    # Demo 1
    print("\n--- Demo 1: Event Sourcing ---")
    tid = str(uuid.uuid4())[:8]
    task = TaskAggregate(tid)
    task.create("Implement OAuth2", "Add Google/GitHub login", "high", "alice")
    task.assign("bob")
    task.change_status("todo")  
    task.change_status("in_progress")
    task.change_status("in_review")
    
    events = task.get_uncommitted()
    store.save_events(tid, events, 0)
    task.mark_committed()
    
    print(f"  Task: {task.title}")
    print(f"  Status: {task.status.value} | Priority: {task.priority.value}")
    print(f"  Assignee: {task.assignee_id} | Version: v{task.version}")
    print(f"  Events:")
    for e in events:
        print(f"    v{e.version} | {e.event_type}")
    
    # Demo 2
    print("\n--- Demo 2: Optimistic Concurrency ---")
    cid = str(uuid.uuid4())[:8]
    
    a = TaskAggregate(cid)
    a.create("Setup CI/CD", "GitHub Actions", "medium", "alice")
    store.save_events(cid, a.get_uncommitted(), 0)
    a.mark_committed()
    print(f"  User A saved: v{a.version}")
    
    a.change_status("todo") 
    store.save_events(cid, a.get_uncommitted(), 1)
    a.mark_committed()
    print(f"  User A moved to todo: v{a.version}")
    
    b = TaskAggregate(cid)
    b.create("Setup CI/CD", "GitHub Actions", "medium", "alice")
    b.version = 1
    b.change_status("todo")  # Same change as A, but with stale version
    
    try:
        store.save_events(cid, b.get_uncommitted(), 1)
        print("  Should have failed!")
    except ValueError as e:
        print(f"  CONFLICT: {e}")
        print("  Must reload latest version before updating!")
    print("\n" + "="*50)
    print("  Demo Complete!")
    print("="*50)


if __name__ == "__main__":
    asyncio.run(demo())
