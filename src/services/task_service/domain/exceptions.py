"""Domain exceptions for Task service."""


class TaskDomainError(Exception):
    """Base exception for task domain errors."""
    def __init__(self, message: str, code: str = "TASK_DOMAIN_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)


class TaskNotFoundError(TaskDomainError):
    """Task not found."""
    def __init__(self, task_id: str):
        super().__init__(
            message=f"Task with ID '{task_id}' not found",
            code="TASK_NOT_FOUND",
        )


class TaskAlreadyDeletedError(TaskDomainError):
    """Task has been deleted."""
    def __init__(self, task_id: str):
        super().__init__(
            message=f"Task '{task_id}' has been deleted",
            code="TASK_DELETED",
        )


class InvalidTaskStateTransitionError(TaskDomainError):
    """Invalid status transition."""
    def __init__(self, from_status: str, to_status: str):
        super().__init__(
            message=f"Cannot transition from '{from_status}' to '{to_status}'",
            code="INVALID_TRANSITION",
        )


class TaskVersionConflictError(TaskDomainError):
    """Optimistic concurrency conflict."""
    def __init__(self, task_id: str, expected: int, actual: int):
        super().__init__(
            message=f"Version conflict for task '{task_id}': expected {expected}, got {actual}",
            code="VERSION_CONFLICT",
        )


class UnauthorizedTaskAccessError(TaskDomainError):
    """User not authorized for task operation."""
    def __init__(self, user_id: str, task_id: str, operation: str):
        super().__init__(
            message=f"User '{user_id}' not authorized to {operation} task '{task_id}'",
            code="UNAUTHORIZED",
        )