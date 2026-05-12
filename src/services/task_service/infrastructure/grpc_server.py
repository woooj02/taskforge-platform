"""
gRPC server implementation for Task Service.
Handles RPC calls with proper error handling and validation.
"""
import asyncio
from typing import AsyncIterator
import grpc
from grpc import aio
import structlog

from libs.common.unit_of_work import UnitOfWork
from libs.common.event_bus import EventPublisher
from ..domain.aggregates import TaskAggregate
from ..domain.commands import (
    CreateTaskCommand, UpdateTaskCommand, AssignTaskCommand, DeleteTaskCommand,
)
from ..domain.exceptions import (
    TaskNotFoundError, TaskAlreadyDeletedError,
    InvalidTaskStateTransitionError, TaskVersionConflictError,
    UnauthorizedTaskAccessError,
)
from .event_store import PostgresEventStore
from .repository import TaskRepository

# Import generated protobuf stubs
# from protos import task_service_pb2, task_service_pb2_grpc

logger = structlog.get_logger(__name__)


class TaskServiceServicer:
    """gRPC service implementation for Task management."""
    
    def __init__(
        self,
        event_store: PostgresEventStore,
        event_publisher: EventPublisher,
    ):
        self.event_store = event_store
        self.event_publisher = event_publisher
        self.repository = TaskRepository(event_store)
    
    async def CreateTask(self, request, context) -> dict:
        """Handle CreateTask gRPC call."""
        logger.info("grpc.create_task.start")
        
        try:
            # Build command from request
            command = CreateTaskCommand(
                title=request.title,
                description=request.description,
                priority=request.priority,
                assignee_id=request.assignee_id or None,
                created_by=request.created_by,
                labels=list(request.labels),
                due_date=request.due_date.ToDatetime() if request.HasField('due_date') else None,
            )
            
            # Create aggregate
            task = TaskAggregate.create(command)
            
            # Save using Unit of Work
            async with UnitOfWork(self.event_store, self.event_publisher) as uow:
                uow.track_aggregate(task)
                await self.repository.save(task)
            
            logger.info(
                "grpc.create_task.success",
                task_id=task.aggregate_id,
            )
            
            return task.to_dict()
            
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except Exception as e:
            logger.error("grpc.create_task.error", error=str(e))
            await context.abort(grpc.StatusCode.INTERNAL, "Internal error")
    
    async def UpdateTask(self, request, context) -> dict:
        """Handle UpdateTask gRPC call."""
        logger.info("grpc.update_task.start", task_id=request.task_id)
        
        try:
            # Load existing task
            task = await self.repository.get_by_id(request.task_id)
            
            if not task:
                raise TaskNotFoundError(request.task_id)
            
            # Build command
            command = UpdateTaskCommand(
                task_id=request.task_id,
                title=request.title.value if request.HasField('title') else None,
                description=request.description.value if request.HasField('description') else None,
                status=request.status if request.HasField('status') else None,
                priority=request.priority if request.HasField('priority') else None,
                labels=list(request.labels) if request.labels else None,
                updated_by=request.updated_by if hasattr(request, 'updated_by') else "system",
                expected_version=request.expected_version,
            )
            
            # Apply update
            task.update(command)
            
            # Save
            async with UnitOfWork(self.event_store, self.event_publisher) as uow:
                uow.track_aggregate(task)
                await self.repository.save(task, expected_version=request.expected_version)
            
            logger.info("grpc.update_task.success", task_id=request.task_id)
            return task.to_dict()
            
        except TaskNotFoundError:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Task {request.task_id} not found")
        except InvalidTaskStateTransitionError as e:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(e))
        except TaskVersionConflictError as e:
            await context.abort(grpc.StatusCode.ABORTED, str(e))
        except ValueError as e:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except Exception as e:
            logger.error("grpc.update_task.error", error=str(e))
            await context.abort(grpc.StatusCode.INTERNAL, "Internal error")
    
    async def AssignTask(self, request, context) -> dict:
        """Handle AssignTask gRPC call."""
        logger.info("grpc.assign_task.start", task_id=request.task_id)
        
        try:
            task = await self.repository.get_by_id(request.task_id)
            
            if not task:
                raise TaskNotFoundError(request.task_id)
            
            command = AssignTaskCommand(
                task_id=request.task_id,
                new_assignee_id=request.new_assignee_id,
                assigned_by=request.assigned_by if hasattr(request, 'assigned_by') else "system",
                expected_version=request.expected_version,
            )
            
            task.assign(command)
            
            async with UnitOfWork(self.event_store, self.event_publisher) as uow:
                uow.track_aggregate(task)
                await self.repository.save(task, expected_version=request.expected_version)
            
            logger.info("grpc.assign_task.success", task_id=request.task_id)
            return task.to_dict()
            
        except TaskNotFoundError:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Task {request.task_id} not found")
        except TaskVersionConflictError as e:
            await context.abort(grpc.StatusCode.ABORTED, str(e))
        except Exception as e:
            logger.error("grpc.assign_task.error", error=str(e))
            await context.abort(grpc.StatusCode.INTERNAL, "Internal error")
    
    async def DeleteTask(self, request, context) -> bool:
        """Handle DeleteTask gRPC call."""
        logger.info("grpc.delete_task.start", task_id=request.task_id)
        
        try:
            task = await self.repository.get_by_id(request.task_id)
            
            if not task:
                raise TaskNotFoundError(request.task_id)
            
            command = DeleteTaskCommand(
                task_id=request.task_id,
                deleted_by=request.deleted_by if hasattr(request, 'deleted_by') else "system",
                reason=request.reason if hasattr(request, 'reason') else None,
                expected_version=request.expected_version,
            )
            
            task.delete(command)
            
            async with UnitOfWork(self.event_store, self.event_publisher) as uow:
                uow.track_aggregate(task)
                await self.repository.save(task, expected_version=request.expected_version)
            
            logger.info("grpc.delete_task.success", task_id=request.task_id)
            return True
            
        except TaskNotFoundError:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Task {request.task_id} not found")
        except TaskVersionConflictError as e:
            await context.abort(grpc.StatusCode.ABORTED, str(e))
        except Exception as e:
            logger.error("grpc.delete_task.error", error=str(e))
            await context.abort(grpc.StatusCode.INTERNAL, "Internal error")
    
    async def GetTask(self, request, context) -> dict:
        """Handle GetTask gRPC call."""
        logger.debug("grpc.get_task", task_id=request.task_id)
        
        task = await self.repository.get_by_id(request.task_id)
        
        if not task:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Task {request.task_id} not found",
            )
        
        return task.to_dict()
    
    async def GetTaskHistory(self, request, context) -> dict:
        """Handle GetTaskHistory gRPC call."""
        logger.debug("grpc.get_task_history", task_id=request.task_id)
        
        events = await self.event_store.get_events(request.task_id)
        
        return {
            "events": [
                {
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "version": e.version,
                    "occurred_at": e.occurred_at.isoformat(),
                    "data": e.to_dict(),
                }
                for e in events
            ]
        }


class TaskGrpcServer:
    """Manages the gRPC server lifecycle."""
    
    def __init__(
        self,
        servicer: TaskServiceServicer,
        host: str = "0.0.0.0",
        port: int = 50051,
        max_workers: int = 10,
    ):
        self.servicer = servicer
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self._server: Optional[aio.Server] = None
    
    async def start(self) -> None:
        """Start the gRPC server."""
        self._server = aio.server(
            options=[
                ('grpc.max_concurrent_rpcs', self.max_workers),
                ('grpc.keepalive_time_ms', 30000),
                ('grpc.keepalive_timeout_ms', 10000),
                ('grpc.http2.max_pings_without_data', 0),
            ],
        )
        
        # Add service to server
        # task_service_pb2_grpc.add_TaskServiceServicer_to_server(
        #     self.servicer, self._server
        # )
        
        # Bind port
        address = f"{self.host}:{self.port}"
        self._server.add_insecure_port(address)
        
        await self._server.start()
        logger.info(
            "grpc.server.started",
            address=address,
        )
    
    async def stop(self) -> None:
        """Stop the gRPC server gracefully."""
        if self._server:
            await self._server.stop(grace=5.0)
            logger.info("grpc.server.stopped")
    
    async def serve_forever(self) -> None:
        """Run server until interrupted."""
        await self.start()
        try:
            await self._server.wait_for_termination()
        except asyncio.CancelledError:
            await self.stop()