"""
Saga pattern implementation for distributed transactions.
Supports both choreography and orchestration-based sagas.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type
import asyncio
import uuid
import structlog

logger = structlog.get_logger(__name__)


class SagaStatus(str, Enum):
    """Saga execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    CANCELLED = "cancelled"


@dataclass
class SagaStep:
    """
    A single step in a saga.
    Each step has an action and a compensating action.
    """
    step_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    action: Callable = None
    compensation: Optional[Callable] = None
    retry_count: int = 3
    retry_delay_seconds: float = 1.0
    timeout_seconds: float = 30.0
    is_compensatable: bool = True
    
    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the step action."""
        logger.info("saga.step.executing", step=self.name)
        
        last_error = None
        
        for attempt in range(self.retry_count + 1):
            try:
                result = await asyncio.wait_for(
                    self.action(context),
                    timeout=self.timeout_seconds,
                )
                logger.info("saga.step.completed", step=self.name, attempt=attempt + 1)
                return result or {}
                
            except asyncio.TimeoutError:
                last_error = TimeoutError(f"Step '{self.name}' timed out")
                logger.warning(
                    "saga.step.timeout",
                    step=self.name,
                    attempt=attempt + 1,
                )
                
            except Exception as e:
                last_error = e
                logger.warning(
                    "saga.step.failed",
                    step=self.name,
                    error=str(e),
                    attempt=attempt + 1,
                )
            
            if attempt < self.retry_count:
                delay = self.retry_delay_seconds * (2 ** attempt)  # Exponential backoff
                await asyncio.sleep(delay)
        
        raise last_error or RuntimeError(f"Step '{self.name}' failed after all retries")
    
    async def compensate(self, context: Dict[str, Any]) -> None:
        """Execute the compensating action."""
        if not self.compensation or not self.is_compensatable:
            logger.info("saga.step.no_compensation", step=self.name)
            return
        
        logger.info("saga.step.compensating", step=self.name)
        
        try:
            await asyncio.wait_for(
                self.compensation(context),
                timeout=self.timeout_seconds,
            )
            logger.info("saga.step.compensated", step=self.name)
            
        except Exception as e:
            logger.error(
                "saga.step.compensation_failed",
                step=self.name,
                error=str(e),
            )
            raise


@dataclass
class SagaDefinition:
    """Defines a saga workflow with steps."""
    saga_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    steps: List[SagaStep] = field(default_factory=list)
    on_success: Optional[Callable] = None
    on_failure: Optional[Callable] = None
    
    def add_step(self, step: SagaStep) -> 'SagaDefinition':
        """Add a step to the saga."""
        self.steps.append(step)
        return self


class SagaInstance:
    """A running instance of a saga."""
    
    def __init__(
        self,
        definition: SagaDefinition,
        initial_context: Optional[Dict[str, Any]] = None,
    ):
        self.instance_id = str(uuid.uuid4())
        self.definition = definition
        self.context = initial_context or {}
        self.context["saga_instance_id"] = self.instance_id
        self.status = SagaStatus.PENDING
        self.current_step_index = 0
        self.completed_steps: List[SagaStep] = []
        self.failed_step: Optional[SagaStep] = None
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.error: Optional[Exception] = None
    
    async def execute(self) -> SagaStatus:
        """
        Execute the saga.
        On failure, automatically runs compensation for completed steps.
        """
        self.status = SagaStatus.RUNNING
        self.started_at = datetime.utcnow()
        
        logger.info(
            "saga.execution.started",
            saga=self.definition.name,
            instance_id=self.instance_id,
            total_steps=len(self.definition.steps),
        )
        
        try:
            # Execute steps sequentially
            for i, step in enumerate(self.definition.steps):
                self.current_step_index = i
                self.context["current_step"] = step.name
                
                result = await step.execute(self.context)
                self.context.update(result)
                self.completed_steps.append(step)
            
            # All steps completed successfully
            self.status = SagaStatus.COMPLETED
            
            if self.definition.on_success:
                await self.definition.on_success(self.context)
            
            logger.info(
                "saga.execution.completed",
                saga=self.definition.name,
                instance_id=self.instance_id,
                steps_completed=len(self.completed_steps),
            )
            
        except Exception as e:
            self.error = e
            self.failed_step = self.definition.steps[self.current_step_index]
            
            logger.error(
                "saga.execution.failed",
                saga=self.definition.name,
                instance_id=self.instance_id,
                failed_step=self.failed_step.name,
                error=str(e),
            )
            
            # Start compensation
            await self._compensate()
        
        finally:
            self.completed_at = datetime.utcnow()
            self.context["saga_duration_seconds"] = (
                self.completed_at - self.started_at
            ).total_seconds()
        
        return self.status
    
    async def _compensate(self) -> None:
        """Compensate completed steps in reverse order."""
        self.status = SagaStatus.COMPENSATING
        
        logger.info(
            "saga.compensation.started",
            instance_id=self.instance_id,
            steps_to_compensate=len(self.completed_steps),
        )
        
        compensation_errors = []
        
        # Compensate in reverse order
        for step in reversed(self.completed_steps):
            try:
                await step.compensate(self.context)
            except Exception as e:
                compensation_errors.append({
                    "step": step.name,
                    "error": str(e),
                })
                logger.error(
                    "saga.compensation.step_failed",
                    step=step.name,
                    error=str(e),
                )
        
        if compensation_errors:
            self.status = SagaStatus.FAILED
            logger.error(
                "saga.compensation.partial_failure",
                errors=compensation_errors,
            )
        else:
            self.status = SagaStatus.COMPENSATED
            logger.info("saga.compensation.completed")
        
        if self.definition.on_failure:
            try:
                await self.definition.on_failure(self.context)
            except Exception as e:
                logger.error("saga.on_failure_handler.error", error=str(e))
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize saga instance state."""
        return {
            "instance_id": self.instance_id,
            "saga_name": self.definition.name,
            "status": self.status.value,
            "current_step_index": self.current_step_index,
            "completed_steps": len(self.completed_steps),
            "total_steps": len(self.definition.steps),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": str(self.error) if self.error else None,
        }


class SagaOrchestrator:
    """
    Orchestrates multiple sagas.
    Tracks running instances and provides management capabilities.
    """
    
    def __init__(self):
        self._running_instances: Dict[str, SagaInstance] = {}
        self._completed_instances: Dict[str, SagaInstance] = {}
        self._lock = asyncio.Lock()
    
    async def start_saga(
        self,
        definition: SagaDefinition,
        context: Optional[Dict[str, Any]] = None,
    ) -> SagaInstance:
        """Start a new saga instance."""
        instance = SagaInstance(definition, context)
        
        async with self._lock:
            self._running_instances[instance.instance_id] = instance
        
        # Execute in background
        asyncio.create_task(self._execute_and_cleanup(instance))
        
        return instance
    
    async def _execute_and_cleanup(self, instance: SagaInstance) -> None:
        """Execute saga and move to completed list."""
        try:
            await instance.execute()
        finally:
            async with self._lock:
                self._running_instances.pop(instance.instance_id, None)
                self._completed_instances[instance.instance_id] = instance
    
    async def get_instance(self, instance_id: str) -> Optional[SagaInstance]:
        """Get saga instance by ID."""
        return (
            self._running_instances.get(instance_id) or
            self._completed_instances.get(instance_id)
        )
    
    async def cancel_saga(self, instance_id: str) -> bool:
        """Cancel a running saga."""
        instance = self._running_instances.get(instance_id)
        if instance:
            instance.status = SagaStatus.CANCELLED
            return True
        return False
    
    def get_active_sagas(self) -> List[Dict[str, Any]]:
        """Get all active saga instances."""
        return [i.to_dict() for i in self._running_instances.values()]
    
    def get_saga_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get completed saga history."""
        recent = list(self._completed_instances.values())[-limit:]
        return [i.to_dict() for i in recent]