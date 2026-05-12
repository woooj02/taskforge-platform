"""
Workflow Service entry point.
Provides saga orchestration for complex task workflows.
"""
import asyncio
import os
import signal
from typing import Optional
import click
import structlog

from libs.common.observability import ObservabilityManager
from libs.saga_framework.saga import SagaOrchestrator, SagaInstance
from .saga.workflows import (
    TaskOnboardingWorkflow,
    TaskApprovalWorkflow,
    TaskBulkOperationWorkflow,
)

logger = structlog.get_logger(__name__)


class WorkflowServiceApp:
    """Main Workflow Service application."""
    
    def __init__(self):
        self.orchestrator = SagaOrchestrator()
        self._running = False
        self._workflow_registry = {
            "task_onboarding": TaskOnboardingWorkflow,
            "task_approval": TaskApprovalWorkflow,
            "task_bulk": TaskBulkOperationWorkflow,
        }
    
    async def initialize(self) -> None:
        """Initialize workflow service."""
        logger.info("workflow_service.initializing")
        
        ObservabilityManager.initialize(
            service_name="workflow-service",
        )
        
        logger.info("workflow_service.initialized")
    
    async def start_workflow(
        self,
        workflow_name: str,
        context: dict,
    ) -> SagaInstance:
        """Start a new workflow."""
        if workflow_name not in self._workflow_registry:
            raise ValueError(f"Unknown workflow: {workflow_name}")
        
        workflow_class = self._workflow_registry[workflow_name]
        
        if workflow_name == "task_bulk":
            definition = workflow_class.create(
                task_ids=context.get("task_ids", []),
                operation=context.get("operation", "archive"),
            )
        else:
            definition = workflow_class.create()
        
        instance = await self.orchestrator.start_saga(definition, context)
        
        logger.info(
            "workflow.started",
            workflow=workflow_name,
            instance_id=instance.instance_id,
        )
        
        return instance
    
    async def get_workflow_status(self, instance_id: str) -> Optional[dict]:
        """Get status of a workflow instance."""
        instance = await self.orchestrator.get_instance(instance_id)
        if instance:
            return instance.to_dict()
        return None
    
    async def cancel_workflow(self, instance_id: str) -> bool:
        """Cancel a running workflow."""
        return await self.orchestrator.cancel_saga(instance_id)
    
    async def list_active_workflows(self) -> list:
        """List all active workflows."""
        return self.orchestrator.get_active_sagas()
    
    async def start(self) -> None:
        """Start workflow service."""
        await self.initialize()
        self._running = True
        
        logger.info("workflow_service.started")
        
        # Keep running
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(self.shutdown())
            )
        
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()
    
    async def shutdown(self) -> None:
        """Graceful shutdown."""
        if not self._running:
            return
        
        logger.info("workflow_service.shutting_down")
        self._running = False
        
        # Wait for running sagas to complete or timeout
        active = self.orchestrator.get_active_sagas()
        if active:
            logger.info(
                "workflow_service.waiting_for_sagas",
                count=len(active),
            )
            await asyncio.sleep(5)
        
        ObservabilityManager.shutdown()
        logger.info("workflow_service.stopped")


# Demo function
async def demo_workflows():
    """Demonstrate workflow execution."""
    app = WorkflowServiceApp()
    await app.initialize()
    
    # Demo 1: Task onboarding
    logger.info("=== Demo 1: Task Onboarding ===")
    
    context = {
        "task_id": "task-123",
        "title": "Implement user authentication",
        "description": "Set up JWT-based auth for the API gateway",
        "priority": "high",
        "assignee_id": "user-456",
        "created_by": "user-789",
        "timestamp": "2026-05-12T10:00:00Z",
    }
    
    instance = await app.start_workflow("task_onboarding", context)
    await asyncio.sleep(2)  # Wait for completion
    
    status = await app.get_workflow_status(instance.instance_id)
    logger.info("Workflow completed", status=status)
    
    # Demo 2: Task approval
    logger.info("=== Demo 2: Task Approval ===")
    
    context = {
        "task_id": "task-456",
        "reviewer_id": "user-100",
        "auto_approve": True,
    }
    
    instance = await app.start_workflow("task_approval", context)
    await asyncio.sleep(2)
    
    status = await app.get_workflow_status(instance.instance_id)
    logger.info("Approval workflow completed", status=status)
    
    # Demo 3: Bulk operation
    logger.info("=== Demo 3: Bulk Archive ===")
    
    context = {
        "task_ids": ["task-001", "task-002", "task-003"],
        "operation": "archive",
    }
    
    instance = await app.start_workflow("task_bulk", context)
    await asyncio.sleep(2)
    
    status = await app.get_workflow_status(instance.instance_id)
    logger.info("Bulk operation completed", status=status)
    
    await app.shutdown()


@click.group()
def cli():
    """Workflow Service CLI."""
    pass


@cli.command()
def serve():
    """Start Workflow Service."""
    app = WorkflowServiceApp()
    asyncio.run(app.start())


@cli.command()
def demo():
    """Run workflow demo."""
    asyncio.run(demo_workflows())


if __name__ == "__main__":
    cli()