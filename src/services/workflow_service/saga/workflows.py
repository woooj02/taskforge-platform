"""
Pre-defined saga workflows for task orchestration.
Implements complex business processes using the saga pattern.
"""
from typing import Dict, Any, Optional
import structlog
from libs.saga_framework.saga import SagaDefinition, SagaStep
from libs.saga_framework.step import GrpcStep, KafkaPublishStep, HttpStep

logger = structlog.get_logger(__name__)


class TaskOnboardingWorkflow:
    """
    Saga workflow for onboarding a new task with notifications.
    
    Steps:
    1. Create task in task service
    2. Notify assignee (if assigned)
    3. Create audit log entry
    4. Publish task.created event
    
    Compensation (on failure):
    1. Delete task
    2. Send cancellation notification
    """
    
    @staticmethod
    def create() -> SagaDefinition:
        """Build the task onboarding saga definition."""
        saga = SagaDefinition(
            name="task_onboarding",
        )
        
        # Step 1: Create task
        create_task_step = SagaStep(
            name="create_task",
            action=TaskOnboardingWorkflow._create_task,
            compensation=TaskOnboardingWorkflow._delete_task,
            retry_count=3,
            retry_delay_seconds=2.0,
            timeout_seconds=30.0,
        )
        saga.add_step(create_task_step)
        
        # Step 2: Notify assignee (only if assignee exists)
        notify_step = SagaStep(
            name="notify_assignee",
            action=TaskOnboardingWorkflow._notify_assignee,
            compensation=TaskOnboardingWorkflow._send_cancellation_notification,
            retry_count=2,
            retry_delay_seconds=1.0,
            timeout_seconds=15.0,
        )
        saga.add_step(notify_step)
        
        # Step 3: Create audit log
        audit_step = SagaStep(
            name="create_audit_log",
            action=TaskOnboardingWorkflow._create_audit_log,
            is_compensatable=False,  # Audit logs are append-only
            retry_count=1,
            timeout_seconds=10.0,
        )
        saga.add_step(audit_step)
        
        # Step 4: Publish event
        publish_step = KafkaPublishStep(
            name="publish_task_created_event",
            topic="task.events",
            event_factory=TaskOnboardingWorkflow._build_task_created_event,
            event_publisher=None,  # Injected at runtime
        )
        saga.add_step(publish_step)
        
        return saga
    
    @staticmethod
    async def _create_task(context: Dict[str, Any]) -> Dict[str, Any]:
        """Create task via gRPC call."""
        logger.info("workflow.create_task", context=context.get("task_id"))
        
        # In production, this calls the Task Service gRPC
        task = {
            "id": context.get("task_id", ""),
            "title": context.get("title", ""),
            "description": context.get("description", ""),
            "priority": context.get("priority", "medium"),
            "status": "backlog",
            "assignee_id": context.get("assignee_id"),
            "created_by": context.get("created_by", "system"),
            "version": 1,
        }
        
        logger.info("workflow.task_created", task_id=task["id"])
        return {"task": task}
    
    @staticmethod
    async def _notify_assignee(context: Dict[str, Any]) -> Dict[str, Any]:
        """Send notification to task assignee."""
        assignee_id = context.get("assignee_id")
        
        if not assignee_id:
            logger.info("workflow.no_assignee_to_notify")
            return {"notification_sent": False}
        
        task = context.get("task", {})
        
        # In production, this calls Notification Service
        notification = {
            "recipient_id": assignee_id,
            "type": "task_assigned",
            "title": f"New task assigned: {task.get('title', 'Untitled')}",
            "body": task.get("description", ""),
            "priority": task.get("priority", "medium"),
        }
        
        logger.info(
            "workflow.assignee_notified",
            assignee_id=assignee_id,
            task_id=task.get("id"),
        )
        
        return {"notification_sent": True, "notification": notification}
    
    @staticmethod
    async def _create_audit_log(context: Dict[str, Any]) -> Dict[str, Any]:
        """Create audit log entry."""
        task = context.get("task", {})
        
        audit_entry = {
            "action": "task_created",
            "entity_type": "task",
            "entity_id": task.get("id", ""),
            "user_id": context.get("created_by", "system"),
            "details": {
                "title": task.get("title"),
                "priority": task.get("priority"),
            },
        }
        
        logger.info("workflow.audit_log_created", entity_id=audit_entry["entity_id"])
        return {"audit_entry": audit_entry}
    
    @staticmethod
    async def _build_task_created_event(context: Dict[str, Any]) -> Dict[str, Any]:
        """Build task created event for Kafka."""
        task = context.get("task", {})
        
        return {
            "event_id": context.get("saga_instance_id", ""),
            "event_type": "task.created",
            "aggregate_id": task.get("id", ""),
            "version": 1,
            "data": {
                "title": task.get("title", ""),
                "description": task.get("description", ""),
                "priority": task.get("priority", "medium"),
                "assignee_id": task.get("assignee_id"),
                "created_by": task.get("created_by", "system"),
            },
        }
    
    # Compensation handlers
    @staticmethod
    async def _delete_task(context: Dict[str, Any]) -> None:
        """Compensation: Delete the created task."""
        task = context.get("task", {})
        logger.warning(
            "workflow.compensation.delete_task",
            task_id=task.get("id"),
        )
        # In production, calls Task Service to delete
    
    @staticmethod
    async def _send_cancellation_notification(context: Dict[str, Any]) -> None:
        """Compensation: Send cancellation notification."""
        assignee_id = context.get("assignee_id")
        if assignee_id:
            logger.warning(
                "workflow.compensation.cancel_notification",
                assignee_id=assignee_id,
            )


class TaskApprovalWorkflow:
    """
    Saga workflow for task approval process.
    
    Steps:
    1. Submit task for review
    2. Wait for approval (simulated)
    3. Update task status to approved
    4. Notify stakeholders
    
    Compensation:
    1. Revert task status
    2. Send rejection notification
    """
    
    @staticmethod
    def create() -> SagaDefinition:
        saga = SagaDefinition(name="task_approval")
        
        # Step 1: Submit for review
        submit_step = SagaStep(
            name="submit_for_review",
            action=TaskApprovalWorkflow._submit_for_review,
            compensation=TaskApprovalWorkflow._revert_to_previous_status,
            retry_count=3,
            timeout_seconds=20.0,
        )
        saga.add_step(submit_step)
        
        # Step 2: Process approval
        approval_step = SagaStep(
            name="process_approval",
            action=TaskApprovalWorkflow._process_approval,
            compensation=TaskApprovalWorkflow._reject_approval,
            retry_count=1,
            timeout_seconds=60.0,  # Longer timeout for human approval
        )
        saga.add_step(approval_step)
        
        # Step 3: Update task status
        update_step = SagaStep(
            name="update_task_status",
            action=TaskApprovalWorkflow._update_to_approved,
            compensation=TaskApprovalWorkflow._revert_status,
            retry_count=3,
            timeout_seconds=15.0,
        )
        saga.add_step(update_step)
        
        # Step 4: Notify stakeholders
        notify_step = SagaStep(
            name="notify_stakeholders",
            action=TaskApprovalWorkflow._notify_stakeholders,
            is_compensatable=False,
            retry_count=2,
            timeout_seconds=10.0,
        )
        saga.add_step(notify_step)
        
        return saga
    
    @staticmethod
    async def _submit_for_review(context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("workflow.submit_for_review", task_id=context.get("task_id"))
        return {"review_submitted": True, "submitted_at": context.get("timestamp")}
    
    @staticmethod
    async def _process_approval(context: Dict[str, Any]) -> Dict[str, Any]:
        """Process approval decision."""
        # Simulated - in production would integrate with approval system
        approved = context.get("auto_approve", True)
        
        if not approved:
            raise RuntimeError("Task was rejected during approval")
        
        logger.info("workflow.approval_processed", task_id=context.get("task_id"))
        return {"approved": True, "approved_by": context.get("reviewer_id", "system")}
    
    @staticmethod
    async def _update_to_approved(context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("workflow.update_to_approved", task_id=context.get("task_id"))
        return {"status_updated": True, "new_status": "approved"}
    
    @staticmethod
    async def _notify_stakeholders(context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info("workflow.notify_stakeholders", task_id=context.get("task_id"))
        return {"stakeholders_notified": True}
    
    # Compensation handlers
    @staticmethod
    async def _revert_to_previous_status(context: Dict[str, Any]) -> None:
        logger.warning("workflow.compensation.revert_to_previous_status")
    
    @staticmethod
    async def _reject_approval(context: Dict[str, Any]) -> None:
        logger.warning("workflow.compensation.reject_approval")
    
    @staticmethod
    async def _revert_status(context: Dict[str, Any]) -> None:
        logger.warning("workflow.compensation.revert_status")


class TaskBulkOperationWorkflow:
    """
    Saga workflow for bulk task operations.
    Demonstrates parallel step execution.
    """
    
    @staticmethod
    def create(task_ids: list, operation: str) -> SagaDefinition:
        from libs.saga_framework.step import ParallelStep
        
        saga = SagaDefinition(name=f"task_bulk_{operation}")
        
        # Create individual steps for each task
        individual_steps = []
        
        for task_id in task_ids:
            step = SagaStep(
                name=f"process_{task_id}",
                action=lambda ctx, tid=task_id: TaskBulkOperationWorkflow._process_task(tid, operation, ctx),
                retry_count=2,
                timeout_seconds=30.0,
            )
            individual_steps.append(step)
        
        # Execute all task operations in parallel
        parallel_step = ParallelStep(
            name=f"bulk_{operation}",
            steps=individual_steps,
        )
        saga.add_step(parallel_step)
        
        # Final summary step
        summary_step = SagaStep(
            name="generate_summary",
            action=TaskBulkOperationWorkflow._generate_summary,
            retry_count=1,
            timeout_seconds=10.0,
        )
        saga.add_step(summary_step)
        
        return saga
    
    @staticmethod
    async def _process_task(task_id: str, operation: str, context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(
            "workflow.bulk.process_task",
            task_id=task_id,
            operation=operation,
        )
        
        if operation == "archive":
            result = {"task_id": task_id, "archived": True}
        elif operation == "delete":
            result = {"task_id": task_id, "deleted": True}
        elif operation == "reassign":
            result = {
                "task_id": task_id,
                "reassigned": True,
                "new_assignee": context.get("new_assignee_id"),
            }
        else:
            result = {"task_id": task_id, "processed": True}
        
        return {f"result_{task_id}": result}
    
    @staticmethod
    async def _generate_summary(context: Dict[str, Any]) -> Dict[str, Any]:
        results = {k: v for k, v in context.items() if k.startswith("result_")}
        
        summary = {
            "total_processed": len(results),
            "succeeded": sum(1 for r in results.values() 
                           if isinstance(r, dict) and r.get("processed", False) or r.get("archived", False) or r.get("deleted", False)),
            "failed": 0,
        }
        
        logger.info("workflow.bulk.summary", **summary)
        return {"batch_summary": summary}