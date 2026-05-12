"""
Pre-built saga steps for common operations.
"""
from typing import Any, Callable, Dict, Optional
import asyncio
import json
import structlog

from .saga import SagaStep

logger = structlog.get_logger(__name__)


class GrpcStep(SagaStep):
    """Saga step that calls a gRPC service."""
    
    def __init__(
        self,
        name: str,
        stub_method: Callable,
        request_factory: Callable[[Dict[str, Any]], Any],
        compensation_stub_method: Optional[Callable] = None,
        compensation_request_factory: Optional[Callable[[Dict[str, Any]], Any]] = None,
        **kwargs,
    ):
        async def action(context: Dict[str, Any]) -> Dict[str, Any]:
            request = request_factory(context)
            response = await stub_method(request)
            return self._response_to_dict(response)
        
        compensation = None
        if compensation_stub_method:
            async def compensation(context: Dict[str, Any]) -> None:
                request = compensation_request_factory(context) if compensation_request_factory else request_factory(context)
                await compensation_stub_method(request)
        
        super().__init__(
            name=name,
            action=action,
            compensation=compensation,
            **kwargs,
        )
    
    def _response_to_dict(self, response) -> Dict[str, Any]:
        """Convert protobuf response to dict."""
        try:
            # For protobuf messages
            from google.protobuf.json_format import MessageToDict
            return MessageToDict(response)
        except Exception:
            return {"response": str(response)}


class KafkaPublishStep(SagaStep):
    """Saga step that publishes an event to Kafka."""
    
    def __init__(
        self,
        name: str,
        topic: str,
        event_factory: Callable[[Dict[str, Any]], Dict[str, Any]],
        event_publisher: Any,
        **kwargs,
    ):
        async def action(context: Dict[str, Any]) -> Dict[str, Any]:
            event = event_factory(context)
            await event_publisher.publish(topic=topic, event=event)
            return {"published_event": event.get("event_id")}
        
        super().__init__(name=name, action=action, is_compensatable=False, **kwargs)


class HttpStep(SagaStep):
    """Saga step that makes an HTTP request."""
    
    def __init__(
        self,
        name: str,
        url: str,
        method: str = "POST",
        body_factory: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        headers: Optional[Dict[str, str]] = None,
        compensation_url: Optional[str] = None,
        compensation_method: str = "POST",
        **kwargs,
    ):
        import aiohttp
        
        async def action(context: Dict[str, Any]) -> Dict[str, Any]:
            async with aiohttp.ClientSession() as session:
                request_body = body_factory(context) if body_factory else context
                
                async with session.request(
                    method=method,
                    url=url,
                    json=request_body,
                    headers=headers or {},
                ) as response:
                    response.raise_for_status()
                    return await response.json()
        
        compensation = None
        if compensation_url:
            async def compensation(context: Dict[str, Any]) -> None:
                async with aiohttp.ClientSession() as session:
                    async with session.request(
                        method=compensation_method,
                        url=compensation_url,
                        json=context,
                    ) as response:
                        response.raise_for_status()
        
        super().__init__(
            name=name,
            action=action,
            compensation=compensation,
            **kwargs,
        )


class ConditionalStep(SagaStep):
    """Saga step that conditionally executes based on context."""
    
    def __init__(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        step: SagaStep,
        else_step: Optional[SagaStep] = None,
        **kwargs,
    ):
        async def action(context: Dict[str, Any]) -> Dict[str, Any]:
            if condition(context):
                return await step.execute(context)
            elif else_step:
                return await else_step.execute(context)
            return {}
        
        compensation = step.compensation if step.is_compensatable else None
        
        super().__init__(
            name=name,
            action=action,
            compensation=compensation,
            **kwargs,
        )


class ParallelStep(SagaStep):
    """Saga step that executes multiple steps in parallel."""
    
    def __init__(
        self,
        name: str,
        steps: list,
        **kwargs,
    ):
        async def action(context: Dict[str, Any]) -> Dict[str, Any]:
            tasks = [
                step.execute(context.copy())
                for step in steps
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            combined_result = {}
            errors = []
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    errors.append({
                        "step": steps[i].name,
                        "error": str(result),
                    })
                elif isinstance(result, dict):
                    combined_result.update(result)
            
            if errors:
                raise RuntimeError(f"Parallel step failed: {json.dumps(errors)}")
            
            return combined_result
        
        async def compensation(context: Dict[str, Any]) -> None:
            # Compensate all steps in parallel
            comp_tasks = [
                step.compensate(context.copy())
                for step in steps
                if step.is_compensatable and step.compensation
            ]
            if comp_tasks:
                await asyncio.gather(*comp_tasks, return_exceptions=True)
        
        super().__init__(
            name=name,
            action=action,
            compensation=compensation,
            **kwargs,
        )