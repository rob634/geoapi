# api_clients/queue_handler.py
from azure.storage.queue import QueueClient
from azure.identity import DefaultAzureCredential
import json
import base64
from typing import Dict, Any, Optional
from utils import logger
import azure.functions as func
import hashlib
import json
import platform
import os
import sys
from datetime import datetime
from functools import wraps

from psycopg2 import sql

from api_clients import DatabaseClient
from environment import STORAGE_ACCOUNT_NAME, TASK_QUEUE_NAME
from vector_api import VectorHandler
class QueueHandler:
    def __init__(self, account_name: str, queue_name: str):
        self.account_url = f"https://{account_name}.queue.core.windows.net"
        self.credential = DefaultAzureCredential()
        self.queue_client = QueueClient(
            account_url=self.account_url,
            queue_name=queue_name,
            credential=self.credential
        )
        
    def enqueue_task(self, task_type: str, parameters: Dict[Any, Any], 
                     priority: int = 5) -> str:
        """Enqueue task with metadata for queue processing"""
        task_message = {
            "task_type": task_type,
            "parameters": parameters,
            "priority": priority,
            "created_at": datetime.utcnow().isoformat()
        }
        
        message_content = base64.b64encode(
            json.dumps(task_message).encode('utf-8')
        ).decode('utf-8')
        
        message = self.queue_client.send_message(
            content=message_content,
            visibility_timeout=0
        )
        
        logger.info(f"Enqueued {task_type} task with message_id: {message.id}")
        return message.id
    
#Enhanced BaseRequest with Queue Integration
class BaseRequest:
    def __init__(self, req: func.HttpRequest, use_json: bool = True):
        # ... existing initialization ...
        
        # Add queue handler for async processing
        self.task_queue = QueueHandler(
            account_name=STORAGE_ACCOUNT_NAME,
            queue_name=TASK_QUEUE_NAME
        )
    
    def enqueue_for_processing(self, task_type: str, parameters: Dict = None) -> str:
        """Enqueue task and return immediately with operation tracking"""
        
        # Generate deterministic request ID
        request_id = self._generate_request_id(task_type, parameters)
        
        # Check if operation already exists
        existing_op = self._get_existing_operation(request_id, task_type)
        if existing_op:
            status = existing_op[0]['operation_status']
            if status == 'completed':
                return self.respond(f"Operation {request_id} already completed", 
                                  existing_op[0]['result'])
            elif status == 'running':
                return self.respond(f"Operation {request_id} already in progress")
        
        # Start operation tracking
        operation_id = self._start_operation(request_id, task_type, parameters)
        
        # Enqueue task with operation metadata
        enhanced_params = {
            **parameters,
            "operation_id": str(operation_id),
            "request_id": request_id
        }
        
        message_id = self.task_queue.enqueue_task(task_type, enhanced_params)
        
        return self.respond(
            f"Task queued successfully",
            {
                "operation_id": str(operation_id),
                "request_id": request_id,
                "message_id": message_id,
                "status": "queued"
            }
        )
    #Servie Tracking Integration
    # Add to BaseRequest class
    def track_published_service(self, operation_id: str, service_info: Dict):
        """Track ArcGIS services published during operation"""
        self.log_db.query("""
            UPDATE hosting.operations 
            SET published_services = COALESCE(published_services, '[]'::jsonb) || %s::jsonb
            WHERE operation_id = %s
        """, [json.dumps(service_info), operation_id])

    def get_operation_services(self, operation_id: str) -> List[Dict]:
        """Get all services published for an operation"""
        result = self.log_db.query("""
            SELECT published_services FROM hosting.operations 
            WHERE operation_id = %s
        """, [operation_id])
        
        if result and result[0]['published_services']:
            return json.loads(result[0]['published_services'])
        return []

#2. Queue-Triggered Function Implementation
#Add to function_app.py
# function_app.py (additions)
@app.queue_trigger(arg_name="msg", queue_name="geospatial-tasks",
                   connection="AzureWebJobsStorage")
def process_queued_task(msg: func.QueueMessage) -> None:
    """Queue trigger to process batched geospatial tasks"""
    logger.info(f"Processing queue message: {msg.id}")
    
    try:
        # Decode message
        message_content = base64.b64decode(msg.get_body()).decode('utf-8')
        task_data = json.loads(message_content)
        
        task_type = task_data['task_type']
        parameters = task_data['parameters']
        operation_id = parameters.get('operation_id')
        
        # Route to appropriate processor
        processor = TaskProcessor()
        result = processor.process_task(task_type, parameters)
        
        # Update operation status
        if operation_id:
            db_client = DatabaseClient.from_vault()
            processor._complete_operation_queue(
                db_client, operation_id, 'completed', result
            )
            
        logger.info(f"Completed task {task_type} for operation {operation_id}")
        
    except Exception as e:
        logger.error(f"Error processing queue message {msg.id}: {e}")
        if operation_id:
            db_client = DatabaseClient.from_vault()
            processor._complete_operation_queue(
                db_client, operation_id, 'failed', {"error": str(e)}
            )
        raise
    
    #Task Processor Class
# api_requests/task_processor.py
class TaskProcessor(BaseRequest):
    def __init__(self):
        # Initialize without HTTP request for queue processing
        self.log_db = DatabaseClient.from_vault()
    
    def process_task(self, task_type: str, parameters: Dict) -> Dict:
        """Route tasks to appropriate processors"""
        
        processors = {
            'vector_processing': self._process_vector_task,
            'raster_processing': self._process_raster_task,
            'service_publishing': self._process_publishing_task,
            'storage_operation': self._process_storage_task
        }
        
        if task_type not in processors:
            raise ValueError(f"Unknown task type: {task_type}")
            
        return processors[task_type](parameters)
    
    def _process_vector_task(self, params: Dict) -> Dict:
        """Process vector data asynchronously"""
        handler = VectorHandler()
        # Use existing vector processing logic
        return handler.process_vector_data(**params)
    
    # Similar methods for other task types...
    
#5. Monitoring and Status Endpoints
@app.route(route='operation_status', methods=['GET'])
def operation_status(req: func.HttpRequest) -> func.HttpResponse:
    """Get status of queued operations"""
    operation_id = req.params.get('operation_id')
    request_id = req.params.get('request_id')
    
    handler = BaseRequest(req)
    
    if operation_id:
        status = handler._get_operation_by_id(operation_id)
    elif request_id:
        status = handler._get_operation_by_request_id(request_id)
    else:
        return handler.return_error("operation_id or request_id required")
    
    return handler.respond("Operation status retrieved", status)
'''
    This implementation provides:

    Decoupled Processing: HTTP triggers immediately queue tasks and return, avoiding timeouts
    Enhanced Idempotency: Comprehensive operation tracking with service management
    Scalability: Queue-based processing allows independent scaling
    Reliability: Retry mechanisms and detailed error tracking
    Monitoring: Operation status endpoints for progress tracking

    The queue pattern allows you to handle large files without HTTP timeouts while maintaining full idempotency and comprehensive job tracking.
'''