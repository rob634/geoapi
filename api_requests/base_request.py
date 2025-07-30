import azure.functions as func
import hashlib
import json
import platform
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
from functools import wraps
from enum import Enum

from psycopg2 import sql

from api_clients import DatabaseClient
from utils import *

class OperationStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"

class BaseRequest:
    """Enhanced BaseRequest with comprehensive idempotent operation tracking"""
    
    def __init__(self, req: func.HttpRequest, use_json: bool = True):
        logger.debug("Initializing IdempotentBaseRequest")
        self.response = None
        self.env_dict = None
        self.content_type = None
        self.req_json = None
        self.req = req
        
        # Idempotency configuration
        self.operation_timeout_hours = 24
        self.enable_idempotency = True
        self.operation_id = None
        self.request_id = None
        
        try:
            logger.debug("Initializing DatabaseClient for operation tracking")
            self.log_db = DatabaseClient.from_vault()
            self._ensure_operations_table()
            logger.debug("DatabaseClient initialized")
            
        except Exception as e:
            message_out = f"Could not initialize DatabaseClient for logging: {e}"
            logger.critical(message_out)
            self.response = self.return_error(message_out)
            self.log_db = None
            self.enable_idempotency = False

        # Parse request content
        self._parse_request_content(use_json)
        
        logger.info("IdempotentBaseRequest initialized")

    def _parse_request_content(self, use_json: bool):
        """Parse request headers and JSON content"""
        try:
            logger.debug("Getting content type from request headers")
            self.content_type = self.req.headers.get("Content-Type")
            logger.debug(f"Content-Type: {self.content_type}")
            
        except Exception as e:
            self.content_type = None
            use_json = False
            message_out = f"Could not get content type from request headers {e}"
            logger.critical(message_out)
            self.response = self.return_error(message_out)
            
        if self.content_type and "application/json" in self.content_type:
            logger.debug("application/json found, parsing JSON")
            try:
                self.req_json = self.req.get_json()
                logger.debug(f"JSON: {self.req_json}")
                logger.info("Request JSON parsed successfully")
            except Exception as e:
                message_out = f"Error: could not get JSON from request {e}"
                logger.critical(message_out)
                self.response = self.return_error(message_out)
                self.req_json = None
                
        elif use_json:
            message_out = "Content-Type must be application/json"
            logger.error(message_out)
            self.response = self.return_error(message_out)
            
        else:
            logger.debug("Request initialized without JSON requirement")
            self.req_json = None

    def _ensure_operations_table(self):
        """Ensure the operations table exists with proper schema"""
        schema_name = HOSTING_SCHEMA_NAME
        table_name = OPERATIONS_TABLE_NAME
        
        try:
            self.log_db.query(
                sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                        operation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        operation_type VARCHAR(50) NOT NULL,
                        request_id VARCHAR(255) NOT NULL,
                        parameters JSONB,
                        operation_status VARCHAR(50) NOT NULL DEFAULT 'queued',
                        priority INTEGER DEFAULT 5,
                        started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        queued_at TIMESTAMP,
                        processing_started_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        expires_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP + INTERVAL '24 hours'),
                        result JSONB,
                        error_details JSONB,
                        retry_count INTEGER DEFAULT 0,
                        max_retries INTEGER DEFAULT 3,
                        logs JSONB,
                        published_services JSONB DEFAULT '[]'::jsonb,
                        CONSTRAINT unique_request UNIQUE (request_id, operation_type)
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_operations_status 
                        ON {schema_name}.{table_name}(operation_status);
                    CREATE INDEX IF NOT EXISTS idx_operations_type_status 
                        ON {schema_name}.{table_name}(operation_type, operation_status);
                    CREATE INDEX IF NOT EXISTS idx_operations_expires 
                        ON {schema_name}.{table_name}(expires_at);
                    CREATE INDEX IF NOT EXISTS idx_operations_request_id 
                        ON {schema_name}.{table_name}(request_id);
                """).format(
                    schema_name=sql.Identifier(schema_name),
                    table_name=sql.Identifier(table_name)
                )
            )
            logger.debug("Operations table schema ensured")
        except Exception as e:
            logger.error(f"Failed to ensure operations table: {e}")
            raise

    def idempotent_operation(self, 
                           operation_type: str,
                           operation_func: callable,
                           parameters: Dict[str, Any] = None,
                           force_new: bool = False) -> func.HttpResponse:
        """
        Decorator-like method for idempotent operations
        
        Args:
            operation_type: Type of operation (e.g., 'stage_raster', 'publish_vector')
            operation_func: Function to execute if operation is new
            parameters: Parameters for operation (defaults to self.req_json)
            force_new: Force new operation even if duplicate exists
        """
        if not self.enable_idempotency:
            logger.warning("Idempotency disabled, executing operation directly")
            return operation_func()
            
        parameters = parameters or self.req_json or {}
        
        # Generate deterministic request ID
        self.request_id = self._generate_request_id(operation_type, parameters)
        
        # Check for existing operation
        if not force_new:
            existing_operation = self._get_existing_operation(self.request_id, operation_type)
            if existing_operation:
                return self._handle_existing_operation(existing_operation, operation_type)
        
        # Start new operation
        self.operation_id = self._start_operation(self.request_id, operation_type, parameters)
        
        try:
            # Execute the operation
            logger.info(f"Executing new {operation_type} operation: {self.operation_id}")
            status_updated = self._update_operation_status(self.operation_id, OperationStatus.RUNNING)
            if not status_updated:
                logger.error(f"Idempotency error - Failed to update operation {self.operation_id} to running status")

            
            result = operation_func()
            
            # Extract result data for storage
            result_data = self._extract_result_data(result)
            
            # Mark operation as completed
            _completed = self._complete_operation(self.operation_id, OperationStatus.COMPLETED, result_data)
            if not _completed:
                logger.error(f"Idempotency error - failed to complete operation {self.operation_id}")

            return result
            
        except Exception as e:
            logger.error(f"Operation {self.operation_id} failed: {e}")
            error_details = {
                "error": str(e),
                "error_type": type(e).__name__,
                "timestamp": datetime.utcnow().isoformat()
            }
            _completed = self._complete_operation(self.operation_id, OperationStatus.FAILED, error_details)
            if not _completed:
                logger.error(f"Idempotency error - failed to complete operation {self.operation_id}")
            raise

    def _generate_request_id(self, operation_type: str, parameters: Dict[str, Any]) -> str:
        """Generate deterministic request ID for idempotency"""
        # Create a stable hash from operation type and parameters
        param_str = json.dumps(parameters or {}, sort_keys=True, default=str)
        hash_input = f"{operation_type}:{param_str}"
        request_id = hashlib.sha256(hash_input.encode()).hexdigest()
        
        logger.debug(f"Generated request_id: {request_id[:12]}... for {operation_type}")
        return request_id

    def _get_existing_operation(self, request_id: str, operation_type: str) -> Optional[Dict]:
        """Get existing operation by request_id and operation_type"""
        schema_name = HOSTING_SCHEMA_NAME
        table_name = OPERATIONS_TABLE_NAME
        
        try:
            result = self.log_db.query(
                sql.SQL("""
                    SELECT operation_id, operation_status, result, error_details, 
                           completed_at, expires_at, retry_count, published_services
                    FROM {schema_name}.{table_name} 
                    WHERE request_id = %s AND operation_type = %s
                    AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
                    ORDER BY started_at DESC
                    LIMIT 1
                """).format(
                    schema_name=sql.Identifier(schema_name),
                    table_name=sql.Identifier(table_name)
                ),
                [request_id, operation_type]
            )
            
            if result:
                operation = {
                    'operation_id': result[0][0],
                    'operation_status': result[0][1], 
                    'result': result[0][2],
                    'error_details': result[0][3],
                    'completed_at': result[0][4],
                    'expires_at': result[0][5],
                    'retry_count': result[0][6],
                    'published_services': result[0][7] or []
                }
                logger.debug(f"Found existing operation: {operation['operation_id']} with status: {operation['operation_status']}")
                return operation
                
        except Exception as e:
            logger.error(f"Error checking for existing operation: {e}")
            
        return None

    def _handle_existing_operation(self, operation: Dict, operation_type: str) -> func.HttpResponse:
        """Handle response for existing operations based on their status"""
        status = operation['operation_status']
        operation_id = operation['operation_id']
        
        if status == OperationStatus.COMPLETED.value:
            logger.info(f"Returning cached result for completed operation: {operation_id}")
            result_data = operation.get('result', {})
            
            # Add operation metadata to response
            if isinstance(result_data, dict):
                result_data.update({
                    'operation_id': str(operation_id),
                    'operation_status': 'completed',
                    'completed_at': operation.get('completed_at'),
                    'cached_result': True
                })
                
            return self.return_success(
                message=f"Operation completed (cached result)",
                json_out=result_data
            )
            
        elif status == OperationStatus.RUNNING.value:
            logger.info(f"Operation {operation_id} still in progress")
            return self.return_success(
                message=f"Operation in progress",
                json_out={
                    'operation_id': str(operation_id),
                    'operation_status': 'in_progress',
                    'started_at': operation.get('started_at')
                },
                code=202  # Accepted
            )
            
        elif status == OperationStatus.FAILED.value:
            logger.warning(f"Previous operation {operation_id} failed")
            retry_count = operation.get('retry_count', 0)
            max_retries = 3  # Could be configurable
            
            if retry_count < max_retries:
                logger.info(f"Retrying failed operation {operation_id} (attempt {retry_count + 1})")
                self._increment_retry_count(operation_id)
                return None  # Signal to execute new operation
            else:
                error_details = operation.get('error_details', {})
                return self.return_error(
                    message=f"Operation failed after {max_retries} retries",
                    json_out={
                        'operation_id': str(operation_id),
                        'operation_status': 'failed',
                        'error_details': error_details,
                        'max_retries_exceeded': True
                    },
                    code=422
                )
        
        return None  # Signal to execute new operation

    def _start_operation(self, request_id: str, operation_type: str, parameters: Dict) -> str:
        """Start a new operation and return operation_id"""
        schema_name = HOSTING_SCHEMA_NAME
        table_name = OPERATIONS_TABLE_NAME
        
        expires_at = datetime.utcnow() + timedelta(hours=self.operation_timeout_hours)
        
        try:
            with self.log_db.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql.SQL("""
                        INSERT INTO {schema_name}.{table_name} 
                            (request_id, operation_type, parameters, operation_status, 
                             queued_at, expires_at)
                        VALUES (%s, %s, %s::jsonb, %s, CURRENT_TIMESTAMP, %s)
                        RETURNING operation_id
                    """).format(
                        schema_name=sql.Identifier(schema_name),
                        table_name=sql.Identifier(table_name)
                    ),
                    [request_id, operation_type, json.dumps(parameters or {}), 
                     OperationStatus.QUEUED.value, expires_at]
                    )
                    operation_id = cursor.fetchone()[0]
                    conn.commit()
                    logger.info(f"Started new operation: {operation_id}")
                    return str(operation_id)
        except (TypeError, ValueError) as e:
            logger.error(f"Data serialization failed: {e}")
            raise  
        except Exception as e:
            logger.error(f"Failed to start operation: {e}")
            raise

    def _update_operation_status(self, operation_id: str, status: OperationStatus):
        """Update operation status"""
        schema_name = HOSTING_SCHEMA_NAME
        table_name = OPERATIONS_TABLE_NAME
        
        status_field_map = {
            OperationStatus.RUNNING: 'processing_started_at'
        }
        
        timestamp_field = status_field_map.get(status)
        
        if timestamp_field:
            query = sql.SQL("""
                UPDATE {schema_name}.{table_name} 
                SET operation_status = %s, {timestamp_field} = CURRENT_TIMESTAMP
                WHERE operation_id = %s
            """).format(
                schema_name=sql.Identifier(schema_name),
                table_name=sql.Identifier(table_name),
                timestamp_field=sql.Identifier(timestamp_field)
            )
        else:
            query = sql.SQL("""
                UPDATE {schema_name}.{table_name} 
                SET operation_status = %s
                WHERE operation_id = %s
            """).format(
                schema_name=sql.Identifier(schema_name),
                table_name=sql.Identifier(table_name)
            )
            
        try:
            self.log_db.query(query, [status.value, operation_id])
            logger.debug(f"Updated operation {operation_id} status to {status.value}")
            return True
        except Exception as e:
            logger.error(f"Idempotency error - Failed to update operation status: {e}")
            return False

    def _complete_operation(self, operation_id: str, status: OperationStatus, result_data: Dict):
        """Mark operation as completed with result data"""
        schema_name = HOSTING_SCHEMA_NAME
        table_name = OPERATIONS_TABLE_NAME
        
        try:
            if status == OperationStatus.FAILED:
                # For failed operations, store error in error_details
                self.log_db.query(sql.SQL("""
                    UPDATE {schema_name}.{table_name} 
                    SET operation_status = %s, 
                        completed_at = CURRENT_TIMESTAMP,
                        error_details = %s::jsonb
                    WHERE operation_id = %s
                """).format(
                    schema_name=sql.Identifier(schema_name),
                    table_name=sql.Identifier(table_name)
                ),
                [status.value, json.dumps(result_data), operation_id]
                )
            else:
                # For successful operations, store in result
                self.log_db.query(sql.SQL("""
                    UPDATE {schema_name}.{table_name} 
                    SET operation_status = %s, 
                        completed_at = CURRENT_TIMESTAMP,
                        result = %s::jsonb
                    WHERE operation_id = %s
                """).format(
                    schema_name=sql.Identifier(schema_name),
                    table_name=sql.Identifier(table_name)
                ),
                [status.value, json.dumps(result_data), operation_id]
                )
                
            logger.info(f"Completed operation {operation_id} with status {status.value}")
            
            return True
            
        except (TypeError, ValueError) as e:
            logger.error(f"Data serialization failed: {e}")
            return False

        except Exception as e:
            logger.error(f"Failed to complete operation: {e}")
            return False

    def _increment_retry_count(self, operation_id: str):
        """Increment retry count for failed operation"""
        schema_name = HOSTING_SCHEMA_NAME
        table_name = OPERATIONS_TABLE_NAME
        
        try:
            self.log_db.query(sql.SQL("""
                UPDATE {schema_name}.{table_name} 
                SET retry_count = retry_count + 1,
                    operation_status = 'queued'
                WHERE operation_id = %s
            """).format(
                schema_name=sql.Identifier(schema_name),
                table_name=sql.Identifier(table_name)
            ),
            [operation_id]
            )
        except Exception as e:
            logger.error(f"Failed to increment retry count: {e}")

    def _extract_result_data(self, result: func.HttpResponse) -> Dict:
        """Extract meaningful data from HTTP response for storage"""
        if not result:
            return {}
            
        try:
            # Try to parse JSON body
            if hasattr(result, 'get_body'):
                body = result.get_body()
                if body:
                    return json.loads(body.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
            
        # Fallback to basic response info
        return {
            'status_code': getattr(result, 'status_code', None),
            'timestamp': datetime.utcnow().isoformat()
        }

    def track_published_service(self, service_info: Dict):
        """Track ArcGIS services published during operation"""
        if not self.operation_id:
            logger.warning("No operation_id available for service tracking")
            return
            
        schema_name = HOSTING_SCHEMA_NAME
        table_name = OPERATIONS_TABLE_NAME
        
        try:
            self.log_db.query(sql.SQL("""
                UPDATE {schema_name}.{table_name} 
                SET published_services = published_services || %s::jsonb
                WHERE operation_id = %s
            """).format(
                schema_name=sql.Identifier(schema_name),
                table_name=sql.Identifier(table_name)
            ),
            [json.dumps(service_info), self.operation_id]
            )
            logger.info(f"Tracked published service for operation {self.operation_id}")
        except Exception as e:
            logger.error(f"Failed to track published service: {e}")

    def cleanup_expired_operations(self, days_old: int = 7):
        """Clean up old expired operations (call periodically)"""
        schema_name = HOSTING_SCHEMA_NAME
        table_name = OPERATIONS_TABLE_NAME
        
        try:
            result = self.log_db.query(sql.SQL("""
                DELETE FROM {schema_name}.{table_name}
                WHERE expires_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
                RETURNING operation_id
            """).format(
                schema_name=sql.Identifier(schema_name),
                table_name=sql.Identifier(table_name)
            ),
            [days_old]
            )
            
            if result:
                logger.info(f"Cleaned up {len(result)} expired operations")
        except Exception as e:
            logger.error(f"Failed to cleanup expired operations: {e}")

    # Keep existing methods from BaseRequest
    def return_error(self, message: str = None, code: int = 400, 
                    json_out: dict = None, headers: dict = None) -> func.HttpResponse:
        """Return error response with error message and code"""
        message = message if message else "General Error"
        logger.error(f"Returning error! - {message} \n code: {code}")

        return self.respond(
            code=code,
            json_out=json_out,
            message=message,
            headers=headers,
            origin="return_error",
        )

    def return_success(self, message: str = None, code: int = 200,
                      json_out: dict = None, headers: dict = None) -> func.HttpResponse:
        """Return success response with message and code"""
        message = message if message else "Success"
        logger.debug(f"Returning success - message: {message} \n code: {code}")

        return self.respond(
            code=code,
            json_out=json_out,
            message=message,
            headers=headers,
            origin="return_success",
        )

    def respond(self, code: int, json_out: dict = None, message: str = None,
               headers: dict = None, origin: str = None) -> func.HttpResponse:
        """Create HTTP response"""
        logger.debug(f"Creating func.HttpResponse for {origin}")
        json_body = dict()
        logger.flush_logger()
        
        if isinstance(headers, dict):
            headers["Content-Type"] = "application/json"
        else:
            headers = {"Content-Type": "application/json"}

        if isinstance(message, str):
            logger.debug(f"respond: Adding message to response: {message}")
            json_body["message"] = message

        if isinstance(json_out, dict):
            logger.debug(f"respond: Adding json_out to response")
            json_body["response"] = json_out  
        
        logger.flush_logger()
           
        if origin == "return_error":
            logger.debug(f"respond: Adding error context to response")
            if isinstance(self.req_json, dict):
                json_body["request_json"] = self.req_json
            json_body['error_log'] = log_list.log_messages
        
        try:
            body = json.dumps(json_body)
            logger.debug(f"Returning func.HttpResponse for {origin}")
            logger.flush_logger()
        
        except Exception as e:
            error_message = f"Could not create JSON body for response: {e}"
            logger.critical(error_message)
            code = 500
            body = json.dumps({"message": error_message, "log": log_list.log_messages})
            
        return func.HttpResponse(body=body, status_code=code, headers=headers)


# Maintain backward compatibility
#BaseRequest = IdempotentBaseRequest

class OldBaseRequest:
    def __init__(self, req: func.HttpRequest, use_json: bool = True):

        logger.debug("Initializing BaseRequest")
        self.response = None
        self.env_dict = None
        self.content_type = None
        self.req_json = None
        self.req = req
        
        try:
            logger.debug("Initializing DatabaseClient")
            self.log_db = DatabaseClient.from_vault()
            logger.debug("DatabaseClient initialized")
            
        except Exception as e:
            message_out = f"Could not initialize DatabaseClient for logging: {e}"
            logger.critical(message_out)
            self.response = self.return_error(message_out)
            self.log_db = None

        try:
            logger.debug("Getting content type from request headers")
            self.content_type = self.req.headers.get(
                "Content-Type"
            )  # look for application/json in headers
            logger.debug(f"Content-Type: {self.content_type}")
            
        except Exception as e:  # if headers are invalid, return error response
            self.content_type = None
            use_json = False
            message_out = f"Could not get content type from request headers {e}"
            logger.critical(message_out)
            
            self.response = self.return_error(message_out)
            
        if self.content_type and "application/json" in self.content_type:
            logger.debug("application/json found, parsing JSON")
            try:
                self.req_json = self.req.get_json()
                logger.debug(f"JSON: {self.req_json}")

                logger.info("BaseRequest initialized")
            except Exception as e:
                message_out = f"Error: could not get JSON from request {e}"

                logger.critical(message_out)
                self.response = self.return_error(message_out)
                self.req_json = None
                
        elif use_json:
            message_out = "Content-Type must be application/json"
            logger.error(message_out)
            self.response = self.return_error(message_out)
            
        else:
            logger.debug("BaseRequest initialized without JSON in request")
            self.req_json = None
        

    def get_environment_info(self):
        # Check the operating system
        try:
            self.env_dict = {
                "os_name": os.name,
                "current_working_directory": os.getcwd(),
                "environment_variables": os.environ,
                "cpu_count": os.cpu_count(),
                "platform_system": platform.system(),
                "platform_release": platform.release(),
                "python_version": sys.version,
                "python_version_info": sys.version_info
            }
            logger.debug(f"Environment: {self.env_dict} ")
        except Exception as e:
            logger.warning(f"Error: could not get environment info {e}")
            self.env_dict = None

        return self.env_dict