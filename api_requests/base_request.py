import azure.functions as func
import hashlib
import json
import platform
import os
import sys

from psycopg2 import sql

from api_clients import DatabaseClient
from utils import *


class BaseRequest:
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
############################################################
# In base_request.py
def return_exception(self, e: Exception, message: str = None, context: Dict[str, Any] = None):
    """Enhanced exception handling with proper HTTP status mapping."""
    
    # HTTP status code mapping
    status_mapping = {
        ValueError: 422,
        FileNotFoundError: 404,
        PermissionError: 403,
        DatabaseClientError: 500,
        StorageHandlerError: 502,
        VectorHandlerError: 422,
        RasterHandlerError: 422,
        EnterpriseClientError: 502,
        GeoprocessingError: 500,
        InvalidGeometryError: 422,
        ChimeraBaseException: 500,
    }
    
    # Determine status code
    status_code = 500  # default
    for exc_type, code in status_mapping.items():
        if isinstance(e, exc_type):
            status_code = code
            break
    
    # Handle Chimera exceptions specially
    if isinstance(e, ChimeraBaseException):
        error_response = e.to_dict()
        if message:
            error_response['additional_context'] = message
        if context:
            error_response['context'].update(context)
    else:
        # Wrap non-Chimera exceptions
        error_response = {
            'error_code': type(e).__name__,
            'message': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }
        if message:
            error_response['context'] = message
    
    return self.return_error(
        message=error_response['message'],
        code=status_code,
        json_out=error_response
    )    
    
#########################################################    
    def return_exception(self,e,message=None):
        exception_mapping = {
            ValueError: ("Invalid parameter configuration", 422),
            StorageHandlerError: ("StorageHandlerError", 500),
            VectorHandlerError: ("VectorHandlerError", 500),
            DatabaseClientError: ("DatabaseClientError", 500),
            EnterpriseClientError: ("EnterpriseClientError", 500),
            GeoprocessingError: ("GeoprocessingError", 500),
            AzureError: ("AzureError", 404),
        }
            # Default message and code for general exceptions
        default_message = "General Error"
        default_code = 500

        # Get the message and code for the exception type
        exception_type = type(e)
        error_message, error_code = exception_mapping.get(
            exception_type, (default_message, default_code)
        )

        # Format the error message
        full_error_message = f"{error_message}: {message} - {e}"
        logger.critical(full_error_message)
        
        return self.return_error(message=full_error_message,code=error_code)
        
    def return_error(
        self,
        message: str = None,
        code: int = 400,
        json_out: dict = None,
        headers: dict = None,

    ) -> func.HttpResponse:

        # Return error response with error message and code
        message = message if message else "General Error"
        logger.error(f"Returning error! - {message} \n code: {code}")

        return self.respond(
            code=code,
            json_out=json_out,
            message=message,
            headers=headers,
            origin="return_error",
        )

    def return_success(
        self,
        message: str = None,
        code: int = 200,
        json_out: dict = None,
        headers: dict = None,
    ) -> func.HttpResponse:
        # Return success response with message and code
        message = message if message else "Success"

        logger.debug(f"Returning success - message: {message} \n code: {code}")

        return self.respond(
            code=code,
            json_out=json_out,
            message=message,
            headers=headers,
            origin="return_success",
        )


    def respond(
        self,
        code: int,
        json_out: dict = None,
        message: str = None,
        headers: dict = None,
        origin: str = None,
    ) -> func.HttpResponse:
        # invoked by return_error or return_success to return HttpResponse object to main function_app.py

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
            logger.debug(f"respond: Adding json_out to response: {json_out}")
            json_body["response"] = json_out  
        
        logger.flush_logger()
           
        if origin == "return_error":
            logger.debug(f"respond: Adding error to response: {json_out}")
            if isinstance(self.req_json, dict):
                json_body["request_json"] = self.req_json
            if isinstance(self.env_dict, dict):
                json_body["environment"] = self.env_dict
            json_body['error_log'] = log_list.log_messages
        
        try:
            body = json.dumps(json_body)
            logger.debug(f"Returning func.HttpResponse for {origin} with body: {body}")
            logger.flush_logger()
        
        except Exception as e:
            error_message = f"Could not create JSON body for error response: {e}"
            logger.critical(error_message)
            code=500

            try:
                invalid_json = str(json_body)
            except Exception as e:
                invalid_json = f"Could not convert json_body to string: {e}"
                logger.error(invalid_json)
                
            body = json.dumps(
                    {"message": error_message, 
                     "invalid_json": invalid_json,
                     "log": log_list.log_messages}
                )
            
        return func.HttpResponse(body=body, status_code=code, headers=headers)
    
    def _operations_table(self):
        schema_name=HOSTING_SCHEMA_NAME
        table_name=OPERATIONS_TABLE_NAME
        
        self.log_db.query(
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    operation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    operation_type VARCHAR(50) NOT NULL,
                    request_id VARCHAR(255) NOT NULL,
                    parameters JSONB,
                    operation_status VARCHAR(50) NOT NULL DEFAULT 'running',
                    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    result JSONB,
                    logs JSONB,
                    CONSTRAINT unique_request UNIQUE (request_id, operation_type)
                );""").format(
                    schema_name=sql.Identifier(schema_name),
                    table_name=sql.Identifier(table_name)
                )
        )
    
    def _generate_request_id(self, operation_type, parameters:dict=None):

        param_str = json.dumps(parameters or {}, sort_keys=True)
        hash_input = f"{operation_type}:{param_str}"
        
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    def _get_existing_operation(self, request_id, operation_type):
        schema_name=HOSTING_SCHEMA_NAME
        table_name=OPERATIONS_TABLE_NAME    
        return self.log_db.query(
            """
                SELECT operation_id, operation_status, result
                FROM {schema_name}.{table_name} 
                WHERE request_id = %s AND operation_type = %s
            """.format(
                    schema_name=schema_name,
                    table_name=table_name),
            [request_id, operation_type]
        )
    
    def _start_operation(self, request_id, operation_type, parameters):
        schema_name=HOSTING_SCHEMA_NAME
        table_name=OPERATIONS_TABLE_NAME
        with self.log_db.connect() as conn:
            with conn.cursor() as cursor:  
                cursor.execute("""
                        INSERT INTO {schema_name}.{table_name} 
                            (request_id, operation_type, parameters, operation_status)
                        VALUES (%s, %s, %s, 'running')
                        RETURNING operation_id
                    """.format(
                        schema_name=schema_name,
                        table_name=table_name),
                    [request_id,
                    operation_type,
                    json.dumps(parameters or {})]
                )
                operation_id = cursor.fetchone()[0]
                conn.commit()
                logger.debug(f"Started operation with ID: {operation_id}")
                
        return operation_id
    
    def _complete_operation(self, operation_id, operation_status, result=None):
        schema_name=HOSTING_SCHEMA_NAME
        table_name=OPERATIONS_TABLE_NAME
        self.log_db.query("""
                UPDATE {schema_name}.{table_name} 
                SET operation_status = %s, 
                    completed_at = CURRENT_TIMESTAMP,
                    result = %s
                WHERE operation_id = %s
            """.format(
                    schema_name=schema_name,
                    table_name=table_name),
            [operation_status, json.dumps(result or {}), operation_id]
        )
    
    def _format_response(self, operation):
        """Format consistent response from operation record"""
        if operation['operation_status'] == 'running':
            return {
                'operation_status': 'in_progress',
                'operation_id': operation['operation_id']
            }
        elif operation['operation_status'] == 'failed':
            return {
                'operation_status': 'failed',
                'operation_id': operation['operation_id'], 
                'details': operation['result'].get('error')
            }
        else:
            # For success, merge the stored result with standard fields
            result = operation['result'] or {}
            result.update({
                'operation_status': 'success',
                'operation_id': operation['operation_id']
            })
            return result
