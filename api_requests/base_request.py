import azure.functions as func
import json
import platform
import os
import sys

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
