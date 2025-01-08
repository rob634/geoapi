import azure.functions as func
from datetime import datetime

from .base_request import BaseRequest
from api_clients import StorageHandler
from utils import *

class UploadRequest(BaseRequest):

    def __init__(self, req: func.HttpRequest):

        logger.debug('Initializing UploadRequest')
        super().__init__(req=req,use_json=False)
        self.default_container = DEFAULT_WORKSPACE_CONTAINER
        self.file = None
        self.env = 'active'

        if self.content_type and 'multipart/form-data' in self.content_type:
            self.response = self.handle_file_request()
        else:
            self.response = self.return_error(f'multipart/form-data missing from request')
   
    def handle_file_request(self,container:str=None) -> func.HttpResponse:
        logger.debug('Handling file upload request')
        container = container if container else self.default_container

        try:
            self.file = self.req.files.get('file')
        except Exception as e:
            message = f'Could not get file from request: {e}'
            return self.return_error(message)

        try:
            storage = StorageHandler(workspace_container_name=container)
        except Exception as e:
            return self.return_error(f'Could not instantiate storage handler: {e}')
        
        logger.debug(
            f'Uploading file: {self.file.filename} to container: {container}')  
        
        try:
            result = storage.upload_blob_data(
                blob_data=self.file,
                dest_blob_name=self.file.filename,
                dest_container_name=container
                )            
        except Exception as e:
            message = f'Unhandled error during file upload: {e}'
            return self.return_error(message)
        
        return self.return_success(message=f'Result: {result} uploaded to {container}')
