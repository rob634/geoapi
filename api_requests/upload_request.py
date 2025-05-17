import azure.functions as func
from datetime import datetime
import os

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
            file_data = self.req.files.get('file')
        except Exception as e:
            message = f'Could not get file from request: {e}'
            return self.return_error(message)

        if not file_data:
            message = 'No file data found in request'
            return self.return_error(message)
        
        try:
            storage = StorageHandler(workspace_container_name=container)
        except Exception as e:
            return self.return_error(f'Could not instantiate storage handler: {e}')
        
        if hasattr(file_data, 'filename'):
            file_name = os.path.basename(getattr(file_data, 'filename'))
            logger.info(f'File name: {file_name}')
        else:
            return self.return_error('File name not found in request')
        
        logger.debug(
            f'Uploading file: {file_name} to container: {container}')  
        
        try:
            result = storage.upload_blob_data(
                blob_data=file_data,
                dest_blob_name=file_name,
                container_name=container,
                overwrite=True
                )            
        except Exception as e:
            message = f'Unhandled error during file upload: {e}'
            return self.return_error(message)
        
        return self.return_success(message=f'Result: {result} uploaded to {container}')
