import azure.functions as func

from api_clients import StorageHandler
from .base_request import BaseRequest
from utils import *


class StorageRequest(BaseRequest): 
    
    def __init__(self, req: func.HttpRequest,
                 use_json:bool=True,
                 command:str=None,
                 params:dict=None):

        logger.debug('Initializing StorageRequest')

        self.params = params if params else {}

        super().__init__(req,use_json=use_json)

        self.default_container = DEFAULT_WORKSPACE_CONTAINER
        self.default_target_container = DEFAULT_HOSTING_CONTAINER

        self.response = self.storage_command(command=command)

    def storage_command(self,command:str=None) -> func.HttpResponse:
        logger.debug(f'Handling storage command: {command}')

        if command == 'copy':
            
            object_name_in = self.json.get('objectNameIn',None)
            if not object_name_in:
                return self.return_error('Error: objectNameIn missing from request')
            
            input_container = self.json.get(
                'inputContainer',self.default_container)
            output_container = self.json.get(
                'outputContainer',self.default_target_container)
            object_name_out = self.json.get(
                'objectNameOut',object_name_in)

            wait = self.json.get('wait',True)
            
            try:
                storage = StorageHandler(workspace_container_name=input_container)
            except Exception as e:
                return self.return_error(
                    f'Error: could not instantiate storage handler: {e}')   
            try:    
                logger.debug(f'Copying {object_name_in} from {input_container} to {output_container} as {object_name_out}')
                copy_result = storage.copy_blob(source_container_name=input_container,
                                source_blob_name = object_name_in,
                                dest_container_name = output_container,
                                dest_blob_name = object_name_out,
                                wait_on_status= wait)
                logger.debug(f'Copy result: {copy_result}')
            except Exception as e:
                return self.return_error(f'Error during copy operation: {e}')
            
            if isinstance(copy_result,str):
                return self.return_success(
                    message=f'{object_name_in} copied from {input_container} to {output_container} as {object_name_out}',
                    json_out={'copy_result':copy_result,
                              'object_name_out':object_name_out,
                              'output_container':output_container})
            elif isinstance(copy_result,dict):
                return self.return_success(
                    message=f'Copy operation started for {object_name_in} from {input_container} to {output_container} as {object_name_out}',
                    json_out={'copy_result':copy_result})
            else:
                return self.return_error(f'Unknown error during copy operation')
            
        elif command == 'list_containers':
            try:
                storage = StorageHandler()
                containers = storage.list_containers()
                return self.return_success(
                    message=f'Containers: {containers}',
                    json_out={'containers':containers})
            
            except Exception as e:
                return self.return_error(f'Could not list containers {e}')

        elif command == 'list_container_contents':
            container_name = self.json.get('containerName',self.default_container)
            try:
                storage = StorageHandler()
                contents = storage.list_container_blobs(container_name)
                return self.return_success(
                    message=f'Contents of {container_name}: {contents}',
                    headers={'container_name':container_name,'contents':contents})
            except Exception as e:
                return self.return_error(
                    f'Could not list contents for container {container_name}: {e}')
        else:
            return self.return_error(f'Unknown storage command: {command}')
