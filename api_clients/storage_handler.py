from azure.storage.blob import  BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
import io

import time

from authorization import BlobAuth
from utils import *


class StorageHandler(BlobAuth):

    def __init__(self,
                 workspace_container_name:str=None,
                 make_fs:bool=False):
        
        logger.debug(f'Initializing StorageHandler')

        super().__init__()

        if self.container_exists(workspace_container_name):
            self.workspace_container_name = workspace_container_name
            
        elif self.container_exists(DEFAULT_WORKSPACE_CONTAINER):
            self.workspace_container_name = DEFAULT_WORKSPACE_CONTAINER
            
            logger.warning(f'StorageHandler defaulting to workspace container:{DEFAULT_WORKSPACE_CONTAINER}')
            
            if workspace_container_name:
                logger.warning(f'Workspace container {workspace_container_name} not found')
                
        else:
            raise FileNotFoundError(f'StorageHandler init fail: No valid workspace container found')

        logger.debug(f'Default workspace container: {self.workspace_container_name}')

    def _get_blob_sas_uri(self,container_name:str, blob_name:str=None):

        logger.debug(f'Generating SAS URI for {blob_name} in {container_name}')
        container_name = container_name if self.container_exists(container_name) else self.workspace_container_name

        if not self.blob_exists(blob_name=blob_name,container_name=container_name):
            raise FileNotFoundError(f'Could not generate SAS URI: {blob_name} not found in container {container_name}')

        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name)
        
        ud_key = self.blob_service_client.get_user_delegation_key(
                key_start_time=datetime.utcnow(), 
                key_expiry_time=datetime.utcnow() + timedelta(hours=1)
            )
        
        sas_token = generate_blob_sas(
            account_name=self.blob_service_client.account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=None,
            user_delegation_key=ud_key,
            permission=BlobSasPermissions(read=True,
                write=True,
                delete=True,
                list=True,
                add=True,
                create=True
            ),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )
        return f'{blob_client.url}?{sas_token}'

        
    def _valid_extension(self,ext):
        return ext.lower() in VALID_EXTENSIONS
    
    def _timestamp(self):
        return f'{datetime.now().strftime("%Y_%m_%d_%H%M%S")}'
    
    def _add_timestamp(self,file_name):
        name_list = file_name.split('.')
        ext = name_list[-1]
        name_base = ''.join(name_list[:-1])
        return f'{name_base}_{self._timestamp()}.{ext}'
    
    def _validate_file_name(self,file_name):
        
        if isinstance(file_name, str):
            if '.' in file_name:
                name_list = file_name.split('.')
            else:
                raise ValueError('File name must have an extension')
        else:
            raise TypeError('File name must be a string')
        
        ext = name_list[-1]
        if self._valid_extension(ext):

            if len(name_list)>2:
                logger.warning('File name has multiple . characters, removing')
            name_base = ''.join(name_list[:-1])
            return f'{name_base}.{ext}'
        else:
            raise ValueError(f'Invalid file extension: .{ext}')
    
    def blob_exists(self,blob_name:str,container_name:str=None) -> bool:
        
        if isinstance(blob_name,str):
            container_name = container_name if self.container_exists(container_name) else self.workspace_container_name
            logger.debug(f'Checking if blob {blob_name} exists in container {container_name}')
            try:
                container_client = self.blob_service_client.get_container_client(container_name)
                blob_client = container_client.get_blob_client(blob_name)
                _exists = blob_client.exists()
            except Exception as e:
                logger.error(f'Error accessing blob {blob_name} in {container_name}: {e}')
                _exists = False
        else:
            _exists = False
            
        return _exists

    def container_exists(self,container_name:str) -> bool:
        
        if isinstance(container_name,str):
            try:
               _exists = self.blob_service_client.get_container_client(container_name).exists()
            except Exception as e:
                logger.error(f'Error accessing {container_name}: {e}')
                _exists = False
        else:
            _exists = False
            
        return _exists
    
    def list_containers(self):
        # List all containers in the storage account
        try:
            container_list = self.blob_service_client.list_containers()
            names = [container.name for container in container_list]
            logger.info(f'Info: Containers: ' + ', '.join(names))
            return names
        except Exception as e:
            logger.error(f'Error listing containers: {e}')
            raise e

    def list_container_blobs(self,container_name:str=None)->list:
        container_name = container_name if self.container_exists(container_name) else self.workspace_container_name
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            blob_names = [blob.name for blob in blob_list]
            logger.info(f'Info: Blobs in {container_name}: ' + ',\n '.join(blob_names))
            return blob_names
        except Exception as e:
            logger.error(f'Error listing blobs in {container_name}: {e}')
            return None
        
    def copy_blob(self,
                    source_blob_name:str,
                    source_container_name:str=None,
                    dest_container_name:str=None,
                    dest_blob_name:str=None,
                    wait_on_status:bool=False):
        
        source_container_name = source_container_name if self.container_exists(
            source_container_name) else self.workspace_container_name
        dest_container_name = dest_container_name if self.container_exists(
            dest_container_name) else self.workspace_container_name

        if not self.blob_exists(blob_name=source_blob_name,
                                container_name=source_container_name):
            raise FileNotFoundError(f'Copy Error {source_blob_name} not found in {source_container_name}')
             
        if not dest_blob_name:
            if source_container_name == dest_container_name:
                dest_blob_name = self._add_timestamp(source_blob_name)
                logger.warning(f'Copying blob in same container: {source_blob_name} -> {dest_blob_name}')
            else:  
                dest_blob_name = source_blob_name
        
        if self.blob_exists(blob_name=dest_blob_name,
                            container_name=dest_container_name):
            new_name = self._add_timestamp(dest_blob_name)
            logger.warning(f'{dest_blob_name} already exists in {dest_container_name} renaming to {new_name} to avoid overwrite')
            dest_blob_name = new_name
        
        try:
            dest_blob_client = self.blob_service_client.get_blob_client(container=dest_container_name,blob=dest_blob_name)
        except Exception as e:
            message = f'Error accessing destination container {dest_container_name}: {e}'
            logger.error(message)
            raise e
        
        try:
            source_blob = self._get_blob_sas_uri(container_name=source_container_name,blob_name=source_blob_name)
            copy_properties = dest_blob_client.start_copy_from_url(source_blob)
            logger.debug(f'Copy initiated: {source_blob_name} -> {dest_blob_name}')
        except Exception as e:
            message = f'Error starting copy operation: {e}'
            logger.error(message)
            raise e
        
        if wait_on_status:
            status = self._check_copy_status(
                container_name=dest_container_name,
                blob_name=dest_blob_name)
            while status != 'success':

                time.sleep(5)
                status = self.check_copy_status(
                    container_name=dest_container_name,
                    blob_name=dest_blob_name)
                
            logger.info(f'{dest_blob_name} copied to {dest_container_name}')
            
            return dest_blob_name
        
        else:
            
            return {'copy_id' : copy_properties.copy_id,
                    'copy_status' : copy_properties.copy_status,
                    'copy_source' : copy_properties.copy_source,
                    'copy_progress' : copy_properties.copy_progress,
                    'copy_completion_time' : copy_properties.copy_completion_time,
                    'copy_status_description' : copy_properties.copy_status_description}

    def _check_copy_status(self, container_name:str, blob_name:str):
        
        try:
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            properties = blob_client.get_blob_properties()
            copy_status = properties.copy.status
        except Exception as e:
            logger.error(f'Error checking copy status for {blob_name}: {e}')
            raise e    
   
        logger.debug(f'Copy status for {blob_name}: {copy_status}')

        if copy_status == 'pending':
            copy_progress = properties.copy.progress
            logger.debug(f'Copy progress for {blob_name}: {copy_progress}')
        elif copy_status == 'success':
            logger.info(f'Copy operation complete for {blob_name}')
            return copy_status
      
    def upload_blob_data(self,
                         blob_data:io.BytesIO,
                         dest_blob_name:str,
                         dest_container_name:str=None,
                         if_exists:str=None):
        
        if if_exists:
            if if_exists in ['duplicate','overwrite','fail','skip']:
                pass
            else:
                logger.warning('Warning: if_exists parameter not recognized, defaulting to duplicate')
                if_exists = 'duplicate'
        else:
            if_exists = 'duplicate'

        try:
            dest_blob_name = self._validate_file_name(dest_blob_name.split('/')[-1])
        except Exception as e:
            logger.error(f'Error validating file name: {e}')
            raise e
        
        dest_container_name = dest_container_name if dest_container_name else self.workspace_container_name
        logger.debug(f'Info: Uploading {dest_blob_name} to {dest_container_name}')

        if self.blob_exists(blob_name=dest_blob_name,container_name=dest_container_name):
            
            logger.warning(f'{dest_blob_name} is already in {dest_container_name}')
            
            if if_exists == 'duplicate':
                dest_blob_name = self._add_timestamp(dest_blob_name)
                logger.warning(f'Warning: Renaming {dest_blob_name} to avoid overwrite')
                
            elif if_exists == 'overwrite':
                logger.warning(f'Warning: Overwriting {dest_blob_name}')
                
            elif if_exists == 'fail':
                logger.error(f'{dest_blob_name} already exists in {dest_container_name}')
                raise ValueError(f'{dest_blob_name} already exists in {dest_container_name}')
            
            elif if_exists == 'skip':
                logger.warning(f'Warning: Skipping {dest_blob_name}')
                return None
            
            else:
                raise ValueError(f'upload_blob_data error: if_exists parameter not recognized: {if_exists}')

        logger.debug(f'Info: Uploading blob {dest_blob_name} to container {dest_container_name}')
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=dest_container_name,
                blob=dest_blob_name)
            blob_client.upload_blob(blob_data)
            logger.info(f'Info: Blob {dest_blob_name} uploaded to container {dest_container_name}')
            
        except Exception as e:
            logger.error(f'Error uploading blob {dest_blob_name} to container {dest_container_name}: {e}')
            raise e
        
        return dest_blob_name
    
    def blob_to_data_object(self,

        blob_name:str,
        container_name:str=None):
        
        container_name = container_name if self.container_exists(
                container_name
            ) else self.workspace_container_name

        logger.debug(f'Downloading {blob_name} from {container_name}')
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name,blob=blob_name)
        except Exception as e:
            logger.error(
                f'Error creating blob client for <{blob_name}> in <{container_name}>: {e}')
            raise e
        
        try:
            blob_data = blob_client.download_blob().readall()
            logger.info(f'Blob {blob_name} downloaded from {container_name}')

            return blob_data

        except Exception as e:
            logger.error(f'Error downloading <{blob_name}> from {container_name}: {e}')

            raise e

    def blob_to_bytesio(self,
                            blob_name:str,
                            container_name:str=None):
          
        container_name = container_name if self.container_exists(
                container_name
            ) else self.workspace_container_name

        try:
            blob_data = self.blob_to_data_object(
                blob_name=blob_name,container_name=container_name)
            return io.BytesIO(blob_data)

        except Exception as e:
            logger.error(f'{e}')

            raise e

    def multi_blobs_to_bytesio(self,
        blob_names:list, 
        container_name=None,
        return_dict=False):

        container_name = container_name if self.container_exists(
            container_name) else self.workspace_container_name
        
        if return_dict:
            combined_data = {}
        else:
            combined_data = io.BytesIO()

        for blob_name in blob_names:

            blob_data = self.blob_file_to_bytesio(
                blob_name,
                container_name)
            
            if return_dict:
                combined_data[blob_name] = blob_data
            else:
                combined_data.write(blob_data.read())

        if not return_dict:
            combined_data.seek(0)  # Reset the pointer 

        return combined_data

    def list_common_files(self,prefix:str, container_name:str=None):
        container_name = container_name if self.container_exists(container_name) else self.workspace_container_name
        files = [f for f in self.list_container_blobs(container_name)]
        common_files = [f for f in files if prefix in f]
        return common_files

        
