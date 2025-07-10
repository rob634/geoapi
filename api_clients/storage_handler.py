from azure.core.credentials import TokenCredential, AzureNamedKeyCredential
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions

from datetime import datetime, timedelta
import io
from functools import wraps
import os
import time
import tempfile
import zipfile
from utils import *


class StorageHandler:
    VALID_EXTENSIONS = [
        "7z",
        "csv",
        "gdb",
        "geojson",
        "geotif",
        "geotiff",
        "gpkg",
        "json",
        "kml",
        "kmz",
        "osm",
        "shp",
        "tif",
        "tiff",
        "txt",
        "xml",
        "zip",
    ]

    def __init__(
        self,
        workspace_container_name: str = None,
        credential: TokenCredential = None,
        account_name: str = None,
        account_url: str = None,
    ):


        self.blob_service_client = None
        self.credential = None
        self.account_key = None
        self.init_errors = []
        
        
        if not isinstance(account_name, str):
            account_name = STORAGE_ACCOUNT_NAME
            logger.debug(f"Using default account name: {account_name}")

        # Credential
        if isinstance(credential, TokenCredential):
            self.credential = credential
            logger.info("TokenCredential provided to StorageHandler")
        elif isinstance(credential, AzureNamedKeyCredential):
            self.credential = credential
            logger.info("AzureNamedKeyCredential provided to StorageHandler")
        else:
            try:
                self.credential = DefaultAzureCredential()
                logger.info("DefaultAzureCredential initialized by StorageHandler")
            except Exception as e:
                error_message = f"Error initializing DefaultAzureCredential: {e}"
                logger.error(error_message)
                self.init_errors.append(error_message)
                self.credential = None

        if isinstance(account_url, str):
            logger.debug(f"Using account_url: {account_url}")
        else:
            account_url = f"https://{account_name}.blob.core.windows.net"

        try:
            self.blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=self.credential,
            )
            logger.info(
                f"StorageHandler initialized with BlobServiceClient for {account_url}"
            )
        except Exception as e:
            error_message = f"Error initializing BlobServiceClient: {e}"
            logger.error(error_message)
            self.init_errors.append(error_message)
            self.blob_service_client = None
            
        if self.blob_service_client:

            if workspace_container_name:
                self.workspace_container_name = workspace_container_name
                logger.debug(
                    f"StorageHandler initialized with workspace container parameter <{self.workspace_container_name}>"
                )
                logger.debug("Checking if workspace container exists")
                if self.container_exists(workspace_container_name):
                    logger.info(
                        f"Workspace container found: {workspace_container_name}"
                    )
                else:
                    error_message = f"Parameter specified workspace container <{workspace_container_name}> not found in storage account: {account_name}"
                    logger.error(error_message)
                    self.init_errors.append(error_message)
                    self.workspace_container_name = None
                    
            else: 

                logger.warning(
                    f"Workspace container not provided: {workspace_container_name}"
                )
                self.workspace_container_name = DEFAULT_WORKSPACE_CONTAINER
                logger.warning(
                    f"Initializing StorageHanndler with default workspace container: {DEFAULT_WORKSPACE_CONTAINER}"
                )
        else:
            logger.critical("BlobServiceClient not initialized")
            self.workspace_container_name = None
            self.init_errors.append("BlobServiceClient not initialized: Uknown Error")
            

        if self.workspace_container_name and self.container_exists(
            self.workspace_container_name):
            logger.info(
                f"StorageHandler initialized with workspace container: <{self.workspace_container_name}>"
            )
        else:
            error_message = f"Error initializing StorageHandler: container {self.workspace_container_name} not found"
            logger.critical(error_message)
            self.workspace_container_name = None
            self.init_errors.append(error_message)
    
    @staticmethod
    def check_container(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            _name = self.__class__.__name__
            
            if hasattr(self, 'blob_service_client') and isinstance(
                getattr(self,'blob_service_client'), BlobServiceClient):

                logger.info(f"{_name} BlobServiceClient is valid")
                
                container_params = [k for k in kwargs if k.endswith("_container_name")]
                if container_params:
                    logger.debug(f"Additional container parameters found: {container_params}")
                    for param in container_params:
                        _container_name = kwargs[param]
                        if isinstance(_container_name, str):
                            logger.debug(f"additional container parameter {param} found "+
                                         f" checking if {_container_name} exists")
                            try:
                                _exists = getattr(
                                    self,'container_exists')(
                                    container_name=_container_name)
                                
                            except Exception as e:
                                logger.error(f"Error checking container {_container_name} for {_name}: {e}")
                                raise e
                            
                            if _exists:
                                logger.info(f"Storage container {_container_name} exists "+
                                            f"passed as parameter {param} to {_name}")
                                
                            else:
                                error_message = f"Storage container {_container_name} passed as {param} to {_name} does not exist "
                                logger.error(error_message)
                                
                                raise ResourceNotFoundError(error_message)
                        
                if 'container_name' in kwargs:
                    container_name = kwargs['container_name']
                    
                    if isinstance(container_name, str):
                        logger.debug(f"Checking if parameter container_name {container_name} exists")
                    
                    elif (hasattr(self, 'workspace_container_name') 
                          and isinstance(getattr(self,'workspace_container_name'),str)):
                        logger.debug(f"Using instance workspace_container_name {container_name}")
                        
                        container_name = getattr(self,'workspace_container_name')

                    else:
                        error_message = f"Container name must be provided or instance {_name} workspace container name must be set"
                        logger.error(error_message)
                        
                        raise ValueError(error_message)
                    
                    try:
                        _exists = getattr(self,'container_exists')(
                            container_name=container_name)
                        
                    except Exception as e:
                        logger.error(f"Error checking container {container_name}: {e}")
                        
                        raise e
                    
                    if _exists:
                        
                        logger.info(f"{_name} storage container {container_name} exists")
                        
                        if kwargs['container_name'] != container_name:
                            logger.warning(
                                f"Warning: Parameter container_name not explicitly passed to {_name}, using instance workspace container name {container_name}")
                               
                            kwargs['container_name'] = container_name
                            
                    else:
                        logger.error(f"Container {container_name} not found")
                        
                        raise ResourceNotFoundError(f"Container {container_name} not found")
                                
            else:
                if hasattr(self, 'init_errors'):
                    error_message = f"storage_handler.BlobServiceClient not valid - errors: {getattr(self,'init_errors')}"
                else:
                    error_message = "storage_handler.BlobServiceClient not valid - unknown errors"
                logger.error(error_message)
                
                raise StorageHandlerError(error_message)
                
            return func(self, *args, **kwargs)
        
        return wrapper   
    
    @check_container
    def blob_exists(self, blob_name: str, container_name: str = None) -> bool:

        if isinstance(blob_name, str):
            logger.debug(f"Checking if blob {blob_name} exists")
        else:
            raise ValueError(
                f"Blob name must be a string, got {type(blob_name)}")
        
        try:
            container_client = self.blob_service_client.get_container_client(container=
                container_name)
            logger.debug(f"Container client for {container_name} created")
            blob_client = container_client.get_blob_client(blob=blob_name)
            logger.debug(f"Blob client for {blob_name} created")
            
            _exists = blob_client.exists()
            
            if _exists:
                logger.info(f"Blob {blob_name} exists in {container_name}")
            else:
                logger.warning(f"Blob {blob_name} not found in {container_name}")
                
            return _exists
            
        except Exception as e:
            error_message = f"Error checking blob {blob_name} in {container_name}: {e}"
            logger.error(error_message)
            raise e


    def container_exists(self, container_name: str):

        if isinstance(container_name, str):
            logger.debug(f"Checking if container {container_name} exists")
            try:
                _exists = self.blob_service_client.get_container_client(
                    container=container_name).exists()
                
                return _exists
            
            except Exception as e:
                logger.error(f"Error accessing {container_name}: {e}")
                raise e
        else:
            raise ValueError(
                f"Container name must be a string, got {type(container_name)}")

        

    def list_containers(self):
        # List all containers in the storage account
        try:
            container_list = self.blob_service_client.list_containers()
            names = [container.name for container in container_list]
            logger.info(f"Info: Containers: " + ", ".join(names))
            return names
        except Exception as e:
            logger.error(f"Error listing containers: {e}")
            raise e

    @check_container
    def list_container_blobs(self, container_name: str = None) -> list:

        try:
            container_client = self.blob_service_client.get_container_client(container=
                container_name
            )
            blob_list = container_client.list_blobs()
            blob_names = [blob.name for blob in blob_list]
            logger.info(f"Info: Blobs in {container_name}: " + ",\n ".join(blob_names))
            
            return blob_names
        
        except Exception as e:
            logger.error(f"Error listing blobs in {container_name}: {e}")
            raise e

    @check_container
    def copy_blob(
        self,
        source_blob_name: str,
        source_container_name: str = None,
        dest_blob_name: str = None,
        dest_container_name: str = None,
        wait_on_status: bool = False,
        overwrite: bool = False,
    ):
        logger.debug(
            f"copy_blob called with source_blob_name: {source_blob_name}, source_container_name: {source_container_name}, dest_blob_name: {dest_blob_name}, dest_container_name: {dest_container_name}, wait_on_status: {wait_on_status}, overwrite: {overwrite}"
        )

        if not self.blob_exists(
            blob_name=source_blob_name, container_name=source_container_name
        ):
            raise ResourceNotFoundError(
                f"Copy Error: blob <{source_blob_name}> not found in, <{source_container_name}>"
            )
        
        if (source_blob_name == dest_blob_name 
            and source_container_name == dest_container_name) or self.blob_exists(
                blob_name=dest_blob_name, container_name=dest_container_name):
                
            if overwrite:
                logger.warning(f"Warning: Overwriting {source_blob_name} in {source_container_name}")
                
            else:
                message = f"Source and destination are the same: {source_blob_name} in {source_container_name} and {dest_blob_name} in {dest_container_name} and overwrite is set to false"
                logger.error(message)
                
                raise ValueError(message)
        
        logger.debug(f"Copying {source_blob_name} in {source_container_name} to {dest_blob_name} in {dest_container_name}")

        try:
            dest_blob_client = self.blob_service_client.get_blob_client(
                container=dest_container_name, blob=dest_blob_name
            )
        except Exception as e:
            message = (
                f"Error accessing destination container {dest_container_name}: {e}"
            )
            logger.error(message)
            raise e

        try:
            source_blob = self._get_blob_sas_uri(
                container_name=source_container_name, blob_name=source_blob_name
            )
        except:
            message = f"Error generating SAS URI for {source_blob_name} in {source_container_name}"
            logger.error(message)
            
            raise e
        
        try:
            copy_properties = dest_blob_client.start_copy_from_url(source_blob)
            logger.info(f"Copy initiated: {source_container_name}//{source_blob_name} -> {dest_container_name}//{dest_blob_name}")
        except Exception as e:
            message = f"Error starting copy operation: {e}"
            logger.error(message)
            raise e

        if wait_on_status:
            status = self._check_copy_status(
                container_name=dest_container_name, blob_name=dest_blob_name
            )
            while status != "success":

                time.sleep(5)
                status = self.check_copy_status(
                    container_name=dest_container_name, blob_name=dest_blob_name
                )

            logger.info(f"{dest_blob_name} copied to {dest_container_name}")

            return dest_blob_name

        else:

            return copy_properties
        
    @check_container
    def delete_blob(self, blob_name: str, container_name: str = None) -> bool:

        if not isinstance(blob_name, str):
            raise ValueError(f"Blob name must be a string, got {type(blob_name)}")
        
        try:
            if self.blob_exists(blob_name=blob_name, container_name=container_name):
                
                blob_client = self.blob_service_client.get_blob_client(
                    container=container_name, 
                    blob=blob_name)
                
                blob_client.delete_blob()
                logger.info(f"Successfully deleted blob {blob_name} from container {container_name}")
                return True
            else:
                logger.debug(f"Blob {blob_name} does not exist in container {container_name}, nothing to delete")
                return False
                
        except Exception as e:
            error_message = f"Error deleting blob {blob_name} from container {container_name}: {e}"
            logger.error(error_message)
            raise e
    
    @check_container
    def upload_blob_data(
        self,
        blob_data: io.BytesIO,
        dest_blob_name: str,
        container_name: str = None,
        overwrite: bool = False,
    ):

        try:
            dest_blob_name = self._validate_file_name(os.path.basename(dest_blob_name))
            logger.debug(f"Info: Validated file name: {dest_blob_name}")
        except Exception as e:
            logger.error(f"Error validating file name: {e}")
            raise e

        logger.debug(f"Info: Uploading {dest_blob_name} to {container_name}")

        if self.blob_exists(
            blob_name=dest_blob_name, container_name=container_name
        ):

            if overwrite:
                logger.warning(f"{dest_blob_name} is already in {container_name} - overwriting")
            else:
                message = f"Error: {dest_blob_name} already exists in {container_name} and overwrite is set to false"
                logger.error(message)
                raise ResourceExistsError(message)         

        logger.debug(
            f"Info: Uploading blob {dest_blob_name} to container {container_name}"
        )
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, blob=dest_blob_name
            )
            blob_client.upload_blob(data=blob_data,overwrite=overwrite)
            logger.info(
                f"Info: Blob {dest_blob_name} uploaded to container {container_name}"
            )

        except Exception as e:
            logger.error(
                f"Error uploading blob {dest_blob_name} to container {container_name}: {e}"
            )
            raise e

        return dest_blob_name

    @check_container
    def big_blob_to_data_object(self, blob_name: str, container_name: str = None):

        logger.debug(f"Downloading {blob_name} from {container_name}")
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, blob=blob_name
            )
        except Exception as e:
            logger.error(
                f"Error creating blob client for <{blob_name}> in <{container_name}>: {e}"
            )
            raise e

        try:
            blob_data = io.BytesIO()
            blob_client.download_blob().readinto(blob_data)
            blob_data.seek(0)
            
            return blob_data
        
        except Exception as e:
            logger.error(f"Error downloading <{blob_name}> from {container_name}: {e}")

            raise e
    
    @check_container
    def blob_to_data_object(self, blob_name: str, container_name: str = None):

        logger.debug(f"Downloading {blob_name} from {container_name}")
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, blob=blob_name
            )
        except Exception as e:
            logger.error(
                f"Error creating blob client for <{blob_name}> in <{container_name}>: {e}"
            )
            raise e

        try:
            blob_data = blob_client.download_blob().readall()
            logger.info(f"Blob {blob_name} downloaded from {container_name}")

            return blob_data

        except Exception as e:
            logger.error(f"Error downloading <{blob_name}> from {container_name}: {e}")

            raise e

    @check_container
    def blob_to_bytesio(self, blob_name: str, container_name: str = None):

        try:
            blob_data = self.blob_to_data_object(
                blob_name=blob_name, container_name=container_name
            )
            return io.BytesIO(blob_data)

        except Exception as e:
            logger.error(f"{e}")

            raise e

    @check_container
    def multi_blobs_to_bytesio(
        self, blob_names: list, container_name=None, return_dict=False
    ):

        if return_dict:
            combined_data = {}
        else:
            combined_data = io.BytesIO()

        for blob_name in blob_names:

            blob_data = self.blob_to_bytesio(blob_name, container_name)

            if return_dict:
                combined_data[blob_name] = blob_data
            else:
                combined_data.write(blob_data.read())

        if not return_dict:
            combined_data.seek(0)  # Reset the pointer

        return combined_data

    @check_container
    def list_common_files(self, prefix: str, container_name: str = None):
        files = [f for f in self.list_container_blobs(container_name)]
        common_files = [f for f in files if prefix in f]
        
        return common_files
        
    @classmethod
    def from_account_and_container_name(
        cls,
        account_name: str = None,
        workspace_container_name: str = None,
        credential: TokenCredential = None,
    ):
        try:
            instance = cls(
                account_name=account_name,
                workspace_container_name=workspace_container_name,
                credential=credential,
            )
        except Exception as e:
            error_message = f"Error initializing StorageHandler: {e}"
            logger.error(error_message)
            raise e

        if instance.blob_service_client:

            if instance.container_exists(workspace_container_name):
                logger.info(
                    f"StorageHandler initialized with workspace container: <{workspace_container_name}>"
                )
                return instance
            else:
                raise ValueError(
                    f"Error initializing StorageHandler: container {workspace_container_name} not found"
                )
        else:
            raise ValueError(
                f"Error initializing StorageHandler blob_service_client: {instance.init_errors}"
            )
            
    @classmethod
    def from_account_key(
        cls,
        account_name: str,
        account_key: str,
        container_name: str = None,
        account_url: str = None,
    ):
        logger.debug(
            f"Initializing StorageHandler from account key: {account_name}, {container_name}"
        )

        try:
            logger.debug(f"Acquiring AzureNamedKeyCredential")
            credential = AzureNamedKeyCredential(account_name, account_key)
            logger.info(
                f"AzureNamedKeyCredential initialized for account: {account_name}")
        except Exception as e:
            error_message = f"Error initializing AzureNamedKeyCredential: {e}" 
            logger.error(error_message)
            raise e
        
        try:
            instance = cls(
                account_name=account_name,
                workspace_container_name=container_name,
                credential=credential,
                account_url=account_url,
            )
            instance.account_key = account_key
            logger.info(
                f"StorageHandler initialized with account key for {account_name}"
            )
            
            return instance
        
        except Exception as e:
            error_message = f"Error initializing StorageHandler: {e}"
            logger.error(error_message)
            
            raise Exception(error_message)



    @check_container
    def _get_blob_sas_uri(self, container_name: str = None, blob_name: str = None):

        logger.debug(f"Generating SAS URI for {blob_name} in {container_name}")
        ak = None
        ud_key = None
        if not self.blob_exists(blob_name=blob_name, container_name=container_name):
            raise ResourceNotFoundError(
                f"Could not generate SAS URI: {blob_name} not found in container {container_name}"
            )

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, blob=blob_name)
        except Exception as e:
            logger.error(
                f"Error creating blob client for {blob_name} in {container_name}: {e}"
            )
            
            raise e
        
        if hasattr(self,"account_key") and isinstance(getattr(self,'account_key'), str):
            ak = self.account_key
            ud_key = None
        else:
            try:
                ak = None
                ud_key = self.blob_service_client.get_user_delegation_key(
                    key_start_time=datetime.utcnow(),
                    key_expiry_time=datetime.utcnow() + timedelta(hours=1),
                )
            except Exception as e:
                logger.error(f"Error getting user delegation key: {e}")
                
                raise e

        try:
            sas_token = generate_blob_sas(
                account_name=self.blob_service_client.account_name,
                container_name=container_name,
                blob_name=blob_name,
                account_key=ak,
                user_delegation_key=ud_key,
                permission=BlobSasPermissions(
                    read=True, write=True, delete=True, list=True, add=True, create=True
                ),
                expiry=datetime.utcnow() + timedelta(hours=1),
            )
            logger.info(f"SAS token generated for {blob_name} in {container_name}")
            
            return f"{blob_client.url}?{sas_token}"
        
        except Exception as e:
            logger.error(f"Error generating SAS token: {e}")
            
            raise e
        
        

    # private methods
    def _valid_extension(self, ext):
        return ext.lower() in self.VALID_EXTENSIONS

    def _timestamp(self):
        return f'{datetime.now().strftime("%Y_%m_%d_%H%M%S")}'

    def _add_timestamp(self, file_name):
        name_list = file_name.split(".")
        ext = name_list[-1]
        name_base = "".join(name_list[:-1])
        return f"{name_base}_{self._timestamp()}.{ext}"

    def _check_copy_status(self, container_name: str, blob_name: str):

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, blob=blob_name
            )
            properties = blob_client.get_blob_properties()
            copy_status = properties.copy.status
        except Exception as e:
            logger.error(f"Error checking copy status for {blob_name}: {e}")
            raise e

        logger.debug(f"Copy status for {blob_name}: {copy_status}")

        if copy_status == "pending":
            copy_progress = properties.copy.progress
            logger.debug(f"Copy progress for {blob_name}: {copy_progress}")
        elif copy_status == "success":
            logger.info(f"Copy operation complete for {blob_name}")
            return copy_status

    def _validate_file_name(self, file_name):

        if isinstance(file_name, str):
            if "." in file_name:
                name_list = file_name.split(".")
            else:
                raise ValueError("File name must have an extension")
        else:
            raise TypeError("File name must be a string")

        ext = name_list[-1]
        if self._valid_extension(ext):

            if len(name_list) > 2:
                logger.warning("File name has multiple . characters, removing")
            name_base = "".join(name_list[:-1])
            return f"{name_base}.{ext}"
        else:
            raise ValueError(f"Invalid file extension: .{ext}")

    def _validate_self(self):
        if hasattr(self, 'blob_service_client') and isinstance(
                getattr(self,'blob_service_client'), BlobServiceClient):
            logger.debug(f"BlobServiceClient is valid")
            
            return True
        
        else:
            raise ValueError("BlobServiceClient not initialized")
