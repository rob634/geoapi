from azure.identity import DefaultAzureCredential, ManagedIdentityCredential, ClientSecretCredential

from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient

from utils import *


#class MICredential:
#    def __init__(self):
        
#        self.credential = ManagedIdentityCredential()
#        logger.info('MICredential Class Initialized')

class SPCredential:
    def __init__(
            self, 
            client_id:str,
            client_secret:str,
            tenant_id:str):
        tenant_id = SP_TENANT_ID
        client_id = SP_CLIENT_ID
        client_secret = SP_SECRET_VALUE
        self.credential = ClientSecretCredential(
            tenant_id,
            client_id,
            client_secret)
        logger.info('SPCredential Class Initialized')

class BaseAuth:


    def __init__(self):
        
        self.credential = None

        logger.debug('Initializing BaseAuth Class')
        self._refresh_azure_identity()
   
    def _refresh_azure_identity(self):
        if not self.credential:
            logger.debug('Initializing Azure credentials')
        else:
            logger.debug('Refreshing Azure credentials')
            
        self.credential = DefaultAzureCredential()
        
class BlobAuth(BaseAuth):
    def __init__(self):
        super().__init__()
        
        self._storage_account_name = STORAGE_ACCOUNT_NAME
        self.blob_service_client = None
        
        try:
            self._refresh_blob_service_client()
            self._test_storage()
        except Exception as e:
            logger.error(f'BlobAuth initialization error: {e}')
            raise e

    def _refresh_blob_service_client(self):
        if not self.blob_service_client:
            logger.debug('Initializing blob storage client')
        else:
            logger.debug('Refreshing blob storage client')

        account_url = f'https://{self._storage_account_name}.blob.core.windows.net'
        try:
            self.blob_service_client = BlobServiceClient(
                account_url=account_url,
                credential=self.credential)
        except Exception as e:
            raise Exception(f'Error initializing blob storage client: {e}')

                
    def _test_storage(self):
        try:
            container_list = self.blob_service_client.list_containers()
            names = [container.name for container in container_list]
            logger.debug(f'Blob client test pass - Containers: ' + ', '.join(names))
            return names
        except Exception as e:
            raise Exception(f'Blob client test fail - error listing containers: {e}')
 