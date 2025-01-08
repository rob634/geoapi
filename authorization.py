from azure.core.exceptions import AzureError, HttpResponseError, ResourceNotFoundError

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
        except (AzureError,Exception) as e:
            raise AzureError(f'Error initializing blob storage client: {e}')

                
    def _test_storage(self):
        try:
            container_list = self.blob_service_client.list_containers()
            names = [container.name for container in container_list]
            logger.debug(f'Blob client test pass - Containers: ' + ', '.join(names))
            return names
        except (AzureError,Exception) as e:
            raise AzureError(f'Blob client test fail - error listing containers: {e}')
  
           
class SecretAuth(BaseAuth):
    def __init__(self):
        super().__init__()
        
        self._vault_name = VAULT_NAME       
        self.secret_client = None

        try:
            self._refresh_secret_client()
            self._test_vault() 
        except (AzureError,Exception) as e:
            raise AzureError(f'SecretAuth initialization error: {e}')
    
        
    def _refresh_secret_client(self):
        if not self.secret_client:
            logger.debug('Initializing secret client')
        else:
            logger.debug('Refreshing secret client')
            
        vault_url = f'https://{self._vault_name}.vault.azure.net'
        try:
            self.secret_client = SecretClient(
                vault_url=vault_url,
                credential=self.credential)
        except (AzureError,Exception) as e:
            raise AzureError(f'Error initializing secret client: {e}')
        
        logger.debug('Secret client refreshed')

    def _test_vault(self):
        try:
            secret_names = self.secret_client.list_properties_of_secrets()
            names = [secret.name for secret in secret_names]
            logger.debug(f'Secret client test pass - Secrets: ' + ', '.join(names))
            return names
        except (AzureError,Exception) as e:
            raise AzureError(f"Secret client test fail - error listing secrets: {e}")


class HostingAuth(SecretAuth):
    def __init__(self):
        super().__init__()
        logger.debug('HostingAuth Class Initialized')
            
    def portal_admin(self):

        try:
            portal_admin = DEFAULT_PORTAL_ADMIN_USER#self.secret_client.get_secret(SECRET_PORTAL_ADMIN).value
            return portal_admin
        except (AzureError, Exception) as e:
            raise AzureError(f'Error obtaining enterprise username from keyvault: {e}')

    def portal_admin_credential(self):
        
        try:
            password = self.secret_client.get_secret(SECRET_PORTAL_ADMIN_CREDENTIAL).value
            return password
        except (AzureError, Exception) as e:
            raise AzureError(f'Error obtaining enterprise password from keyvault: {e}')

class AuthAll(BlobAuth, SecretAuth):
    def __init__(self):
        super().__init__()

class DatabaseAuth(SecretAuth):
    def __init__(self,user:str=None):
        super().__init__()
   
    def db_host(self):
        return self.secret_client.get_secret(SECRET_DB_HOST).value
    
    def db_name(self):
        return self.secret_client.get_secret(SECRET_DB_NAME).value
        
    def database_credential(self,user):
        secret_name = f'{user}-credential'
        return self.secret_client.get_secret(secret_name).value

