from azure.core.credentials import TokenCredential
from azure.core.exceptions import AzureError, HttpResponseError, ResourceNotFoundError
from azure.identity import (
    DefaultAzureCredential,
    ManagedIdentityCredential,
    ClientSecretCredential,
)
from azure.keyvault.secrets import SecretClient

from utils import logger, VAULT_NAME

class VaultAuth:
    def __init__(self, vault_name: str = None, credential=None):

        logger.debug("Initializing VaultAuth Class")
        self.credential = None
        self._vault_name = vault_name if vault_name else VAULT_NAME
        self.secret_client = None
        self.init_errors = []
        # Azure Credential
        if isinstance(credential, TokenCredential):
            self.credential = credential
            logger.debug("Using provided credential")
        else:
            try:
                self.credential = DefaultAzureCredential()
                logger.debug("Using DefaultAzureCredential")
            except (AzureError, Exception) as e:
                error_message = (
                    f"VaultAuth Critical Error initializing Azure credentials: {e}"
                )
                self.init_errors.append(error_message)
                logger.critical(error_message)
                self.credential = None
        # Vault
        if self.credential:
            try:
                self.secret_client = SecretClient(
                    vault_url=f"https://{self._vault_name}.vault.azure.net",
                    credential=self.credential,
                )
            except (AzureError, Exception) as e:
                error_message = (
                    f"VaultAuth Critical Error initializing SecretClient: {e}"
                )
                self.init_errors.append(error_message)
                logger.critical(error_message)
                self.secret_client = None
        else:
            error_message = "VaultAuth Critical Error - No credential available"
            self.init_errors.append(error_message)
            logger.critical(error_message)