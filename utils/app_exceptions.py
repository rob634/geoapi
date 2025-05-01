from functools import wraps

from azure.core.exceptions import AzureError, HttpResponseError, ResourceNotFoundError, ResourceExistsError
from .logger_config import logger

class DatabaseClientError(Exception):
    """Base class for exceptions in VectorHandler."""
    def __init__(self,message=None):
        logger.error(f"DatabaseClientError: {message}")
        super().__init__(message)

class EnterpriseClientError(Exception):
    def __init__(self, message):
        logger.critical(f"EnterpriseClientError: {message}")
        super().__init__(message)

class RasterHandlerError(Exception):
    """Base class for exceptions in RasterHandler."""
    def __init__(self, message=None):
        logger.error(f"RasterHandlerError: {message}")
        super().__init__(message)
        
class VectorHandlerError(Exception):
    """Base class for exceptions in VectorHandler."""
    def __init__(self,message=None):
        logger.error(f"VectorHandlerError: {message}")
        super().__init__(message)

class StorageHandlerError(Exception):

    def __init__(self,message=None,exception=None):
        logger.error(f"StorageHandlerError: {message}")
        
        super().__init__(message)

class InvalidFileTypeError(VectorHandlerError):
    """Exception raised for invalid file types."""
    def __init__(self, file_name, message="Invalid file type"):
        self.file_name = file_name
        self.message = f"{message}: {file_name}"

        super().__init__(self.message)

class EnterpriseClientError(Exception):

    def __init__(self,message=None):
        logger.error(f"EnterpriseClientError: {message}")
        super().__init__(message)
        
class GeoprocessingError(EnterpriseClientError):

    def __init__(self,message=None):
        logger.error(f"GeoprocessingError: {message}")
        super().__init__(message)
        
def storage_exceptions(func):
    """Decorator to handle storage exceptions."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ResourceNotFoundError, ResourceExistsError) as e:
            logger.error(f"Storage error: {e}")
            raise StorageHandlerError(f"Storage error: {e}")
        except HttpResponseError as e:
            logger.error(f"HTTP response error: {e}")
            raise StorageHandlerError(f"HTTP response error: {e}")
        except AzureError as e:
            logger.error(f"Azure error: {e}")
            raise StorageHandlerError(f"Azure error: {e}")
    return wrapper