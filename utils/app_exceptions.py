from functools import wraps

from enum import Enum
from typing import Optional, Dict, Any
import traceback
from datetime import datetime

from azure.core.exceptions import AzureError, HttpResponseError, ResourceNotFoundError, ResourceExistsError
from .logger_config import logger
'''
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
'''
###########################################
# utils/app_exceptions.py


class ErrorSeverity(Enum):
    LOW = "warning"
    MEDIUM = "error" 
    HIGH = "critical"

class ChimeraBaseException(Exception):
    """Base exception for all Chimera application errors."""
    
    def __init__(
        self, 
        message: str,
        error_code: str = None,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        context: Dict[str, Any] = None,
        cause: Exception = None,
        user_message: str = None
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.severity = severity
        self.context = context or {}
        self.cause = cause
        self.user_message = user_message or self._get_user_friendly_message()
        self.timestamp = datetime.utcnow()
        self.stack_trace = traceback.format_exc() if cause else None
        
        # Log immediately with appropriate level
        self._log_error()
    
    def _log_error(self):
        """Log error with appropriate level and context."""
        #logger = logging.getLogger(self.__class__.__module__)
        
        log_data = {
            'error_code': self.error_code,
            'error_message': self.message,
            'context': self.context,
            'timestamp': self.timestamp.isoformat()
        }
        
        if self.cause:
            log_data['caused_by'] = str(self.cause)
            log_data['stack_trace'] = self.stack_trace
        
        log_message = f"[{self.error_code}] {self.message}"
        
        if self.severity == ErrorSeverity.LOW:
            logger.warning(log_message, extra=log_data)
        elif self.severity == ErrorSeverity.MEDIUM:
            logger.error(log_message, extra=log_data)
        else:  # HIGH
            logger.critical(log_message, extra=log_data)
    
    def _get_user_friendly_message(self) -> str:
        """Override in subclasses for user-friendly messages."""
        return "An error occurred while processing your request."
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for API responses."""
        return {
            'error_code': self.error_code,
            'message': self.user_message,
            'timestamp': self.timestamp.isoformat(),
            'context': self.context
        }

# Specific Exception Classes
class DatabaseClientError(ChimeraBaseException):
    """Database operation errors."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message, 
            error_code="DB_ERROR",
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
    
    def _get_user_friendly_message(self) -> str:
        return "Database operation failed. Please try again."

class StorageHandlerError(ChimeraBaseException):
    """Storage operation errors."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            error_code="STORAGE_ERROR", 
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )
    
    def _get_user_friendly_message(self) -> str:
        return "File storage operation failed. Please check your file and try again."

class VectorHandlerError(ChimeraBaseException):
    """Vector processing errors."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            error_code="VECTOR_ERROR",
            severity=ErrorSeverity.MEDIUM, 
            **kwargs
        )

class InvalidGeometryError(VectorHandlerError):
    """Invalid geometry specific error."""
    def __init__(self, geometry_type: str, **kwargs):
        message = f"Invalid geometry type: {geometry_type}"
        super().__init__(
            message,
            error_code="INVALID_GEOMETRY",
            context={'geometry_type': geometry_type},
            **kwargs
        )

class RasterHandlerError(ChimeraBaseException):
    """Raster processing errors."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            error_code="RASTER_ERROR",
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )

class EnterpriseClientError(ChimeraBaseException):
    """ArcGIS Enterprise integration errors."""
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            error_code="ENTERPRISE_ERROR",
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

class GeoprocessingError(EnterpriseClientError):
    """Geoprocessing operation errors."""
    def __init__(self, message: str, operation: str = None, **kwargs):
        context = {'operation': operation} if operation else {}
        super().__init__(
            message,
            error_code="GEOPROCESSING_ERROR",
            context=context,
            **kwargs
        )

