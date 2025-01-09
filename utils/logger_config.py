
from azure.identity import DefaultAzureCredential

from datetime import datetime
import io
import logging

from .defaults import *

  

class CustomFormatter(logging.Formatter):
    """Custom formatter to add colors to log messages."""
    def format(self, record):
        # Save the original format
        original_format = self._style._fmt

        # Define format with color for error messages
        if record.levelno == logging.ERROR:
            self._style._fmt = "\033[91m" + original_format + "\033[0m"  # Red color
        elif record.levelno == logging.WARNING:
            self._style._fmt = "\033[38;5;214m" + original_format + "\033[0m"  # Yellow color
        elif record.levelno == logging.INFO:
            self._style._fmt = "\033[94m" + original_format + "\033[0m"  # Blue color
        elif record.levelno == logging.DEBUG:
            self._style._fmt = "\033[92m" + original_format + "\033[0m"  # Green color

        # Format the message
        result = logging.Formatter.format(self, record)

        # Restore the original format
        self._style._fmt = original_format

        return result
 

formatter = CustomFormatter(
    fmt='%(asctime)s - %(levelname)s - %(processName)s - %(funcName)s line %(lineno)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    style='%', 
    )

logger = logging.getLogger('AzureFunctionAppLogger')
logger.setLevel(logging.DEBUG)
logger.propagate = False

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

log_stream = io.StringIO()
stream_handler = logging.StreamHandler(log_stream)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

#db_handler = DatabaseLog()

#logger.addHandler(db_handler)