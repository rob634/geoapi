import os
import logging
from logging.handlers import MemoryHandler

BUFFER_SIZE = 1

class BufferedLogger(logging.Logger):
    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        self.memory_handler = None

    def set_memory_handler(self, memory_handler):
        self.memory_handler = memory_handler
        self.addHandler(memory_handler)

    def flush_logger(self):
        if self.memory_handler:
            self.memory_handler.flush()

class ColorFormatter(logging.Formatter):

    def _green(self, string):
        return f'\033[92m{string}\033[0m'
    
    def _yellow(self, string):
        return f'\033[93m{string}\033[0m'
    
    def _red(self, string):
        return f'\033[91m{string}\033[0m'

    def format(self, record):
        original_format = self._style._fmt

        if record.levelno == logging.ERROR:
            self._style._fmt = self._red(original_format)
        elif record.levelno == logging.WARNING:
            self._style._fmt = self._yellow(original_format)
        elif record.levelno == logging.INFO:
            self._style._fmt = self._green(original_format)

        result = logging.Formatter.format(self, record)
        self._style._fmt = original_format

        return result

class ListHandler(logging.Handler):
    """Custom logging handler to store log messages in a list."""
    def __init__(self):
        super().__init__()
        self.log_messages = []

    def emit(self, record):
        # Add log messages with WARNING or higher level to the list
        if record.levelno >= logging.WARNING:
            self.log_messages.append(self.format(record))

try:
    amd64 = 'AMD64' in os.environ['PROCESSOR_ARCHITECTURE']
except Exception as e:
    amd64 = False
    
if amd64:
    formatter_class = ColorFormatter
else:
    formatter_class = logging.Formatter
    
formatter = formatter_class(
    fmt="%(asctime)s - %(levelname)s - %(processName)s - %(funcName)s line %(lineno)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    style="%",
)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

if amd64:
    logger = logging.getLogger("LocalLogger")
    logger.addHandler(console_handler)

else:
    
    logger = BufferedLogger("AzureFunctionAppLogger")
    memory_handler = MemoryHandler(
        capacity=BUFFER_SIZE, 
        flushLevel=logging.WARNING,
        target=console_handler)
    
    logger.set_memory_handler(memory_handler)
    
logger.setLevel(logging.DEBUG)
logger.propagate = False


log_list = ListHandler()
log_list.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
logger.addHandler(log_list)

#################
class GeospatialLogger(BufferedLogger):
    def log_etl_stage(self, stage: str, file_path: str, duration: float, status: str):
        self.info(f"ETL_STAGE={stage} FILE={file_path} DURATION={duration}s STATUS={status}")
        
    def log_geometry_stats(self, feature_count: int, invalid_count: int, bounds: tuple):
        self.info(f"GEOMETRY_STATS features={feature_count} invalid={invalid_count} bounds={bounds}")