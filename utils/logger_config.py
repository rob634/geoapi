from azure.core.exceptions import AzureError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from datetime import datetime
import io
import logging
import sys

import psycopg2
from psycopg2 import sql

from .defaults import *

  
class DatabaseLog(logging.Handler):
    
    LOG_COLUMNS = ['session_id',
                    'message_level',
                    'message',
                    'asctime',
                    'message_timestamp',
                    'func_name',
                    'process_name',
                    'lineno']
    
    def __init__(self, level=logging.NOTSET):
        
        super().__init__(level)
        
        self.session_id = f'{datetime.now().strftime("%Y_%b_%d_%H%M%S")}'

        self.table_name = LOG_TABLE_NAME
        self.schema_name = LOG_SCHEMA_NAME
        self.columns_dtypes = LOG_COLUMNS
        self.columns = [col.split(' ')[0] for col in LOG_COLUMNS]
            
        try:
            
            self.secret_client = SecretClient(
                vault_url=f'https://{VAULT_NAME}.vault.azure.net',
                credential=DefaultAzureCredential())
            
        except (AzureError,Exception) as e:
        
            raise AzureError(
                f'Logger Error - failure initializing Key Vault {VAULT_NAME} client: {e}')
        
        if self.table_exists(self.table_name, self.schema_name):
            pass
        else:
            pass
    
    def connect(self):

        return psycopg2.connect(
                dbname=self.secret_client.get_secret(SECRET_DB_NAME).value,
                user=DEFAULT_DB_ADMIN,
                host=self.secret_client.get_secret(SECRET_DB_HOST).value,
                port=DEFAULT_DB_PORT,
                password=self.secret_client.get_secret(f'{DEFAULT_DB_ADMIN}-credential').value)
     
        
    def create_log_table(self):
        
        query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns})").format(
            table=sql.Identifier(self.table_name),
            schema=sql.Identifier(self.schema_name),
            columns=sql.SQL(', '.join(self.columns_dtypes))
        )
        
        try:
            with self.connect() as conn:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise e
        except Exception as e:
            raise e
            
    def emit(self, record):#native method intended to be overridden
        log_entry = self.format(record)
        if self.should_insert(record):
            self.insert_log(record)

    def should_insert(self, record):
        # Define the condition under which the log should be inserted into the database
        return record.levelno >= logging.INFO
    

    
    def insert_log(self, record):

        values = (
            self.session_id,
            record.levelname, 
            record.getMessage(),
            record.asctime,
            record.created,
            record.funcName,
            record.processName,
            record.lineno
        )
        
        query = sql.SQL(
            """
            INSERT INTO {schema}.{table} ({fields})
                VALUES ({values})
            """
            ).format(
                    schema=sql.Identifier(self.schema_name),
                    table=sql.Identifier(self.table_name),
                    fields = sql.SQL(', ').join(
                        map(sql.Identifier, self.columns)),
                    values=sql.SQL(', ').join(sql.Placeholder() * len(values))
                )
        
        with self.connect() as conn:
            with conn.cursor() as cursor:

                try:
                    cursor.execute(query, values)
                    conn.commit()
                except Exception as e:
                    print(f"Error inserting log into database: {e}")
                    conn.rollback()

    def table_exists(self, table_name:str, schema_name:str=None) -> str:

            query = sql.SQL("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_catalog={dbname} 
                    AND table_schema={schema_name} 
                    AND table_name={table_name});
                """).format(
                    dbname=sql.Literal(self.secret_client.get_secret(SECRET_DB_NAME).value),
                    schema_name=sql.Literal(schema_name),
                    table_name=sql.Literal(table_name)
                )
            
            try:
                with self.connect() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        exists = cursor.fetchall()[0][0]
            except Exception as e:
                logger.error(f"Error checking if table exists: {e}")
                raise e
            return exists

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