#Macbook edits Nov 2024
from numpy import isnan
import psycopg2
from psycopg2 import sql

#from local_auth import DatabaseAuth
from authorization import DatabaseAuth
from utils import *


class DatabaseClient(DatabaseAuth):
    
    RESERVED_WORDS = {
        'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC',
        'BACKUP','BEGIN', 'BETWEEN',
        'CASE', 'CHECK', 'COLUMN', 'CONSTRAINT','CREATE',
        'DATABASE', 'DEFAULT', 'DELETE', 'DESC', 'DISTINCT', 'DROP',
        'END', 'EXEC', 'EXISTS', 'FOREIGN', 'FROM', 'FULL', 'GROUP', 'HAVING',
        'IN', 'INDEX', 'INNER', 'INSERT', 'INTO', 'IS', 'JOIN', 'LEFT', 'LIKE', 'LIMIT',
        'NOT', 'NULL', 'OR', 'ORDER', 'OUTER', 'PRIMARY', 'PROCEDURE',
        'RIGHT', 'ROWNUM', 'SELECT', 'SET', 
        'TABLE', 'TIMESTAMP', 'TOP', 'TRUNCATE',
        'UNION', 'UNIQUE', 'UPDATE', 'VALUES', 'VIEW', 'WHERE'
    }
    
    def __init__(self, user:str=None): 
        super().__init__(user)
        
        try:
            self.test_connection()
        except Exception as e:
            logger.error(f"Error initializing database client: {e}")
            raise e
    
    def connect(self,user:str=None):
        user = user if user else DEFAULT_DB_USER
        return psycopg2.connect(
                dbname=(self.db_name()),
                user=user,
                host=self.db_host(),
                port=DEFAULT_DB_PORT,
                password=self.database_credential(user))
        
    def query(self, query: str = None, param_list: list = None):

        if not query:
            logger.error("No query provided")
            raise ValueError("No query provided")

        with self.connect() as conn:

            try:
                with conn.cursor() as cursor:
                    
                    if any([isinstance(param_list, dtype) for dtype in [list,tuple,dict]]):

                        cursor.execute(query, param_list)
                    else:
                        cursor.execute(query)
                        
                    if query.strip().lower().startswith("select"):
                        try:
                            results = cursor.fetchall()
                            
                            return results
                        
                        except Exception as e:
                            logger.error(f"Error fetching query results: {e}")
                            raise e
                        
                    else:
                        conn.commit()

            except Exception as e:
                logger.error(f"Error querying database: {e}")
                raise e
   
    def test_connection(self):
        logger.info('Testing database connection')
        try:

            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('select version();')
                    version = cursor.fetchone()
            message = f'Connection to {self.host} established: {version}'
            logger.info(message)
            return message
        except Exception as e:
            message = f'Credential errors could not connect to {self.host}: {e}'
            logger.error(message)
            raise e

    def table_exists(self, table_name:str, schema_name:str=None) -> str:

        query = sql.SQL("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_catalog={dbname} 
                AND table_schema={schema_name} 
                AND table_name={table_name});
            """).format(
                dbname=sql.Literal(self.dbname),
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

    def delete_table(self, table_name: str, schema_name: str):

        query = sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name)
        )

        try:
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    conn.commit()
                    logger.info(f"Table {schema_name}.{table_name} deleted successfully.")
        except Exception as e:
            logger.error(f"Error deleting table {schema_name}.{table_name}: {e}")
            raise e

    def replace_reserved_word(self,column_name:str,suffix:str=None) -> str:
        suffix = suffix if suffix else '_1'
        if column_name.upper() in self.RESERVED_WORDS:
            return f'{column_name}{suffix}' 
        else: 
            return column_name
    
    @staticmethod
    def build_insert_row_statement(
        table_name:str,
        schema_name:str,
        columns:list,
        values:list,
        geometry_name:str=None
        ) -> str:

        columns = [sql.Identifier(column) for column in columns]
        values = [sql.Placeholder() for column in columns]
        query = sql.SQL("""
            INSERT INTO {schema}.{table} ({fields}, {geom_field})
            VALUES ({values}, ST_GeomFromText(%s, 4326))
        """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            geom_field=sql.Identifier(geometry_name),
            values=sql.SQL(', ').join(sql.Placeholder() * len(values))
        )
        return query, values

    @staticmethod   
    def is_valid_dtype(dtype:str) -> bool:
        dtype = dtype if isinstance(dtype,str) else str(dtype)
        valid_dtypes = ['int','float','object','date','geometry']
        return any(valid_dtype in dtype for valid_dtype in valid_dtypes)
    
    @staticmethod
    def to_sql_type(
        obj,
        to_dtype:str=None,
        length:int=None) -> str:

        if to_dtype and isinstance(to_dtype,str):
            
            dtype = to_dtype.lower()
            
        elif obj:
        
            dtype = str(type(obj)).lower()

        else:

            return None

        if any(s in dtype for s in ['int', 'integer', 'smallint', 'bigint']):
            if '64' in dtype:
                return f'BIGINT'
            else:
                return f'INTEGER'
        
        elif any(s in dtype for s in ['float', 'double', 'numeric']):
            return f'FLOAT'
        
        elif any(s in dtype for s in ['object','str','varchar','category']):
            return f'VARCHAR({length})' if length else 'VARCHAR'
        
        elif any(s in dtype for s in ['datetime','time']):
            return f'TIMESTAMP'
        
        elif any(s in dtype for s in ['nattype','none']):
            return f'NaN'
        
        elif 'date' in dtype:
            return f'DATE'
        
        elif 'geometry' in dtype:
            return f'GEOMETRY'
        
        elif 'bool' in dtype:
            return f'BOOLEAN'
        
        else:
            logger.warning(f'Invalid data type: {dtype}')
            return None
    
    def _is_integer_number(num):
        return isinstance(num, (int, float)) and num == int(num)


    def convert_to_sql_type(self,obj,sql_type:str=None) -> object:   

        obj_type = self.to_sql_type(obj)

        if obj_type is None or obj_type == 'NaN':
            return None

        elif any(
            [s in obj_type for s in [
                'VARCHAR',
                'TIMESTAMP',
                'DATE'
                ]]):
            
            return str(obj)
        
        elif any(
            [s in obj_type for s in [
                'INTEGER',
                'BIGINT',
                ]]):
            return int(obj)
        
        elif 'FLOAT' in obj_type:
            return float(obj)

        else:
            logger.warning(f'Invalid data type: {obj_type} not converted')
            return None
        

    @staticmethod
    def dbtype_to_pdtype(dtype):
        if "integer" in dtype:
            return "int"
        elif "double" in dtype:
            return "float"
        elif "timestamp" in dtype:
            return "timestamp"
        elif "date" in dtype:
            return "date"
        elif "boolean" in dtype:
            return "bool"
        else:
            return "object"
