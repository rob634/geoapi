# Dev edits March 2025
import datetime
from dateutil import parser
import re
import time

from pandas.api.types import (
    is_float_dtype as is_numpy_float,
    is_integer_dtype as is_numpy_int)
import psycopg2
from psycopg2 import sql
from shapely.geometry.base import BaseGeometry

from authorization import VaultAuth
from utils import *

class DatabaseClient:


    def __init__(
        self,
        db_host: str = None,
        db_name: str = None,
        db_user: str = None,
        db_credential: str = None,
        db_port: int = None,
        credential=None,
    ):

        self.db_host = db_host if db_host else ENTERPRISE_GEODATABASE_HOST
        self.db_name = db_name if db_name else ENTERPRISE_GEODATABASE_DB
        self.db_user = db_user if db_user else DEFAULT_DB_USER
        self.db_credential = db_credential  #'PGKoMd8-L]abcd'
        self.db_port = db_port if db_port else DEFAULT_DB_PORT

        if self.db_credential:
            if not self.db_host:
                logger.error("No database host provided")
            if not self.db_name:
                logger.error("No database name provided")
            if not self.db_user:
                logger.error("No database user provided")
            if not self.db_credential:
                logger.error("No database credential provided")
            if not self.db_port:
                logger.error("No database port provided")
        else:
            try:
                logger.debug("Initializing database parameters from vault")
                self.get_params_from_vault(
                    db_host=db_host,
                    db_name=db_name,
                    db_user=db_user,
                    credential=credential,
                )
                logger.info("Database credentials retrieved from vault")
            except Exception as e:
                logger.error(
                    f"No database credential provided and could not retrieve credentials from vault: {e}"
                )
                self.db_credential = None

    def __del__(self):
        logger.debug("Database client closed")

    # Core Database Methods
    def connect(self):
        if not self.db_credential:
            raise DatabaseClientError("Database credential not initialized")
        try: 
            conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.db_user,
                host=self.db_host,
                port=self.db_port,
                password=self.db_credential,
            )
            return conn
        except psycopg2.Error as e:
            logger.error(f"psycopg2 error connecting to database: {e}")
            raise e
        except Exception as e:
            logger.error(f"Unknown error connecting to database: {e}")
            raise e
    
    def test_connection(self):
        # Test the database connection and raise an exception if it fails
        logger.debug("Testing database connection")
        logger.debug(f"DB Host: {self.db_host}")
        logger.debug(f"DB Name: {self.db_name}")
        logger.debug(f"DB User: {self.db_user}")
        logger.debug(f"DB Credential: {self.db_credential}")
        try:
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("select version();")
                    version = cursor.fetchone()
            message = f"Connection to {self.db_host} established: {version}"
            logger.info(message)

            return message

        except Exception as e:
            message = f"Credential errors could not connect to {self.db_host}: {e}"
            logger.error(message)
            raise e

    def query(self, query_expression: str = None, param_list: list = None):
        select = None
        query_str = None
        
        if not query_expression:
            logger.error("No query_expression provided")
            raise ValueError("No query_expression provided")
        
        elif isinstance(query_expression, str):
            select = query_expression.strip().lower().startswith("select")
            query_str = query_expression
            
        elif isinstance(query_expression, sql.Composed):
            try:
                with self.connect() as conn:
                    query_str = query_expression.as_string(conn).lower()
                    select = "select" in query_str
            except Exception as e:
                logger.error(f"Error checking query_expression type: {e}")
                raise e
                
        else:
            raise ValueError("Invalid query_expression format: must by str or sql.Composed")
        
        logger.debug(f"Querying database with expression: {query_str[:500]}")
        with self.connect() as conn:
            try:
                with conn.cursor() as cursor:

                    if param_list and any(
                        [isinstance(param_list, dtype) 
                         for dtype in [list, tuple, dict]]):

                        cursor.execute(query_expression, param_list)
                    else:
                        cursor.execute(query_expression)

                    if select:
                        try:
                            results = cursor.fetchall()

                            return results

                        except Exception as e:
                            logger.error(f"Error fetching query_expression results: {e}")
                            raise e

                    else:
                        try:
                            conn.commit()
                            return True
                        except Exception as e:
                            logger.error(f"Error committing query_expression: {e}")
                            raise e

            except (
                psycopg2.IntegrityError,
                psycopg2.ProgrammingError,
                psycopg2.OperationalError,
                psycopg2.DataError,
                psycopg2.InternalError,
                psycopg2.DatabaseError,
            ) as e:
                logger.error(f"psycopg2 Database Error: {e}")
                raise e

            except psycopg2.Error as e:
                logger.error(f"psycopg2 General Error: {e}")
                raise e

            except Exception as e:
                logger.error(f"Unknown error querying database: {e}")
                raise DatabaseClientError(f"Unknown error querying database: {e}")

    # Describe Table Methods
    def table_exists(self, table_name: str, schema_name: str = None) -> str:

        query_expression = sql.SQL(
            """
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_catalog={dbname} 
                AND table_schema={schema_name} 
                AND table_name={table_name});
            """
        ).format(
            dbname=sql.Literal(self.db_name),
            schema_name=sql.Literal(schema_name),
            table_name=sql.Literal(table_name),
        )

        try:
            result = self.query(query_expression)
            exists = result[0][0]
        except Exception as e:
            logger.error(f"Error checking if table exists: {e}")
            raise e
        if exists:
            logger.debug(f"Table {schema_name}.{table_name} exists.")
        else:
            logger.warning(f"Table {schema_name}.{table_name} does not exist.")

        return exists

    def get_max_length(self, table_name: str, schema_name: str, column_name:str) -> int:
        # Find existing max length for varchar column
        query_expression = sql.SQL(
            """
                SELECT MAX(LENGTH({column})) AS max_length
                FROM {schema}.{table};
            """
            ).format(
                schema=sql.Identifier(schema_name),
                column=sql.Identifier(column_name),
                table=sql.Identifier(table_name)
        )
        
        r = self.query(query_expression)
        return r[0][0]

    def column_dict_from_db_table(
        self,
        table_name: str,
        schema_name: str,
        get_length:bool=False
        ) -> dict:

        if not schema_name:
            if "." in table_name:
                schema_name, table_name = table_name.split(".")
            else:
                raise ValueError("No schema name provided")
            
        col_attrs = ['column_name', 
                     'ordinal_position', 
                     'data_type', 
                     'character_maximum_length', 
                     'numeric_precision']
        
        query_expression = sql.SQL(
            """
                SELECT {columns}
                FROM information_schema.columns
                WHERE table_schema={schema_name}
                AND table_name={table_name};
            """
        ).format(
            columns = sql.SQL(', ').join(sql.Identifier(col) for col in col_attrs),
            schema_name=sql.Literal(schema_name),
            table_name=sql.Literal(table_name)
        )

        try:
            result = self.query(query_expression)
            columns = {
                row[0]:dict(zip(col_attrs, row)) for row in result
            }
        except Exception as e:
            logger.error(
                f"Error getting column info for {schema_name}.{table_name}: {e}"
            )
            raise e
        
        if get_length:
            for column in columns:
                if columns[column]['data_type'] == 'character varying':
                    try:
                        len = self.get_max_length(column_name=column, schema_name=schema_name, table_name=table_name)
                        columns[column]['max_length'] = len
                    except Exception as e:
                        logger.error(f"Error getting max length for {schema_name}.{table_name}.{column}: {e}")
                        columns[column]['max_length'] = None
        return columns
 
    def sql_list_from_column_dict(self,column_dict: dict=None,geo_column:str=None,geo_type:str=None) -> str:
        column_exp = []
        if not isinstance(column_dict, dict):
            raise ValueError("Column dictionary must be provided")
        
        if all(isinstance(column_dict[col], (str,float,int,bool)) for col in column_dict):
            column_exp = [
                f"{column} {self.type_to_sql_string(dtype=column_dict[column])}"
                for column in column_dict
            ]
        elif all(
            isinstance(column_dict[col],dict) for col in column_dict) and all('data_type' in column_dict[col] for col in column_dict):
            logger.debug("Building column expression from dictionary")
            
            for column in column_dict:
                logger.debug(f"Building column expression for <{column}>")
                column_length = None
                
                if 'character' in column_dict[column]['data_type']:
                    if 'character_maximum_length' in column_dict[column]:
                        column_length = column_dict[column]['character_maximum_length']
                    elif 'max_length' in column_dict[column]:
                        column_length = column_dict[column]['max_length']
                        
                logger.debug(f'Column: {column} Data Type: {column_dict[column]["data_type"]} Length: {column_length}')
                
                if geo_column and geo_type and column == geo_column:
                    logger.debug(f"Adding geometry column {geo_column} with type {geo_type}")
                    column_exp.append(f"{geo_column} GEOMETRY({geo_type}, 4326)")
                else:
                    column_exp.append(
                        f"{column} {self.type_to_sql_string(dtype=column_dict[column]['data_type'],length=column_length)}"
                        )
        else:
            raise ValueError("Invalid column dictionary format")
        
        return column_exp

    def column_list_from_database_table(self, table_name: str, schema_name: str = None,get_length:bool=False) -> list:
        try:
            column_dict = self.column_dict_from_db_table(table_name=table_name, schema_name=schema_name,get_length=get_length)
        except Exception as e:
            logger.error(f"Error getting column list from database table: {e}")
            raise e
        try:
            columns = self.sql_list_from_column_dict(column_dict)
        except Exception as e:
            logger.error(f"Error building column expression: {e}")
            raise e
        return columns

    # Describe Schema Methods
    def schema_exists(self, schema_name):
        query_expression = sql.SQL(
            """
            SELECT EXISTS(
                SELECT 1 FROM pg_namespace WHERE nspname = {schema_name});
            """
        ).format(schema_name=sql.Literal(schema_name))
        
        try:
            result = self.query(query_expression)
            exists = result[0][0]
        except Exception as e:
            logger.error(f"Error checking if table exists: {e}")
            raise e
        if exists:
            logger.debug(f"Schema {schema_name} exists")
        else:
            logger.warning(f"Schema {schema_name} does not exist")

        return exists

    def list_tables(
        self,
        schema_name: str = None,
        geo_only: bool = True,
        return_columns: bool = False,
    ) -> list:

        if geo_only:
            query_expression = sql.SQL(
                """
                SELECT DISTINCT table_name
                FROM information_schema.columns
                JOIN pg_catalog.pg_type t ON columns.udt_name = t.typname
                WHERE table_schema = {schema_name}
                AND t.typname = 'geometry';
                """
            ).format(schema_name=sql.Literal(schema_name))
        else:
            query_expression = sql.SQL(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema={schema_name};
                """
            ).format(schema_name=sql.Literal(schema_name))

        try:
            result = self.query(query_expression)
            tables = [
                row[0] for row in result if not self.gdb_is_system_table(row[0])
            ]
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            raise e

        if return_columns:
            tables = {
                table: self.column_dict_from_db_table(table, schema_name) for table in tables
            }

        return tables

    # SQL Generating Methods
    def build_insert_row_statement(
        self,
        table_name: str,
        schema_name: str,
        columns: list,
        values: list,
        geometry_name: str = None,
    ) -> str:

        columns = [sql.Identifier(column) for column in columns]
        values = [sql.Placeholder() for column in columns]
        query_expression = sql.SQL(
            """
            INSERT INTO {schema}.{table} ({fields}, {geom_field})
            VALUES ({values}, ST_GeomFromText(%s, 4326))
        """
        ).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            geom_field=sql.Identifier(geometry_name),
            values=sql.SQL(", ").join(sql.Placeholder() * len(values)),
        )
        return query_expression, values

    def build_insert_statement(
        self,
        table_name: str,
        schema_name: str,
        column_names: list,
        row_values: list = None,
        matrix: list = None, # df.values.tolist()
        geometry_name: str = None,
        column_types: dict = None, # For validation not yet implemented passed from GDF handler
    ) -> str:

        if not matrix:
            if isinstance(row_values,list):
                matrix = [row_values]
            else:
                raise ValueError("No matrix or row_values provided")
            
        column_count = len(column_names)
        logger.debug(f"Building insert statement for {schema_name}.{table_name} with {column_count} columns")
        logger.debug(f"Column names: {column_names}")
        
        column_identifiers = [sql.Identifier(_column_name) 
                                for _column_name in column_names] 
        
        logger.debug(f"Column identifiers: {column_identifiers}")

        if isinstance(matrix,list) and len(matrix) > 0:
            if not all([len(row) == column_count for row in matrix]):
                raise ValueError(f"Matrix is misshapen: found {column_count} column_names")
        else:
            raise ValueError("Matrix must be a list of lists")
        values_placeholders = []
        for row in matrix:
            row_list = []
            for col in column_names:
                if col != geometry_name:
                    row_list.append(sql.Placeholder())
                else:
                    row_list.append(sql.SQL("ST_GeomFromText({})").format(sql.Placeholder()))
            values_placeholders.append(sql.SQL(", ").join(row_list))

        query_expression = sql.SQL(
            """
                INSERT INTO {schema}.{table} ({fields})
                VALUES {values}
            """
        ).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(column_identifiers),
            values=sql.SQL(", ").join(
                sql.SQL("({})").format(value) for value in values_placeholders
            ),
        )
        logger.debug("Flattening batch values")
        # Validation of values to match column type can happen here
        flattened_values = [self.to_insert_value_type(i) for row in matrix for i in row]

        return query_expression, flattened_values
  
    # Table Methods
    def delete_table(self, table_name: str, schema_name: str = None):

        schema_name = schema_name if schema_name else HOSTING_SCHEMA_NAME
        
        query_expression = sql.SQL("DROP TABLE IF EXISTS {schema}.{table} CASCADE").format(
            schema=sql.Identifier(schema_name), table=sql.Identifier(table_name)
        )

        try:
            logger.debug(f"Deleting table {schema_name}.{table_name}")
            deleted = self.query(query_expression)
            if deleted:
                logger.info(f"Table {schema_name}.{table_name} deleted successfully.")
                
                return True
            else:
                raise Exception(
                    f"WARNING: Table {schema_name}.{table_name} not deleted by query_expression."
                )
        except Exception as e:
            logger.error(f"Error deleting table {schema_name}.{table_name}: {e}")
            raise e

    def create_table(
        self,
        table_name: str,
        columns,
        schema_name: str = None,
        geo_column: str = None,
        geo_type: str = None,
    ):
        
        if not schema_name:
            if not "." in table_name:
                raise ValueError("No schema name provided, specify in table_name or schema_name")
            elif len(table_name.split(".")) > 2:
                raise ValueError(f"Invalid table name format for {table_name}: follow schema.table format")
            else:
                schema_name, table_name = table_name.split(".")
                
        if isinstance(columns, dict):
            logger.debug("Building column expression from dictionary")
            try:
                column_exp = self.sql_list_from_column_dict(
                    column_dict=columns,
                    geo_column=geo_column,
                    geo_type=geo_type
                )
            except Exception as e:
                logger.error(f"Error building column expression: {e}")
                raise e
            
        elif isinstance(columns, list):
            if all(
                isinstance(col,str) for col in columns) and all(
                    len(col.split(" ")) == 2 for col in columns):
                column_exp = columns
            else:
                raise ValueError("Invalid column list format: must be list of strings <name> <data_type>")
        else:
            raise ValueError("Invalid column format: must be dict or list")
        
        logger.debug(f"Creating table {schema_name}.{table_name}")

        create_table_query = sql.SQL(
            """
                CREATE TABLE IF NOT EXISTS {schema}.{table} ({fields});
            """
            ).format(
                schema=sql.Identifier(schema_name),
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(
                    sql.SQL(field) for field in column_exp)
            )

        try:
            logger.debug(f"Creating table {schema_name}.{table_name} with schema: {column_exp}")
            self.query(create_table_query)
            logger.info(f"Table {schema_name}.{table_name} created successfully")
            return True
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise e
      
    # Data Type Methods

    def py_obj_to_sql_type(
        self,
        obj,
        to_dtype: str = None,  # Force to a specific data type
        length: int = None,  # length if relevant
    ) -> str:
        """Infers a SQL data type from a Python object or data type string"""
        dtype = None
        if to_dtype and isinstance(to_dtype, str):
            dtype = to_dtype.lower()
        elif obj:
            dtype = str(type(obj)).lower()
        else:
            # logger.warning("No object or data type provided")
            dtype = "none"
            
        return self.type_to_sql_string(dtype=dtype,length=length)

    def type_to_sql_string(self, dtype: str, length: int = None) -> str:

        if isinstance(dtype, str):
            dtype = dtype.lower()
            
        elif isinstance(dtype, dict):
            if 'data_type' in dtype:
                try:
                    dtype = self.type_to_sql_string(
                        type=dtype['data_type'],
                        length=dtype.get('character_maximum_length',None)
                    )
                    
                    return dtype
                    
                except Exception as e:
                    logger.error(f"Error converting data type dictionary: {e}")
                    raise e
            else:
                raise ValueError(f"Invalid data type dictionary format, could not find data_type: {dtype}")
        
        else:
            raise ValueError(f"Invalid data type: {dtype} - must be str or dict")

        logger.debug(f"Converting data type {dtype} with length {length} to SQL string")
        
        if any(s in dtype for s in ["int", "integer", "smallint", "bigint"]):
            if "64" in dtype or "big" in dtype:
                return f"BIGINT"
            else:
                return f"INTEGER"

        elif any(s in dtype for s in ["float", "double", "numeric"]):
            return f"FLOAT"

        elif any(s in dtype for s in ["object", "str", "varchar", "category","character", "text", "string","char","unicode"]):
            if 'varchar' in dtype and not length:
                pattern = r'\((\d+)\)'
                r = re.search(pattern, "VARCHAR")
                if r:
                    try:
                        length = int(r.group(1))
                    except Exception as e:
                        logger.error(f"Error extracting length from VARCHAR: {e}")
                        length = None
            return f"VARCHAR({length})" if length else "VARCHAR"

        elif any(s in dtype for s in ["datetime", "time", "date", "timestamp"]):
            return f"TIMESTAMP"

        elif any(s in dtype for s in ["nattype", "none", "nan", "null", "nat"]):
            return None#f"NaN"

        elif "geometry" in dtype:
            return f"GEOMETRY"

        elif any(s in dtype for s in ["bool", "boolean"]):
            return f"BOOLEAN"

        else:
            raise ValueError(f"Could not convert data type: {dtype}")

    def to_insert_value_type(self, obj) -> object:
        #fast convert
        
        if isinstance(obj,(int, float, bool, str)):
            
            return obj
        # Float
        elif is_numpy_float(obj):
            try:
                obj = float(obj)
                
                return obj
            except Exception as e:
                logger.error(f"Error converting numpy float to float: {e}")
                return None
        # Integer
        elif is_numpy_int(obj):
            try:
                obj = int(obj)
                
                return obj
            except Exception as e:
                logger.error(f"Error converting numpy int to int: {e}")
                return None
        # Geometry
        elif isinstance(obj, BaseGeometry):
            try:
                #logger.debug(f"Converting geometry to WKT")
                wkt = obj.wkt
                
                return wkt
            
            except Exception as e:
                logger.error(f"Error converting geometry <{obj}> to WKT: {e}")
                return None
        # Timestamp
        elif isinstance(obj, (datetime.datetime, datetime.date, datetime.time, time.struct_time)):
            try:
                obj = parser.parse(str(obj)).strftime("%Y-%m-%d %H:%M:%S")
                
                return obj
            
            except Exception as e:
                logger.error(f"Error converting datetime to isoformat: {e}")
                return None
        else:
            return None
    
    def object_to_insert_value_type(self, obj, force_type: str = None) -> object:
        #converts obj to python data type based on sql_type
        
        if force_type:
            obj_type = self.type_to_sql_string(dtype=force_type)
        
        elif isinstance(obj,(int, float, bool)):
            
            return obj
        
        elif isinstance(obj, BaseGeometry):
            try:
                #logger.debug(f"Converting geometry to WKT")
                wkt = obj.wkt
                
                return wkt
            
            except Exception as e:
                logger.error(f"Error converting geometry <{obj}> to WKT: {e}")
                return None
            
        else:
            try:
                obj_type = self.type_to_sql_string(dtype=str(type(obj)))
                
            except Exception as e:
                logger.error(f"Error converting to SQL type: {e}")
                raise e

        if not obj_type or obj_type in ["NaN", "NULL", "None", "none"]:

            return None

        elif any([s in obj_type for s in ["VARCHAR"]]):

            return str(obj)

        elif any([s in obj_type for s in ["TIMESTAMP", "DATE", "TIME", "DATETIME"]]):

            return self.infer_and_convert_date(obj)

        elif any([s in obj_type for s in ["INTEGER", "BIGINT"]]):

            return int(obj)

        elif "FLOAT" in obj_type:

            return float(obj)

        elif any([s in obj_type for s in ["BOOLEAN", "BOOL"]]):
            if isinstance(obj, bool):
                return obj

        else:
            logger.warning(f"Invalid data type: {obj} {obj_type} not converted")

            return None

    def timestamp_to_sql_string(self,obj):
        try:
            date_string = str(obj)
            parsed_date = parser.parse(date_string)
            formatted_date = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
            
            return formatted_date

        except Exception as e:
            logger.error(f"Error parsing date string <{date_string}>: {e}")
            return None
            
        

    def infer_and_convert_date(self, date_string, format=None):
        if not isinstance(format, str):
           format = "%Y-%m-%d %H:%M:%S"
        if not isinstance(date_string, str):
            try:
                date_string = str(date_string)
            except Exception as e:
                logger.error(f"Error converting date <{date_string}> to string: {e}")
                return None
        try:
            # Parse the date string to a datetime object
            parsed_date = parser.parse(date_string)
            # Convert the datetime object to the desired format
            formatted_date = parsed_date.strftime(format)

            return formatted_date

        except Exception as e:
            logger.error(f"Error parsing date string <{date_string}>: {e}")
            return None

    # Validation Methods
    def is_valid_table_name(self,table_name):
        
        if not isinstance(table_name, str):
            raise ValueError("Table name must be a string")
        
        existing_tables = self.list_tables(geo_only=False)
        
        if (len(table_name) < GDB_TABLE_NAME_MAX_LENGTH
            and not self.gdb_is_system_table(table_name)
            and table_name not in DATABASE_RESERVED_WORDS
            and all([char in DATABASE_ALLOWED_CHARACTERS for char in table_name])
            and not table_name[0].isnumeric()
            and not table_name.startswith("_")
            and table_name.lower() not in existing_tables
            and table_name not in existing_tables):
            logger.info(f"Table {table_name} is a valid name")
            
            return True
        
        else:
            logger.warning(f"Table {table_name} is not a valid name")
            
            return False

    def fix_table_name(self, table_name: str) -> str:
        if not isinstance(table_name, str):
            raise ValueError("Table name must be a string")
        
        if self.is_valid_table_name(table_name):
            if table_name.islower():
                logger.info(f"Table name <{table_name}> is valid")
                
                return table_name
            else:
                logger.warning(
                    f"Table name <{table_name}> is valid but will be converted to lowercase"
                    )
                
                return table_name.lower()
        else:
            logger.warning(f"Table name <{table_name}> is invalid and will be edited")
            
        existing_tables = self.list_tables(geo_only=False)
        
        init_table_name = table_name
        table_name = table_name.lower()

        if len(table_name) > GDB_TABLE_NAME_MAX_LENGTH:
            logger.warning(
                f"Table name <{table_name}> is too long. Truncating to {GDB_TABLE_NAME_MAX_LENGTH} characters"
            )
            table_name = table_name[:GDB_TABLE_NAME_MAX_LENGTH]
        
        if table_name in DATABASE_RESERVED_WORDS:
            logger.warning(
                f"Table name <{table_name}> is a reserved word. Appending t_ to table name"
            )
            table_name = f"t_{table_name}"

        if not all([char in DATABASE_ALLOWED_CHARACTERS for char in table_name]):
            logger.warning(
                f"Table name <{table_name}> can only contain alphanumeric characters and underscores"
            )
            for char in table_name:
                if char not in DATABASE_ALLOWED_CHARACTERS:
                    table_name = table_name.replace(char, "_")

        if self.gdb_is_system_table(table_name):
            logger.warning(
                f"Table name <{table_name}> follows system table naming style, renaming to t_{table_name}_table"
            )
            table_name = f"t_{table_name}_table"
        
        while table_name.startswith("_"):
            logger.warning(f"Table name <{table_name}> cannot start with an underscore. Removing leading underscore")
            table_name = table_name[1:]
        
        if table_name[0].isnumeric():
            logger.warning(
                f"Table name <{table_name}> cannot begin with a numeric. Appending 't' to table name"
            )
            table_name = f"t{table_name}"
        
        if table_name in existing_tables:
            logger.warning(f"Table name <{table_name}> already exists, appending '_number'")
            c = 1
            while table_name in existing_tables:
                if f"{table_name}_{c}" in existing_tables:
                    logger.debug(f"Table name <{table_name}_{c}> already exists, incrementing number")
                    c+=1
                else:
                    table_name = f"{table_name}_{c}"
            logger.info(f"Table name <{table_name}> validated")
            
        if table_name != init_table_name:
            logger.warning(f"Table name <{init_table_name}> changed to <{table_name}>")

        logger.info(f"Table name <{table_name}> validated")

        return table_name.lower()

    def replace_db_reserved_word(self, the_word: str, new_suffix: str = None) -> str:

        new_suffix = new_suffix if new_suffix else "_dbrnm1"
        if the_word in DATABASE_RESERVED_WORDS:
            output = f"{the_word}{new_suffix}"
        else:
            output = the_word

        return output.lower()

    def get_params_from_vault(
        self,
        db_host: str = None,
        db_name: str = None,
        db_user: str = None,
        vault_name: str = None,
        credential=None,
        secret_name: str = None,
    ):
        self.db_credential = None
        
        vault_name = vault_name if vault_name else VAULT_NAME
        db_user = db_user if db_user else DEFAULT_DB_USER
        vault = VaultAuth(vault_name=vault_name, credential=credential)

        if not db_name:
            try:
                self.db_name = vault.secret_client.get_secret(SECRET_DB_NAME).value
            except Exception as e:
                logger.error(f"Error getting database name from vault: {e}")
                raise e
        if not db_host:
            try:
                self.db_host = vault.secret_client.get_secret(SECRET_DB_HOST).value
            except Exception as e:
                logger.error(f"Error getting database host from vault: {e}")
                raise e
        try:
            secret_name = secret_name if secret_name else f"{db_user}-credential"
            self.db_credential = vault.secret_client.get_secret(secret_name).value
        except Exception as e:
            logger.error(f"Error getting database credential from vault: {e}")
            raise e

        logger.info(
            f"Database credentials retrieved from vault: {self.db_host}, {self.db_name}, {self.db_user}"
        )

        return True

    @classmethod
    def from_params(
        cls,
        db_host: str = None,
        db_name: str = None,
        db_user: str = None,
        db_credential: str = None,
    ):
        instance = cls(db_host=db_host, db_name=db_name, db_user=db_user)

        if isinstance(db_credential, str):
            instance.db_credential = db_credential
        elif db_credential:
            raise TypeError("Invalid database credential type - must be str")
        else:
            raise ValueError("No database credential provided")

        try:
            instance.test_connection()
        except Exception as e:
            logger.error(f"Error creating DatabaseClient from params: {e}")
            raise e

        return instance

    @classmethod
    def from_vault(
        cls,
        db_host: str = None,
        db_name: str = None,
        db_user: str = None,
        vault_name: str = None,
        credential=None,
        secret_name: str = None,
    ):
        instance = cls(db_host=db_host, db_name=db_name, db_user=db_user)

        try:
            vaulted = instance.get_params_from_vault(
                db_host=db_host,
                db_name=db_name,
                db_user=db_user,
                vault_name=vault_name,
                credential=credential,
                secret_name=secret_name,
            )
        except Exception as e:
            logger.error(f"Error creating DatabaseClient from vault: {e}")
            raise e

        if instance.db_credential:
            try:
                instance.test_connection()
            except Exception as e:
                logger.error(f"Error creating DatabaseClient from params: {e}")
                raise e
        else:
            raise ResourceNotFoundError("No database credential found in vault")

        return instance

# GDB Methods
    def gdb_is_registered_table(self, table_name: str, schema_name: str = None, db_name: str = None):
        db_name = db_name if db_name else self.db_name
        schema_name = schema_name if schema_name else HOSTING_SCHEMA_NAME
        try:
            reg_tables = self.gdb_registered_tables(db_name=db_name, schema_name=schema_name)
        except Exception as e:
            logger.error(f"Error getting registered tables: {e}")
            raise e

        return table_name in reg_tables
   
    def gdb_unregister_table(self, table_name: str, schema_name: str = None, db_name: str = None):

        try:
            reg_tables = self.gdb_registered_tables(db_name=db_name, schema_name=schema_name)
        except Exception as e:
            logger.error(f"Error getting registered tables: {e}")
            raise e
        
        if table_name in reg_tables:
            logger.debug(f"Unregistering table {schema_name}.{table_name}")
            query_expression = sql.SQL(
                """
                DELETE FROM {schema}.{table} WHERE name = {value};
                """
            ).format(
                schema=sql.Identifier(schema_name),
                table=sql.Identifier("gdb_items"),
                value = sql.Literal(f"{db_name}.{schema_name}.{table_name}")
            )
            try:
                unregistered = self.query(query_expression)
                if unregistered:
                    logger.info(f"Table {schema_name}.{table_name} unregistered successfully.")
                else:
                    logger.error(f"Table {schema_name}.{table_name} not unregistered.")
                
                return True
            
            except Exception as e:
                logger.error(f"Error unregistering table {schema_name}.{table_name}: {e}")
                raise e
        else:
            logger.warning(f"Table {schema_name}.{table_name} not registered.")
            return True

    def gdb_is_system_table(self, table_name: str) -> bool:
        return any([table_name.endswith(suffix) for suffix in GDB_RESERVED_SUFFIXES]) or any([table_name.startswith(prefix) for prefix in GDB_RESERVED_PREFIXES]) or table_name in GDB_RESERVED_NAMES

    def gdb_item_type_id(
            self, db_name: str = None,
            schema_name: str = None, 
            item_type: str = None
        ) -> str:
        
        item_type = item_type if item_type else "Feature Class"
        db_name = db_name if db_name else self.db_name
        schema_name = schema_name if schema_name else HOSTING_SCHEMA_NAME
        
        logger.debug(f"Getting item type ID for {item_type} in {schema_name}")
        query_expression = sql.SQL(
            """
            SELECT uuid FROM {schema}.{table} WHERE name = {type_};
            """
        ).format(schema=sql.Identifier(schema_name),table=sql.Identifier("gdb_itemtypes"),type_=sql.Literal(item_type))
        try:
            result = self.query(query_expression)
            type_id = result[0][0]
            logger.info(f"Item type ID for {item_type}: {type_id}")
            return type_id

        except psycopg2.Error as e:
            logger.error(f"psycopg2 error getting item type ID for {item_type}: {e}")
            raise e

        except Exception as e:
            logger.error(f"Error getting item type ID for {item_type}: {e}")
            raise e

    def gdb_registered_tables(self, db_name: str = None, schema_name: str = None):
        db_name = db_name if db_name else self.db_name
        schema_name = schema_name if schema_name else HOSTING_SCHEMA_NAME
        logger.debug(f"Getting registered tables in {schema_name}")
        try:
            fc_type_id = self.gdb_item_type_id(db_name=db_name, schema_name=schema_name)
        except Exception as e:
            logger.error(f"Error getting feature class type ID: {e}")
            raise e

        query_expression = sql.SQL(
            """
            SELECT name
            FROM {schema}.{table}
            WHERE type = {type_id}
            """
        ).format(schema=sql.Identifier(schema_name), table=sql.Identifier("gdb_items"), type_id=sql.Literal(fc_type_id))

        try:
            result = self.query(query_expression)
            tables = [
                row[0].split('.')[-1] for row in result if not self.gdb_is_system_table(row[0])
            ]
            logger.debug(f"Tables found in schema {schema_name}: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Error querying tables in schema {schema_name}: {e}")
            raise e

