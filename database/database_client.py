from dateutil import parser
import datetime
from math import ceil
import re
import time

import psycopg2
from psycopg2 import sql
from pandas.api.types import (
    is_float_dtype as is_numpy_float,
    is_integer_dtype as is_numpy_int,
)


from shapely.geometry.base import BaseGeometry

from utils import (
    logger, 
    DATABASE_PORT, 
    DATABASE_DICT, 
    DatabaseClientError,
    DEFAULT_SCHEMA)


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
        self.init_time = time.time()
        self.init_localtime = time.localtime()
        self.end_time = None
        
        
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_credential = db_credential
        self.db_port = db_port if db_port else DATABASE_PORT
        self.schema_name = None
        self.table_name = None
        
        logger.info(f"Database client initialized for {db_host} {db_name} {db_user}")

    def __str__(self):
        return f"DatabaseClient <{self.db_name}> at <{self.db_host}>"
    

    # Core Database Methods
    def connect(self):

        if any(
            not i
            for i in [self.db_credential, self.db_host, self.db_name, self.db_user]
        ):
            error_message = f"Database credential not initialized: host {self.db_host} database {self.db_name} user {self.db_user} password {self.db_credential}"
            logger.critical(error_message)
            raise Exception(error_message)

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
            raise Exception(message)

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
                    select = query_str.strip().lower().startswith("select")
            except Exception as e:
                logger.error(f"Error checking query_expression type: {e}")
                raise DatabaseClientError(e)

        else:
            raise ValueError(
                "Invalid query_expression format: must by str or sql.Composed"
            )

        logger.debug(f"Querying database with expression: {query_str[:500]}")
        with self.connect() as conn:
            try:
                with conn.cursor() as cursor:

                    if param_list and any(
                        [isinstance(param_list, dtype) for dtype in [list, tuple, dict]]
                    ):

                        cursor.execute(query_expression, param_list)
                    else:
                        cursor.execute(query_expression)
                    
                    try:
                        logger.debug(f"{cursor.rowcount} rows affected - status: {cursor.statusmessage}")
                    except Exception as e:
                        logger.error(f"Error getting cursor status: {e}")
                    
                    if select:
                        try:
                            results = cursor.fetchall()
                            
                            return results

                        except Exception as e:
                            logger.error(
                                f"Error fetching query_expression results: {e}"
                            )
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

    # Describe Methods
    def table_exists(self, table_name: str, schema_name: str = None):

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

    def get_column_max_length(
        self, table_name: str, schema_name: str, column_name: str
    ) -> int:
        # Find existing max length for varchar column
        query_expression = sql.SQL(
            """
                SELECT MAX(LENGTH({column})) AS max_length
                FROM {schema}.{table};
            """
        ).format(
            schema=sql.Identifier(schema_name),
            column=sql.Identifier(column_name),
            table=sql.Identifier(table_name),
        )

        r = self.query(query_expression)
        return r[0][0]

    def column_dict_from_db_table(
        self, table_name: str, schema_name: str, get_length: bool = False
    ) -> dict:

        if not schema_name:
            if "." in table_name:
                schema_name, table_name = table_name.split(".")
            else:
                raise ValueError("No schema name provided")

        col_attrs = [
            "column_name",
            "ordinal_position",
            "data_type",
            "character_maximum_length",
            "numeric_precision",
        ]

        query_expression = sql.SQL(
            """
                SELECT {columns}
                FROM information_schema.columns
                WHERE table_schema={schema_name}
                AND table_name={table_name};
            """
        ).format(
            columns=sql.SQL(", ").join(sql.Identifier(col) for col in col_attrs),
            schema_name=sql.Literal(schema_name),
            table_name=sql.Literal(table_name),
        )

        try:
            result = self.query(query_expression)
            columns = {row[0]: dict(zip(col_attrs, row)) for row in result}
        except Exception as e:
            logger.error(
                f"Error getting column info for {schema_name}.{table_name}: {e}"
            )
            raise e

        if get_length:
            for column in columns:
                if columns[column]["data_type"] == "character varying":
                    try:
                        len = self.get_column_max_length(
                            column_name=column,
                            schema_name=schema_name,
                            table_name=table_name,
                        )
                        columns[column]["max_length"] = len
                    except Exception as e:
                        logger.error(
                            f"Error getting max length for {schema_name}.{table_name}.{column}: {e}"
                        )
                        columns[column]["max_length"] = None
        return columns

    def sql_list_from_column_dict(
        self, column_dict: dict = None, geo_column: str = None, geo_type: str = None
    ) -> str:
        column_exp = []
        if not isinstance(column_dict, dict):
            raise ValueError("Column dictionary must be provided")

        if all(
            isinstance(column_dict[col], (str, float, int, bool)) for col in column_dict
        ):
            column_exp = [
                f"{column} {self.type_to_sql_string(dtype=column_dict[column])}"
                for column in column_dict
            ]
        elif all(isinstance(column_dict[col], dict) for col in column_dict) and all(
            "data_type" in column_dict[col] for col in column_dict
        ):
            logger.debug("Building column expression from dictionary")

            for column in column_dict:
                logger.debug(f"Building column expression for <{column}>")
                column_length = None

                if "character" in column_dict[column]["data_type"]:
                    if "character_maximum_length" in column_dict[column]:
                        column_length = column_dict[column]["character_maximum_length"]
                    elif "max_length" in column_dict[column]:
                        column_length = column_dict[column]["max_length"]

                logger.debug(
                    f'Column: {column} Data Type: {column_dict[column]["data_type"]} Length: {column_length}'
                )

                if geo_column and geo_type and column == geo_column:
                    logger.debug(
                        f"Adding geometry column {geo_column} with type {geo_type}"
                    )
                    column_exp.append(f"{geo_column} GEOMETRY({geo_type}, 4326)")
                else:
                    column_exp.append(
                        f"{column} {self.type_to_sql_string(dtype=column_dict[column]['data_type'],length=column_length)}"
                    )
                    
            return column_exp
        
        else:
            raise ValueError("Invalid column dictionary format")

        

    def column_list_from_database_table(self, table_name: str, schema_name: str = None, get_length: bool = False):
        try:
            column_dict = self.column_dict_from_db_table(
                table_name=table_name, schema_name=schema_name, get_length=get_length
            )
        except Exception as e:
            logger.error(f"Error getting column list from database table: {e}")
            raise e
        try:
            columns = self.sql_list_from_column_dict(column_dict)
        except Exception as e:
            logger.error(f"Error building column expression: {e}")
            raise e
        return columns

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
            tables = [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            raise e

        if return_columns:
            tables = {
                table: self.column_dict_from_db_table(table, schema_name)
                for table in tables
            }

        return tables

    # SQL Generating Methods
    
    def build_insert_statement(
        self,
        table_name: str,
        schema_name: str,
        column_names: list,
        row_values: list = None,
        matrix: list = None,  # df.values.tolist()
        geometry_name: str = None,
        column_types: dict = None,  # For validation not yet implemented passed from GDF handler
    ) -> str:

        if not matrix:
            if isinstance(row_values, list):
                matrix = [row_values]
            else:
                raise ValueError("No matrix or row_values provided")

        first_row_length = len(matrix[0])
        column_count = len(column_names)
        if first_row_length != column_count:
            logger.error(
                f"Column count {column_count} does not match row length {first_row_length}")
            logger.debug(f"Column names: {column_names}")
            logger.debug(f"Row values: {matrix[0]}")
            raise ValueError(
                f"Column count {column_count} does not match row length {first_row_length}"
            )
            
        logger.debug(
            f"Building insert statement for {schema_name}.{table_name} with {column_count} columns"
        )
        logger.debug(f"Inserting data with {len(matrix)} rows ")

        column_identifiers = [
            sql.Identifier(_column_name) for _column_name in column_names
        ]

        logger.debug(f"Column identifiers: {str(column_identifiers)[:100]}")

        if isinstance(matrix, list) and len(matrix) > 0:
            if all([len(row) == column_count 
                    for row in matrix]):
                logger.debug(f"Matrix is well formed with {len(matrix)} rows")
            else:
                raise ValueError(
                    f"Matrix is misshapen: found {column_count} column_names"
                )
        else:
            raise ValueError("Matrix must be a list of lists")
        values_placeholders = []
        for row in matrix:
            row_list = []
            for col in column_names:
                if col != geometry_name:
                    row_list.append(sql.Placeholder())
                else:
                    row_list.append(
                        sql.SQL("ST_GeomFromText({})").format(sql.Placeholder())
                    )
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
        try:
            flattened_values = [
                self.to_insert_value_type(i) 
                for row in matrix 
                for i in row
            ]
            logger.info(f"Flattened values: {flattened_values[:10]}...")
        except Exception as e:
            logger.error(f"Error flattening values: {e}")
            raise e
        flattened_values = [self.to_insert_value_type(i) for row in matrix for i in row]

        return query_expression, flattened_values

    # Table to Table Methods

    def table_columns_match(self, table_in, table_out, schema_in, schema_out=None):

        if isinstance(table_out, str):
            out_table = self
            if not schema_out:
                logger.warning(
                    "No schema name provided for output table, defaulting to schema_in"
                )
                schema_out = schema_in

        elif isinstance(table_out, DatabaseClient):
            out_table = table_out
            table_out = out_table.table_name
            if hasattr(out_table, "schema_name"):
                schema_out = out_table.schema_name
            elif isinstance(schema_out, str):
                pass
            else:
                logger.warning(
                    "No schema name provided for output table, defaulting to schema_in"
                )
                schema_out = schema_in

        columns_in = self.column_dict_from_db_table(
            table_name=table_in, schema_name=schema_in
        )

        columns_out = out_table.column_dict_from_db_table(
            table_name=table_out, schema_name=schema_out
        )

        match = True
        logger.debug(f"Comparing column names for {table_in} and {table_out}")

        if not columns_in.keys() == columns_out.keys():
            logger.warning(f"Column names do not match for {table_in} and {table_out}")
            match = False

            return match

        logger.debug(f"Comparing column attributes for {table_in} and {table_out}")
        for column in columns_in.keys():
            dtype_in = self.type_to_sql_string(dtype=columns_in[column]["data_type"])
            dtype_out = self.type_to_sql_string(dtype=columns_out[column]["data_type"])
            if not dtype_in == dtype_out:
                logger.warning(
                    f"Column {column} does not share a data type: {columns_in[column] } does not match {columns_out[column]}"
                )
                match = False

        if match:
            logger.info(
                f"Column names and attributes match for {table_in} and {table_out}"
            )
        else:
            logger.error(
                f"Column names and attributes do not match for {table_in} and {table_out}"
            )

        return match

    def table_counts_match(self, table_in, table_out, schema_in, schema_out=None):

        if isinstance(table_out, str):
            out_table = self
            if not schema_out:
                logger.warning(
                    "No schema name provided for output table, defaulting to schema_in"
                )
                schema_out = schema_in

        elif isinstance(table_out, DatabaseClient):
            out_table = table_out
            table_out = out_table.table_name
            if hasattr(out_table, "schema_name"):
                schema_out = out_table.schema_name
            elif isinstance(schema_out, str):
                pass
            else:
                logger.warning(
                    "No schema name provided for output table, defaulting to schema_in"
                )
                schema_out = schema_in

        count_in = self.query(
            sql.SQL("SELECT COUNT(*) FROM {schema}.{table}").format(
                schema=sql.Identifier(schema_in), table=sql.Identifier(table_in)
            )
        )[0][0]

        count_out = out_table.query(
            sql.SQL("SELECT COUNT(*) FROM {schema}.{table}").format(
                schema=sql.Identifier(schema_out), table=sql.Identifier(table_out)
            )
        )[0][0]
        match = count_in == count_out
        if match:
            logger.info(f"Table counts match for {table_in} and {table_out}")
        else:
            logger.warning(f"Table counts do not match for {table_in} and {table_out}")

        return match

    def select_batches(self, table_name: str, schema_name: str, batch_size: int, order_by: str = None):
        offset = 0
        while True:
            if order_by:
                query_expression = sql.SQL(
                    """
                    SELECT * FROM {schema}.{table}
                    ORDER BY {order_by}
                    LIMIT {limit} OFFSET {offset};
                    """
                ).format(
                    table=sql.Identifier(table_name),
                    schema=sql.Identifier(schema_name),
                    limit=sql.Literal(batch_size),
                    offset=sql.Literal(offset),
                    order_by=sql.Identifier(order_by)
                )
                
            else:
                query_expression = sql.SQL(
                    """
                    SELECT * FROM {schema}.{table}
                    
                    LIMIT {limit} OFFSET {offset};
                    """
                ).format(
                    table=sql.Identifier(table_name),
                    schema=sql.Identifier(schema_name),
                    limit=sql.Literal(batch_size),
                    offset=sql.Literal(offset),
                )
            
            try:
                results = self.query(query_expression)
                if not results:
                    break
                yield results
                offset += batch_size
            except Exception as e:
                logger.error(f"Error fetching batch: {e}")
                break

    def batch_transfer(
        self, table_in, table_out, schema_in, schema_out=None, batch_size=10000
    ):
        # Transfer data from one table to another in batches (Postgres only)
        init_time = time.time()
        logger.debug(f"Transferring data from {table_in} to {table_out}")
        same_db = None
        if isinstance(table_out, str):
            out_table = self
            same_db = True
            if not schema_out:
                logger.warning(
                    "No schema name provided for output table, defaulting to schema_in"
                )
                schema_out = schema_in

        elif isinstance(table_out, DatabaseClient):
            out_table = table_out
            table_out = out_table.table_name
            same_db = self.db_host == out_table.db_host
            
            if hasattr(out_table, "schema_name") and isinstance(getattr(out_table, "schema_name"),str):
                schema_out = out_table.schema_name
            elif isinstance(schema_out, str):
                logger.debug(f"Schema name provided for output table: {schema_out}")
            else:
                logger.warning(
                    "No schema name provided for output table, defaulting to schema_in"
                )
                schema_out = schema_in

        if same_db:
            target = table_out
        else:
            logger.info(f'Transfering tables between {self.db_host} {out_table.db_host}')
            target = out_table

        match = self.table_columns_match(
            table_in=table_in,
            table_out=target,
            schema_in=schema_in,
            schema_out=schema_out,
        )
        
        if not match:
            raise ValueError("Column mismatch between tables")
        
        try:
            row_count = self.query(
                sql.SQL("SELECT COUNT(*) FROM {schema}.{table}").format(
                    schema=sql.Identifier(schema_in), table=sql.Identifier(table_in)
                )
            )[0][0]
            batch_count = ceil(row_count / batch_size)
            
        except Exception as e:
            logger.error(f"Error getting row count for {table_in}: {e}")
            raise e
        
        logger.debug(f"Starting data transfer of {batch_count} batches of {batch_size} rows")
        logger.debug(f"Transfering data from {self.db_host} to {out_table.db_host}")
        cols = self.column_dict_from_db_table(
            table_name=table_in, schema_name=schema_in
        )
        c = 1
        for query_matrix in self.select_batches(
            table_name=table_in,
            schema_name=schema_in,
            batch_size=batch_size):
            
            q, v = self.build_insert_statement(
                table_name=table_out,
                schema_name=schema_out,
                matrix=query_matrix,
                column_names=cols.keys(),
                column_types=cols,
            )
            out_table.query(q, v)
            logger.info(f"Batch {c} of {batch_count} complete")
            c +=1
            
        return True
    # Table Methods

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
                raise ValueError(
                    "No schema name provided, specify in table_name or schema_name"
                )
            elif len(table_name.split(".")) > 2:
                raise ValueError(
                    f"Invalid table name format for {table_name}: follow schema.table format"
                )
            else:
                schema_name, table_name = table_name.split(".")

        if isinstance(columns, dict):
            logger.debug("Building column expression from dictionary")
            try:
                column_exp = self.sql_list_from_column_dict(
                    column_dict=columns, geo_column=geo_column, geo_type=geo_type
                )
            except Exception as e:
                logger.error(f"Error building column expression: {e}")
                raise e

        elif isinstance(columns, list):
            if all(isinstance(col, str) for col in columns) and all(
                len(col.split(" ")) == 2 for col in columns
            ):
                column_exp = columns
            else:
                raise ValueError(
                    "Invalid column list format: must be list of strings <name> <data_type>"
                )
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
            fields=sql.SQL(", ").join(sql.SQL(field) for field in column_exp),
        )

        try:
            logger.debug(
                f"Creating table {schema_name}.{table_name} with schema: {column_exp}"
            )
            self.query(create_table_query)
            logger.info(f"Table {schema_name}.{table_name} created successfully")
            
            return True
        
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise e

    def clear_table(
        self,
        table_name: str,
        schema_name: str = None,):
        
        q = sql.SQL("""
                    DELETE FROM {schema}.{table};
                    """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),)
        try:
            self.query(q)
            logger.info(f"Table {schema_name}.{table_name} cleared successfully")
            return True
        except Exception as e:
            logger.error(f"Error clearing table {schema_name}.{table_name}: {e}")
            raise e
        
    def insert_data_block(self, table_name, schema_name, matrix, column_names: list=None):
        # Insert data into a table in batches
        logger.debug(f"Inserting data block into {schema_name}.{table_name}")
        
        if not column_names:
            column_names = self.column_dict_from_db_table(
                table_name=table_name, schema_name=schema_name
            ).keys()

        q, v = self.build_insert_statement(
            table_name=table_name,
            schema_name=schema_name,
            matrix=matrix,
            column_names=column_names
        )
        logger.debug("Inserting data")
        try:
            self.query(q, v)
            logger.info("Data inserted successfully")
            
            return True
        
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            raise e  
            
    def delete_table(self, table_name: str, schema_name: str = None):

        schema_name = schema_name if schema_name else DEFAULT_SCHEMA

        query_expression = sql.SQL(
            "DROP TABLE IF EXISTS {schema}.{table} CASCADE"
        ).format(schema=sql.Identifier(schema_name), table=sql.Identifier(table_name))

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

    # Data Type Methods

    def type_to_sql_string(
        self, obj: object = None, dtype=None, length: int = None
    ) -> str:

        if isinstance(dtype, str):
            dtype = dtype.lower()

        elif isinstance(dtype, dict):
            if "data_type" in dtype and isinstance(dtype["data_type"], str):
                try:
                    new_dtype = self.type_to_sql_string(
                        dtype=dtype["data_type"],
                        length=dtype.get("character_maximum_length", None),
                    )

                    return new_dtype

                except Exception as e:
                    logger.error(f"Error converting data type dictionary: {e}")
                    raise e
            else:
                raise ValueError(
                    f"Invalid data type dictionary format, could not find data_type: {dtype}"
                )

        elif obj:
            dtype = str(type(obj)).lower()

        else:
            raise ValueError(
                f"Invalid data type: {dtype} - must be str or dict or pass objectas obj"
            )

        logger.debug(f"Converting data type {dtype} with length {length} to SQL string")

        if any(s in dtype for s in ["int", "integer", "smallint", "bigint"]):
            if "64" in dtype or "big" in dtype:
                return f"BIGINT"
            else:
                return f"INTEGER"

        elif any(s in dtype for s in ["float", "double", "numeric"]):
            return f"FLOAT"

        elif any(
            s in dtype
            for s in [
                "object",
                "str",
                "varchar",
                "category",
                "character",
                "text",
                "string",
                "char",
                "unicode",
            ]
        ):
            if "varchar" in dtype and not length:
                pattern = r"\((\d+)\)"
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
            return None  # f"NaN"

        elif any(s in dtype for s in ["geometry", "geom", "user-defined"]):
            return f"GEOMETRY"

        elif any(s in dtype for s in ["bool", "boolean"]):
            return f"BOOLEAN"

        else:
            raise ValueError(f"Could not convert data type: {dtype}")

    def to_insert_value_type(self, obj) -> object:
        # fast convert
        if not obj:
            return None
        
        if isinstance(obj, (int, float, bool, str)):

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
                # logger.debug(f"Converting geometry to WKT")
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
        
    def timestamp_to_sql_string(self, obj, format= None):
        
        if not isinstance(format, str):
            format = "%Y-%m-%d %H:%M:%S"

        if isinstance(obj, str):
            date_string = obj
        else:
            try:
                date_string = str(obj)
            except Exception as e:
                logger.error(f"Error converting date <{obj}> to string: {e}")
                return None
        
        try:
            parsed_date = parser.parse(date_string)
            formatted_date = parsed_date.strftime(format)

            return formatted_date

        except Exception as e:
            logger.error(f"Error parsing date string <{date_string}>: {e}")
            return None

    # Factory Methods

    @classmethod
    def from_param_dict(cls, params: dict):

        if not isinstance(params, dict):
            raise ValueError("Params must be a dictionary")

        instance = cls()

        try:
            instance.db_user = params.get(
                [key for key in params.keys() if "user" in key][0]
            )
            instance.db_host = params.get(
                [key for key in params.keys() if "host" in key][0]
            )
            instance.db_name = params.get(
                [
                    key
                    for key in params.keys()
                    if "data" in key or ("db" in key and "name" in key)
                ][0]
            )
            instance.db_credential = params.get(
                [
                    key
                    for key in params.keys()
                    if "credential" in key or "password" in key
                ][0]
            )
        except Exception as e:
            logger.error(
                f"Error extracting database params from dictionary {params}: {e}"
            )
            raise e

        if any(
            not i
            for i in [
                instance.db_user,
                instance.db_host,
                instance.db_name,
                instance.db_credential,
            ]
        ):
            raise ValueError(f"Missing database parameter in dictionary {params}")

        # for key, value in params.items():
        #    setattr(instance, value["var"], params.get(key, value["default"]))

        return instance

    @classmethod
    def from_postgres(cls, env_name: str = None):

        if not env_name:
            env_name = "local"

        try:
            instance = cls.from_param_dict(DATABASE_DICT[env_name])
            logger.info(f"Database client created for {env_name} postgres environment")
        except Exception as e:
            logger.error(
                f"Error creating DatabaseClient from params {DATABASE_DICT[env_name]}: {e}"
            )
            raise e

        if instance.db_credential:
            try:
                instance.test_connection()
            except Exception as e:
                logger.error(f"Error creating DatabaseClient from params: {e}")
                raise e
        else:
            raise Exception("Database credential not initialized")

        return instance
    
class PGTable(DatabaseClient):
    def __init__(
        self,
        db_host: str = None,
        db_name: str = None,
        db_user: str = None,
        db_credential: str = None,
        db_port: int = None,
        schema_name: str = None,
        table_name: str = None,

    ):
        
        super().__init__(
            db_host=db_host,
            db_name=db_name,
            db_user=db_user,
            db_credential=db_credential,
            db_port=db_port,
        )
        logger.debug(f"Initializing PGTable {schema_name}.{table_name}")
        
        self.schema_name = schema_name
        self.table_name = table_name
        self.name = table_name
        self.row_count = None
        self.column_dict = None
        self.column_names = None
        
        if not table_name:
            logger.warning(
                "No table name provided upon PGTable initialization")
        if not schema_name:
            logger.warning(
                "No schema name provided upon PGTable initialization")
        
           
    def clear(self):
        try:
            logger.debug(f"Clearing table {self.schema_name}.{self.table_name}")
            self.clear_table(
                table_name=self.table_name,
                schema_name=self.schema_name,
            )
            logger.info(f"Table {self.schema_name}.{self.table_name} cleared successfully")
            return True

        except Exception as e:
            logger.error(f"Error clearing table {self.schema_name}.{self.table_name}: {e}")
            raise e

    @classmethod
    def table_from_postgres(
        cls,
        table_name: str,
        schema_name: str,
        db_params: dict = None,
        env_name: str = None,
    ):

        if isinstance(env_name, str):
            if env_name in DATABASE_DICT:
                try:
                    db_client = DatabaseClient.from_postgres(env_name)
                except Exception as e:
                    raise e
            else:
                raise ValueError(f"Invalid environment name {env_name}")

        elif isinstance(db_params, dict):
            try:
                db_client = DatabaseClient.from_param_dict(db_params=db_params)
            except Exception as e:
                raise e
        else:
            raise ValueError(
                "Invalid parameters provided - db_params or env_name required"
            )
        
        exists = db_client.table_exists(table_name=table_name, schema_name=schema_name)
        if not exists:
            raise ValueError(f"Table {table_name} does not exist in schema {schema_name} in {db_client.db_host}")

        instance = cls(
            db_host=db_client.db_host,
            db_name=db_client.db_name,
            db_user=db_client.db_user,
            db_credential=db_client.db_credential,
            db_port=db_client.db_port,
            table_name=table_name,
            schema_name=schema_name,
        )
        logger.info("PGTable instance created")
        
        logger.debug(f"Loading table {schema_name}.{table_name} from {db_client.db_host}")
        
        try:
            logger.debug(f"Getting row count for {schema_name}.{table_name}")
            instance.row_count = instance.query(
                sql.SQL("SELECT COUNT(*) FROM {schema}.{table}").format(
                    schema=sql.Identifier(schema_name), table=sql.Identifier(table_name)
                )
            )[0][0]

        except Exception as e:
            instance.row_count = None
            logger.error(f"Error getting row count for {schema_name}.{table_name}: {e}")
            raise e
            
        try:
            logger.debug(f"Getting column information for {schema_name}.{table_name}")
            instance.column_dict = instance.column_dict_from_db_table(
                table_name=table_name, schema_name=schema_name)
            instance.column_names = list(instance.column_dict.keys())
        except Exception as e:
            logger.error(f"Error getting column information for {schema_name}.{table_name}: {e}")
            instance.column_dict = None
            instance.column_names = None
            raise e
        
        logger.info(
            f"Table {instance.schema_name}.{instance.table_name} with {instance.row_count} records loaded from {instance.db_host}"
        )

        return instance