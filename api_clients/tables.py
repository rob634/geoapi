from .database_client import DatabaseClient
from pandas import DataFrame
from utils import *
from math import ceil 

class TableHandler(DatabaseClient):
    
    
    def __init__(self,user:str=None):
        super().__init__(user=user)
        # columns building methods
        
    def schema_list_from_df(self, df:DataFrame, use_oid: bool = False):
        # Pandas types to SQL types in a list
        logger.info("Building columns")
        columns = []
        if use_oid:
            columns.append(f"objectid SERIAL")
        for col in df.columns:
            dtype = str(df[col].dtype)
            if self.is_valid_dtype(dtype) and dtype != "geometry":
                columns.append(f"{col} {self.to_sql_type(dtype)}")
            elif dtype == "geometry":
                continue
            else:
                logger.warning(
                    f"columns Warning: Column {col} has an unsupported datatype {dtype} and is not being included"
                )
        logger.info(f"columns built for gdf")

        return columns

    def schema_dict_from_db(
        self, schema_name: str = None, table_name: str = None):
        # Get schema from database
        if None in [table_name, schema_name]:
            raise ValueError("Table and schema names must be provided")

        try:
            self.query()
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    """,
                        (schema_name, table_name),
                    )
                    table_schema = cursor.fetchall()
            table_schema_dict = {col[0]: col[1] for col in table_schema}
        except Exception as e:
            logger.error(f"Error querying table schema: {e}")
            raise e

        return table_schema_dict
    
    def create_table(
        self,
        table_name: str = None,
        schema_name: str = None,
        schema_list: list = None,
        gdf: DataFrame = None,
    ):
        logger.trace("Creating table")
        if not table_name:
            logger.error("Table name must be provided")
            raise ValueError("Table name must be provided")

        schema_list = schema_list if schema_list else self.gdf_to_sql_column_list(gdf)
        obj_name = f"{schema_name}.{table_name}" if schema_name else table_name
        create_table_query = (
            f"CREATE TABLE IF NOT EXISTS {obj_name} ({','.join(schema_list)});"
        )
        logger.trace(f"Creating table {obj_name} with schema: {','.join(schema_list)}")
        try:
            with self.connect() as conn:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(create_table_query)
                        conn.commit()
                        logger.info(f"Table {obj_name} created successfully.")
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error creating table {obj_name}: {e}")
                    raise e
            logger.info(
                f"Table {obj_name} created successfully."
            )
            return table_name
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise e

    def insert_rows_from_df(self, table_name: str = None,
                    schema_name: str = None,
                    column_list: list = None,
                    data: list = None):
        
        if isinstance(data, DataFrame):
            column_list = list(data.columns)
            values_list = [tuple(l) for l in data.values.tolist()]
        elif isinstance(column_list,list) and isinstance(data, list):
            values_list = data
        else:
            values_list = None
        
        if any([not v for v in [table_name, schema_name, column_list, values_list]]):
            raise ValueError("Table name, schema name, and data must be provided")

        column_string = ", ".join(column_list)
        placeholders = ", ".join(["%s"] * len(column_list))

        query = f"INSERT INTO {schema_name}.{table_name} ({column_string}) VALUES {', '.join(['(' + placeholders + ')'] * len(values_list))}"

        flattened_values = [item for sublist in values_list for item in sublist]

        try:
            logger.trace(f"Inserting {len(values_list)} rows into {schema_name}.{table_name}")
            self.query(query, flattened_values)

        except Exception as e:
            logger.error(f"Error inserting rows into {schema_name}.{table_name}")
            raise e

    def insert_data_from_df(
        self,
        schema_name: str = None,
        table_name: str = None,
        df: DataFrame = None,
        batch_size: int = None,
        use_index: bool = False,
        validate: bool = False,
    ):

        if not isinstance(df, DataFrame) or df.empty:
            raise ValueError("Empty or invalid DataFrame provided")
        if not table_name:
            raise ValueError("Table name must be provided")

        obj_name = f"{schema_name}.{table_name}" if schema_name else table_name

        if validate:
            logger.trace(f"Validating DataFrame against {obj_name}")
            if not self._table_exists(schema_name, table_name):
                raise ValueError(
                    f"Table {schema_name}.{table_name} does not exist in database"
                )

            db_cols = [
                k for k in self.schema_dict_from_db(schema_name, table_name).keys()
            ]
            df_cols = [k for k in df.columns]
            if not all([col in db_cols for col in df_cols]):
                raise ValueError(
                    f"DataFrame columns {list(set(df_cols).difference(db_cols))} are not in database table {schema_name}.{table_name}"
                )
            logger.trace(f"Table {obj_name} exists and contains all columns in DataFrame")

        if not use_index:
            logger.trace("Resetting DataFrame index")
            df.reset_index(drop=True, inplace=True)
        
        idx_end = df.index[-1]
        values_list = []
        batch_size = batch_size if batch_size else idx_end+1
        batch_count = ceil(idx_end / batch_size)
        column_list = list(df.columns)
        
        logger.trace(
            f"Inserting {df.shape[0]} rows into {obj_name} in {batch_count} batches"
        )
        
        for idx, row in df.iterrows():

            values_list.append(tuple(row[col] for col in row.index))

            if len(values_list) >= batch_size or idx == idx_end:

                row_count = len(values_list)
                logger.trace(
                    f"Inserting {row_count} rows into {obj_name}: batch {ceil(idx/batch_size)} of {batch_count}"
                )

                try:
                    self.insert_rows(
                        table_name=table_name,
                        schema_name=schema_name,
                        column_list=column_list,
                        data=values_list,
                    )
                    values_list = []
                    logger.info(
                        f"{row_count} rows inserted succesfully into {obj_name}: batch {ceil(idx/batch_size)} of {batch_count}")

                except Exception as e:
                    logger.error(f"Error {row_count} rows into {obj_name}: batch {ceil(idx/batch_size)} of {batch_count}")
                    logger.error(e)
                    raise e

        return table_name
    
