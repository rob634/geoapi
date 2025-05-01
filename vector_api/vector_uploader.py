
from functools import wraps
from math import ceil
import os
from multiprocessing import Pool, current_process
import numpy as np

from geopandas import GeoDataFrame
from psycopg2 import sql
from pyproj import CRS

from database_client import DatabaseClient
from vector_api import VectorHandler
from utils import logger
from utils import (
    DEFAULT_GEOMETRY_NAME,
    DEFAULT_SCHEMA,
    VectorHandlerError,
    DEFAULT_DB_NAME,
    DEFAULT_DB_USERNAME,
    DEFAULT_EPSG_CODE,
    DATABASE_ALLOWED_CHARACTERS,
    DATABASE_RESERVED_WORDS,
    GDF_VALID_DATATYPES,
    VALID_GEOMETRY_TYPES
    
)

class EnterprisePostGIS(DatabaseClient):
    
    def __init__(

        self,
        schema_name=None,
        table_name=None,
        column_dict=None,  # optional column mapping of SQL types
        geometry_type=None,  # optional geometry type       
        
        credential=None,
        db_params=None,  # local testing only
        db_user=None,  # sde, gisowner, other
        epsg_code=None,  # testing only, this is set globally
        geometry_name=None,  # testing only, this is set globally
        ):
        # database config
        db_user = db_user if db_user else DEFAULT_DB_USERNAME
        db_name = DEFAULT_DB_NAME
        super().__init__(db_user=db_user, db_name=db_name, credential=credential)

        # instance attributes
        self.column_dict = column_dict
        self.epsg_code = epsg_code if epsg_code else DEFAULT_EPSG_CODE
        self.geometry_name = geometry_name if geometry_name else DEFAULT_GEOMETRY_NAME
        self.schema_name = schema_name if schema_name else DEFAULT_SCHEMA
        
        self.gdf = None
        self.geometry_type = geometry_type
        self.table_name = table_name
        self.valid_gdf = False
    
    @staticmethod
    def valid_geometry(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if 'gdf' in kwargs:
                gdf = kwargs['gdf']
                if isinstance(gdf, GeoDataFrame):
                    geometry_types = list(set(gdf.geometry.type))
                    if len(geometry_types) == 1:
                        if geometry_types[0] in VALID_GEOMETRY_TYPES:
                            logger.info(f"GeoDataFrame geometry type is valid: {geometry_types[0]}")
                            if any(gdf.geometry.is_empty):
                                raise ValueError("GeoDataFrame contains empty geometries")
                            if any(~gdf.geometry.is_valid):
                                raise ValueError("GeoDataFrame contains invalid geometries")
                            if any(gdf.geometry.isna()):
                                raise ValueError("GeoDataFrame contains NaN geometries")
                            if any(gdf.geometry.isnull()):
                                raise ValueError("GeoDataFrame contains null geometries")
                            if any(gdf.geometry.has_z):
                                raise ValueError(f"GeoDataFrame contains z values - z values are not supported")
                        else:
                            raise ValueError(f"GeoDataFrame geometry type is not valid: {geometry_types[0]}")
                    elif len(geometry_types) > 1:
                        raise ValueError(f"GeoDataFrame contains multiple geometry types: {geometry_types}")
                    else:
                        raise ValueError("GeoDataFrame contains no geometry")
                else:
                    raise ValueError("Invalid GeoDataFrame provided in kwargs")
            else:
                logger.error("No GeoDataFrame provided in kwargs")
            
            return func(self, *args, **kwargs)
        
        return wrapper
    
    @valid_geometry
    def gdf_valid_for_postgis(self, gdf: GeoDataFrame) -> bool:
        # Returns True if GeoDataFrame is valid for PostGIS otherwise raises error

        if not gdf.geometry.name == self.geometry_name:
            raise ValueError(f"GeoDataFrame geometry name does not match database geometry name {gdf.geometry.name} != {self.geometry_name}")
        
        if gdf.crs and isinstance(gdf.crs, CRS):
            if gdf.crs.to_epsg() == DEFAULT_EPSG_CODE:
                logger.info(f"GeoDataFrame CRS is valid: {gdf.crs}")
            else:
                raise ValueError(f"GeoDataFrame CRS must be EPSG:{self.epsg_code} CRS found: {gdf.crs}")
        else:
            raise ValueError("GeoDataFrame CRS is not valid")
        
        column_names = list(gdf.columns)
        if 'objectid' in column_names:
            raise ValueError("Column name 'objectid' is reserved - cannot be used as a column name")
        
        for _name in column_names:
            if _name.isnumeric():
                raise ValueError(f"Column name {_name} is numeric - alphanumeric names only")
            if _name[0].isdigit():
                raise ValueError(f"Column name {_name} starts with a number - must start with letter or underscore")
            if not _name.islower():
                raise ValueError(f"Column name {_name} is not lowercase - lowercase names only")
            if not all(char in DATABASE_ALLOWED_CHARACTERS for char in _name):
                raise ValueError(f"Column name {_name} contains invalid characters - allowed characters: abcdefghijklmnopqrstuvwxyz0123456789_")
            if _name in DATABASE_RESERVED_WORDS:
                raise ValueError(f"Column name {_name} is a reserved word - names cannot be in {DATABASE_RESERVED_WORDS}")
            dtype = str(gdf[_name].dtype).lower()
            if not any(valid_dtype in dtype for valid_dtype in GDF_VALID_DATATYPES):
                raise ValueError(f"Column name {_name} has invalid data type <{dtype}> - allowed types: {GDF_VALID_DATATYPES}")
            
        return True
     
    # GeoDataFrame 
    @valid_geometry
    def sql_column_list_from_gdf(
        self,
        gdf: GeoDataFrame,
        epsg: int = None,
        timestamp_uidx_name: str = None,
    ) -> list:
        #creates a list of space separated column name and SQL types
        #columnname VARCHAR
        logger.debug("Building column list")

        
        epsg = DEFAULT_EPSG_CODE

        geometry_type = str(list(set(gdf.geometry.type))[0])

        column_list = []
        column_list.append(f"objectid SERIAL")
        column_list.extend(
            f"{col} {self.py_obj_to_sql_type(gdf[col].dtype)}"
            for col in gdf.columns
            if str(gdf[col].dtype).lower() != "geometry" and col != timestamp_uidx_name
        )
        
        if isinstance(timestamp_uidx_name, str):
            logger.debug(f"Adding timestamp column {timestamp_uidx_name}")
            column_list.append(f"{timestamp_uidx_name} TIMESTAMP")

        column_list.append(f"shape GEOMETRY({geometry_type}, {epsg})")

        logger.info(f"column_list built for gdf")
        return column_list

    def create_postgis_table(
        self,
        table_name: str = None,
        schema_name: str = None,
        geometry_name: str = DEFAULT_GEOMETRY_NAME,
        columns: str = None,
        #uidx_name: str = None,
        #gist_name: str = None,
        timestamp_uidx_name: str = None,
        gdf: GeoDataFrame = None,
        if_exists: str = "fail",  # fail,replace
        use_instance: bool = False,
    ):
        if use_instance:
            logger.debug("Using instance gdf for table creation")
            if isinstance(self.gdf, GeoDataFrame):
                gdf = self.gdf
            else:
                logger.error("Instance gdf is not a GeoDataFrame")
                raise ValueError("Instance gdf is not a GeoDataFrame")
            
            if isinstance(self.table_name,str):
                table_name = self.table_name
                logger.debug(f"Using instance table name: {table_name}")
            else:
                logger.error("Instance table name not valid")
                raise ValueError("Instance table name not valid")
            
            if isinstance(self.schema_name,str):
                schema_name = self.schema_name
                logger.debug(f"Using instance schema name: {schema_name}")
            else:
                logger.error("Instance schema name not valid")
                raise ValueError("Instance schema name not valid")

        else:
            logger.debug("Using provided gdf for table creation")
            if not isinstance(gdf, GeoDataFrame):
                logger.error("Provided gdf is not a GeoDataFrame")
                raise ValueError("Provided gdf is not a GeoDataFrame")
            if not isinstance(table_name, str):
                logger.error("Missing table_name parameter")
                raise ValueError("Missing table_name parameter")
            if not isinstance(schema_name, str):
                logger.error("Missing schema_name parameter")
                raise ValueError("Provided schema name is not a string")

        # Handle table name conflicts if table already exists
        if self.table_exists(table_name=table_name, schema_name=schema_name):
            if if_exists == "replace":
                logger.warning(f"Table {table_name} already exists - replacing")
                try:
                    self.delete_table(table_name=table_name, schema_name=schema_name)
                    logger.info(f"Table {table_name} deleted successfully")
                except Exception as e:
                    logger.error(f"Error deleting table {table_name}: {e}")
                    raise e
            elif if_exists == "fail":
                logger.error(f"Table {table_name} already exists")
                raise ValueError(f"Table {table_name} already exists")
            else:
                logger.error(f"Invalid if_exists parameter: {if_exists}")
                raise ValueError(f"Invalid if_exists parameter: {if_exists}")

        try:
            logger.debug(f"Getting columns")
            columns = self.sql_column_list_from_gdf(gdf=gdf)
            logger.info(f"Got columns")
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            raise e

        logger.debug("Creating table query")

        create_table_query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns})"
        ).format(
            table=sql.Identifier(table_name),
            schema=sql.Identifier(schema_name),
            columns=sql.SQL(", ".join(columns)),
        )
        
        try:
            logger.debug(f"Creating table {table_name} with columns: {columns}")
            self.query(create_table_query)
            logger.info(f"Table {table_name} created successfully.")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            logger.error(f"invalid query: {create_table_query}")
            raise e
        
        gist_name = f"{table_name}_gist"
        create_gist_query = sql.SQL(
                """
                CREATE INDEX {gist_index} ON {schema}.{table} USING gist ({geometry});
                """
            ).format(
                gist_index=sql.Identifier(gist_name),
                table=sql.Identifier(table_name),
                geometry=sql.Identifier(geometry_name),
                schema=sql.Identifier(schema_name)
            )
        try:
            logger.debug(f"Creating geometry index on table {table_name}")
            self.query(create_gist_query)
            logger.info(f"Geometry index created successfully for table {table_name}")
        except Exception as e:
            logger.error(f"Error creating geometry index for table {table_name}: {e}")
            raise e
            
        uidx_name = f"{table_name}_idx"
        create_oid_index_query = sql.SQL(
                """
                CREATE UNIQUE INDEX {unique_index} ON {schema}.{table} USING btree ({column_name}) WITH (fillfactor='75');
                """
            ).format(
                table=sql.Identifier(table_name),
                unique_index=sql.Identifier(uidx_name),
                schema=sql.Identifier(schema_name),
                column_name = sql.Identifier("objectid")
            )
        try:
            logger.debug(f"Creating objectid index table {table_name}")
            self.query(create_oid_index_query)
            logger.info(f"ObjectID created successfully for table {table_name}")
        except Exception as e:
            logger.error(f"Error creating ObjectID index for table {table_name}: {e}")
            raise e
        
        if timestamp_uidx_name:
            tidx_name = f"{timestamp_uidx_name}_tidx"
            logger.debug("Creating timestamp index")
            create_tidx_query = sql.SQL(
                """
                CREATE UNIQUE INDEX {index_name} ON {schema}.{table} USING btree ({timestamp_index}) WITH (fillfactor='75');
                """
            ).format(
                index_name=sql.Identifier(tidx_name),
                table=sql.Identifier(table_name),
                schema=sql.Identifier(schema_name),
                timestamp_index=sql.Identifier(timestamp_uidx_name),
            )

            try:
                logger.debug(f"Creating timestamp index {timestamp_uidx_name} for table {table_name}")
                self.query(create_tidx_query)
                logger.info(f"Timestamp index created successfully for table {table_name}")
            except Exception as e:
                logger.error(f"Error timestamp index {timestamp_uidx_name} for table {table_name}: {e}")
                

        return table_name

    def insert_gdf_as_batch(
        self,
        gdf: GeoDataFrame = None,
        table_name: str = None,
        schema_name: str = None,
        geometry_name: str = DEFAULT_GEOMETRY_NAME,
        batch_name: int = None,):
        
        # This function assumes that table exists and GeoDataFrame has been validated
        try:
            current_proc = current_process().name
        except Exception as e:
            current_proc = None
            
        if table_name is None:
            raise ValueError("Table name must be provided")

        if not isinstance(gdf, GeoDataFrame):
            raise ValueError("No GeoDataFrame provided")
            
        the_matrix = None
        try:
            the_matrix = gdf.values.tolist()
            batch_length = len(the_matrix)
        except Exception as e:
            logger.error(f"Error converting GeoDataFrame to matrix: {e}")
            raise e
        
        if current_proc and batch_name:
            logger.debug(f"Process {current_proc} initiated - inserting data into table {table_name} batch #{batch_name} with {batch_length} rows")
        
        try:
            exp, values = self.build_insert_statement(
                table_name=table_name,
                schema_name=schema_name,
                column_names=[col for col in gdf.columns],
                matrix=the_matrix,
                geometry_name=geometry_name)
        except Exception as e:
            logger.error(f"Error building insert statement: {e}")
            raise e
        
        try:
            logger.debug(f"Inserting {batch_length} rows into {schema_name}.{table_name}")
            executed = self.query(exp, values)
            if executed:
                logger.info(f"{batch_length} rows inserted into {table_name} successfully.")
            else:
                raise VectorHandlerError(f"Unknown error inserting data into table {table_name}")
        except Exception as e:
            logger.error(f"Error inserting data into table: {e}")
            raise e
        
    @valid_geometry
    def insert_whole_gdf(
            self, 
            gdf:GeoDataFrame, 
            table_name, 
            schema_name, 
            geometry_name,
            batch_size=None,
            multiproc=True
        ):
        
        if gdf.empty:
            error_message = "GeoDataFrame is empty. No data to insert."
            logger.error(error_message)
            raise VectorHandlerError(error_message)
        
        if not isinstance(gdf, GeoDataFrame):
            raise ValueError(f"gdf parameter must be a GeoDataFrame, found {type(gdf)}")
        
        if not batch_size:
            # method to determine batch size
            batch_size = 5000
        gdf_length = len(gdf)
        if gdf_length < batch_size:
            batch_size = gdf_length
        batch_count = ceil(gdf_length / batch_size)
        cpu_count = os.cpu_count()
        
        # Split GeoDataFrame into batches
        try:                
            logger.debug(f"Splitting GeoDataFrame into {batch_count} batches with {batch_size} rows each")
            chunks = np.array_split(gdf, batch_count)
            batches = list(zip(range(1, batch_count+1), chunks))
        except Exception as e:
            logger.error(f"Error splitting GeoDataFrame into batches: {e}")
            raise e
        
        logger.info(f"Inserting data from GeoDataFrame with {gdf_length} rows into {table_name} using {cpu_count} processes in {batch_count} batches of {batch_size} rows each")
        
        if multiproc:
            logger.debug(f"Distributing {batch_count} batches across {cpu_count} parallel processes")      
            try:
                with Pool(processes=cpu_count) as pool:
                    pool.starmap(
                        self.insert_gdf_as_batch,
                        [
                            *[# batch is a tuple of (batch_number, GeoDataFrame)
                                (batch[1], table_name, schema_name, geometry_name,batch[0]) 
                                    for batch in batches
                                ]
                            ],
                        ) 
                    pool.close()
                    pool.join()
            
            except Exception as e:
                logger.error(f"Error inserting data into table with multiprocessing: {e}")
                raise e
                            
            logger.info(f"{batch_count} batches inserted into {table_name} using multiprocessing")
            
            return True
            
        else:# Sequential batch insert
            logger.debug(f"Inserting {batch_count} into table using sequential batch insert")
            b = 1
            for batch in batches:
                try:
                    logger.debug(f"Inserting data into table using batch insert - batch {b} of {batch_count}")
                    
                    self.insert_gdf_as_batch(
                        table_name=table_name,
                        schema_name=schema_name,
                        gdf=batch[1],
                        geometry_name=geometry_name)
                    
                    logger.info(f"Batch {b} of {batch_count} inserted successfully")
                    b+=1
                    
                except Exception as e:
                    error_message = f"Error inserting batch {b} of {batch_count} of  data into table: {e}"
                    logger.error(error_message)
                    raise VectorHandlerError(error_message)
                
            logger.info(f"{batch_count} batches inserted into {table_name} successfully")
            
            return True

    def column_sql_dict_from_gdf(self, gdf: GeoDataFrame, inplace=True) -> dict:
        # returns a dictionary of column names and their sql data types as strings
        #
        try:
            cdict = {
                col: self.py_obj_to_sql_type(str(gdf[col].dtype)) for col in gdf.columns
            }
            logger.debug("Column dictionary created")
            if inplace:
                self.column_dict = cdict

            return cdict
        except Exception as e:
            error_message = f"Error creating column dictionary: {e}"
            logger.error(error_message)
            raise Exception(error_message)

    def instance_to_table(
        self,
        table_name: str,
        schema_name: str = None,
        geometry_name: str = None,
        if_exists: str = "fail",
        timestamp_uidx_name: str = None,
        batch_size: int = None,
        multiproc: bool = False,
    ):

        logger.debug("instance_to_table: Uploading GeoDataFrame instance to table")
        if isinstance(self.gdf, GeoDataFrame):
            if self.valid_gdf:
                logger.info("instance_to_table: instance GeoDataFrame pre-validated for PostGIS upload")
            else:
                logger.debug("instance_to_table: Validating GeoDataFrame instance")
                try:
                    self.valid_gdf = self.gdf_valid_for_postgis(self.gdf)
                    logger.info("instance_to_table: GeoDataFrame is valid for PostGIS upload")
                except Exception as e:
                    logger.error(f"instance_to_table: GeoDataFrame is not valid for PostGIS upload: {e}")
                    raise e
        else:
            error_message = "instance_to_table: GeoDataFrame instance is missing"
            logger.error(error_message)
            raise ValueError(error_message)

        if isinstance(geometry_name, str):
            logger.debug(f"instance_to_table: Using geometry name: {geometry_name}")
        else:
            try:
                geometry_name = self.gdf.geometry.name
                logger.debug(f"instance_to_table: Using geometry name from GeoDataFrame: {geometry_name}")
            except Exception as e:
                error_message = f"instance_to_table: Error getting geometry name from GeoDataFrame: {e}"
                logger.error(error_message)
                raise e
        
        if not batch_size:
            # method to determine batch size
            batch_size = 5000
            
        # Test database connection    
        try:
            logger.debug(f"instance_to_table: Testing database connection")
            self.test_connection()
        except Exception as e:
            error_message = f"instance_to_table: VectorHandler could not connect to database: {e}"
            logger.critical(error_message)
            raise e

        # Create table
        try:
            logger.debug(f"instance_to_table: Creating table <{schema_name}.{table_name}>")
            
            self.create_postgis_table(
                table_name=table_name,
                schema_name=schema_name,
                timestamp_uidx_name=timestamp_uidx_name,
                use_instance=True,
                if_exists=if_exists,
            )
            
            logger.info(f"Table {table_name} created successfully")
        except Exception as e:
            logger.error(f"Error creating table:{e}")
            raise e

        # Insert data
        try:
            logger.debug(f"instance_to_table: Inserting data into table {table_name}")
            
            self.insert_whole_gdf(
                gdf=self.gdf,
                table_name=table_name,
                schema_name=schema_name,
                geometry_name=geometry_name,
                batch_size=batch_size,
                multiproc=multiproc,
            )
            
            logger.info(f"Instance GDF data inserted into {table_name}") 
        except Exception as e:
            logger.error(f"instance_to_table: Error inserting data into table: {e}")
            raise e
        
        return True

    @classmethod
    def from_valid_gdf(
        cls,
        gdf: GeoDataFrame,
        table_name: str,
        schema_name: str = None,
        db_user: str = None,
        geometry_name: str = None,
        epsg_code: int = None,
        column_dict: dict = None,

    ):
        instance = cls(
            table_name=table_name,
            schema_name=schema_name,
            db_user = db_user,
            geometry_name=geometry_name,
            epsg_code=epsg_code,
            column_dict=column_dict,
        )
        if isinstance(gdf, GeoDataFrame):
            try:
                valid = instance.gdf_valid_for_postgis(gdf)
                logger.info(f"GeoDataFrame is valid")

                instance.gdf = gdf.copy()
                instance.valid_gdf = True
            except Exception as e:
                logger.error(f"GeoDataFrame is not valid: {e}")
                raise e
        elif isinstance(gdf, VectorHandler):
            if getattr(gdf,'valid_gdf'):
                instance.gdf = gdf.gdf.copy()
                instance.valid_gdf = True
            else:
                raise ValueError("VectorHandler GeoDataFrame is not valid")
        else:
            raise ValueError("gdf parameter must be a GeoDataFrame or VectorHandler")
        
        return instance
                
