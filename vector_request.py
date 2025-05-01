import azure.functions as func
from datetime import datetime

from request_handler import BaseRequest
#from api_clients import DatabaseClient
#from vector_api import VectorHandler, VectorLoader, EnterprisePostGIS
#from enterprise_api import EnterpriseClient
from utils import (
    logger,
    WORKSPACE_CONTAINER_NAME,
    DEFAULT_DB_USERNAME,
    DEFAULT_EPSG_CODE,
    DEFAULT_SCHEMA
)


class VectorRequest(BaseRequest):

    def __init__(self, req: func.HttpRequest, command: str = None):

        logger.info("Initializing VectorRequest")
        super().__init__(req)
        self.append = None
        self.attribute_index = None
        self.batch_size = None
        self.column_dict = None
        self.db_user = None
        self.epsg_code = None
        self.file_name = None
        self.file_type = None
        self.geometry_name = None
        self.if_exists = None
        self.indices_to_add = None
        self.lat_attr_name = None
        self.layer_name = None
        self.lon_attr_name = None
        self.multiprocessing = None
        self.overwrite = None
        self.schema_name = None
        self.table_name = None
        self.time_index = None
        self.time_indices_to_add = None
        self.wkt_column = None

        self.workspace_container_name = None

        error_message = None

        params_defaults = {
            "append": {"var": "append", "default": False},
            "attributeIndex": {"var": "attribute_index", "default": None},
            "batchSize": {"var": "batch_size", "default": None},
            "columnDict": {"var": "column_dict", "default": None},
            "containerName": {
                "var": "workspace_container_name",
                "default": WORKSPACE_CONTAINER_NAME,
            },
            "dbUser": {"var": "db_user", "default": DEFAULT_DB_USERNAME},
            "epsgCode": {"var": "epsg_code", "default": DEFAULT_EPSG_CODE},
            "fileName": {"var": "file_name", "default": None},
            "fileType": {"var": "file_type", "default": None},
            "geometryName": {"var": "geometry_name", "default": "shape"},
            "geometryType": {"var": "geometry_type", "default": None},
            "latName": {"var": "lat_attr_name", "default": None},
            "layerNames": {"var": "layer_name", "default": None},
            "lonName": {"var": "lon_attr_name", "default": None},
            "multiprocessing": {"var": "multiprocessing", "default": False},
            "overwrite": {"var": "overwrite", "default": False},
            "schemaName": {"var": "schema_name", "default": DEFAULT_SCHEMA},
            "tableName": {"var": "table_name", "default": None},
            "timeIndex": {"var": "time_index", "default": None},
            "WKTColumn": {"var": "wkt_column", "default": None},
        }

        for key, value in params_defaults.items():
            setattr(self, value["var"], self.req_json.get(key, value["default"]))

        if command == "stage":
            if self.file_name:
                if not self.table_name:
                    try:
                        inf_name = self.file_name.split(".")[0]
                        logger.warning(
                            f"tableName not provided for file <{self.file_name}> - inferring from file name table name: {inf_name}"
                        )
                        self.table_name = inf_name
                    except Exception as e:
                        error_message = f"Error processing command <{command}> Table Name not provided and could not infer table name from filename {self.file_name}"
                        logger.critical(f"{error_message} {e}")
                        self.response = self.return_exception(e, message=error_message)
                        command = None

            else:
                error_message = (
                    f"Error processing command stage_vector File Name not provided"
                )
                logger.critical(error_message)
                self.response = self.return_exception(
                    ValueError(error_message), message=error_message
                )
                command = None

        if command == "stage":
            if self.overwrite and self.append:
                error_message = f"Error processing command <{command}> parameters overwrite and append cannot both be True"
                logger.critical(error_message)
                self.response = self.return_exception(
                    ValueError(error_message), message=error_message
                )
                command = None
            elif self.append:
                self.if_exists = "append"
            elif self.overwrite:
                self.if_exists = "replace"
            else:
                self.if_exists = "fail"

        if command == "stage":
            # indices to add
            if isinstance(self.attribute_index, str):

                self.indices_to_add = [self.attribute_index]
            elif isinstance(self.attribute_index, list):

                self.indices_to_add = self.attribute_index
            else:
                self.indices_to_add = None

            if isinstance(self.time_index, str):
                self.time_indices_to_add = [self.time_index]
            elif isinstance(self.time_index, list):
                self.time_indices_to_add = self.attribute_index
            else:
                self.time_indices_to_add = None

            if self.indices_to_add:
                logger.info(f"Indices to add: {self.indices_to_add}")
            if self.time_indices_to_add:
                logger.info(f"Time Indices to add: {self.time_indices_to_add}")

            self.response = self._stage()

        elif command == "list_tables":
            self.response = self._list_tables()

        elif command == "return_error":
            self.response = self.return_error(error_message)

        else:
            self.response = self.return_error(
                f"Invalid VectorRequest command: {command}: {error_message}"
            )

    def _list_tables(self):

        if hasattr(self, "req_json") and self.req_json:
            geo_only = self.req_json.get("geoOnly", True)
            return_columns = self.req_json.get("returnColumns", False)
            schema_name = self.req_json.get("schemaName", DEFAULT_SCHEMA)
        else:
            geo_only = True
            return_columns = False
            schema_name = DEFAULT_SCHEMA

        try:
            D = DatabaseClient.from_vault()
        except Exception as e:
            error_message = f"Error connecting to database"
            logger.critical(f"{error_message} {e}")
            return self.return_exception(e, error_message)

        try:

            tables = D.list_tables(
                schema_name=schema_name,
                geo_only=geo_only,
                return_columns=return_columns,
            )

            return self.return_success(
                message=f"Tables in database: {tables}",
                json_out={"tables": tables},
            )

        except Exception as e:
            error_message = f"Error listing tables in database"
            logger.critical(f"{error_message} {e}")
            return self.return_exception(e, message=error_message)

    def _stage(self):

        try:
            logger.debug("Creating DatabaseClient instance")
            D = DatabaseClient.from_vault()
            logger.info("DatabaseClient instance created")
        except Exception as e:
            error_message = f"Error creating DatabaseClient instance"
            logger.critical(f"{error_message} {e}")
            return self.return_exception(e, message=error_message)

        if D.table_exists(table_name=self.table_name, schema_name=self.schema_name):
            if self.if_exists == "fail":
                return self.return_error(
                    f"Table {self.schema_name}.{self.table_name} already exists in database. Please rename table or set overwrite parameter to True to replace the table or set append to True to append data to the existing table"
                )
            elif self.if_exists == "replace":
                try:
                    D.delete_table(
                        table_name=self.table_name, schema_name=self.schema_name
                    )
                    logger.info(
                        f"Table {self.schema_name}.{self.table_name} dropped from database"
                    )
                except Exception as e:
                    error_message = f"Error dropping table {self.schema_name}.{self.table_name} from database"
                    logger.critical(f"{error_message} {e}")

                    return self.return_exception(e, message=error_message)

            elif self.if_exists == "append":
                logger.info(
                    f"Table {self.schema_name}.{self.table_name} already exists in database and if_exists=APPEND"
                )
                ### Implement append here ###
        else:
            try:
                self.table_name = D.fix_table_name(self.table_name)
            except Exception as e:
                return self.return_error(
                    f"Error validating table name {self.table_name}: {e}"
                )

        try:
            logger.debug(
                f"Instantiating VectorHandler with blob file: {self.file_name} in container: {self.workspace_container_name}"
            )

            vector_gdf = VectorLoader.from_blob_file(
                file_name=self.file_name,
                file_type=self.file_type,
                layer_name=self.layer_name,
                lat_name=self.lat_attr_name,
                lon_name=self.lon_attr_name,
                wkt_column=self.wkt_column,
                container_name=self.workspace_container_name,
            )
            logger.info(f"GeoDataFrame instance created from {self.file_name}")

        except Exception as e:
            error_message = f"Error creating GeoDataFrame from blob file {self.file_name} in container {self.workspace_container_name}"
            logger.critical(f"{error_message} {e}")
            return self.return_exception(e, message=error_message)

        try:
            logger.debug(f"Creating VectorHandler instance from GeoDataFrame")
            vector = VectorHandler.from_gdf(
                gdf=vector_gdf,
                geometry_name=self.geometry_name,
                column_dict=self.column_dict,
            )
            logger.info("VectorHandler instance created from GeoDataFrame")
        except Exception as e:
            error_message = "Error creating VectorHandler instance from GeoDataFrame"
            logger.critical(f"{error_message} {e}")
            return self.return_exception(e, message=error_message)

        try:
            logger.debug(f"Preparing GeoDataFrame for database")
            vector.prepare_gdf(
                epsg_code=self.epsg_code,
                geometry_name=self.geometry_name,
                inplace=True
            )
            if vector.valid_gdf:
                logger.info("GeoDataFrame prepared for database")
            else:
                logger.error(
                    "GeoDataFrame not valid for database - check geometry type and column names"
                )
        except Exception as e:
            error_message = f"Error preparing GeoDataFrame for database"
            logger.critical(f"{error_message} {e}")
            return self.return_exception(e, message=error_message)

        try:
            logger.debug(f"Initiating database upload")
            vpg = EnterprisePostGIS.from_valid_gdf(
                gdf=vector,
                table_name=self.table_name,
                schema_name=self.schema_name,
                geometry_name=self.geometry_name,
                epsg_code=self.epsg_code,
                db_user=self.db_user,
            )
            logger.info("PostGIS instance created from GeoDataFrame")
        except Exception as e:
            error_message = f"Error creating PostGIS instance from <{type(vector)}>"
            logger.critical(f"{error_message} {e}")
            return self.return_exception(e, message=error_message)

        try:
            logger.debug(
                f"Creating table {self.schema_name}.{self.table_name} in database"
            )
            vpg.instance_to_table(
                table_name=self.table_name,
                schema_name=self.schema_name,
                geometry_name=self.geometry_name,
                if_exists=self.if_exists,
                batch_size=self.batch_size,
                multiproc=self.multiprocessing,
            )
            logger.info(
                f"GeoDataFrame instance inserted into table {self.schema_name}.{self.table_name}"
            )

        except Exception as e:

            error_message = (
                f"VectorRequest Critical Error creating table from VectorHandler.gdf"
            )
            logger.critical(f"{error_message} {e}")
            return self.return_exception(e, message=error_message)

        logger.info(f"Table created: {self.schema_name}.{self.table_name} ")

        return self.return_success(
            message=out_message,
            json_out={"table_name": self.table_name, "schema_name": self.schema_name},
        )
