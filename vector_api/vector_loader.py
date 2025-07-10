from functools import wraps
from math import ceil, floor
import os
import tempfile
import zipfile


from geopandas import GeoDataFrame
from geopandas import read_file as gpd_read_file
from pandas import DataFrame
from pandas import read_csv as pd_read_csv
#from pyogrio.errors import DataLayerError
#from osgeo import ogr
from shapely import Point, wkt
from shapely.errors import WKTReadingError, ShapelyError

from api_clients import StorageHandler

from utils import (
    logger,
    DEFAULT_CRS_STRING,
    DEFAULT_WORKSPACE_CONTAINER,
    VECTOR_FILE_EXTENSIONS,
    VECTOR_FILE_DICT,
    ZIP_FORMATS,
    StorageHandlerError,
    VectorHandlerError,
)

class VectorLoader:
    
    
    def __init__(
        self,
        file_name=None,
        file_type=None, 
        layer_name=None,
        lat_name=None,
        lon_name=None,
        wkt_column=None,
        
        credential=None,
        container_name=None):
        
        self.file_name = file_name
        self.file_type = file_type
        self.layer_name = layer_name
        self.lat_name = lat_name
        self.lon_name = lon_name
        self.wkt_column = wkt_column
        
        # Instance attributes that may or may not be relevant
        self.gdf = None  
        self.file_extension = None
        self.storage = None
        self.valid_gdf = None
        
        self.loader = None
        self.loaders = {
                'csv':self.csv_to_gdf, #vector_file_name: str, lat_name: str=None, lon_name: str=None, wkt_column: str=None
                'gdb':None,
                'geojson':self.geojson_to_gdf,#vector_file_name
                'gpkg':self.gpkg_to_gdf, # vector_file_name: str, layer_name: str=None             
                'json':self.geojson_to_gdf, 
                'kml':self.kml_to_gdf, #vector_file_name
                'kmz':self.kmz_to_gdf, # kmz_name: str, kml_name: str=None
                'shp':self.shp_zip_to_gdf, # zip_name: str, shp_name: str=None
                'txt':None, 
                'zip':None
             }
        
        try:
            logger.debug(f"Initializing VectorLoader storage with container {container_name}")
            self.storage_init(
                container_name=container_name,
                credential=credential,
            )
        except Exception as e:
            error_message = f"VectorHandler could not initialize StorageHandler: {e}"
            logger.error(error_message)
            self.storage = None
            
    @staticmethod
    def check_storage(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if hasattr(self, 'storage'):
                if isinstance(getattr(self,"storage"), StorageHandler):
                    
                    logger.info(f"{self.__class__.__name__} storage validated")
                    
                else:
                    raise StorageHandlerError("Storage handler is not initialized.")
            else:
                raise StorageHandlerError(f"{self.__class__.__name__} does not have a StorageHandler attribute.")
            
            return func(self, *args, **kwargs)
        
        return wrapper        
    
    def storage_init(self, container_name: str = None, credential=None):

        container_name = container_name if container_name else DEFAULT_WORKSPACE_CONTAINER
        
        try:
            logger.debug(f"Initializing VectorHandler storage with container {container_name}")
            self.storage = StorageHandler(
                workspace_container_name=container_name,
                credential=credential)
            logger.info(f"StorageHandler initialized with container {container_name}")
            
        except Exception as e:
            error_message = f"Could not initialize StorageHandler: {e}"
            logger.error(error_message)
            raise StorageHandlerError(error_message)

    # Validation Methods
    
    def xy_df_to_gdf(self, df: DataFrame, lat_name: str, lon_name: str):
        
        if not isinstance(df, DataFrame):
            error_message = f"Invalid DataFrame provided: {type(df)}"
            logger.error(error_message)
            raise ValueError(error_message)
    
        df_len = len(df)
        
        if lat_name in df.columns and lon_name in df.columns:
            logger.debug(f"Validating latitude and longitude values in DataFrame columns {lat_name}, {lon_name}")
            valid_rows = df[~
                ((df[lat_name].apply(floor) < -180) |
                (df[lat_name].apply(ceil) > 180))  |
                ((df[lon_name].apply(floor) < -180) |
                (df[lon_name].apply(ceil) > 180))
            ]
            valid_len = len(valid_rows)
            
            if valid_len == 0:
                error_message = f"no valid lat/lon values not found in DataFrame columns {df.columns}"
                logger.error(error_message)
                raise ValueError(error_message)
            
            elif df_len > valid_len: 
                bad_count = df_len - valid_len
                logger.error(f"Invalid lat/lon values found in {bad_count} rows")
                df = valid_rows.copy()
                df_len = len(df)
                logger.warning(f"Dropped {bad_count} rows from DataFrame")
                
            else:
                logger.info(f"All {df_len} rows are valid lat/lon values")
                
            try:
                logger.debug(f"Building GeoDataFrame from lat/lon table {lat_name}, {lon_name} with {df_len} rows")
                gdf = GeoDataFrame(
                    df,
                    geometry=[Point(xy) 
                        for xy in zip(df[lon_name], df[lat_name])],
                    crs=DEFAULT_CRS_STRING)
                logger.info(f"GeoDataFrame created from lat/lon table succesfully")
                
                return gdf
                
            except Exception as e:
                error_message = f"Error building GeoDataFrame from lat/lon table {lat_name}, {lon_name}: {e}"
                logger.error(error_message)
                raise Exception(error_message)
            
        else:
            error_message = f"lat_name: <{lat_name}> and lon_name: <{lon_name}> not found in DataFrame columns"
            logger.error(error_message)
            raise ValueError(error_message)

    def wkt_df_to_gdf(self, df: DataFrame, wkt_column: str):
        if isinstance(df, DataFrame):
            if wkt_column in df.columns:
                try:
                    logger.debug(f"Loading WKT data from DataFrame column {wkt_column} into GeoDataFrame")
                    gdf = GeoDataFrame(df, geometry=df[wkt_column].apply(wkt.loads))
                    logger.info(f"GeoDataFrame created from WKT table {wkt_column} with {len(gdf)} rows")
                    
                    return gdf
                
                except Exception as e:
                    error_message = f"Error building GeoDataFrame from WKT table {wkt_column}: {e}"
                    logger.error(error_message)
                    raise VectorHandlerError(f"Could not build GeoDataFrame from WKT table {e}")
            else:
                error_message = f"WKT column {wkt_column} not found in DataFrame columns {df.columns}"
                logger.error(error_message)
                raise ValueError(error_message)
        else:
            raise ValueError("Invalid DataFrame")

    def get_file_extension(self, vector_file_name: str):
        
        logger.debug(f"Getting file extension from file name: {vector_file_name}")
        
        if (isinstance(vector_file_name,str) 
            and "." in vector_file_name 
            and isinstance(vector_file_name.split(".")[-1],str)):
            
            ext = vector_file_name.split(".")[-1]
            logger.debug(f"File extension: .{ext} found in file name: {vector_file_name}")
            if ext in VECTOR_FILE_EXTENSIONS:
                logger.info(f"File extension: {ext} is valid")

                return ext
            
            else:
                error_message = f"Invalid file extension: {ext} for file name: {vector_file_name}"
                logger.error(error_message)
                raise ValueError(error_message)
        
        else:
            error_message = f"Invalid file name: {vector_file_name}"
            logger.error(error_message)
            raise ValueError(error_message)
    
    def match_vector_type(self,search_term:str):
        
        _name = None
        if "." in search_term:
            search_term = search_term.split(".")[-1]
        for key, values in VECTOR_FILE_DICT.items():
            if search_term in values:
                _name = key
        if _name:
            
            return _name
        
        else:
            error_message = f"Could not match {search_term} to a valid vector type"
            logger.error(error_message)
            raise ValueError(error_message)

        
    # GPKG
    
    @check_storage
    def get_gpkg_layers(self, vector_file_name: str):
        try:
            bytes_data = self.storage.blob_to_bytesio(vector_file_name)
        except Exception as e:
            error_message = f"VectorHandler.get_gpkg_layers error reading data for {vector_file_name} from container: {e}"
            logger.error(error_message)
            raise e
        layers = []
        with tempfile.TemporaryDirectory() as tmpdirname:
            temp_file_path = f"{tmpdirname}/temp.gpkg"
            try:
                logger.debug(f"Writing GeoPackage to temporary file {temp_file_path}")
                with open(temp_file_path, "wb") as temp_file:
                    temp_file.write(bytes_data.getvalue())
                logger.info(f"GeoPackage written to temporary file {temp_file_path}")
            except Exception as e:
                error_message = f"Error writing GeoPackage to temporary file: {e}"
                logger.error(error_message)
                raise e
            try:
                logger.debug(f"Opening GeoPackage from temporary file {temp_file_path}")
                #with ogr.Open(temp_file_path) as gpkg_source:
                #    for i in range(gpkg_source.GetLayerCount()):
                #        layer = gpkg_source.GetLayerByIndex(i)
                #        logger.debug(f"Layer {i}: {layer.GetName()}")
                #        layers.append(layer.GetName())
                #    logger.info(f"Layers found in GeoPackage: {layers}")
                    
            except Exception as e:
                error_message = f"Error writing GeoPackage to temporary file: {e}"
                logger.error(error_message)
                raise e
            
        if len(layers) > 0:
            logger.debug(layers)
            
            return layers
        else:
            error_message = f"No layers found in GeoPackage {vector_file_name}"
            logger.error(error_message)
            raise ValueError(error_message)
            
 
    @check_storage
    def gpkg_to_gdf(self, vector_file_name: str, layer_name: str=None):
        
        if isinstance(layer_name,str):
            logger.info(f"Layer name provided: {layer_name}")

        else:
            logger.error(f"GPKG requires layer name to be provided")
            
            raise ValueError(f"GPKG requires layer name to be provided")
        
        if self.storage.blob_exists(
            blob_name=vector_file_name, 
            container_name=self.storage.workspace_container_name):
            
            logger.info(f"File {vector_file_name} found in container {self.storage.workspace_container_name}")
        else:
            error_message = f"File {vector_file_name} not found in container {self.storage.workspace_container_name}"
            logger.error(error_message)
            
            raise FileNotFoundError(error_message)
        
        try:
            logger.debug(f"Reading gpkg file {vector_file_name} from blob storage")
            bytes_data = self.storage.blob_to_bytesio(vector_file_name)
            gdf = gpd_read_file(bytes_data, layer=layer_name)
            logger.info(f"GeoDataFrame created from gpkg file {vector_file_name} layer {layer_name}")
            
            return gdf
        
        except ValueError as e:
            if "not found" in str(e):
                
                error_message = f"Layer {layer_name} not found in GeoPackage {vector_file_name}: {e}"
                logger.error(error_message)
                
            else:
                error_message = f"VectorHandler.gpkg_to_gdf error reading data for {vector_file_name} from container: {e}"
                logger.error(error_message)
                
            raise e
            
        except Exception as e:
            
            error_message = f"VectorHandler.gpkg_to_gdf error reading data for {vector_file_name} from container: {e}"
            logger.error(error_message)
            
            raise e


    # CSV
    
    def xy_df_to_gdf(self, df: DataFrame, lat_name: str, lon_name: str):
        
        if not isinstance(df, DataFrame):
            error_message = f"Invalid DataFrame provided: {type(df)}"
            logger.error(error_message)
            
            raise ValueError(error_message)
    
        df_len = len(df)
        
        if lat_name in df.columns and lon_name in df.columns:
            logger.debug(f"Validating latitude and longitude values in DataFrame columns {lat_name}, {lon_name}")
            valid_rows = df[~
                ((df[lat_name].apply(floor) < -180) |
                (df[lat_name].apply(ceil) > 180))  |
                ((df[lon_name].apply(floor) < -180) |
                (df[lon_name].apply(ceil) > 180))
            ]
            valid_len = len(valid_rows)
            
            if valid_len == 0:
                error_message = f"no valid lat/lon values not found in DataFrame columns {df.columns}"
                logger.error(error_message)
                raise ValueError(error_message)
            
            elif df_len > valid_len: 
                bad_count = df_len - valid_len
                logger.error(f"Invalid lat/lon values found in {bad_count} rows")
                df = valid_rows.copy()
                df_len = len(df)
                logger.warning(f"Dropped {bad_count} rows from DataFrame")
                
            else:
                logger.info(f"All {df_len} rows are valid lat/lon values")
                
            try:
                logger.debug(f"Building GeoDataFrame from lat/lon table {lat_name}, {lon_name} with {df_len} rows")
                gdf = GeoDataFrame(
                    df,
                    geometry=[Point(xy) 
                        for xy in zip(df[lon_name], df[lat_name])],
                    crs=DEFAULT_CRS_STRING)
                logger.info(f"GeoDataFrame created from lat/lon table succesfully")
                
                return gdf
                
            except Exception as e:
                error_message = f"Error building GeoDataFrame from lat/lon table {lat_name}, {lon_name}: {e}"
                logger.error(error_message)
                
                raise e
            
        else:
            error_message = f"lat_name: <{lat_name}> and lon_name: <{lon_name}> not found in DataFrame columns"
            logger.error(error_message)
            
            raise ValueError(error_message)

    def wkt_df_to_gdf(self, df: DataFrame, wkt_column: str):
        if isinstance(df, DataFrame):
            if wkt_column in df.columns:
                try:
                    logger.debug(f"Loading WKT data from DataFrame column {wkt_column} into GeoDataFrame")
                    gdf = GeoDataFrame(df, geometry=df[wkt_column].apply(wkt.loads),crs=DEFAULT_CRS_STRING)
                    logger.info(f"GeoDataFrame created from WKT table {wkt_column} with {len(gdf)} rows")
                    
                    return gdf
                
                except WKTReadingError as e:
                    error_message = f"WKTReadingError reading data from DataFrame column {wkt_column}: {e}"
                    logger.error(error_message)
                    
                    raise e
                
                except ShapelyError as e:
                    error_message = f"ShapelyError reading data from DataFrame column {wkt_column}: {e}"
                    logger.error(error_message)
                    
                    raise e
                
                except TypeError as e:
                    error_message = f"TypeError reading data from DataFrame column {wkt_column}: {e}"
                    logger.error(error_message)
                    
                    raise e
                
                except Exception as e:
                    error_message = f"Error building GeoDataFrame from WKT table {wkt_column}: {e}"
                    logger.error(error_message)
                    
                    raise VectorHandlerError(f"Could not build GeoDataFrame from WKT table {e}")
            else:
                error_message = f"WKT column {wkt_column} not found in DataFrame columns {df.columns}"
                logger.error(error_message)
                raise ValueError(error_message)
        else:
            raise ValueError("Invalid DataFrame")

    @check_storage
    def csv_to_gdf(self, vector_file_name: str, lat_name: str=None, lon_name: str=None, wkt_column: str=None):
        
        if isinstance(lat_name,str) and isinstance(lon_name,str):
            logger.info(f"lat/lon columns provided: {lat_name}, {lon_name}")
        elif isinstance(wkt_column,str):
            logger.info(f"WKT column provided: {wkt_column}")
        else:
            error_message = f"lat/lon or WKT column names not provided"
            logger.error(error_message)
            
            raise ValueError(error_message)
        
        # Read to bytes
        try:
            logger.debug(f"Reading csv file {vector_file_name} from blob storage")
            bytes_data = self.storage.blob_to_bytesio(vector_file_name)
            logger.info(f"Bytes data created from csv file {vector_file_name}")
            df = pd_read_csv(bytes_data)
            logger.info(f"DataFrame created from csv file {vector_file_name} with {len(df)} rows")
            
        except Exception as e:
            error_message = f"VectorHandler.csv_to_gdf error reading data for {vector_file_name} from container: {e}"
            logger.error(error_message)
            raise Exception(error_message)

        if wkt_column:
            try:
                logger.debug(f"Building GeoDataFrame from WKT table {wkt_column}")
                gdf = self.wkt_df_to_gdf(df, wkt_column)
                logger.info(f"GeoDataFrame created from WKT table {wkt_column} with {len(gdf)} rows")
                
                return gdf
            
            except Exception as e:
                error_message = f"VectorHandler.csv_to_gdf error building GeoDataFrame from WKT table {wkt_column}: {e}"
                logger.error(error_message)
                raise Exception(error_message)

        elif lat_name and lon_name:
            try:
                logger.debug(f"Building GeoDataFrame from lat/lon table {lat_name}, {lon_name}")
                gdf = self.xy_df_to_gdf(df, lat_name, lon_name)
                logger.info(f"GeoDataFrame created from lat/lon table {lat_name}, {lon_name} with {len(gdf)} rows")
                
                return gdf
            
            except Exception as e:
                error_message = f"VectorHandler.csv_to_gdf error building GeoDataFrame from lat/lon table {lat_name}, {lon_name}: {e}"
                logger.error(error_message)
                raise Exception(error_message)
        else:
            error_message = f"Unknown csv import error for {vector_file_name}"
            logger.error(error_message)
            raise ValueError(error_message)

    
    # KML and KMZ
    
    @check_storage
    def kmz_to_gdf(self, kmz_name: str, kml_name: str=None):
        logger.debug(f"kmz_to_gdf called with kmz_name: {kmz_name} and kml_name: {kml_name}")   
        
        try:
            logger.debug(f"Listing kmz contents of {kmz_name}")
            contents = self.list_zip_contents(kmz_name)
            logger.info(f"Contents of kmz file {kmz_name}: {contents}")

        except Exception as e:
            error_message = f"VectorHandler.kmz_to_gdf error reading data for {kmz_name} from container: {e}"
            logger.error(error_message)
            
            raise Exception(error_message)
        
        if kml_name and isinstance(kml_name, str):
            
            if kml_name.endswith(".kml"):
                logger.info(f"kml_name {kml_name} provided")
                
            elif kml_name == "kml":
                logger.warning("kml_name provided as 'kml' - using first available kml in kmz")
                
            elif not "." in kml_name:
                logger.info(f"kml_name {kml_name} provided without extension")
                kml_name = f"{kml_name}.kml"

            else:
                error_message = f"Invalid KML name: {kml_name}"
                logger.error(error_message)
                
                raise ValueError(error_message)
            
            kml_found = False
            for _name in contents:
                if _name.endswith(".kml"):
                    logger.info(f"KML found in KMZ contents")
                    kml_name = _name
                    kml_found = True
                    break
                
            if kml_found:
                logger.info(f"KML name found in KMZ contents: {kml_name}")
                
            else:
                error_message = f"KML {kml_name} not found in kmz file {kmz_name}"
                logger.error(error_message)
                
                raise ValueError(error_message)
            
        else:
            logger.warning(f"KML name not provided - searching for KMLs in KMZ")
            for _name in contents:
                if _name.endswith(".kml"):
                    logger.info(f"KML found in KMZ contents")
                    kml_name = _name
                    break
            
            if kml_name:
                logger.info(f"KML name found in KMZ contents: {kml_name}")
                
            else:
                error_message = f"No KML files found in KMZ {kmz_name} contents {contents}"
                logger.error(error_message)
                
                raise ValueError(error_message)
            
        try:
            logger.debug(f"Reading zipped file {kmz_name} into GeoDataFrame")
            gdf = self.zip_content_to_gdf(zip_file=kmz_name, vector_file_name=kml_name)
            logger.info(f"GeoDataFrame created from kmz file <{kmz_name}> with {len(gdf)} rows")
            
            return gdf
        
        except Exception as e:
            error_message = f"VectorHandler.kmz_to_gdf error reading data for {kmz_name} from container: {e}"
            logger.error(error_message)
            
            raise Exception(error_message)
            
    @check_storage
    def kml_to_gdf(self, vector_file_name: str):
        
        if not vector_file_name.endswith(".kml"):
            error_message = f"Invalid KML file name: {vector_file_name}"
            logger.error(error_message)
            raise ValueError(error_message)
        try:
            logger.debug(f"Reading kml file {vector_file_name} from blob storage")
            bytes_data = self.storage.blob_to_bytesio(vector_file_name)
            logger.debug(f"Loading data into GeoDataFrame")
            gdf = gpd_read_file(bytes_data)
            logger.info(f"GeoDataFrame created from kml file {vector_file_name} with {len(gdf)} rows")
            
            return gdf
        
        except Exception as e:
            error_message = f"VectorHandler.kml_to_gdf error reading data for {vector_file_name} from container: {e}"
            logger.error(error_message)
            raise Exception(error_message)
 
   
    # SHP
    
    @check_storage
    def shp_zip_to_gdf(self, zip_name: str, shp_name: str=None):

        if shp_name and isinstance(shp_name, str):
            if shp_name.endswith(".shp"):
                logger.info(f"shp name provided: {shp_name}")
            elif shp_name == "shp":
                logger.warning("shp name provided as 'shp' - using first available shp in zip")
            elif not "." in shp_name:
                logger.warning(f"shapefile name provided without extension")
                shp_name = f"{shp_name}.shp"
        else:
            logger.warning(f"shapefile name not provided")
            shp_name = 'shp'
            logger.warning("Using first available shp in zip")
        try:
            shp_in_zip = self.zip_contains(zip_file=zip_name, search_term=shp_name)
        except Exception as e:
            error_message = f"VectorHandler.zip_shp_to_gdf error reading data for {zip_name} from container: {e}"
            logger.error(error_message)
            raise Exception(error_message)
        
        if shp_in_zip:    
            logger.info(f"shp found in zip contents")
            try:
                logger.debug(f"Reading zipped file {zip_name} into GeoDataFrame")
                gdf = self.zip_content_to_gdf(zip_file=zip_name,vector_file_name= shp_name)
                logger.info(f"GeoDataFrame created from shp file {zip_name} with {len(gdf)} rows")
                
                return gdf
            
            except Exception as e:
                error_message = f"VectorHandler.shp_to_gdf error reading data for {zip_name} from container: {e}"
                logger.error(error_message)
                
                raise Exception(error_message)

        else:
            error_message = f"{shp_name} not found in zipfile {zip_name}"
            logger.error(error_message)
            
            raise ValueError(error_message)


    # GeoJSON
    
    @check_storage
    def geojson_to_gdf(self, vector_file_name: str):
        
        if isinstance(vector_file_name,str):
            if "." in vector_file_name and vector_file_name.split(".")[-1] in ["geojson", "json"]:
                logger.info(f"Valid geojson file name: {vector_file_name}")
                
            else:
                error_message = f"Invalid geojson file name: {vector_file_name}"
                logger.error(error_message)
                
                raise ValueError(error_message)
            
        else:
            error_message = f"Invalid geojson file name: {vector_file_name}"
            logger.error(error_message)
            
            raise ValueError(error_message)
        
        if self.storage.blob_exists(
            blob_name=vector_file_name, 
            container_name=self.storage.workspace_container_name):

            logger.info(f"File {vector_file_name} found in container")
        else:
            error_message = f"File {vector_file_name} not found in container {self.storage.workspace_container_name}"
            logger.error(error_message)
            raise FileNotFoundError(error_message)
        
        try:
            json_uri = self.storage._get_blob_sas_uri(
                blob_name=vector_file_name,
                container_name=self.storage.workspace_container_name)
            logger.debug(f"Reading geojson file {vector_file_name} from blob storage")
            gdf = gpd_read_file(json_uri)
            logger.info(f"GeoDataFrame created from geojson file {vector_file_name} with {len(gdf)} rows")
            
            return gdf
        
        except Exception as e:
            error_message = f"VectorHandler.geojson_to_gdf error reading data for {vector_file_name} from container: {e}"
            logger.error(error_message)
            raise Exception(error_message)


    # Zipfiles
   
    @check_storage
    def zip_contains(self,zip_file,search_term):
        try:
            contents = self.list_zip_contents(zip_file)
            if any(search_term in _name for _name in contents):
                logger.info(f"{search_term} found in zip file {zip_file} contents {contents}")
                return True
            else:
                logger.warning(f"{search_term} not found in zip file {zip_file} contents {contents}")
                return False
        except Exception as e:
            error_message = f"Error reading zip file {zip_file}: {e}"
            logger.error(error_message)
            
            raise e 

    
    @check_storage
    def list_zip_contents(self, zip_file: str):
        
        if not zip_file.split(".")[-1] in ZIP_FORMATS:
            error_message = f"Invalid zipped file name or type: {zip_file}"
            logger.error(error_message)
            
            raise ValueError(error_message)

        try:
            logger.debug(f"Reading zipped file {zip_file} from blob storage")
            bytes_data = self.storage.blob_to_bytesio(zip_file)
            logger.info(f"Bytes data created from zipped file {zip_file}")
            
            with zipfile.ZipFile(bytes_data) as z:

                try:
                    logger.debug("Listing zip contents")
                    file_names = z.namelist()
                    logger.debug(f"Extracted files: {file_names}")
                    
                    return file_names
                
                except Exception as e:
                    error_message = f"Error listing zip contents from {zip_file}: {e}"
                    logger.error(error_message)
                    
                    raise e 
                         
        except Exception as e:
            error_message = f"VectorHandler.list_zip_contents error reading data for {zip_file} from container <{self.storage.workspace_container_name}>: {e}"
            logger.error(error_message)
            
            raise Exception(error_message)

    def vector_list_from_zip(self,zip_file:str, ):
        logger.warning(f"File type not provided - inferring file type from zip contents")
        try:
            logger.debug(f"listing zip contents")
            zip_contents = self.list_zip_contents(zip_file)
            logger.info(f"Zip contents: {zip_contents}")
            
        except ValueError as e:
            raise e
        
        except Exception as e:
            error_message = f"VectorLoader.from_blob_file error reading data for {zip_file} from container: {e}"
            logger.error(error_message)
            
            raise Exception(error_message)
        
        for zipfilename in zip_contents:
            if zipfilename.split(".")[-1] in VECTOR_FILE_DICT.keys():
                logger.warning(f"Vector file {zipfilename} found in zip file {zip_file}")
                matched_file_name = zipfilename
                matched_file_ext = zipfilename.split(".")[-1]
                logger.info(f"File type {matched_file_ext} found in zip file {zip_file}")
                break

    @check_storage      
    def zip_content_to_gdf(self, zip_file:str, vector_file_name: str=None, infer=False):

        if zip_file.split(".")[-1] in ZIP_FORMATS:
            logger.debug(f"Valid zipped file name: {zip_file}")
            zip_type = zip_file.split(".")[-1]
        else:
            error_message = f"Invalid zipped file name or type: {zip_file}"
            logger.error(error_message)
            raise ValueError(error_message)

        if vector_file_name and isinstance(vector_file_name,str):
            if vector_file_name.split(".")[-1] in VECTOR_FILE_DICT.keys(): 
                logger.info(f"File name {vector_file_name} provided in {zip_type} file {zip_file} is a valid vector file type")
                logger.info(f"Provided filename {vector_file_name} in is a valid vector file type")

            else:
                error_message = f"Filename {vector_file_name} does not have a valid vector extension"
                logger.error(error_message)
                
                raise ValueError(error_message)
        
        else:
            error_message = f"File name not provided or invalid file name: {vector_file_name}"
            logger.error(error_message)
            
            raise ValueError(error_message)
        
        try:
            logger.debug(f"Checking for {vector_file_name} in {zip_type} file {zip_file}")
            contains_file =  self.zip_contains(zip_file=zip_file, search_term=vector_file_name)
        except Exception as e:
            error_message = f"VectorLoader.from_blob_file error reading data for {zip_file} from container: {e}"
            logger.error(error_message)
            
            raise Exception(error_message)
        
        if contains_file:
            logger.info(f"{vector_file_name} found in {zip_type} file {zip_file}")
        else:
            error_message = f"{vector_file_name} not found in {zip_type} file {zip_file}"
            logger.error(error_message)
            raise ValueError(error_message)

        try:
            logger.debug(f"Reading {zip_type} file {zip_file} from blob storage")
            bytes_data = self.storage.blob_to_bytesio(zip_file)
            logger.info(f"Bytes data created from {zip_type} file {zip_file}")
            
        except Exception as e:
            error_message = f"VectorHandler.get_zip_contents error reading data for {zip_file} from container: {e}"
            logger.error(error_message)
            
            raise Exception(error_message)

        with zipfile.ZipFile(bytes_data) as z:
            with tempfile.TemporaryDirectory() as tmpdirname:
                try:
                    z.extractall(tmpdirname)
                except Exception as e:
                    error_message = f"Error extracting {zip_type} contents from {zip_file}: {e}"
                    logger.error(error_message)
                    raise VectorHandlerError(error_message)

                try:
                    logger.debug(f"Listing {zip_type} contents")
                    file_names = z.namelist()
                    logger.debug(f"Extracted files: {file_names}")
                    
                    file_paths = [
                        os.path.join(tmpdirname, name)
                        for name in file_names
                        if name.endswith(vector_file_name)
                    ]

                    logger.debug(f"File paths: {file_paths}")
                    
                except Exception as e:
                    error_message = f"Could not list {zip_type} contents: {e}"
                    logger.error(error_message)
                    
                    raise VectorHandlerError(error_message)
                
                if len(file_paths) == 0:
                    error_message = f"No vector files matching {vector_file_name} found in {zip_type} file {zip_file}"
                    logger.error(error_message)
                    
                    raise ValueError(error_message)
                
                elif len(file_paths) ==1:
                    logger.info(f"Single file {vector_file_name} found in {zip_type} file {zip_file}")

                elif len(file_paths) > 1:
                    logger.warning(f"Multiple files found in {zip_type} file {zip_file}: {file_paths}")
                    logger.warning(f"Using first file: {file_paths[0]}")
                
                else:
                    error_message = f"Unknown error finding file {vector_file_name} in {zip_type} file {zip_file}"
                    logger.error(error_message)
                    
                    raise ValueError(error_message)
                
                file_path = file_paths[0]
                        
                try:
                    logger.debug(f"Reading {file_path} from {zip_type} file {zip_file} into GeoDataFrame")
                    gdf = gpd_read_file(file_path)
                    logger.info(f"GeoDataFrame created from {zip_type} file {zip_file} with {len(gdf)} rows")
                    
                    return gdf
                
                except Exception as e:
                    error_message = f"Could not create GeoDataFrame from {zip_type} file {zip_file}: {e}"
                    logger.error(error_message)
                    
                    raise VectorHandlerError(error_message)
         
    @classmethod
    def from_blob_file(
        cls,
        file_name: str,
        file_type: str = None,
        layer_name: str = None,
        lat_name: str = None,
        lon_name: str = None,
        wkt_column: str = None,

        credential=None,
        container_name=None,
        
        params_only = True):

        logger.debug(f"Initializing VectorHandler.from_blob_file")

        instance = cls(
            file_name=file_name,
            file_type=file_type,
            layer_name=layer_name,
            lat_name=lat_name,
            lon_name=lon_name,
            wkt_column=wkt_column,
            
            credential=credential,
            container_name=container_name, 
        )
        
        # Check if storage handler is initialized
        if not instance.storage:
            error_message = f"VectorLoader.from_blob_file could not initialize StorageHandler"
            logger.error(error_message)
            
            raise StorageHandlerError(error_message)
        
        # Check if file exists
        try:
            logger.debug(f"Checking if blob <{file_name}> exists in container <{instance.storage.workspace_container_name}>")
            file_exists = instance.storage.blob_exists(
                blob_name=file_name,container_name=instance.storage.workspace_container_name)
            
            if file_exists:
                logger.info(f"File {file_name} found in container")

            else:
                error_message = f"File {file_name} not found in container {container_name}"
                logger.error(error_message)
                
                raise FileNotFoundError(error_message)
            
        except FileNotFoundError as e:
            error_message = f"File {file_name} not found in container {container_name}: {e}"
            logger.error(error_message)
            
            raise e
        
        except Exception as e:
            error_message = f"Error checking file existence in container: {e}"
            logger.error(error_message)
            
            raise StorageHandlerError(error_message)
        
        # Determine file type
        try:
            logger.debug(f"Inferring file type from {file_name}")
            instance.file_extension = instance.get_file_extension(file_name)
            logger.info(f"File type inferred: {instance.file_extension}")
            #logger.info(f"File type matched to {instance.loader}")
        
        except ValueError as e:
            error_message = f"Invalid filename or file extension: {e}"
            logger.error(error_message)
            
            raise e
        
        except Exception as e:
            error_message = f"Error inferring file type: {e}"
            logger.error(error_message)
            
            raise VectorHandlerError(error_message)


        params = dict()
        # Zip
        if instance.file_extension in ["zip"]:
            matched_file_name = None
            matched_file_ext = None
            if isinstance(file_type,str):

                logger.debug(f"File type provided: {file_type}")
                try:
                    matched_file_ext = instance.match_vector_type(file_type)
                    instance.loader = matched_file_ext
                    logger.info(f"File type {file_type} matched to {matched_file_ext}")
                
                except ValueError as e:
                    error_message = f"{file_type} could not be matched with a valid vector type in {list(VECTOR_FILE_DICT.keys())}: {e}"
                    logger.error(error_message)
                    
                    raise e
                
                except Exception as e:
                    error_message = f"VectorLoader.from_blob_file error matching file type {file_type}: {e}"
                    logger.error(error_message)
                    
                    raise VectorHandlerError(error_message)
                
            elif params_only:
                error_message = f"Vector file type must be specified for zipfiles"
                logger.error(error_message)
               
                raise ValueError(error_message)
                
            else:    
                logger.warning(f"File type not provided - inferring file type from zip contents")
                # Some fishing expedition into zipfiles here

            if matched_file_ext == 'shp':
                params = {'zip_name': file_name, 'shp_name': 'shp'}
                instance.loader = 'shp'
                logger.info(f"File type {matched_file_ext} found in zip file {file_name}")

            elif matched_file_ext:
                error_message = f"File type {matched_file_ext} not yet implemented for zipped files"
                logger.error(error_message)
                
                raise NotImplementedError(error_message)
            
            else:
                error_message = f"Unknown error inferring file type from zip contents"
                logger.error(error_message)
                raise ValueError(error_message)
        
        # KMZ    
        elif instance.file_extension in ["kmz"]:
            params = {'kmz_name': file_name, 'kml_name': 'kml'}
            instance.loader = instance.file_extension
        # KML, GeoJSON, JSON
        elif instance.file_extension in ["kml","json","geojson"]:
            params = {'vector_file_name': file_name}
            instance.loader = instance.file_extension
        # CSV
        elif instance.file_extension in ["csv"]:
            params = {'vector_file_name': file_name, 'lat_name': lat_name, 'lon_name': lon_name, 'wkt_column': wkt_column}
            instance.loader = instance.file_extension
        # GPKG
        elif instance.file_extension in ["gpkg"]:
            params = {'vector_file_name': file_name, 'layer_name': layer_name}
            instance.loader = instance.file_extension
        
        try:
            logger.debug(f"Calling <{instance.loader}> loader with params: {params}")
            gdf = instance.loaders[instance.loader](**params)
            logger.info(f"GeoDataFrame created from {file_name} with {len(gdf)} rows")
            
            return gdf
        
        except ValueError as e:
            raise e
        
        except Exception as e:
            error_message = f"VectorLoader.from_blob_file error reading data for {file_name} from container: {e}"
            logger.critical(error_message)
            raise e
        

            