#Macbook edits NOV 2024
from math import ceil
from psycopg2 import sql

#import fiona

#from osgeo import gdal
from geopandas import GeoDataFrame
from geopandas import read_file as gpd_read_file
from geopandas import read_postgis as gpd_read_postgis
from pandas import DataFrame
from pandas import read_csv as pd_read_csv
from shapely import Polygon,MultiPolygon,LineString,MultiLineString,Point,MultiPoint

from .database_client import DatabaseClient
from .storage_handler import StorageHandler
from utils import *


class VectorHandler(DatabaseClient):

#init with vector in blob storage and throws error if file is invalid
#the purpose of this class is to load data into a GDF
# prepare it for database upload, and upload it to the database
    GEOM_DICT = {
            'Polygon': Polygon,
            'LineString': LineString,
            'Point': Point,
            'MultiPolygon': MultiPolygon,
            'MultiLineString': MultiLineString,
            'MultiPoint': MultiPoint
        }
    
    def __init__(
            self,
            db_user:str=None,
            table_name:str=None,
            column_dict=None,
            geometry_type=None,
            geometry_name=None,
            epsg_code=None,
            schema_name=None):
        


        self.output_table_name = table_name if table_name else None
        
        db_user = db_user if db_user else DEFAULT_DB_USER
        
        super().__init__(
            user=db_user
        )          
        self.gdf = None
        self.output_table_name = table_name
        self.column_dict = column_dict
        self.geometry_type = geometry_type
        self.geometry_name = geometry_name if geometry_name else DEFAULT_GEOMETRY_NAME
        
        self.epsg_code = epsg_code if epsg_code else DEFAULT_EPSG_CODE
        self.schema_name = schema_name if schema_name else db_user

        

        logger.info(f'VectorHandler initialized')


    #Factory Methods

    @classmethod
    def from_geojson(cls,
        file_name:str,
        db_user:str=None,
        table_name:str=None,
        column_dict=None,
        geometry_type=None,
        geometry_name=None,
        epsg_code=None,
        schema_name=None,
        container_name:str=None):
        
        logger.debug(
            f'Initializing VectorHandler with file {file_name}')
        if not file_name.lower().endswith('json'):
            raise ValueError(f'Invalid file type: {file_name} - file type must be json or geojson')
        
        inst = cls(
            db_user=db_user,
            table_name=table_name,
            column_dict=column_dict,
            geometry_type=geometry_type,
            geometry_name=geometry_name,
            epsg_code=epsg_code,
            schema_name=schema_name
            )
        
        container_name = container_name if container_name else DEFAULT_WORKSPACE_CONTAINER
        storage = StorageHandler(workspace_container_name=container_name)

        if not storage.blob_exists(file_name):
            raise FileNotFoundError(f'File not found: {file_name}')
    
        try:
            inst.gdf = gpd_read_file(
                storage._get_blob_sas_uri(
                    container_name=container_name,
                    blob_name=file_name
                    )
                )
            inst.gdf.set_crs(epsg=epsg_code, inplace=True)
            logger.info(f'GeoDataFrame created from {file_name}')
            inst.column_dict = inst._sql_type_dict(gdf=inst.gdf)
            return inst

        except Exception as e:
            logger.error(f'Error creating GeoDataFrame from file URI for {file_name} in container {container_name}: {e}')
            raise e
        
    @classmethod
    def from_kml(cls,
        file_name:str, 
        db_user:str=None,
        table_name:str=None,
        column_dict=None,
        geometry_type=None,
        geometry_name=None,
        epsg_code=None,
        schema_name=None,
        container_name:str=None):

        logger.debug(
            f'Initializing VectorHandler with file {file_name}')
        if not file_name.lower().endswith('kml'):
            raise ValueError(f'Invalid file type: {file_name} - file type must be kml')

        inst = cls(
            db_user=db_user,
            table_name=table_name,
            column_dict=column_dict,
            geometry_type=geometry_type,
            geometry_name=geometry_name,
            epsg_code=epsg_code,
            schema_name=schema_name
            )
        container_name = container_name if container_name else DEFAULT_WORKSPACE_CONTAINER

        storage = StorageHandler(workspace_container_name=container_name)

        if not storage.blob_exists(file_name):
            raise FileNotFoundError(f'File not found: {file_name}')
        
        try:
            bytes_data = storage.blob_to_bytesio(
                file_name, container_name)
            
        except Exception as e:
            logger.error(f'Error reading kml file {file_name} from container {container_name}: {e}')
            raise e
        
        try:
            inst.gdf = gpd_read_file(bytes_data)
            logger.info(f'GeoDataFrame created from kml file {file_name}')
            inst.column_dict = inst._sql_type_dict(gdf=inst.gdf)
            return inst
        
        except Exception as e:
            logger.error(f'Error creating GeoDataFrame from kml file {file_name}: {e}')
            raise e

    @classmethod
    def from_csv(cls,
        file_name:str, 
        db_user:str=None,
        table_name:str=None,
        column_dict=None,
        geometry_type=None,
        geometry_name=None,
        epsg_code=None,
        schema_name=None,
        container_name:str=None,
        lat_name:str=None,
        lon_name:str=None):

        FILE_TYPE = 'csv'
        
        #logic for inferring lat and lon columns
        #lat_options = ['lat','latitude','y']
        #lon_options = ['lon','longitude','x']

        logger.debug(
            f'Initializing VectorHandler with file {file_name}')
        if not file_name.lower().endswith(FILE_TYPE):
            raise ValueError(f'Invalid file type: {file_name} - file type must be {FILE_TYPE}')

        inst = cls(
            db_user=db_user,
            table_name=table_name,
            column_dict=column_dict,
            geometry_type=geometry_type,
            geometry_name=geometry_name,
            epsg_code=epsg_code,
            schema_name=schema_name
            )
        container_name = container_name if container_name else DEFAULT_WORKSPACE_CONTAINER

        storage = StorageHandler(workspace_container_name=container_name)

        if not storage.blob_exists(file_name):
            raise FileNotFoundError(f'File not found: {file_name}') 
        try:

            obj = storage.blob_to_bytesio(file_name)
        except Exception as e:
            logger.error(f'Error reading csv file {file_name} from container {container_name}: {e}')
            raise e
        
        try:
            df = pd_read_csv(obj)
        except Exception as e:
            logger.error(f'Error reading csv file {file_name} from container {container_name}: {e}')
            raise e
        try:
            geometry = [Point(xy) for xy in zip(df[lon_name], df[lat_name])]
            inst.gdf = GeoDataFrame(df, geometry=geometry)
            inst.gdf.set_crs(epsg=epsg_code, inplace=True)
            logger.info(f'GeoDataFrame created from csv file {file_name}')
            inst.column_dict = inst._sql_type_dict(gdf=inst.gdf)
            return inst
        except Exception as e:
            logger.error(f'Error creating GeoDataFrame from csv file {file_name}: {e}')
            raise e

    #Public Methods

    def prepare_gdf(self,#table_name:str=None,
                    gdf:GeoDataFrame=None,
                    geometry_name:str=None,
                    geometry_type:str=None,
                    epsg_code:int=None,
                    inplace:bool=False)-> GeoDataFrame:

        if not isinstance(gdf, GeoDataFrame) or inplace:
            if isinstance(self.gdf, GeoDataFrame):
                gdf = self.gdf
            elif inplace:
                raise ValueError('Instance GeoDataFrame is empty')
            else:
                raise ValueError('No GeoDataFrame provided')
        
        epsg_code = epsg_code if epsg_code else DEFAULT_EPSG_CODE
        geometry_name = geometry_name if geometry_name else self.geometry_name

        #Data types
        try:
            gdf = self._validate_columns(gdf=gdf,geometry_name=geometry_name)
        except Exception as e:
            logger.error('Error validating column data types')
            raise e     
        #Geometry types
        try:
            geometry_type = self._validate_geometry_types(
                gdf=gdf
            )
            if inplace:
                self.geometry_type = geometry_type
        except Exception as e:
            logger.error('Error validating geometry types')
            raise e
        
        try:
            gdf = self._convert_geometry_types(
                gdf=gdf,
                to_geometry_type=geometry_type,
                geometry_name=geometry_name
            )
        except Exception as e:
            logger.error('Error converting geometry types')
            raise e

        #Z Values
        try:
            gdf = self._remove_zvals(gdf=gdf,
                                     geometry_name=geometry_name,
                                     geometry_type=geometry_type)
        except Exception as e:
            logger.error('Error removing z values from geometry column')
            raise e


        #Null Geometries
        try:
            gdf = self._remove_nulls(gdf=gdf, geometry_name=geometry_name)
        except Exception as e:
            logger.error(f'Error removing null geometries: {e}')
            raise e
        #M Values
            #logic
        #CRS
        try:
            gdf = self._update_crs(gdf=gdf, epsg_code=epsg_code)
        except Exception as e:
            logger.error(f'Error updating CRS to EPSG:{epsg_code} {e}')
            raise e

        
        logger.info(f'GeoDataFrame prepared for database upload')
        if inplace:
            self.gdf = gdf
        else:
            return gdf
 
    def gdf_to_sql_column_list(
            self, 
            gdf:GeoDataFrame=None, 
            geometry_type:str=None,
            epsg:int=None,
            inplace=False)-> list:
        
        logger.debug('Building column list')

        if not epsg or inplace:
            epsg = self.epsg_code

        if not geometry_type or inplace:
            geometry_type = self.geometry_type

        if not isinstance(gdf, GeoDataFrame) or inplace:
            if isinstance(self.gdf, GeoDataFrame):
                gdf = self.gdf
            elif inplace:
                raise ValueError('Instance GeoDataFrame is empty')
            else:
                raise ValueError('No GeoDataFrame provided')

        column_list = []
        column_list.append(f'objectid SERIAL')
        column_list.extend(
            f'{col} {self.to_sql_type(gdf[col].dtype)}'
            for col in gdf.columns if str(gdf[col].dtype) != 'geometry'
        )
        column_list.append(f'shape GEOMETRY({geometry_type}, {epsg})')

        logger.info(f'column_list built for gdf')
        return column_list
    
    def create_table(
        self, 
        table_name:str=None,
        schema_name:str=None,
        geometry_name:str=None,
        columns:str=None,
        uidx_name:str=None,
        gist_name:str=None, 
        gdf: GeoDataFrame=None,
        if_exists:str='fail',#fail,replace
        instance:bool=False):
        
        if not isinstance(table_name,str):
            logger.error('Table name must be provided')
            raise ValueError('Table name must be provided')
        
        if not isinstance(schema_name,str) or instance:
            schema_name = self.schema_name

        if self.table_exists(table_name=table_name,schema_name=schema_name):
            if if_exists == 'replace':
                logger.warning(f'Table {table_name} already exists - replacing')
                self.delete_table(table_name=table_name,schema_name=schema_name)
                logger.info(f'Table {table_name} deleted successfully')
            elif if_exists == 'fail':
                logger.error(f'Table {table_name} already exists')
                raise ValueError(f'Table {table_name} already exists')
            else:
                logger.error(f'Invalid if_exists parameter: {if_exists}')
                raise ValueError(f'Invalid if_exists parameter: {if_exists}')

        if not isinstance(gdf, GeoDataFrame) or instance:
            if isinstance(self.gdf, GeoDataFrame):
                gdf = self.gdf
            elif instance:
                raise ValueError('Instance GeoDataFrame is empty')
            else:
                raise ValueError('No GeoDataFrame provided')

        if not geometry_name or instance:
            geometry_name = self.geometry_name

        uidx_name = uidx_name if uidx_name else f'{table_name}_idx' 
        gist_name = gist_name if gist_name else f'{table_name}_gist'

        if not columns or instance:
            columns = self.gdf_to_sql_column_list(gdf)

        logger.debug('Creating table')

        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns})").format(
            table=sql.Identifier(table_name),
            schema=sql.Identifier(schema_name),
            columns=sql.SQL(', '.join(columns))
        )
                
        create_index_query = sql.SQL("""
            CREATE INDEX {gist_index} ON {table} USING gist ({geometry});
            CREATE UNIQUE INDEX {unique_index} ON {table} USING btree (objectid) WITH (fillfactor='75');
            """).format(
            gist_index=sql.Identifier(gist_name),
            table=sql.Identifier(table_name),
            geometry=sql.Identifier(geometry_name),
            unique_index=sql.Identifier(uidx_name)
        )
        
        logger.info(f"Creating table {table_name} with columns: {columns}")
        try:
            with self.connect() as conn:
                logger.debug('Creating table')
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    logger.info(f"Table {table_name} created successfully.")

                conn.commit()

                logger.debug('Creating indexes')
                with conn.cursor() as cursor:
                    cursor.execute(create_index_query)
                    logger.info(f"UID index {uidx_name} and spatial index{gist_name} created on {table_name} successfully")

                conn.commit()
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise e

        logger.info(f"Table {table_name} created successfully.")
        return table_name

    
    def row_insert_statement(
            self,
            table_name:str=None,
            schema_name:str=None,
            gdf:GeoDataFrame=None,
            idx:int=0,
            geometry_name:str=DEFAULT_GEOMETRY_NAME,
            inplace:bool=False) -> sql.SQL:
        #logger.debug('Building insert statement')

        if not isinstance(gdf, GeoDataFrame) or inplace:
            if isinstance(self.gdf, GeoDataFrame):
                gdf = self.gdf
            elif inplace:
                raise ValueError('Instance GeoDataFrame is empty')
            else:
                raise ValueError('No GeoDataFrame provided')

        if not schema_name or inplace:
            schema_name = self.schema_name

        row = gdf.iloc[idx]
        # Extract geometry in WKT format
        geom_wkt = gdf.loc[idx, geometry_name].wkt
        # Prepare the columns and values for the parameterized query
        columns = [col for col in row.index if col != geometry_name]
        values = [
            self.convert_to_sql_type(row[col]
                                     #sql_type=column_dict[col]
                                     ) for col in columns]
        #logger.debug(f'values: {values}')
        
        
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
        return query, values + [geom_wkt]

    def insert_gdf_data(self, 
                        table_name:str=None,
                        schema_name:str=None, 
                        gdf:GeoDataFrame=None, 
                        geometry_name:str=DEFAULT_GEOMETRY_NAME,
                        instance:bool=False,
                        batch_size:int=5000) -> str:
        
        if not isinstance(gdf, GeoDataFrame) or instance:
            if isinstance(self.gdf, GeoDataFrame):
                gdf = self.gdf
            elif instance:
                raise ValueError('Instance GeoDataFrame is empty')
            else:
                raise ValueError('No GeoDataFrame provided')

        row_count = 0
        #gdf.columns = gdf.columns.str.lower()
        logger.debug('Inserting data into table')
        with self.connect() as conn:
            with conn.cursor() as cursor:
                for idx, row in gdf.iterrows():
                    try:
                        q,v = self.row_insert_statement(
                            table_name=table_name,
                            schema_name=schema_name,
                            gdf=gdf,
                            idx=idx,
                            geometry_name=geometry_name)

                        cursor.execute(q,v)
                        row_count += 1

                    except Exception as e:
                        logger.error(f"Error inserting row {idx} into {table_name}:{e}")
                    if row_count % batch_size == 0:
                        #conn.commit()
                        logger.debug(f'{row_count} rows inserted')
                conn.commit()

        logger.info(f'{row_count} rows inserted into {table_name} successfully.')

        return table_name
    
    def instance_to_table(
            self,
            table_name:str,
            schema_name:str=None,
            epsg_code:int=None,
            geometry_name:str=None,
            geometry_type:str=None,
            if_exists:str='fail'
            ):
        
        if not isinstance(self.gdf, GeoDataFrame):
            raise ValueError('No GeoDataFrame instance provided')
        elif self.gdf.empty:
            raise ValueError('GeoDataFrame instance is empty')
        try:
            self.prepare_gdf(
                inplace=True,
                geometry_name=geometry_name,
                geometry_type=geometry_type,
                epsg_code=epsg_code)
        except Exception as e:
            logger.error(f'Error preparing GeoDataFrame: {e}')
            raise e
        try:
            self.create_table(
                table_name=table_name,
                schema_name=schema_name,
                instance=True,
                if_exists=if_exists)
        except Exception as e:
            logger.error(f'Error creating table:{e}')
            raise e

        try:
            self.insert_gdf_data(
                table_name=table_name,
                instance=True)
        except Exception as e:
            logger.error(f'Error inserting data into table: {e}')
            raise e
        
        logger.info(f'GeoDataFrame instance uploaded to table {table_name} successfully')

    def has_valid_gdf(self):
        if not isinstance(self.gdf, GeoDataFrame):
            logger.error('No GeoDataFrame instance provided')
            return False
        elif isinstance(self.gdf, GeoDataFrame) and self.gdf.empty:
            logger.error('GeoDataFrame instance is empty')
            return False
        else:
            return True





    @staticmethod
    def valid_vector_name(vector_name:str=None) -> bool:

        if not vector_name:
            logger.error('No vector name provided')
            return False

        if not any(
            vector_name.endswith(ext) for ext in [
                '.shp','.geojson', '.json', '.csv', '.kml']):
            
            message = f'Invalid file extension for {vector_name}'
            logger.warning(message)
            return False
        else:
            return True

    #private methods

    def _convert_geometry_types(self,gdf:GeoDataFrame,
                               to_geometry_type:str,
                               geometry_name:str=DEFAULT_GEOMETRY_NAME) -> GeoDataFrame:
        
        geoms = set(gdf.geometry.type)
        if not to_geometry_type in geoms:
            raise ValueError(f'GeoDataFrame does not contain geometry type {to_geometry_type}')
        
        if len(geoms) > 1:
            logger.debug(f'Converting geometry types to: {to_geometry_type}')
            try:
                gdf[geometry_name] = gdf[geometry_name].apply(
                    lambda shape: self.GEOM_DICT[to_geometry_type](
                        [shape]) if shape.type != to_geometry_type else shape)
                
            except Exception as e:
                logger.error(f'Error converting geometry types to {to_geometry_type}')
                raise e
        
            logger.info(f'Geometry type set to {to_geometry_type}')

        else:
            logger.info('Geometry types are already consistent')

        return gdf
        
    def _remove_nulls(self,gdf:GeoDataFrame, geometry_name:str='shape'):
        logger.debug('Checking for null geometries')
        if not isinstance(gdf, GeoDataFrame):
            raise ValueError('No or invalid GeoDataFrame provided')
        if any(gdf.geometry.isna()):
            logger.warning('Removing null geometries')
            try:
                gdf = gdf[gdf[geometry_name].notnull()].copy()
                logger.info('Null geometries removed')
            except Exception as e:
                logger.error('Error removing null geometries')
            raise e
        else:
            logger.info('No null geometries detected')

        return gdf

    def _remove_zvals(self,gdf:GeoDataFrame,
                     geometry_name:str=DEFAULT_GEOMETRY_NAME,
                     geometry_type:str=None,):

        if any(gdf.geometry.has_z):
            logger.warning(f'Geometry column contains z values - geometry type {geometry_type}')
            try:
                if geometry_type == 'Polygon':
                    gdf[geometry_name] = gdf[geometry_name].apply(lambda shape: Polygon([(x,y) for x,y,z in shape.exterior.coords]))
                elif geometry_type == 'LineString':
                    gdf[geometry_name] = gdf[geometry_name].apply(lambda shape: LineString([(x,y) for x,y,z in shape.coords]))
                elif geometry_type == 'Point':
                    gdf[geometry_name] = gdf[geometry_name].apply(lambda shape: Point(shape.x, shape.y))
                elif geometry_type == 'MultiPolygon':
                    gdf[geometry_name] = gdf[geometry_name].apply(lambda shape: MultiPolygon([
                        Polygon([(x,y) for x,y,z in poly.exterior.coords]) for poly in shape.geoms]))
                elif geometry_type == 'MultiLineString':
                    gdf[geometry_name] = gdf[geometry_name].apply(lambda shape: MultiLineString([LineString([(x,y) for x,y,z in line.coords]) for line in shape.geoms]))
                else:
                    logger.warning('Unsupported geometry type')
                    raise ValueError('Unsupported geometry type')
                logger.info('Z values removed from geometry column') 
            except Exception as e:
                logger.error('Error removing z values from geometry column')
                raise e
        else:
            logger.info('Geometry column does not contain z values')
        
        return gdf
    
    def _sql_type_dict(self,gdf:GeoDataFrame,inplace=True) -> dict:
        return {col: self.to_sql_type(str(gdf[col].dtype)) for col in gdf.columns}

    def _validate_columns(self,gdf:GeoDataFrame,geometry_name:str,suffix:str=None) -> GeoDataFrame:
        logger.debug('Validating columns')
        try:
            gdf.rename(
                columns=dict(
                    zip([c for c in gdf.columns],
                        [self.replace_reserved_word(
                        c,suffix=suffix) for c in gdf.columns])
                        ),
                inplace=True
                )
            gdf.columns = gdf.columns.str.lower()
            logger.info(f'Info: GeoDataFrame columns converted to lowercase')
        except Exception as e:
            logger.error('Error converting GeoDataFrame columns to lowercase')
            raise e
        
        if 'objectid' in gdf.columns:#maybe rename instead of drop
            gdf = gdf.drop(columns=['objectid'])
            logger.warning(f'ObjectID column removed')

        if gdf.geometry.name != geometry_name:
            try:
                gdf = gdf.rename(columns={
                            gdf.geometry.name: geometry_name
                        }
                    ).set_geometry(geometry_name)
                
                logger.info(f'Info: Geometry column renamed to {geometry_name}')
            except Exception as e:
                logger.error(f'Error renaming geometry column to {geometry_name}')
                raise e

        logger.debug('Validating column data types')
        for col in gdf.columns:
            if not self.is_valid_dtype(str(gdf[col].dtype)):
                logger.warning(f'Column {col} has an unsupported datatype {gdf[col].dtype} and is not being included')
                try:
                    gdf = gdf.drop(columns=[col])
                except Exception as e:
                    logger.error(f'Error removing column {col}')
                    raise e
                logger.info(f'Info: Column {col} removed')
        
        logger.info('Columns validated')

        return gdf

    def _validate_geometry_types(self,gdf:GeoDataFrame) -> str:
        #checks for mixed geometry types and returns the most complex type if valid combination

        geometry_types = list(set(gdf.geometry.type))

        if not all([t in self.GEOM_DICT.keys() for t in geometry_types]):
            raise ValueError(f'Unsupported geometry types detected: {geometry_types}')

        if len(geometry_types) == 1:

            logger.info(f'Single valid geometry type detected: {geometry_types[0]}')

            return geometry_types[0]

        elif len(geometry_types) == 2:
            logger.warning(f'Multiple geometry types detected: {geometry_types}')

            if 'Polygon' in geometry_types and 'MultiPolygon' in geometry_types:

                return 'MultiPolygon'

            elif 'LineString' in self.geometry_types and 'MultiLineString' in self.geometry_types:

                return 'MultiLineString'

            elif 'Point' in self.geometry_types and 'MultiPoint' in self.geometry_types:

                return 'MultiPoint'
            
        raise ValueError(f'Unsupported combination of geometry types detected: {geometry_types}')


    
    def _update_crs(self,gdf:GeoDataFrame, epsg_code:int=None):
        
        epsg_code = epsg_code if epsg_code else DEFAULT_EPSG_CODE

        if not gdf.crs.to_string() == f'EPSG:{epsg_code}':
            logger.warning(f'GeoDataFrame is in {gdf.crs.to_string()}, reprojecting to EPSG:{epsg_code}')
            try:
                gdf = gdf.to_crs(epsg=epsg_code)
                logger.info(f'GeoDataFrame reprojected to EPSG:{epsg_code}')

            except Exception as e: 
                logger.error(f'Error reprojecting GeoDataFrame to EPSG:{epsg_code}')
                raise e
        else:
            logger.info(f'GeoDataFrame is already in EPSG:{epsg_code}')
    
        return gdf
  