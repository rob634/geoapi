#Macbook edits Nov 2024
import azure.functions as func
from datetime import datetime

from .base_request import BaseRequest
from api_clients import VectorHandler
from utils import *

class VectorRequest(BaseRequest):
        
    proc_dict = {
        'kml':VectorHandler.from_kml,
        'geojson':VectorHandler.from_geojson,
        #'shp':VectorHandler.from_zip_shp,
        'csv':VectorHandler.from_csv
    }

    def __init__(self, req: func.HttpRequest,command:str=None):

        logger.info('Initializing VectorRequest')
        super().__init__(req)

        
        if command == 'stage':
                #logger.info(f'Staging vector: {self.file_name}')
                self.response = self._stage()
        else:
            self.response = self.return_error(f'Invalid VectorRequest command: {command}')

    def _stage(self):

        try:
            self._validate_parameters()

        except Exception as e:

            return self.return_error(f'Error validating parameters: {e}')
        
        try:
            logger.debug(f'Instantiating VectorHandler with method: {self._proc_method}')
            vector = self.proc_dict[self._proc_method](
                file_name=self.file_name,
                db_user=self.db_user,
                schema_name=self.schema_name,
                table_name=self.table_name,
                container_name=self.container_name,
                epsg_code=self.epsg_code,
                geometry_name=self.geometry_name,
                geometry_type=self.geometry_type
            )
            logger.info(f'VectorHandler instance created from {self._proc_method} method')

        except Exception as e:

            return self.return_error(f'Error instantiating VectorHandler: {e}')
        
        if vector.has_valid_gdf():
            try:

                vector.prepare_gdf(
                    geometry_name=self.geometry_name,
                    geometry_type=self.geometry_type,
                    epsg_code=self.epsg_code,
                    inplace=True)
                
            except Exception as e:
                return self.return_error(
                    f'Error preparing GeoDataFrame for database upload: {e}')
        
        logger.info(f'GeoDataFrame prepared for upload')

        try:

            vector.create_table(
                table_name=self.table_name,
                schema_name=self.schema_name,
                instance=True,
                if_exists=self.if_exists)
            
        except Exception as e:

            return self.return_error(f'Error creating table: {e}')

        logger.info(f'Table created: {self.schema_name}.{self.table_name} ')

        try:
            logger.debug(f'Inserting data into table: {self.schema_name}.{self.table_name}')
            output_name = vector.insert_gdf_data(
                table_name=self.table_name,
                schema_name=self.schema_name,
                geometry_name=self.geometry_name,
                batch_size=self.batch_size,
                instance=True)
            
        except Exception as e:

            return self.return_error(f'Error inserting data into table: {e}')
        
        return self.return_success(message=f'Data inserted into table: {self.schema_name}.{output_name}',
                                   json_out={'table_name':output_name,
                                             'schema_name':self.schema_name})


    def _validate_parameters(self):
        #required parameters
        self.file_name=self.json.get('fileName',None)
        self.file_type=self.json.get('fileType',None)
        self.table_name = self.json.get('tableName',None)
        #configuration parameters
        self.schema_name = self.json.get('schemaName',DEFAULT_DB_USER)
        self.db_user = self.json.get('dbUser', DEFAULT_DB_USER)
        self.container_name=self.json.get('containerName',DEFAULT_WORKSPACE_CONTAINER)
        self.lat_attr_name = self.json.get('latName', None)
        self.lon_attr_name = self.json.get('lonName', None)
        self.epsg_code = self.json.get('epsgCode',DEFAULT_EPSG_CODE)
        self.geometry_name = self.json.get('geometryName','shape')#DEFAULT_GEOMETRY_NAME
        self.geometry_type = self.json.get('geometryType',None)#should not be provided, testing only
        self.batch_size = self.json.get('batchSize', 5000)#DEFAULT_BATCH_SIZE
        self.append = self.json.get('append',False)
        self.overwrite = self.json.get('overwrite',False)
        self.attribute_index = self.json.get('attributeIndex',None)
        self.time_index = self.json.get('timeIndex',None)

        if self.append:
            self.if_exists = 'append'
        elif self.overwrite:
            self.if_exists = 'replace'
        else:
            self.if_exists = 'fail'
        
        if not self.file_name:
            raise ValueError('fileName is a required parameter')

        try:
            self._proc_method = self._method_from_file_type()#kml, geojson, shp, csv or raise error
        except Exception as e:
            logger.error(f'Error determining processing method: {e}')
            raise e

        if not self.table_name:
            try:
                inf_name = self.file_name.split('.')[0]
                logger.warning(f'tableName not provided for file <{self.file_type}> - inferring from file name table name: {inf_name}')
                self.table_name = inf_name
            except Exception as e:
                logger.error(f'Error inferring table name: {e}')
                raise e
            
        if self.overwrite and not self.append:
            self.if_exists = 'replace'
        elif not self.overwrite and self.append:
            self.if_exists = 'append'
        elif self.overwrite and self.append:
            raise ValueError('overwrite and append cannot both be True')


        #indices to add
        if isinstance(self.attribute_index,str):
            self.indices_to_add = [self.attribute_index]
        elif isinstance(self.attribute_index,list):
            self.indices_to_add = self.attribute_index
        else:
            self.indices_to_add = None

        if isinstance(self.time_index,str):
            self.time_indices_to_add = [self.time_index]
        elif isinstance(self.time_index,list):
            self.time_indices_to_add = self.attribute_index
        else:
            self.time_indices_to_add = None








    def _method_from_file_type(self):

        proc_method = None
        file_type = None
        logger.debug(f'Determining file type for file: {self.file_name}')
        ext = self.file_name.split('.')[-1].lower()

        if not self.file_type:
            logger.warning(f'fileType not provided, inferring from extension: {ext}')

            if ext == 'kml':

                proc_method = 'kml'###
            
            elif ext.endswith('json'):

                proc_method = 'geojson'###
            
            else:
                raise ValueError(f'fileType not provided - could not infer file type of file <{file_type}> from extension: {ext}')
            
        else:
            file_type = self.file_type.lower()
        
        if isinstance(file_type,str):
            logger.debug(f'fileType provided: {file_type}')

            if any([s in file_type for s in ['shp','shapefile']]):

                if ext == 'zip':

                    proc_method = 'shp'###

                else:

                    raise TypeError(f'Invalid file type for shapefile: {ext} - zip required for shapefile')
                
            elif any([s in file_type for s in ['geojson','json']]):

                proc_method = 'geojson'###
            
            elif any([s in file_type for s in ['kml']]):

                proc_method = 'kml'###
            
            elif any([s in file_type for s in ['csv']]):

                if isinstance(self.lat_attr_name,str) and isinstance(self.lon_attr_name,str):
                    
                    proc_method = 'csv'###
                else:
                    raise ValueError('latName and lonName parameters are missing - latitude and longitude attribute names required for csv and other tabular data')

        if isinstance(proc_method,str):

            logger.info(f'Processing method for file <{self.file_name}> with fileType <{file_type}> determined: {proc_method}')

            return proc_method
        
        else:
            raise Exception(f'Invalid or unrecognizable file type for file <{self.file_name}> with file type parameter <{file_type}>') 