import azure.functions as func
#from datetime import datetime

from .base_request import BaseRequest
from api_clients import RasterHandler
from utils import *


class RasterRequest(BaseRequest):


    def __init__(self, req: func.HttpRequest,command:str=None):

        logger.info('Initializing RasterRequest')
        super().__init__(req)
        error_message = None
                

        self.raster_name = self.req_json.get('rasterName',None)

        self.epsg_code = self.req_json.get('EPSGCode', DEFAULT_EPSG_CODE)
        self.input_epsg_code = self.req_json.get('inputEPSGCode', None)
        self.cloud_optimize = self.req_json.get('COG', True)
        self.container_name = self.req_json.get('containerName', DEFAULT_WORKSPACE_CONTAINER)
        self.output_container_name = self.req_json.get('outputContainerName', DEFAULT_HOSTING_CONTAINER)
        self.raster_name_out = self.req_json.get('rasterNameOut', None)
        self.overwrite = self.req_json.get('overwrite', False)
        
        logger.debug(f'Raster name: {self.raster_name}')
        logger.debug(f'Raster name type: {type(self.raster_name)}')
        logger.debug(f'EPSG code: {self.epsg_code}')
        logger.debug(f'Cloud optimized: {self.cloud_optimize}')
        logger.debug(f'Container name: {self.container_name}')
        logger.debug(f'Output container name: {self.output_container_name}')
        logger.debug(f'Raster name out: {self.raster_name_out}')
        
        if isinstance(self.raster_name,str):
            logger.debug(f'String found: <{self.raster_name}> validating as single raster')
            try:
                if RasterHandler.valid_raster_name(self.raster_name):
                    logger.debug(f'Staging single raster: {self.raster_name}')
                        
                elif ';' in raster_names:
                    logger.debug(f'Semicolon separated list found: {self.raster_name}')
                    raster_names = raster_names.split(';')

                else:
                    error_message = (f'Stage Raster failed: invalid raster name: {self.raster_name}')
                    logger.critical(error_message)
                    command = 'fail'
                    
            except Exception as e:
                error_message = (f'Stage Raster failed: validation failure - {e}')
                logger.critical(error_message)
                command = 'fail'
                
        elif command == 'stage':
            error_message = 'Staging raster failed: no raster name provided'
            logger.critical(error_message)
            command = 'fail'

        try:
            if RasterHandler.is_valid_epsg_code(self.epsg_code):
                logger.info(f'Output CRS: {RasterHandler.CRS_from_epsg(self.epsg_code)}')
            else:
                logger.warning(f'Invalid output CRS provided - defaulting to WGS84')
                self.epsg_code = DEFAULT_EPSG_CODE 
        except Exception as e:
            logger.error(f'Error validating EPSG code: {e}')
            self.epsg_code = DEFAULT_EPSG_CODE
            
        if command == 'stage':
            self.response = self._stage()
        
        elif command == 'fail':
            error_message = error_message if error_message else 'Uknown RasterHandler Error'
            self.response = self.return_error(error_message)

        else:
            self.response = self.return_error(f'Invalid RasterRequest command: {command}')

    def _stage(self):
        logger.debug('Handling stage raster request')

        try:#initialize RasterHandler
            R = RasterHandler(
                workspace_container_name=self.container_name,
                output_container_name=self.output_container_name,
                epsg_code=self.epsg_code,
                epsg_code_in=self.input_epsg_code,
                cloud_optimize=self.cloud_optimize)
            
            logger.debug('RasterHandler instance initialized by request')
        except Exception as e:
            error_message = f'Stage Raster failed: could not initialize RasterHandler: {e}'
            logger.critical(error_message)
            return self.return_error(error_message)
        
        if self.raster_name and isinstance(self.raster_name,str):
            logger.debug(f'Staging single raster: {self.raster_name} from container {self.container_name}')
            try:
                result = R.stage_raster_file(
                    raster_name_in=self.raster_name,
                    raster_name_out=self.raster_name_out,
                    workspace_container_name=self.container_name,
                    output_container_name=self.output_container_name,
                    epsg_code=self.epsg_code,
                    epsg_code_in=self.input_epsg_code,
                    cloud_optimize=self.cloud_optimize,
                    overwrite=self.overwrite)
                
                message = f'Raster {result["raster_name_in"]} staged as {result["raster_name_out"]} in container {result["output_container_name"]}'
                logger.info(message)
                
                return self.return_success(
                    message=message,
                    json_out=result)
            
            except Exception as e:
                error_message = f'Stage Raster failed'
                logger.critical(f"{error_message}: {e}")
                return self.return_exception(e,message=error_message)
        else:
            error_message = f'Stage Raster failed: invalid raster name: {self.raster_name}'
            logger.critical(error_message)
            return self.return_exception(ValueError,message=error_message)
    
    def validate_raster_name(self,raster_name:str):
        
        if raster_name and isinstance(raster_name,str):
            logger.debug(f'Staging single raster: {raster_name} from container {self.container_name}')
            try:
                result = RasterHandler.valid_raster_name(raster_name)
                return result
            
            except Exception as e:
                error_message = f'Stage Raster failed: validation failure - {e}'
                logger.critical(error_message)
                return self.return_exception(e,message=error_message)
                
