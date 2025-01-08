import azure.functions as func
#from datetime import datetime

from .base_request import BaseRequest
from api_clients import RasterHandler
from utils import *


class RasterRequest(BaseRequest):


    def __init__(self, req: func.HttpRequest,command:str=None):

        logger.info('Initializing RasterRequest')
        super().__init__(req)

        self.default_target_container = DEFAULT_HOSTING_CONTAINER
        
        if command == 'stage':
            self.response = self._stage()

        else:
            self.response = self.return_error(f'Invalid RasterRequest command: {command}')

    def _stage(self):
        logger.debug('Handling stage raster request')
        
        try:
            raster_names = self._validate_raster_names(self.json.get('rasterNames',None))       
        except Exception as e:
            return self.return_error(f'Stage Raster failed: invalid raster names: {e}')
        #validation and logging
        logger.debug(f'Raster names: {raster_names}')
        logger.debug(f'Raster names type: {type(raster_names)}')
        epsg_code = self._validate_epsg_code(self.json.get('EPSGCode',None))
        logger.debug(f'EPSG code: {epsg_code}')
        cloud_optimize = self.json.get('COG',True)
        logger.debug(f'Cloud optimized: {cloud_optimize}')
        container_name = self.json.get('containerName',None)
        logger.debug(f'Container name: {container_name}')
        output_container_name = self.json.get('outputContainerName',None)
        logger.debug(f'Output container name: {output_container_name}')
        raster_name_out = self.json.get('rasterNameOut',None)
        logger.debug(f'Raster name out: {raster_name_out}')

        try:#initialize RasterHandler
            R = RasterHandler(
                workspace_container_name=container_name,
                output_container_name=output_container_name,
                epsg_code=epsg_code,
                cloud_optimize=cloud_optimize)
            
            logger.debug('RasterHandler instance initialized by request')
        except Exception as e:
            
            return self.return_error(f'Stage Raster failed: could not initialize RasterHandler: {e}')
        
        if isinstance(raster_names,str):#single raster
            logger.debug(f'Staging single raster: {raster_names} from container {container_name}')
            try:
                result = R.stage_raster(
                    raster_name_in=raster_names,
                    raster_name_out=raster_name_out,
                    workspace_container_name=container_name,
                    output_container_name=output_container_name,
                    epsg_code=epsg_code,
                    cloud_optimize=cloud_optimize)

                return self.return_success(
                    message=f'Raster {result["raster_name_in"]} staged as {result["raster_name_out"]} in container {result["output_container_name"]}',
                    json_out=result)
            
            except Exception as e:
                return self.return_error(f'Stage Raster failed: {e}')
            
        elif isinstance(raster_names,list):#multiple rasters
            logger.debug(f'Staging multiple rasters: {raster_names} from container {container_name}')
            results = []
            for raster_name in raster_names:
                try:
                    results.append(
                        R.stage_raster(
                            raster_name_in=raster_name,
                            workspace_container_name=container_name,
                            output_container_name=output_container_name,
                            epsg_code=epsg_code,
                            cloud_optimize=cloud_optimize
                            )
                        )
                except Exception as e:
                    
                    return self.return_error(f'Stage Rasters failed for {raster_name}: {e}')
                
            return self.return_success(
                message=', '.join(
                    [f'Raster {result["raster_name_in"]} staged as {result["raster_name_out"]}' 
                    for result in results]
                ) 
                + f' in container {results[0]["output_container_name"]}',
                json_out=results)

       
    def _validate_raster_names(self,raster_names):
                
        if isinstance(raster_names,str):
            
            if RasterHandler.valid_raster_name(raster_names):
                logger.debug(f'Staging single raster: {raster_names}')
                return raster_names
                     
            elif ';' in raster_names:
                logger.debug(f'Semicolon separated list found: {raster_names}')
                raster_names = raster_names.split(';')
                
            else:
                raise ValueError(f'Stage Raster failed: invalid raster name: {raster_names}')
                        
        if isinstance(raster_names,list):
            if all(RasterHandler.valid_raster_name(raster_name) 
                for raster_name in raster_names
                ):
                logger.debug(f'Staging multiple rasters: {raster_names}')
                return raster_names
                        
            else:
                raise ValueError(f'Invalid raster names in list: {raster_names}')
        
        else:
            raise TypeError(f'Invalid raster name type: {type(raster_names)}')
    
    def _validate_epsg_code(self,epsg_code):
                
        if RasterHandler.is_valid_epsg_code(epsg_code):
            logger.info(f'Output CRS: {RasterHandler.CRS_from_epsg(epsg_code)}')
            return epsg_code
        else:
            logger.warning(f'Invalid output CRS provided - defaulting to WGS84')
            return 4326
