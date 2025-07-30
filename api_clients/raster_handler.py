from os import cpu_count
import uuid

from rasterio import band as rasterio_band
from rasterio import errors as rasterio_errors
from rasterio import open as rasterio_open
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rio_cogeo.cogeo import cog_validate, cog_translate
from rio_cogeo.profiles import cog_profiles


from .storage_handler import StorageHandler
from utils import *


class RasterHandler(StorageHandler):
    # init requires valid raster name
    def __init__(
        self,
        workspace_container_name: str = None,
        output_container_name: str = None,
        raster_name: str = None,
        epsg_code: int = None,
        epsg_code_in: int = None,
        cloud_optimize: bool = True,
        credential=None,
        storage_account_name: str = None,
        operation_id: str = None,
        **kwargs,
    ):
        logger.debug(f"RasterHandler init called")
        logger.debug(f'Raster name: {raster_name}')
        logger.debug(f'EPSG code: {epsg_code}')
        logger.debug(f'Cloud optimized: {cloud_optimize}')
        logger.debug(f'Workspace container name: {workspace_container_name}')
        logger.debug(f'Output container name: {output_container_name}')

        super().__init__(
            workspace_container_name=workspace_container_name,
            credential=credential,
            account_name=storage_account_name,
        )
        self.data_map = None
        self.proc_params = {
                    "valid_raster": False,
                    "force_epsg_code": epsg_code_in,
                    'epsg_code': epsg_code,
                    "epsg_code_in": None,
                    "error_messages": list(),
                }
        
        self.output_container_name = DEFAULT_HOSTING_CONTAINER
        
        if operation_id:
            self.operation_id = operation_id
            logger.debug(f"Operation ID set to {self.operation_id}")
        else:
            self.operation_id = str(uuid.uuid4())[:8]
            logger.debug(f"No operation ID provided, generated new one: {self.operation_id}")
            
        logger.debug(f"Operation ID: {self.operation_id}")

        #EPSG code validation
        if epsg_code:
            logger.debug(f"Provided EPSG code: {epsg_code}")
            if self.is_valid_epsg_code(epsg_code=epsg_code):
                if epsg_code != DEFAULT_EPSG_CODE:
                    logger.warning(f"Non-default EPSG code provided: {epsg_code}")
                else:
                    logger.info(f"Default EPSG code provided: {epsg_code}")
                    
                self.epsg_code = epsg_code
                self.proc_params['epsg_code'] = epsg_code
            else:
                logger.error(f"Invalid EPSG code provided: {epsg_code}")
                logger.warning(f"Using default EPSG code: {DEFAULT_EPSG_CODE}")
                self.epsg_code = DEFAULT_EPSG_CODE
                self.proc_params['epsg_code'] = DEFAULT_EPSG_CODE
        else:
            logger.info(f"No EPSG code provided, using default: {DEFAULT_EPSG_CODE}")
            self.epsg_code = DEFAULT_EPSG_CODE
            self.proc_params['epsg_code'] = DEFAULT_EPSG_CODE
        
        self._set_containers(
            workspace_container_name=workspace_container_name,
            output_container_name=output_container_name
        )

        if raster_name:
            self._raster_init(raster_name=raster_name)
            logger.debug(f"RasterHandler initialized with raster: {raster_name}")
        else:
            logger.warning(f"No raster name provided, RasterHandler initialized without raster")
            self.proc_params['valid_raster'] = False
            self.data_map = None

        logger.info(f"RasterHandler initialized")

    def _set_containers(self, workspace_container_name: str = None, output_container_name: str = None):
        
        logger.debug(f"Setting workspace and output containers")
        
        if workspace_container_name and self.container_exists(
            container_name=workspace_container_name):
            
            if self.workspace_container_name != workspace_container_name:
                logger.warning(f"Workspace container name changed from {self.workspace_container_name} to {workspace_container_name}")
            else:
                logger.debug(f"Workspace container name remains the same: {self.workspace_container_name}")
                
            self.workspace_container_name = workspace_container_name

        else:
            logger.debug(f"No workspace container name provided, using default: {DEFAULT_WORKSPACE_CONTAINER}")
            self.workspace_container_name = DEFAULT_WORKSPACE_CONTAINER

        # Output container validation
        if output_container_name and self.container_exists(
            container_name=output_container_name):

            if self.output_container_name != output_container_name:
                logger.warning(f"Output container name changed from default {self.output_container_name} to {output_container_name}")
            else:
                logger.debug(f"Output container name remains the same: {self.output_container_name}")
            logger.info(f"Output container {output_container_name} exists")
            
            self.output_container_name = output_container_name
            
        else:
            logger.error(f"Output container {output_container_name} does not exist")
            logger.warning(f"Using workspace container {self.workspace_container_name} as output container")
            
            self.output_container_name = DEFAULT_HOSTING_CONTAINER


    def _raster_init(
        self, raster_name,
        raster_name_out,
        workspace_container_name=None,
        output_container_name=None,
        ):
        # Validate raster and parameters
        # self.proc_params['valid_raster'] = True is the success outcome
        logger.debug(f"RasterHandler _raster_init called")
        
        name_base = raster_name.split('.')[0][:10]
        
        # Intermediate data mapping
        if (
                workspace_container_name and
                workspace_container_name != self.workspace_container_name and
                self.container_exists(
                    container_name=workspace_container_name)
            ):
            logger.warning(f"Changing workspace container to {workspace_container_name}")
            self.workspace_container_name = workspace_container_name
        else:
            logger.debug(f"Using existing workspace container: {self.workspace_container_name}")
            workspace_container_name = self.workspace_container_name
        
        if (
                output_container_name and
                output_container_name != self.output_container_name and
                self.container_exists(
                    container_name=output_container_name)
            ):
            logger.warning(f"Changing output container to {output_container_name}")
            self.output_container_name = output_container_name
        else:
            logger.debug(f"Using existing output container: {self.output_container_name}")
            output_container_name = self.output_container_name
        
        self.data_map = {
            "raster_name": {
                'name': raster_name,
                'container': workspace_container_name
            },
            "projected_raster": {
                'name': f"{name_base}_{self.operation_id}_reproj.tif",
                'container': workspace_container_name
            },
            "COG_scratch": {
                'name': f"{name_base}_{self.operation_id}_cog_temp.tif",
                'container': workspace_container_name
            },
            "COG_output": {
                'name': raster_name_out,
                'container': output_container_name
            },
        }
        logger.debug(f"Intermediate data mapping initialized: {self.data_map}")
        
        if self.blob_exists(
            blob_name=self.data_map['raster_name']['name'], 
            container_name=self.data_map['raster_name']['container']):
                
            logger.debug(f"Raster {raster_name} found in workspace container {self.data_map['raster_name']['container']}")
        
        else:
            error_message = f"Raster {self.data_map['raster_name']['name']} not found in workspace container {self.data_map['raster_name']['container']}"
            logger.error(error_message)
            
            self.data_map['raster_name']['name'] = None
            self.proc_params['error_messages'].append(ResourceNotFoundError(error_message))
            self.proc_params['valid_raster'] = False
            
            return
        
        # Determine input CRS

        inferred_crs_in = None
        try:
            logger.debug(f"Inferring CRS for {raster_name}")
            inferred_crs_in = self.get_epsg_code(
                raster_name=self.data_map['raster_name']['name'], 
                container_name=self.data_map['raster_name']['container']
            )
            logger.debug(f"Inferred CRS for {raster_name}: EPSG:{inferred_crs_in}")
                
        except AttributeError as e:
            logger.error(f"Invalid or missing CRS for {raster_name}: {e}")
            inferred_crs_in = None
        
        except Exception as e:
            logger.error(f"Error inferring CRS for {raster_name}: {e}")
            inferred_crs_in = None

        if inferred_crs_in:
            if self.proc_params['force_epsg_code']:
                # Log if provided CRS parameter does not match inferred CRS and ignore it
                if inferred_crs_in != self.proc_params['force_epsg_code']:
                    logger.error(f"Input EPSG code {self.proc_params['force_epsg_code']} does not match inferred CRS {inferred_crs_in}")
                    logger.warning(f"Using inferred CRS {inferred_crs_in} as input EPSG code and ignorint parameter")
                else:
                    logger.info(f"Input CRS for {raster_name} matches provided EPSG code {self.proc_params['force_epsg_code']}")
            else:
                
                logger.info(f"Input CRS for {raster_name} validated as EPSG:{inferred_crs_in}")
            
            self.proc_params['epsg_code_in'] = inferred_crs_in
            self.proc_params['force_epsg_code'] = None
            self.proc_params['valid_raster'] = True
            
        elif self.proc_params['force_epsg_code']:
            # Log if provided CRS parameter is valid
            if self.is_valid_epsg_code(epsg_code=self.proc_params['force_epsg_code']):
                logger.warning(f"Using parameter input EPSG code {self.proc_params['force_epsg_code']} as CRS for {raster_name}")
                
                self.proc_params['epsg_code_in'] = self.proc_params['force_epsg_code']
                self.proc_params['valid_raster'] = True
                
            else:
                error_message = f"Could not infer CRS and invalid input EPSG code provided: {self.proc_params['force_epsg_code']}"
                logger.critical(error_message)
                self.proc_params['error_messages'].append(ValueError(error_message))
                self.proc_params['valid_raster'] = False
                
                return
            
        else:
            error_message = f"Could not infer CRS for {raster_name} and no input EPSG code provided"
            logger.critical(error_message)
            self.proc_params['error_messages'].append(ValueError(error_message))
            self.proc_params['valid_raster'] = False
            
            return

    def get_epsg_code(self, raster_name, container_name: str = None) -> int:

        if self.blob_exists(blob_name=raster_name, container_name=container_name):
            logger.debug(f"Getting CRS for {raster_name}")
        else:
            logger.error(f"Error: Raster {raster_name} not found in container {container_name}")
            raise FileNotFoundError(
                f"Error: Raster {raster_name} not found in container {container_name}"
            )

        crs = None
        try:
            with rasterio_open(
                self._get_blob_sas_uri(
                    container_name=container_name, blob_name=raster_name
                )
            ) as src:
                try:
                    crs = src.crs
                    logger.debug(f"CRS found for {raster_name}: {crs}")
                except rasterio_errors.RasterioError as e:
                    logger.error(f"rasterio error reading CRS from {raster_name}: {e}") 
                    raise
                except Exception as e:
                    logger.error(f"Error reading CRS from {raster_name}: {e}")
                    raise
        
        except rasterio_errors.RasterioError as e:    
            logger.error(f"rasterio error reading {raster_name} into memory: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading reading {raster_name} into memory: {e}")
            raise

        if crs:
            if hasattr(crs,'is_valid') and getattr(crs,'is_valid'):  # valid CRS
                logger.info(
                    f"Info: Valid CRS found for {raster_name}: {type(crs)} - EPSG:{crs.to_epsg()}"
                )
                
                return crs.to_epsg()  # return code
            
            else:
                raise AttributeError(f"CRS of {raster_name} <{crs}> is invalid")
        else:
            raise AttributeError(f"CRS of {raster_name} is missing")

    def reproject_geotiff(
        self,
        raster_name_in: str,
        raster_name_out: str,
        container_name: str,
        output_container_name: str,
        epsg_code: int,
        epsg_code_in: int = None,
        overwrite: bool = True,
    ) -> str:

        logger.debug(f"reproject_geotiff called")
        logger.debug(f"reproject_geotiff called with:")
        logger.debug(f"  raster_name_in: {raster_name_in}")
        logger.debug(f"  raster_name_out: {raster_name_out}")
        logger.debug(f"  container_name: {container_name}")
        logger.debug(f"  output_container_name: {output_container_name}")
        logger.debug(f"  epsg_code: {epsg_code}")
        logger.debug(f"  epsg_code_in: {epsg_code_in}")
        
        # Validate EPSG
        if self.is_valid_epsg_code(epsg_code):
            logger.debug(f"Valid EPSG code provided: {epsg_code}")
        else:
            error_message = f"Error: Invalid EPSG code provided: {epsg_code}"
            logger.error(error_message)
            
            raise ValueError(error_message)
        
        # Validate raster input
        if self.blob_exists(
            blob_name=raster_name_in,
            container_name=container_name):
            
            logger.debug(f"Raster {raster_name_in} found in container {container_name}")
        else:
            error_message = f"Error: Raster {raster_name_in} not found in container {container_name}"
            logger.error(error_message)
            
            raise ResourceNotFoundError(error_message)
        
        # Check if output raster already exists
        if self.blob_exists(blob_name=raster_name_out, container_name=output_container_name):
            logger.warning(f"Raster {raster_name_in} found in container {container_name}")
            if overwrite:
                logger.warning(f"Overwriting existing raster {raster_name_out} in {output_container_name}")
                try:
                    self.delete_blob(blob_name=raster_name_out, container_name=output_container_name)
                    logger.info(f"Existing raster {raster_name_out} deleted successfully")
                except Exception as e:
                    message = f"Error deleting existing raster {raster_name_out} in {output_container_name}: {e}"
                    logger.error(message)
                    
                    raise
            else:
                message = f"Error: Raster {raster_name_out} already exists in {output_container_name}"
                logger.error(message)
                
                raise ResourceExistsError(message)
        else:
            logger.debug(f"Output raster {raster_name_out} does not exist in {output_container_name}, proceeding with reprojection")

        # Create CRS from EPSG code
        try:
            logger.debug(f"Creating CRS from EPSG code {epsg_code}")
            CRS_out = CRS.from_epsg(epsg_code)
            logger.debug(f"CRS created: {CRS_out}")
        except Exception as e:
            message = f"Error creating CRS from EPSG code {epsg_code}: {e}"
            logger.error(message)
            
            raise ValueError(message)
        
        inferred_crs_in = None
        
        try:
            logger.debug(f"Getting CRS for {raster_name_in}")
            inferred_crs_in = self.get_epsg_code(
                    raster_name=raster_name_in,
                    container_name=container_name
                )
            logger.debug(f"CRS for {raster_name_in} inferred as EPSG:{inferred_crs_in}")
            logger.debug(f"Converting EPSG:{inferred_crs_in} to CRS")
            CRS_in = CRS.from_epsg(inferred_crs_in)
            logger.info(f"CRS for {raster_name_in} sucesfully read as {CRS_in}")
            
        except Exception as e:
            logger.error(f"Error getting CRS for {raster_name_in}: {e}")
            inferred_crs_in = None
            if epsg_code_in:
                if self.is_valid_epsg_code(epsg_code_in):
                    try:
                        CRS_in = CRS.from_epsg(epsg_code_in)
                        logger.warning(
                            f"Using input EPSG code {epsg_code_in} as CRS for {raster_name_in}"
                        )
                    except:
                        message = f"Could not determine CRS from input raster and provided input EPSG code is invalid: <{epsg_code_in}> {e}"
                        logger.error(message)
                        
                        raise
                    
                else:
                    message = f"Could not determine CRS from input raster and invalid input EPSG code provided: {epsg_code_in}"
                    logger.error(message)
                    
                    raise ValueError(message)
            else:
                message = f"Error: Could not determine CRS for {raster_name_in} and no input EPSG code provided: {e}"
                logger.error(message)
                
                raise ValueError(message)
            
        # Check if reprojection is needed
        if CRS_in == CRS_out:
            logger.info(f"{raster_name_in} is already in {CRS_out}")
            # No reprojection needed, return original raster
            return raster_name_in

        # If reprojection is needed, proceed
        logger.debug(f"{raster_name_in} is in {CRS_in} - reprojecting to {CRS_out}")

        with rasterio_open(
            self._get_blob_sas_uri(
                container_name=container_name,
                blob_name=raster_name_in)
        ) as src:
            logger.debug(f"Calculating transform for {raster_name_in}")

            try:
                transform, width, height = calculate_default_transform(
                        src.crs, CRS_out, src.width, src.height, *src.bounds
                    )
                logger.debug(f"Transform calculated: {transform}, {width}, {height}")
                kwargs = src.meta.copy()
                kwargs.update(
                    {
                        "crs": CRS_out,
                        "transform": transform,
                        "width": width,
                        "height": height,
                    }
                )

            except Exception as e:
                logger.error(f"Error calculating transform for {raster_name_in}: {e}")
                
                raise

            logger.debug(f"Reprojecting GeoTIFF {raster_name_in} to {CRS_out}")

            with MemoryFile() as memfile:
                with memfile.open(**kwargs) as dst:
                    # Reproject and write each band
                    for _band_id in range(1, src.count + 1):
                        try:
                            reproject(
                                source=rasterio_band(src, _band_id),
                                destination=rasterio_band(dst, _band_id),
                                src_transform=src.transform,
                                src_crs=src.crs,
                                dst_transform=transform,
                                dst_crs=CRS_out,
                                resampling=Resampling.bilinear,
                            )
                        except Exception as e:
                            logger.error(
                                f"Error reprojecting band {_band_id} of {raster_name_in}: {e}"
                            )
                            raise

                memfile.seek(0)

                logger.info(
                    f"GeoTIFF reprojected successfully in-memory: {memfile.name}"
                )
                try:
                    logger.debug(
                        f"Uploading reprojected raster {raster_name_out} to container {output_container_name}"
                    )
                    self.upload_blob_data(
                        blob_data=memfile.read(),
                        dest_blob_name=raster_name_out,
                        container_name=output_container_name,
                        overwrite=True,
                    )
                    logger.debug("Upload attempt completed, checking for success")
                    
                    # CRITICAL: Verify upload succeeded
                    if self.blob_exists(
                        blob_name=raster_name_out,
                        container_name=container_name):
                        
                        logger.info(
                        f"GeoTIFF {raster_name_in} reprojected to {CRS_out} and written to container {output_container_name} as {raster_name_out}"
                        )
                    
                        return raster_name_out
                    else:
                
                        logger.error(f"Failed to upload reprojected raster: {raster_name_out}")
                        
                        raise RuntimeError(f"Failed to upload reprojected raster: {raster_name_out}")

                except Exception as e:
                    logger.error(f"Upload failed for reprojected raster {raster_name_out}: {e}")
                    
                    raise

    def create_rasterio_cog(
        self,
        raster_name_in: str,
        raster_name_out: str,
        container_name: str,
        output_container_name: str,
        overwrite: bool = True,
    ) -> str:
        
        logger.debug(f"create_rasterio_cog called")

        if self.blob_exists(
            blob_name=raster_name_in,
            container_name=container_name):
            logger.debug(f"Source raster {raster_name_in} found in container {container_name}")
        else:
            error_msg = f"Source raster not found for COG creation: {raster_name_in} in {container_name}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
    
        logger.debug(f"Validated source exists: {raster_name_in}")

        if self.blob_exists(
            blob_name=raster_name_out,
            container_name=output_container_name):
            
            if overwrite:
                logger.warning(f"Overwriting existing raster {raster_name_out} in {output_container_name}")
                
                try:
                    self.delete_blob(
                        blob_name=raster_name_out,
                        container_name=output_container_name
                    )
                    logger.info(f"Existing raster {raster_name_out} deleted successfully")
                    
                except Exception as e:
                    message = f"Error deleting existing raster {raster_name_out} in {output_container_name}: {e}"
                    logger.error(message)
                    
                    raise
            else:
                message = f"Error: Raster {raster_name_out} already exists in {output_container_name}"
                logger.error(message)
                
                raise ResourceExistsError(message)
        
        logger.info(f"Validation complete - Creating COG for {raster_name_in} in container {container_name} to {raster_name_out} in container {output_container_name}")

        try:
            with rasterio_open(
                    self._get_blob_sas_uri(
                        container_name=container_name,
                        blob_name=raster_name_in)
                ) as src:

                with MemoryFile() as memfile:
                    logger.debug(f"Translating {raster_name_in} to COG format in-memory")
                    cog_details = cog_translate(
                        source=src,
                        dst_path=memfile.name,
                        dst_kwargs=cog_profiles.get("lzw"),
                        web_optimized=True,
                        in_memory=True,
                        #additional_cog_metadata=None,#Probably useful in the future for adding tags
                    )

                    memfile.seek(0)

                    logger.info(f"COG created successfully in-memory: {cog_details}")
                    logger.debug(
                        f"Uploading COG {raster_name_out} to container {output_container_name}"
                    )
                    try:
                        self.upload_blob_data(
                            blob_data=memfile.read(),
                            dest_blob_name=raster_name_out,
                            container_name=output_container_name,
                            overwrite=overwrite,
                        )
                    except Exception as e:
                        logger.error(
                            f"Error uploading COG {raster_name_out} to container {output_container_name}: {e}"
                        )
                        raise

            logger.info(
                f"COG {raster_name_out} written to container {output_container_name}"
            )

            return raster_name_out

        except Exception as e:
            logger.error(
                f"Error creating COG for {raster_name_out} {type(e).__name__}: {e}"
            )
            raise

    def stage_raster_file(
        self,
        raster_name_in: str,
        raster_name_out: str = None,
        workspace_container_name: str = None,
        output_container_name: str = None,
        epsg_code: int = None,
        epsg_code_in: int = None,
        cloud_optimize: bool = True,
        overwrite: bool = True,
    ):
        
        logger.debug(f"stage_raster_file called with parameters:")
        logger.debug(f"  raster_name_in: {raster_name_in}")
        logger.debug(f"  raster_name_out: {raster_name_out}")
        logger.debug(f"  workspace_container_name: {workspace_container_name}")
        logger.debug(f"  output_container_name: {output_container_name}")
        logger.debug(f"  epsg_code: {epsg_code}")
        logger.debug(f"  epsg_code_in: {epsg_code_in}")
        
        if self.data_map:
            logger.debug(f"Intermediate data mapping initialized: {self.data_map}")
        else:
            try:
                logger.debug(f"Intermediate data mapping not initialized, initializing now")
                self._raster_init(
                    raster_name=raster_name_in,
                    raster_name_out=raster_name_out,
                    workspace_container_name=workspace_container_name,
                    output_container_name=output_container_name,)
            except Exception as e:
                logger.error(f"Error initializing intermediate data mapping: {e}")
                
                raise
        
        if self.proc_params['error_messages']:
            logger.error(f"Errors found in processing parameters: {self.proc_params['error_messages']}")
            
            raise self.proc_params['error_messages'][-1]
        
        if not self.proc_params['valid_raster']:
            error_message = f"Unknown failure validating raster parameters: {self.proc_params}"
            logger.error(error_message)
            
            raise ValueError(error_message)
         
        # called after validation of parameters
        raster_name = raster_name_in
 
        try:
            logger.debug(f"Reprojecting {raster_name}")
            reprojected_name = self.reproject_geotiff(
                raster_name_in=self.data_map['raster_name']['name'],
                raster_name_out=self.data_map['projected_raster']['name'],
                container_name=self.data_map['raster_name']['container'],
                output_container_name=self.data_map['projected_raster']['container'],
                epsg_code=self.proc_params['epsg_code'],
                epsg_code_in=self.proc_params['epsg_code_in'],
                overwrite=overwrite
            )
            if reprojected_name == self.data_map['raster_name']['name']:
                logger.info(f"Reprojection not needed, using original raster: {self.data_map['raster_name']['name']}")
                self.data_map['projected_raster']['name'] = self.data_map['raster_name']['name']
                self.data_map['projected_raster']['container'] = self.data_map['raster_name']['container']
                
            elif reprojected_name == self.data_map['projected_raster']['name']:
                if self.blob_exists(
                    blob_name=self.data_map['projected_raster']['name'], 
                    container_name=self.data_map['projected_raster']['container']
                ):
                    
                    logger.info(f"Reprojection successful, using new raster: {self.data_map['projected_raster']['name']}")
                    
                else:
                    error_message = f"Unknown reprojection failure, expected {self.data_map['projected_raster']['name']} not found in {self.data_map['projected_raster']['container']}"
                    logger.error(error_message)
                    
                    raise FileNotFoundError(error_message)

            else:
                error_message = f"Unexpected reprojection result: {reprojected_name} does not match expected names"
                logger.error(error_message)
                
                raise ValueError(error_message)

            
        except Exception as e:
            logger.error(f"Reprojection failed for {raster_name}: {e}")
            
            raise ValueError(f"Cannot proceed with COG creation: reprojection failed: {e}")

        if cloud_optimize:
            try:
                logger.debug(
                    f"Creating COG from validated source: {self.data_map['projected_raster']['name']} in container {self.data_map['projected_raster']['container']}")
                
                logger.debug(f"Output COG will be {self.data_map['COG_scratch']['name']} in container {self.data_map['COG_scratch']['container']}")
                
                cog_result = self.create_rasterio_cog(
                    raster_name_in=self.data_map['projected_raster']['name'],
                    raster_name_out=self.data_map['COG_scratch']['name'],
                    container_name=self.data_map['projected_raster']['container'],
                    output_container_name=self.data_map['COG_scratch']['container'],
                    overwrite=overwrite
                )

                logger.debug(f"COG process complete: {cog_result}")
            except Exception as e:
                logger.error(f"COG creation failed for {self.data_map['projected_raster']['name']}: {e}")
                
                raise
            
            if self.blob_exists(
                    blob_name=self.data_map['COG_scratch']['name'], 
                    container_name=self.data_map['COG_scratch']['container']
                ):
                    
                    logger.info(f"COG creation successful, using new raster: {self.data_map['COG_scratch']['name']}")
                    
            else:
                error_message = f"Unknown COG failure, expected {self.data_map['COG_scratch']['name']} not found in {self.data_map['COG_scratch']['container']}"
                logger.error(error_message)
                
                raise FileNotFoundError(error_message)
            
        else:
            logger.debug(f"Bypassing cloud optimization for {raster_name}")
            self.data_map['COG_scratch']['name'] = self.data_map['projected_raster']['name']
            self.data_map['COG_scratch']['container'] = self.data_map['projected_raster']['container']
            
        try:
            logger.debug(
                f"Copying COG from scratch to output: {self.data_map['COG_scratch']['name']} to {self.data_map['COG_output']['name']}")
            
            self.copy_blob(
                source_blob_name = self.data_map['COG_scratch']['name'],
                source_container_name = self.data_map['COG_scratch']['container'],
                dest_blob_name = self.data_map['COG_output']['name'],
                dest_container_name = self.data_map['COG_output']['container'],
                wait_on_status=True,
                overwrite=overwrite
            )
            
            logger.info(
                f"Successfully copied COG {self.data_map['COG_scratch']['name']} to {self.data_map['COG_output']['name']} in container {self.data_map['COG_output']['container']}")

        except Exception as e:
            logger.error(
                f"Failed to copy {raster_name_in} as {raster_name_out} in {output_container_name}"
            )
            raise

        # Final validation of output
        logger.debug(
            f"Validating output raster {self.data_map['COG_output']['name']} in container {output_container_name}"
        )
        if self.blob_exists(
            blob_name=self.data_map['COG_output']['name'], 
            container_name=self.data_map['COG_output']['container']
        ):
            logger.info(
                f"{raster_name_in} staged as {raster_name_out} in {self.data_map['COG_output']['container']}"
            )
            
            return {
                "raster_name_in": self.data_map['raster_name']['name'],
                "raster_name_out": self.data_map['COG_output']['name'],
                "output_container_name": self.data_map['COG_output']['container'],
            }
            
        else:
            error_message = f"Error: {self.data_map['COG_output']['name']} not found in {self.data_map['COG_output']['container']} - unknown error staging {self.data_map['raster_name']['name']}"
            logger.error(error_message)
            
            raise FileNotFoundError(error_message)
    
        
    @staticmethod
    def valid_raster_name(raster_name: str = None) -> bool:

        if raster_name and isinstance(raster_name, str):
            logger.debug(f"Validating raster name: {raster_name}")
            
        else:
            raise ValueError(f"Invalid raster name: {raster_name}")

        if raster_name.count(".") == 1:
            split_name = raster_name.split(".")
            if split_name[-1] in ["tif", "tiff", "geotiff", "geotif"]:
                logger.debug(f"{raster_name} has valid extension")
                name_root = split_name[0]
            else:
                
                raise ValueError(f"{raster_name} has invalid extension - must be .tif, .tiff, .geotiff, or .geotif")
            
        elif raster_name.count(".") == 0:
            
            raise ValueError(f"{raster_name} has no extension - must be .tif, .tiff, .geotiff, or .geotif")
        
        elif raster_name.count(".") > 1:
            
            raise ValueError(f"{raster_name} contains invalid character: '.' ")

        if len(raster_name) > MAX_RASTER_NAME_LENGTH:
            
            raise ValueError(f"{raster_name} exceeds maximum length of {MAX_RASTER_NAME_LENGTH} characters")
        
        valid_chars = set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
            )
        invalid_chars = list()
        
        for char in name_root:
            if char not in valid_chars:
                invalid_chars.append(char)
        
        if invalid_chars:
            
            raise ValueError(f"{raster_name} contains invalid characters: {invalid_chars} - alphanumeric only")
        
        logger.info(f"{raster_name} is a valid raster name")
        
        return True
        
    @staticmethod
    def CRS_from_epsg(epsg_code: int) -> CRS:
        try:
            return CRS.from_epsg(epsg_code)
        except Exception as e:
            logger.error(f"Error creating CRS from EPSG code {epsg_code}: {e}")
            return None

    @staticmethod
    def is_valid_epsg_code(epsg_code: int = None) -> bool:

        if epsg_code and isinstance(epsg_code, int):
            try:
                CRS.from_epsg(epsg_code)
                return True
            except Exception as e:
                # logger.error(f'Error: Invalid EPSG code: {epsg_code}')
                return False
        else:
            return False    
