from os import cpu_count

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
        # StorageHandler init validates container name or uses default or raises error if default is not valid
        if output_container_name:
            self.output_container_name = output_container_name
        else:
            self.output_container_name = DEFAULT_HOSTING_CONTAINER
        
        if self.container_exists(container_name=self.output_container_name):
            logger.debug(f"Output container {self.output_container_name} exists")
        else:
            logger.error(f"Output container {self.output_container_name} does not exist")
            logger.warning(f"Using workspace container {self.workspace_container_name} as output container")
            self.output_container_name = self.workspace_container_name

        if raster_name:
            try:
                logger.debug(f"Setting raster source to {raster_name}")
                self.set_raster_source(raster_name=raster_name,container_name=self.workspace_container_name)
            except Exception as e:
                logger.error(f"Error setting raster source: {e}")
                self.raster_source = None
        else:
            logger.warning("No raster source provided")
            self.raster_source = None

        self.epsg_code = (
            epsg_code if self.is_valid_epsg_code(epsg_code=epsg_code) else DEFAULT_EPSG_CODE
        )
        self.CRS_out = CRS.from_epsg(epsg_code)
        
        if self.raster_source and epsg_code_in:
            try:
                inferred_crs_in = self.get_epsg_code(raster_name=self.raster_source, container_name=self.workspace_container_name)
                
            except Exception as e:
                logger.error(f"Error getting CRS for {self.raster_source}: {e}")
                logger.warning(f"Using input EPSG code {epsg_code_in} as CRS for {self.raster_source}")
                inferred_crs_in = None
                self.epsg_code_in = epsg_code_in
                
            if inferred_crs_in:
                if inferred_crs_in != epsg_code_in:
                    logger.error(f"Input EPSG code {epsg_code_in} does not match inferred CRS {inferred_crs_in}")
                    self.epsg_code_in = None
                else:
                    
                    logger.info(f"Input CRS for {self.raster_source} matches provided EPSG code {epsg_code_in}")
                    self.epsg_code_in = epsg_code_in
                    
        elif epsg_code_in:
            self.epsg_code_in = epsg_code_in
            
        else:
            self.epsg_code_in = None
                
                
        self.cloud_optimize = cloud_optimize

        logger.info(f"RasterHandler initialized")

    def set_raster_source(self, raster_name: str, container_name: str = None):

        if self.valid_raster_name(raster_name=raster_name):
            logger.debug(
                f"Setting raster source to {raster_name} in container {container_name}"
            )
            if self.blob_exists(blob_name=raster_name, container_name=container_name):

                self.raster_source = raster_name
                logger.debug(f"Raster source set to {raster_name}")
            else:
                self.raster_source = None
                raise FileNotFoundError(
                    f"Raster {raster_name} not found in container {container_name}"
                )
        else:
            self.raster_source = None
            raise ValueError(f"Invalid raster name: {raster_name}")

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
                    raise e
                except Exception as e:
                    logger.error(f"Error reading CRS from {raster_name}: {e}")
                    raise e
        
        except rasterio_errors.RasterioError as e:    
            logger.error(f"rasterio error reading {raster_name} into memory: {e}")
            raise e
        except Exception as e:
            logger.error(f"Error reading reading {raster_name} into memory: {e}")
            raise e

        if crs:
            if crs.is_valid:  # valid CRS
                logger.info(
                    f"Info: Valid CRS found for {raster_name}: {type(crs)} - EPSG:{crs.to_epsg()}"
                )
                return crs.to_epsg()  # return code
            else:
                raise AttributeError(f"CRS of {raster_name} <{crs}> is invalid")
        else:
            raise AttributeError(f"CRS of {raster_name} is invalid")

    def check_cog(self, container_name: str = None, raster_name: str = None):
        logger.info(f"Info: check_cog called")
        if not container_name:
            container_name = self.container_name
        if not raster_name:
            raster_name = self.raster_source
        logger.info(f"Info: check_cog Checking COG format for {raster_name}")
        raster_uri = self._get_blob_sas_uri(container_name, raster_name)
        try:
            tiff_details = cog_validate(raster_uri, quiet=True)
            if not tiff_details[0]:
                logger.info(f"{raster_name} is not in COG format")
                for error in tiff_details[1]:
                    logger.info(f"Error: {error}")
                for warning in tiff_details[2]:
                    logger.info(f"Warning: {warning}")
                return False
            else:
                logger.info(f"Info: check_cog {raster_name} is a valid COG")
                return True
        except Exception as e:
            logger.error(f"Error: checking COG for {raster_name}: {e}")
            return None
        
    def reproject_geotiff(
        self,
        raster_name_in: str,
        raster_name_out: str = None,
        container_name: str = None,
        output_container_name: str = None,
        epsg_code: int = None,
        epsg_code_in: int = None,
        overwrite: bool = False,
    ) -> str:

        logger.debug(f"reproject_geotiff called")
        raster_name_out = (
            raster_name_out
            if self.valid_raster_name(raster_name_out)
            else f'{raster_name_in.split(".")[0]}_reprojected.tif'
        )

        if container_name:
            logger.debug(f"Using provided container name: {container_name}")
        else:
            container_name = self.workspace_container_name
            logger.warning(f"Using workspace container name: {self.workspace_container_name}")
            
        if self.blob_exists(blob_name=raster_name_in, container_name=container_name):
            logger.debug(f"Raster {raster_name_in} found in container {container_name}")
        else:
            message = f"Error: Raster {raster_name_in} not found in container {container_name}"
            logger.error(message)
            
            raise FileNotFoundError(message)
        
        if self.blob_exists(blob_name=raster_name_out, container_name=output_container_name):
            if overwrite:
                logger.warning(f"Overwriting existing raster {raster_name_out} in {output_container_name}")

            else:
                message = f"Error: Raster {raster_name_out} already exists in {output_container_name}"
                logger.error(message)
                
                raise ResourceExistsError(message)

        if epsg_code:
            if self.is_valid_epsg_code(epsg_code):
                logger.debug(f"Using provided output EPSG code: {epsg_code}")
        else:
            logger.warning(f"No output EPSG code provided - defaulting to EPSG:4326")
            epsg_code = 4326
            
        CRS_out = CRS.from_epsg(epsg_code)
        
        epsg_code_in = epsg_code_in if self.is_valid_epsg_code(epsg_code_in) else None
        
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
                try:
                    CRS_in = CRS.from_epsg(epsg_code_in)
                    logger.warning(
                        f"Using input EPSG code {epsg_code_in} as CRS for {raster_name_in}"
                    )
                except:
                    message = f"Could not determine CRS from input raster or input EPSG code: <{self.epsg_code_in}> {e}"
                    logger.error(message)
                    raise ValueError(message)
            else:
                message = f"Error: Could not determine CRS for {raster_name_in} and no input EPSG code provided: {e}"
                logger.error(message)
                raise ValueError(message)
            
        if inferred_crs_in and epsg_code_in:
            if inferred_crs_in != epsg_code_in:
                message = f"Input EPSG code {epsg_code_in} does not match inferred CRS {inferred_crs_in} from raster {raster_name_in}"
                logger.error(message)
                raise ValueError(message)
            
        if CRS_in == CRS_out:
            logger.warning(f"{raster_name_in} is already in {CRS_out}")
            return raster_name_in

        logger.debug(f"{raster_name_in} is in {CRS_in} - reprojecting to {CRS_out}")

        with rasterio_open(
            self._get_blob_sas_uri(container_name=container_name, blob_name=raster_name_in)
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
                raise e

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
                            raise e

                memfile.seek(0)

                logger.info(
                    f"GeoTIFF reprojected successfully in-memory: {memfile.name}"
                )
                try:
                    self.upload_blob_data(
                        blob_data=memfile.read(),
                        dest_blob_name=raster_name_out,
                        container_name=output_container_name,
                        overwrite=overwrite,
                    )

                    logger.info(
                        f"GeoTIFF {raster_name_in} reprojected to {CRS_out} and written to container {output_container_name} as {raster_name_out}"
                    )
                    return raster_name_out

                except Exception as e:
                    logger.error(
                        f"Error writing output GeoTIFF {raster_name_out} to container {output_container_name}: {e}"
                    )
                    raise e

    def create_rasterio_cog(
        self,
        raster_name_in: str = None,
        raster_name_out: str = None,
        container_name: str = None,
        output_container_name: str = None,
        overwrite: bool = False,
    ) -> str:
        logger.info(f"create_rasterio_cog called")
        
        if not isinstance(raster_name_out, str) or not self.valid_raster_name(raster_name_out):
            raster_name_out = f'{raster_name_in.split(".")[0]}_cog.tif'
            logger.debug(f"Using default raster name: {raster_name_out}")

        if self.blob_exists(
            blob_name=raster_name_in, container_name=container_name):
            logger.debug(f"Raster {raster_name_in} found in container {container_name}")
        else:
            message = f"Error: Raster {raster_name_in} not found in container {container_name}"
            logger.error(message)
            
            raise FileNotFoundError(message)
        
        if self.blob_exists(
            blob_name=raster_name_out, container_name=output_container_name):
            if overwrite:
                logger.warning(f"Overwriting existing raster {raster_name_out} in {output_container_name}")
            else:
                message = f"Error: Raster {raster_name_out} already exists in {output_container_name}"
                logger.error(message)
                
                raise ResourceExistsError(message)
        
        logger.info(f"Creating COG for {raster_name_in} to {raster_name_out} in container {output_container_name}")

        try:
            with rasterio_open(
                    self._get_blob_sas_uri(container_name=container_name, blob_name=raster_name_in)
                ) as src:

                with MemoryFile() as memfile:
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
                        f"Uploading COG {raster_name_out} to container {container_name}"
                    )
                    try:
                        self.upload_blob_data(
                            blob_data=memfile.read(),
                            dest_blob_name=raster_name_out,
                            container_name=output_container_name,
                            overwrite=overwrite,
                        )
                    except Exception as e:
                        raise e

            logger.info(
                f"COG {raster_name_out} written to container {output_container_name}"
            )

            return raster_name_out

        except Exception as e:
            logger.error(
                f"Error creating COG for {raster_name_out} {type(e).__name__}: {e}"
            )
            raise e

    def stage_raster_file(
        self,
        raster_name_in: str,
        raster_name_out: str = None,
        workspace_container_name: str = None,
        output_container_name: str = None,
        epsg_code: int = None,
        epsg_code_in: int = None,
        cloud_optimize: bool = True,
        overwrite: bool = False,
    ):
        # called after validation of parameters
        raster_name = raster_name_in

        try:
            logger.debug(f"Setting raster source to {raster_name}")
            self.set_raster_source(raster_name=raster_name, container_name=workspace_container_name)
        except Exception as e:
            message = f"Error setting raster source: {e}"
            logger.error(message)
            raise e
        try:
            if raster_name_out and self.valid_raster_name(raster_name_out):
                logger.debug(f"Using provided output raster name: {raster_name_out}")
            else:
                logger.warning(f"Invalid output raster name provided: {raster_name_out}")
                logger.debug(f"Using default output raster name")
                raster_name_out = f'{raster_name_in.split(".")[0]}_staged.tif'

        except Exception as e:
            raise e

        try:
            inferred_crs_in = self.get_epsg_code(
                raster_name=raster_name, container_name=workspace_container_name
            )
        except Exception as e:
            logger.error(f"Error getting CRS for {raster_name}: {e}")
            inferred_crs_in = None
            if epsg_code_in:
                logger.warning(
                    f"Using input EPSG code {epsg_code_in} as CRS for {raster_name}"
                )
            else:
                message = f"Could not determine CRS from input raster or input EPSG code: <{self.epsg_code_in}> {e}"
                logger.error(message)
                raise ValueError(message)

        if inferred_crs_in and inferred_crs_in == epsg_code:
            logger.info(f"{raster_name} is already in EPSG:{epsg_code}")
        else:
            try:
                raster_name = self.reproject_geotiff(
                    raster_name_in=raster_name,
                    container_name=workspace_container_name,
                    epsg_code=epsg_code,
                    epsg_code_in=epsg_code_in,
                    output_container_name=output_container_name,
                    overwrite=overwrite,
                )
            except Exception as e:
                raise e

        if cloud_optimize:
            try:
                logger.debug(f"Creating COG for {raster_name}")
                raster_name = self.create_rasterio_cog(
                            raster_name_in = raster_name,
                            raster_name_out = raster_name_out,
                            container_name = workspace_container_name,
                            output_container_name = output_container_name,
                            overwrite = overwrite

                )
            except Exception as e:
                raise e

        else:
            logger.debug(f"Bypassing cloud optimization for {raster_name}")
            try:
                self.copy_blob(
                    source_blob_name = raster_name,
                    source_container_name = workspace_container_name,
                    dest_blob_name = raster_name_out,
                    dest_container_name = output_container_name,
                    wait_on_status=True,
                    overwrite=overwrite
                )

            except Exception as e:
                logger.error(
                    f"Failed to copy {raster_name_in} as {raster_name_out} in {output_container_name}"
                )
                raise e

        if self.blob_exists(
            blob_name=raster_name_out, container_name=output_container_name
        ):
            logger.info(
                f"{raster_name_in} staged as {raster_name_out} in {output_container_name}"
            )
            return {
                "raster_name_in": raster_name_in,
                "raster_name_out": raster_name_out,
                "output_container_name": output_container_name,
            }
        else:
            raise FileNotFoundError(
                f"Error: {raster_name_out} not found in {output_container_name} - uknown error staging {raster_name_in}"
            ) 
        
    @staticmethod
    def valid_raster_name(raster_name: str = None) -> bool:

        if not raster_name:
            logger.error("No raster name provided")
            return False

        if not any(
            raster_name.endswith(ext)
            for ext in [".tif", ".tiff", ".geotiff", ".geotif"]
        ):
            message = f"Invalid file extension for {raster_name}"
            logger.error(message)
            return False
        elif ";" in raster_name:
            return False
        else:
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
