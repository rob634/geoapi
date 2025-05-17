from functools import wraps

from geopandas import GeoDataFrame
from shapely import (
    Polygon,
    MultiPolygon,
    LineString,
    MultiLineString,
    Point,
    MultiPoint
)

from utils import (
    logger,
    DATABASE_ALLOWED_CHARACTERS,
    DEFAULT_DB_USER,
    DEFAULT_EPSG_CODE,
    DEFAULT_GEOMETRY_NAME,
    ENTERPRISE_GEODATABASE_DB,
    HOSTING_SCHEMA_NAME,
    DATABASE_RESERVED_WORDS,
    GDF_VALID_DATATYPES,
    VectorHandlerError)


class VectorHandler:

    GEOM_DICT = {
        "Polygon": Polygon,
        "LineString": LineString,
        "Point": Point,
        "MultiPolygon": MultiPolygon,
        "MultiLineString": MultiLineString,
        "MultiPoint": MultiPoint,
    }

    def __init__(
        self,
        column_dict=None,  # optional column mapping of SQL types
        epsg_code=None,  # testing only, this is set globally
        geometry_name=None,  # testing only, this is set globally

        geometry_type=None,

    ):

        # instance attributes
        self.column_dict = column_dict
        self.epsg_code = epsg_code if epsg_code else DEFAULT_EPSG_CODE
        self.geometry_name = geometry_name if geometry_name else DEFAULT_GEOMETRY_NAME      
        self.geometry_type = geometry_type

        self.gdf = None
        self.valid_gdf = False
        
        logger.info(
            f"VectorHandler instance created with geometry name: {self.geometry_name}, epsg code: {self.epsg_code}")
            
    @staticmethod
    def one_geometry_type(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if 'gdf' in kwargs:
                gdf = kwargs['gdf']
                if isinstance(gdf, GeoDataFrame):
                    geometry_type = gdf.geometry.type
                    if len(set(geometry_type)) > 1:
                        raise ValueError(
                            f"GeoDataFrame contains multiple geometry types: {set(geometry_type)}"
                        )
                    else:
                        logger.info(f"GeoDataFrame contains single geometry type: {set(geometry_type)}")
                else:
                    raise ValueError("No GeoDataFrame provided in kwargs")
            else:
                logger.warning("No GeoDataFrame provided in kwargs")
            
            return func(self, *args, **kwargs)
        
        return wrapper 

    # GDF validation
    
    def enforce_geometry_name(self, gdf: GeoDataFrame) -> GeoDataFrame:
        geom_name = self.geometry_name if self.geometry_name else DEFAULT_GEOMETRY_NAME
        logger.debug("Validating geometry name")
        if gdf.geometry.name != geom_name:
            try:
                logger.debug(f"Renaming geometry column to {geom_name}")
                gdf = gdf.rename(
                    columns={gdf.geometry.name: geom_name}
                ).set_geometry(geom_name)

                logger.info(f"Geometry column renamed to {geom_name}")
            except Exception as e:
                error_message = f"Error renaming geometry column to {geom_name} {e}"
                logger.critical(error_message)
                raise e
        else:
            logger.info("Geometry column name is valid")
            
        return gdf
    
    def enforce_column_names(self, gdf: GeoDataFrame, suffix:str=None) -> GeoDataFrame:
        logger.debug("Validating reserved words in gdf")
        if not suffix:
            suffix = "_dbrnm1"
        column_names = list(gdf.columns)
        cols_in = list()
        cols_out = list()
        for _name in column_names:
            logger.debug(f"Validating column name: {_name}")
            if _name == 'objectid':
                logger.warning("Reserved word 'objectid' found in column names and will be renamed")
                cols_in.append(_name)
                cols_out.append(f"{_name}{suffix}")
            elif _name in DATABASE_RESERVED_WORDS:
                logger.warning(f"Column name <{_name}> will be renamed to {_name}{suffix}")
                cols_in.append(_name)
                cols_out.append(f"{_name}{suffix}")

            elif any(_char not in DATABASE_ALLOWED_CHARACTERS for _char in _name):
                
                invalid_chars = []
                for _char in _name:
                    if _char not in DATABASE_ALLOWED_CHARACTERS:
                        invalid_chars.append(_char)
                cols_in.append(_name)
                name_out = _name
                for _char in invalid_chars:
                    name_out = name_out.replace(_char, "_")
                logger.warning(f"Column name <{_name}> contains invalid characters - will be replaced <{name_out}>")
                cols_out.append(f"{name_out}")
                    
            elif _name[0].isdigit():
                logger.warning(f"Column name <{_name}> starts with a digit and will be renamed to c{_name}{suffix}")
                cols_in.append(_name)
                cols_out.append(f"c{_name}{suffix}")
                      
        if cols_in:
            logger.warning(f"Invalid column names found in: {cols_in}")
            if len(cols_out) != len(set(cols_out)):
                error_message = f"Duplicate column names found in {cols_out} after validation please fix the column names to be uniuqe and contain only {DATABASE_ALLOWED_CHARACTERS}"
                logger.critical(error_message)
                raise ValueError(error_message)
            try:
                new_column_dict = dict(zip(cols_in, cols_out))
                logger.debug(f"Renaming columns: {new_column_dict}")     
                gdf = gdf.rename(columns=new_column_dict)
                logger.info(f"Info: GeoDataFrame columns renamed to avoid reserved words")
                
            except Exception as e:
                error_message = f"Error renaming columns to avoid reserved database words{e}"
                logger.critical(error_message)
                raise VectorHandlerError(error_message)
        else:
            logger.info("GeoDataFrame column names do not contain database reserved words")
            
        return gdf

    def lowercase_column_names(self, gdf: GeoDataFrame) -> GeoDataFrame:
        column_names = list(gdf.columns)
        if any(any(
            map(lambda char: char.isupper(), column_name)) 
               for column_name in column_names):
            
            logger.warning(
                f"GeoDataFrame contains uppercase column names: {column_names} - renaming columns")
            
            try:
                gdf.columns = gdf.columns.str.lower()
                logger.info(f"Info: GeoDataFrame columns renamed to lowercase")
                
            except Exception as e:
                error_message = f"Error renaming columns to lowercase {e}"
                logger.critical(error_message)
                raise VectorHandlerError(error_message)
        else:
            logger.info("GeoDataFrame column names are lowercase")
        
        return gdf
    
    def validate_gdf_dtypes(self, gdf: GeoDataFrame) -> GeoDataFrame:
        
        logger.debug("Validating column data types")
        dropped_columns = list()
        for col in gdf.columns:
            col_dtype = str(gdf[col].dtype).lower()
            
            if not any([_dtype in col_dtype for _dtype in GDF_VALID_DATATYPES]):
                logger.error(f"Column {col} has an unsupported datatype <{col_dtype}>")
                
                try:
                    logger.warning(f"Removing column {col} with unsupported datatype <{col_dtype}>")
                    gdf = gdf.drop(columns=[col])
                    dropped_columns.append(col)
                    logger.info(f"Info: Column {col} removed")
                except Exception as e:
                    error_message = f"Error removing column {col} with unsupported datatype <{col_dtype}>"
                    logger.critical(error_message)
                    raise VectorHandlerError(error_message)
            else:
                logger.debug(f"Column {col} has valid datatype <{col_dtype}>")
                
        if dropped_columns:
            logger.warning(f"Columns removed: {dropped_columns}")
            logger.info(f"Datatypes validated with {len(dropped_columns)} invalid columns removed")
        else:
            logger.info("All columns have valid datatypes")
        
        return gdf

    def uniform_geometry_type(self, gdf: GeoDataFrame) -> str:
        # checks for mixed geometry types and returns the most complex compatible type if valid combination e.g. Polygons and MultiPolygons

        geom_type = None
        try:
            logger.debug("Reading geometry types from gdf")
            geometry_types = list(set(gdf.geometry.type))
        except Exception as e:
            error_message = f"Error reading geometry types from gdf: {e}"
            logger.error(error_message)
            raise VectorHandlerError(error_message)

        logger.debug(f"Geometry types detected: {geometry_types}")
        
        if len(geometry_types) == 0:
            error_message = "No geometry types detected in gdf"
            logger.error(error_message)
            
            raise ValueError(error_message)
        
        if not all([geometry_type in self.GEOM_DICT.keys() 
                    for geometry_type in geometry_types]):
            error_message = f"Unsupported geometry types detected: {geometry_types}"
            logger.error(error_message)
            
            raise ValueError(error_message)

        if len(geometry_types) == 1:

            logger.info(f"Single valid geometry type detected: {geometry_types[0]}")

            geom_type = geometry_types[0]

        elif len(geometry_types) == 2:
            logger.warning(f"Multiple geometry types detected: {geometry_types}")

            if "Polygon" in geometry_types and "MultiPolygon" in geometry_types:
                logger.debug(
                    "Polygon and MultiPolygon found- Converting to MultiPolygon"
                )
                geom_type = "MultiPolygon"

            elif "LineString" in geometry_types and "MultiLineString" in geometry_types:

                geom_type = "MultiLineString"

            elif "Point" in geometry_types and "MultiPoint" in geometry_types:

                geom_type = "MultiPoint"

            else:
                raise ValueError(
                    f"Unsupported combination of geometry types detected: <{geometry_types}> table cannot mix points, lines, and polygons"
                )
                
            logger.info(f"Validated geometry type: {geom_type} can be used for entire GeoDataFrame")
            
        elif len(geometry_types) > 2:
            error_message = f"Unsupported combination of geometry types detected: <{geometry_types}> table cannot mix points, lines, and polygons"
            logger.error(error_message)
            
            raise ValueError(error_message)

        else:
            error_message = f"Unknown error validating geometry types: {geometry_types}"
            logger.error(error_message)
            
            raise VectorHandlerError(error_message)
        
        return geom_type
    
    def set_uniform_geometry_type(
        self,
        gdf: GeoDataFrame,
        to_geometry_type: str = None,
        geometry_name: str = DEFAULT_GEOMETRY_NAME,
    ) -> GeoDataFrame:

        if isinstance(to_geometry_type,str) and to_geometry_type in self.GEOM_DICT.keys():
            logger.info(f"Geometry type set to provided parameter: {to_geometry_type}")
        else:
            try:
                logger.debug("Determining geometry type for GeoDataFrame")
                to_geometry_type = self.uniform_geometry_type(gdf=gdf)
                logger.info(f"Geometry type found: {to_geometry_type}")
            except Exception as e:
                logger.error("Error determining geometry type")
                raise e
        
        geoms = set(gdf.geometry.type)
        
        if not to_geometry_type in geoms:
            raise ValueError(
                f"GeoDataFrame does not contain geometry type {to_geometry_type}"
            )

        if len(geoms) > 1:
            logger.debug(f"Converting geometry types to: {to_geometry_type}")
            try:
                gdf[geometry_name] = gdf[geometry_name].apply(
                    lambda shape: (
                        self.GEOM_DICT[to_geometry_type]([shape])
                        if shape.geom_type != to_geometry_type
                        else shape
                    )
                )

            except Exception as e:
                logger.error(f"Error converting geometry types to {to_geometry_type}")
                raise e

            logger.info(f"Geometry type set to {to_geometry_type}")

        elif len(geoms) == 1:
            logger.info("Geometry types are already consistent")
            
        else:
            error_message = "No geometry types found in GeoDataFrame"
            logger.error(error_message)
            raise ValueError(error_message)

        return gdf

    @one_geometry_type
    def remove_gdf_z_values(
        self,
        gdf: GeoDataFrame,
        geometry_name: str = None,
    ):
        if isinstance(geometry_name, str):
            logger.debug(f"Using provided geometry name: {geometry_name}")
        elif isinstance(self.geometry_name, str):
            geometry_name = self.geometry_name
            logger.debug(f"Using instance geometry name: {geometry_name}")
        else:
            geometry_name = DEFAULT_GEOMETRY_NAME
            logger.debug(f"Using default geometry name: {geometry_name}")
        
        geometry_types = gdf.geometry.type
        if len(set(geometry_types)) > 1:
            error_message = f"GeoDataFrame contains multiple geometry types: {set(geometry_types)}"
            logger.error(error_message)
            raise ValueError(error_message)
        elif len(set(geometry_types)) == 1:
            geometry_type = geometry_types[0]
        else:
            error_message = "No geometry types found in GeoDataFrame"
            logger.error(error_message)
            raise ValueError(error_message)
        
        logger.debug(f"Geometry type detected: {geometry_type}")
        if any(gdf.geometry.has_z):
            logger.warning(
                f"Geometry column contains z values - geometry type {geometry_type}"
            )
            try:
                if geometry_type == "Polygon":
                    gdf[geometry_name] = gdf[geometry_name].apply(
                        lambda shape: Polygon(
                            [(x, y) for x, y, z in shape.exterior.coords]
                        )
                    )
                elif geometry_type == "LineString":
                    gdf[geometry_name] = gdf[geometry_name].apply(
                        lambda shape: LineString([(x, y) for x, y, z in shape.coords])
                    )
                elif geometry_type == "Point":
                    gdf[geometry_name] = gdf[geometry_name].apply(
                        lambda shape: Point(shape.x, shape.y)
                    )
                elif geometry_type == "MultiPolygon":
                    gdf[geometry_name] = gdf[geometry_name].apply(
                        lambda shape: MultiPolygon(
                            [
                                Polygon([(x, y) for x, y, z in poly.exterior.coords])
                                for poly in shape.geoms
                            ]
                        )
                    )
                elif geometry_type == "MultiLineString":
                    gdf[geometry_name] = gdf[geometry_name].apply(
                        lambda shape: MultiLineString(
                            [
                                LineString([(x, y) for x, y, z in line.coords])
                                for line in shape.geoms
                            ]
                        )
                    )
                else:
                    logger.warning("Unsupported geometry type")
                    raise ValueError("Unsupported geometry type")
                logger.info("Z values removed from geometry column")
            except Exception as e:
                logger.error("Error removing z values from geometry column")
                raise e
        else:
            logger.info("Geometry column does not contain z values")

        return gdf

    def remove_nulls_from_gdf(self, gdf: GeoDataFrame):
        logger.debug("Removing null geometries from gdf")
        gdf_length = len(gdf)
        dropped_count = 0
        
        if any(gdf.geometry.isnull()):
            logger.warning("GeoDataFrame contains null geometries")
            dropped_count += len(gdf[gdf.geometry.isnull()])
            logger.warning(f"Null geometries detected: {dropped_count}")
            try:
                logger.debug("Removing null geometries")
                gdf = gdf[~gdf.geometry.isnull()].copy()
                logger.info("Null geometries removed")
            except Exception as e:
                error_message = "Error removing null geometries"
                logger.error(error_message)
                raise VectorHandlerError(error_message)
            
        if any(gdf.geometry.isna()):
            logger.warning("GeoDataFrame contains NaN geometries")
            na_count = len(gdf[gdf.geometry.isna()])
            logger.warning(f"{na_count} NaN geometries detected")
            dropped_count += na_count
            logger.warning(f"Total invalid geometries detected: {dropped_count}")
            try:
                logger.debug("Removing NaN geometries")
                gdf = gdf[~gdf.geometry.isna()].copy()
                logger.info("NaN geometries removed")
            except Exception as e:
                error_message = ("Error removing NaN geometries")
                logger.error(error_message)
                raise VectorHandlerError(error_message)
        
        if any(~gdf.geometry.is_valid):
            logger.warning("GeoDataFrame contains invalid geometries")
            invalid_count = len(gdf[~gdf.geometry.is_valid])
            logger.warning(f"Invalid geometries detected: {invalid_count}")
            dropped_count += invalid_count
            logger.warning(f"Total invalid geometries detected: {dropped_count}")
            try:
                logger.debug("Removing invalid geometries")
                gdf = gdf[gdf.geometry.is_valid].copy()
                logger.info("Invalid geometries removed")
            except Exception as e:
                error_message = f"Error removing invalid geometries: {e}"
                logger.error(error_message)
                raise VectorHandlerError(error_message)
        
        if any(gdf.geometry.is_empty):
            logger.warning("GeoDataFrame contains empty geometries")
            empty_count = len(gdf[gdf.geometry.is_empty])
            logger.warning(f"Empty geometries detected: {empty_count}")
            dropped_count += empty_count
            logger.warning(f"Total invalid geometries detected: {dropped_count}")
            try:
                logger.debug("Removing empty geometries")
                gdf = gdf[~gdf.geometry.is_empty].copy()
                logger.info("Empty geometries removed")
            except Exception as e:
                error_message = "Error removing empty geometries"
                logger.error(error_message)
                raise VectorHandlerError(error_message)
        
        if dropped_count > 0:
            logger.warning(f"Total invalid geometries removed: {dropped_count} out of {gdf_length}")
            if len(gdf) == 0:
                logger.critical(
                    f"All geometries removed from GeoDataFrame: {dropped_count} invalid geometries"
                )
                raise ValueError(
                    f"GeoDataFrame is empty after removing invalid geometries: {dropped_count} invalid geometries"
                )
        else:
            logger.info("No invalid geometries detected")
            logger.info(f"GeoDataFrame has {gdf_length} valid geometries")

        return gdf

    def update_gdf_crs(self, gdf: GeoDataFrame, epsg_code: int = None):

        if isinstance(epsg_code, int):
            pass
        elif isinstance(self.epsg_code, int):
            epsg_code = self.epsg_code
        else:
            epsg_code = DEFAULT_EPSG_CODE
            
        logger.debug(f"Updating GeoDataFrame CRS to EPSG:{epsg_code}")
        if not gdf.crs:
            logger.warning("GeoDataFrame does not have a CRS set")
            logger.debug("Setting GeoDataFrame CRS to EPSG:4326")
            gdf = gdf.set_crs(f"EPSG:{epsg_code}")
            
        if gdf.crs.to_string() == f"EPSG:{epsg_code}":
            logger.info(f"GeoDataFrame is already in EPSG:{epsg_code}")
        else:
            logger.warning(
                f"GeoDataFrame is in {gdf.crs.to_string()}, reprojecting to EPSG:{epsg_code}"
            )
            try:
                gdf = gdf.to_crs(epsg=epsg_code)
                logger.info(f"GeoDataFrame reprojected to EPSG:{epsg_code}")

            except Exception as e:
                logger.error(f"Error reprojecting GeoDataFrame to EPSG:{epsg_code}")
                raise e

        return gdf


    # Single function runs

    def prepare_gdf(
        self,  # table_name:str=None,
        gdf: GeoDataFrame = None,
        geometry_name: str = None,
        epsg_code: int = None,
        inplace: bool = True,
    ) -> GeoDataFrame:

        if inplace:
            logger.debug("Preparing instance GeoDataFrame")
            if isinstance(self.gdf, GeoDataFrame):
                gdf = self.gdf
            else:
                logger.error("Instance GeoDataFrame is empty")
                raise ValueError("Instance GeoDataFrame is empty")
            epsg_code = self.epsg_code
            geometry_name = self.geometry_name
        else:
            epsg_code = epsg_code if epsg_code else DEFAULT_EPSG_CODE
            geometry_name = geometry_name if geometry_name else DEFAULT_GEOMETRY_NAME
            if not isinstance(gdf, GeoDataFrame):
                logger.error("No GeoDataFrame provided")
                raise ValueError("No GeoDataFrame provided")
        
        # Column Names
        try:
            logger.debug("Checking geometry name")
            gdf = self.enforce_geometry_name(gdf=gdf)
            logger.info("Geometry name validation completed")
        except Exception as e:
            logger.error("Error validating geometry name")
            raise e
        
        try:
            logger.debug("Checking reserved words in column names")
            gdf = self.enforce_column_names(gdf=gdf)
            logger.info("Reserved words validation completed")
        except Exception as e:
            logger.error("Error validating reserved words in column names")
            raise e

        try:
            logger.debug("Checking lowercase column names")
            gdf = self.lowercase_column_names(gdf=gdf)
            logger.info("Lowercase validation completed")
        except Exception as e:
            logger.error("Error validating lowercase column names")
            raise e
        
        # Column types
        try:
            logger.debug("Validating column data types")
            gdf = self.validate_gdf_dtypes(gdf=gdf)
            logger.info("Column data type validation completed")
        except Exception as e:
            logger.error("Error validating column data types")
            raise e
        
        # Geometry
        try:
            logger.debug("Removing null and invalid geometries")
            gdf = self.remove_nulls_from_gdf(gdf=gdf)
            logger.info("Invalid geometry validation completed")
        except Exception as e:
            logger.error(f"Error removing null geometries: {e}")
            raise e
        try:
            logger.debug("Determining geometry type for GeoDataFrame")
            geometry_type = self.uniform_geometry_type(gdf=gdf)
            logger.info(f"Geometry type found: {geometry_type}")
        except Exception as e:
            logger.error(f"Error determining geometry type: {e}")
            raise e
        try:
            logger.debug("Validating uniform geometry type")
            gdf = self.set_uniform_geometry_type(gdf=gdf)
            logger.info(f"Geometry type set to {geometry_type}")
        except Exception as e:
            logger.error(f"Error converting geometry types: {e}")
            raise e
        try:
            logger.debug("Removing z values from geometry column")
            gdf = self.remove_gdf_z_values(gdf=gdf, geometry_name=geometry_name)
            logger.info("Z value validation completed")
        except Exception as e:
            logger.error(f"Error removing z values from geometry column: {e}")
            raise e

        # CRS
        try:
            logger.debug(f"Updating CRS to EPSG:{epsg_code}")
            gdf = self.update_gdf_crs(gdf=gdf, epsg_code=epsg_code)
            logger.info(f"GeoDataFrame CRS updated to EPSG:{epsg_code}")
        except Exception as e:
            logger.error(f"Error updating CRS to EPSG:{epsg_code} {e}")
            raise e

        logger.info(f"GeoDataFrame prepared for database upload")
        
        if inplace:
            self.geometry_type = geometry_type
            self.gdf = gdf
            self.valid_gdf = True

        return gdf

    @classmethod
    def from_gdf(
        cls,
        gdf: GeoDataFrame,
        geometry_name: str = DEFAULT_GEOMETRY_NAME,
        geometry_type: str = None,
        epsg_code: int = DEFAULT_EPSG_CODE,
        column_dict: dict = None,
        validate: bool = False,

    ):
        """Creates a VectorHandler instance from a GeoDataFrame"""
        logger.debug("Creating VectorHandler instance from GeoDataFrame")
        if not isinstance(gdf, GeoDataFrame):
            raise ValueError("No GeoDataFrame provided")
        
        if gdf.empty:
            raise ValueError("GeoDataFrame is empty")
        
        instance = cls(
                geometry_name=geometry_name,
                geometry_type=geometry_type,
                epsg_code=epsg_code,
                column_dict=column_dict,
            )
        instance.gdf = gdf.copy()
        
        if validate:
            try:
                logger.debug("Validating GeoDataFrame")
                instance.prepare_gdf(
                    gdf=gdf,
                    geometry_name=geometry_name,
                    epsg_code=epsg_code,
                    inplace=True,
                )
            except Exception as e:
                logger.error(f"Error validating GeoDataFrame: {e}")
                raise e
            
        return instance
