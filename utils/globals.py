MAX_RASTER_NAME_LENGTH = 100
# Vector Data

VALID_GEOMETRY_TYPES = ['Polygon', 'LineString', 'Point', 'MultiPolygon', 'MultiLineString', 'MultiPoint']

VECTOR_FILE_EXTENSIONS = ['csv', 'gdb', 'geojson', 
                          'gpkg', 'json', 'kml', 
                          'kmz', 'shp', 'txt', 'zip']

VECTOR_FILE_DICT = {
    "csv": ["csv"],
    "geojson": ["geojson", "json"],
    "gdb": ["gdb", "filegeodatabase", "geodatabase"],
    "gpkg": ["gpkg", "geopackage"],
    "kml": ["kml"],
    "kmz": ["kmz"],
    "shp": ["shp", "shapefile", "shapefiles", "shape", "arcgis"],
    #"txt": ["txt", "text"],
    #"zip": ["zip", "zipfile", "7z"],
}

ZIP_FORMATS = [
    "zip",
    "kmz",
    #"tar",
    #"7z",
    #"tar.gz",
    #"tar.bz2",
    #"tar.xz",
]

DATABASE_ALLOWED_CHARACTERS = set(
"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
)

DATABASE_RESERVED_WORDS = [
        'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'BACKUP', 'BEGIN', 
        'BETWEEN', 'CASE', 'CHECK', 'COLUMN', 'CONSTRAINT', 'CREATE', 
        'DATABASE', 'DEFAULT', 'DELETE', 'DESC', 'DISTINCT', 'DROP', 'END', 
        'EXEC', 'EXISTS', 'FOREIGN', 'FROM', 'FULL', 'GROUP', 'HAVING', 'IN', 
        'INDEX', 'INNER', 'INSERT', 'INTO', 'IS', 'JOIN', 'LEFT', 'LIKE', 
        'LIMIT', 'NOT', 'NULL', 'OBJECTID', 'OR', 'ORDER', 'OUTER', 'PRIMARY', 'PROCEDURE', 
        'RIGHT', 'ROWNUM', 'SELECT', 'SET', 'TABLE', 'TIMESTAMP', 'TOP', 
        'TRUNCATE', 'UNION', 'UNIQUE', 'UPDATE', 'VALUES', 'VIEW', 'WHERE', 
        'add', 'all', 'alter', 'and', 'any', 'as', 'asc', 'backup', 'begin', 
        'between', 'case', 'check', 'column', 'constraint', 'create', 'database', 
        'default', 'delete', 'desc', 'distinct', 'drop', 'end', 'exec', 'exists',
        'foreign', 'from', 'full', 'group', 'having', 'in', 'index', 'inner', 
        'insert', 'into', 'is', 'join', 'left', 'like', 'limit', 'not', 'null',
        'objectid', 'or', 'order', 'outer', 'primary', 'procedure', 'right', 'rownum', 
        'select', 'set', 'table', 'timestamp', 'top', 'truncate', 'union', 'unique',
        'update', 'values', 'view', 'where'
    ]
GDF_VALID_DATATYPES = ["int", "float", "object", "date", "time", "bool", "geometry"]
# Enterprise Geodatabase

GDB_ITEMS_TABLE = "gdb_items"
GDB_ITEM_RELATIONSHIPS_TABLE = "gdb_itemrelationships"
GDB_ITEM_TYPES_TABLE = "gdb_itemtypes"
GDB_RESERVED_PREFIXES = ["amd_", "gdb_", "sde_","AMD_", "GDB_", "SDE_"]
GDB_RESERVED_NAMES = [
    GDB_ITEMS_TABLE,
    GDB_ITEM_RELATIONSHIPS_TABLE,
    GDB_ITEM_TYPES_TABLE,
]
GDB_RESERVED_SUFFIXES = ['_a', '_ana', '_bnd', '_cat', '_csl', '_d', '_evw', '_h', '_idx', '_s', '_t', '_A', '_ANA', '_BND', '_CAT', '_CSL', '_D', '_EVW', '_H', '_IDX', '_S', '_T']
GDB_TABLE_NAME_MAX_LENGTH = 30


# Idempotent Processing
REQUESTS_TABLE_NAME = "processing_requests"
OPERATIONS_TABLE_NAME = "processing_operations"
LOG_TABLE_NAME = "proc_logs"
LOG_SCHEMA_NAME = "app"
LOG_COLUMNS = [
    "session_id VARCHAR",
    "message_level VARCHAR",
    "message VARCHAR",
    "asctime VARCHAR",
    "message_timestamp VARCHAR",
    "func_name VARCHAR",
    "process_name VARCHAR",
    "lineno INTEGER",
]

VALID_EXTENSIONS = [
    "tif",
    "tiff",
    "geotiff",
    "kml",
    "kmz",
    "zip",
    "7z",
    "json",
    "geojson",
    "shp",
    "csv",
    "txt",
    "xml",
]

