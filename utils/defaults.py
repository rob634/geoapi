
#Environment Constants
ENV = 'rmhazureq'

STORAGE_ACCOUNT_NAME = f'rmhazureqstrorage'
#VAULT_NAME = f'itses-gddatahub-{ENV}-keys'
ENTERPRISE_GEODATABASE_HOST = f'rmhazureqdb.postgres.database.azure.com'

FUNCTION_APP_NAME = f'rmhazureqfn'
FUNCTION_APP_ID = 'e66ab156-ea9e-4684-a60e-950d1ac705a6'

STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=rmhazureqstrorage;AccountKey=KN3pdNN8tmqN/8Dx/apEt+p0KjSkcRxOYtRjJMQZTNYxHwMxmoYLHPbR4OHHEqWAT0+Fmj4ehIEc+AStyt3vUQ==;EndpointSuffix=core.windows.net"
SP_CLIENT_ID = '0de34300-a946-4084-94a2-e5ca038b6aa8'
SP_TENANT_ID = 'bed4cf02-1d3c-4ba8-b420-cee7f1a868b7'
SP_SECRET_VALUE = 'eqG8Q~a9QlSKhspaxOsaIrOYHHlVjfCVbpgnUbNW'
SP_SECRET_ID = '14233354-3032-451f-b557-5d4297aadb47'
#ArcGIS Enterprise
DEFAULT_PORTAL_CONTEXT_NAME = 'portal'
DEFAULT_PORTAL_ADMIN_USER = 'ddhgeo'
SP_SCOPE = 'api://0de34300-a946-4084-94a2-e5ca038b6aa8'
DEFAULT_VECTOR_CONTEXT_NAME = 'hosting'
ENTERPRISE_GEODATABASE_DB = 'geodb'
DEFAULT_DATASTORE_SERVER_FOLDER = 'hosted_vector'

DEFAULT_IMAGERY_CONTEXT_NAME = 'imagery'
HOSTED_IMAGERY_SERVER_FOLDER = 'Imagery'

#Blob Storage
DEFAULT_WORKSPACE_CONTAINER = 'scratch-workspace'
DEFAULT_HOSTING_CONTAINER = 'hosted-geotiffs'

#PostGIS 
DEFAULT_EPSG_CODE = 4326
DEFAULT_DB_ADMIN = 'sde'
DEFAULT_DB_USER = 'sde'
DEFAULT_DB_PORT = 5432
DEFAULT_APP_SCHEMA = 'app'
DEFAULT_GEOMETRY_NAME = 'shape'

#Logging
LOG_TABLE_NAME = 'proc_logs'
LOG_SCHEMA_NAME = 'app'
LOG_COLUMNS = [
        'session_id VARCHAR',
        'message_level VARCHAR',
        'message VARCHAR',
        'asctime VARCHAR',
        'message_timestamp VARCHAR',
        'func_name VARCHAR',
        'process_name VARCHAR',
        'lineno INTEGER'
        ]

VALID_EXTENSIONS = [
        'tif', 'tiff', 'geotiff',
        'kml', 'kmz',
        'zip','7z',
        'json','geojson',
        'shp',
        'csv','txt','xml'
        ]


SECRETS = {
        'acs-path':'',
        'cloudstore-id':'',
        'cloudstore-path':'',
        'cname':'',
        'db-host':'',
        'db-name':'',
        'enterprise-credential':'',
        'enterprise-user':'',
        'gisowner-credential':'',
        'sde-credential':''
        }