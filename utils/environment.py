
ENV = "dev"

ENVIRONMENT_CNAME = f"geo{ENV}"
STORAGE_ACCOUNT_NAME = f"host{ENV}strg"
VAULT_NAME = f"host-{ENV}-keys"
TASK_QUEUE_NAME = f"host-{ENV}-task-queue"
ENTERPRISE_GEODATABASE_HOST = f"host-{ENV}.postgres.database.azure.com"

DEFAULT_DATASTORE_ID = "7e67835493214af4b9d9886"
DEFAULT_DATASTORE_NAME = "azpostgres_ds_v11m9ziol"
DEFAULT_CLOUDSTORE_PATH = "hosted_geotiffs_ds_f855
DEFAULT_CLOUDSTORE_ID = "d1555edf299d43fea067bcde5dc"


# Application Constants
ENTERPRISE_GEODATABASE_DB = "geodb"
HOSTING_SCHEMA_NAME = "sde"

DEFAULT_PORTAL_CONTEXT_NAME = "portal"
DEFAULT_PORTAL_ADMIN_USER = "geo"

DEFAULT_VECTOR_CONTEXT_NAME = "hosting"
DEFAULT_DATASTORE_SERVER_FOLDER = "hosted_vector"

DEFAULT_IMAGERY_CONTEXT_NAME = "imagery"
HOSTED_IMAGERY_SERVER_FOLDER = "Imagery"

DEFAULT_WORKSPACE_CONTAINER = "scratch-workspace"
DEFAULT_HOSTING_CONTAINER = "hosted-geotiffs"

# PostGIS
APP_SCHEMA_NAME = "app"
DEFAULT_DB_ADMIN = "sde"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_USER = "sde"
DEFAULT_EPSG_CODE = 4326
DEFAULT_CRS_STRING = f"EPSG:{DEFAULT_EPSG_CODE}"
DEFAULT_GEOMETRY_NAME = "shape"
DEFAULT_INSERT_BATCH_SIZE = 1000

# Secret names
SECRET_DB_NAME = "db-name"
SECRET_DB_HOST = "db-host"
SECRET_CNAME = "cname"
SECRET_PORTAL_ADMIN = "enterprise-user"
SECRET_PORTAL_ADMIN_CREDENTIAL = "enterprise-credential"

SECRET_SP_CLIENT = "sp-client"

SECRETS = {
    "acs-path": "",
    "cloudstore-id": "",
    "cloudstore-path": "",
    "cname": "",
    "db-host": "",
    "db-name": "",
    "enterprise-credential": "",
    "enterprise-user": "",
    "gisowner-credential": "",
    "sde-credential": "",
}
