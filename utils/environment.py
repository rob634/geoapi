AZURE_STORAGE_ACCOUNT_NAME = "rmhazuregeo"

WORKSPACE_CONTAINER_NAME = "scratch"

DATABASE_DICT = {
    "rmhpgflex": {
        "username": "rob634",
        "password": "B@lamb634@",
        "host": "rmhpgflex.postgres.database.azure.com",
        "port": 5432,
        "database": "geopgflex",
        "schemas": ["public", "geo", "sde"],
    }  
}

DATABASE_HOST = "rmhpgflex.postgres.database.azure.com"
DATABASE_PORT = 5432
DEFAULT_SCHEMA = "geo"
DEFAULT_DB_NAME = "geopgflex"
DEFAULT_DB_USERNAME = "rob634"
DEFAULT_GEOMETRY_NAME = "shape"