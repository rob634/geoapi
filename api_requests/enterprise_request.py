import azure.functions as func
from enterprise_api import ImageServer, EnterpriseClient, MapServer
from .base_request import BaseRequest
from utils import *

class EnterpriseRequest(BaseRequest):
    def __init__(self, req: func.HttpRequest,
                 command: str = None,
                 params: dict = None):

        logger.info('Initializing EnterpriseRequest')
        super().__init__(req, use_json=True)
        self.params = params if params else {}
        self.cloudstore_id = None
        self.container_folder_name = None
        self.container_name = None
        self.context_name = None
        self.datastore_id = None
        self.db_user = None
        self.raster_collection = None
        self.raster_name = None
        self.schema_name = None
        self.server_folder_name = None
        self.service_name = None
        self.service_types = None
        self.table_name = None
        self.time_index = None

        params_defaults = {
            'cloudstoreID': {'var': 'cloudstore_id', 'default': DEFAULT_CLOUDSTORE_ID},
            'containerFolderName': {'var': 'container_folder_name', 'default': None},
            'containerName': {'var': 'container_name', 'default': DEFAULT_HOSTING_CONTAINER},
            'contextName': {'var': 'context_name', 'default': None},
            'datastoreId': {'var': 'datastore_id', 'default': DEFAULT_DATASTORE_ID},
            'dbUser': {'var': 'db_user', 'default': DEFAULT_DB_USER},
            'rasterCollection': {'var': 'raster_collection', 'default': None},
            'rasterName': {'var': 'raster_name', 'default': None},
            'schemaName': {'var': 'schema_name', 'default': DEFAULT_DB_USER},
            'serverFolderName': {'var': 'server_folder_name', 'default': None},
            'serviceName': {'var': 'service_name', 'default': None},
            'serviceTypes': {'var': 'service_types', 'default': None},
            'tableName': {'var': 'table_name', 'default': None},
            'timeIndex': {'var': 'time_index', 'default': None}
        }

        for key, value in params_defaults.items():
            logger.debug(f'Checking for {key} in request')
            
            if key in self.req_json:
                logger.debug(f'Found {key} in request, setting {value["var"]} to {self.req_json.get(key)}')
                setattr(self, value['var'], self.req_json.get(key))
            else:
                logger.debug(
                    f'{key} not found in request, setting {value["var"]} to default {value["default"]}')
                setattr(self, value['var'], value['default'])
        
        logger.debug(f'EnterpriseRequest initialized with command {command} request: {self.req_json}')
        
        self.response = self.enterprise_api_response(command=command)
    
    def enterprise_api_response(self,command:str=None) -> func.HttpResponse:
        logger.debug(f'Handling hosting command: {command}')

        if command == 'publish_raster':
            return self.publish_image_service()
        
        elif command == 'publish_raster_collection':
            return self.publish_image_service(collection=True)
        
        elif command == 'publish_vectors':
            return self.sync_map_server()
        
        elif command == 'enable_wfs':
            return self.enable_wfs()
        
        elif command == 'enable_wcs':
            return self.enable_wcs_req()
        
        elif command == 'register_table':
            return self.register_table()
        
        elif command == 'share_all':
            return self.share_all()
        
        elif command == 'list_services':
            return self.list_services()
        
        elif command == 'query_datastore_status':
            return self.query_datastore_status()
        
        else:
            return self.return_error(f'Unknown hosting command: {command}')
        
    def publish_image_service(self,collection=False):
        
        logger.debug('Publishing image service')
        self.wcs_url = None
        self.image_service_url = None
        publish_wcs = None
        error_message = None
        
        if not self.service_name:
            error_message = f'serviceName missing from request: {self.req_json}'
            logger.error(error_message)
            
            return self.return_error(error_message)

            
        if not self.context_name:
            logger.debug(f'contextName missing from request, defaulting to {DEFAULT_IMAGERY_CONTEXT_NAME}')
            self.context_name = DEFAULT_IMAGERY_CONTEXT_NAME
        
        if collection:
            logger.debug('Publishing image collection')
            if not self.raster_collection:
                error_message = f'rasterCollection missing from request: {self.req_json}'
                logger.error(error_message)
                
                return self.return_error(error_message)
            
            if isinstance(self.raster_collection, str):
                try:
                    self.raster_collection = self._parse_raster_collection(raster_collection=self.raster_collection)
                except Exception as e:
                    error_message = f'Error parsing rasterCollection: {e}'
                    logger.error(error_message)
                    
                    return self.return_error(error_message)
            
            elif isinstance(self.raster_collection, list):
                logger.info(f'Using rasterCollection list: {self.raster_collection}')
                
            else:
                error_message = f'Invalid rasterCollection type: {type(self.raster_collection)} - expected str or list'
                logger.error(error_message)
                
                return self.return_error(error_message)
            
        else:
            if not self.raster_name:
                error_message = f'rasterName missing from request: {self.req_json}'
                logger.error(error_message)
                
                return self.return_error(error_message)
            
        try:
            logger.debug('Instantiating ImageServer')
            #logger.debug(f'Using vault {self.vault_name} and context {self.context_name}')
            R = ImageServer.from_vault()
        except Exception as e:
            error_message = f'Could not instantiate ImageServer: {e}'
            logger.error(error_message)
            return self.return_error(error_message)
        
        if collection:
            try:
                logger.debug('Publishing image collection enterprise request')
                self.image_service_url = R.publish_raster_collection(
                    raster_names= self.raster_collection,
                    service_name = self.service_name,
                    desc =None,
                    cloudstore_id = self.cloudstore_id)
                logger.debug("Image collection published")

            except Exception as e:
                return self.return_error(f'Error during publish_raster_collection: {e}')
        else:
            try:
                logger.debug('Publish single image enterprise request')
                self.image_service_url = R.publish_raster(
                    raster_name=self.raster_name,
                    service_name=self.service_name,
                    desc=None,
                    cloudstore_id=self.cloudstore_id)
                
            except Exception as e:
                return self.return_error(f'Error during publish_raster: {e}')
        
        response = {'imageServiceUrl':self.image_service_url}
        message = f'Image service published: {self.image_service_url}'
        logger.info(message)
        #ping service URL to test


        if self.service_types:
            if isinstance(self.service_types, list) and 'wcs' in [s.lower() for s in self.service_types]:
                publish_wcs = True
            elif isinstance(self.service_types, str) and 'wcs' in self.service_types.lower():
                publish_wcs = True
            else:
                publish_wcs = False
        else:
            publish_wcs = False
        
        if publish_wcs: 
            logger.debug('Enabling WCS')
            try:
                self.wcs_url = R.enable_wcs(
                    service_name=self.service_name,
                    server_folder=self.server_folder_name,
                    context_name=self.context_name)
            except Exception as e:
                error_message = f'Error during enable_wcs: {e}'
                logger.error(error_message)

            if self.wcs_url:
                message += f' WCS enabled: {self.wcs_url}'
                response['wcsUrl'] = self.wcs_url
            elif error_message:
                message += f'WARNING Error enabling WCS: {error_message}'
                response['wcsUrl'] = 'error'
        else:
            logger.debug(f'WCS not enabled: no wcs in service types: {self.service_types}')
            
        return self.return_success(message=message, json_out=response)


    def enable_wcs_req(self):
        logger.debug('Enabling WCS')
        
        try:
            R = ImageServer.from_vault()
        except Exception as e:
            return self.return_error(f'Could not instantiate ImageServer: {e}')
        
        try:
            result = R.enable_wcs(
                service_name=self.service_name,
                server_folder=self.server_folder_name,
                context_name=self.context_name)
        except Exception as e:
            return self.return_error(f'Error during enable_wcs: {e}')
        
        return self.return_success(message=f'WCS enabled: {result}',json_out={'wcsUrl':result})
    
    
    def query_datastore_status(self):
        
        logger.debug(f'Querying datastore {self.datastore_id}')
        if not self.datastore_id:
            logger.warning(f'Instance datastore_id missing - attempting to retrieve from {self.req_json}')
            if not self.req_json.get('datastoreId', None):
                error_message = f'datastoreId missing from request, attempting to use default {DEFAULT_DATASTORE_ID}'
                logger.warning(error_message)
                self.datastore_id = DEFAULT_DATASTORE_ID

            else:
                self.datastore_id = self.req_json.get('datastoreId', None)
        try:
            logger.debug('Instantiating Enterprise API Client')
            E = EnterpriseClient.from_vault()
        except Exception as e:
            return self.return_error(f'Error instantiating Enterprise API Client: {e}')
        
        logger.debug(f'Checking if datastore {self.datastore_id} exists')
        try:
           datastore_exists = E.item_exists(item_id=self.datastore_id)
        except Exception as e:
            return self.return_error(f'Error searching for datastore: {e}')
        
        if datastore_exists:
            logger.debug(f'Datastore {self.datastore_id} exists, querying status')
        else:
            return self.return_error(f'Datastore {self.datastore_id} could not be found')

        try:
            json_out = E.query_datastore_status(datastore_id=self.datastore_id)
        except Exception as e:
            return self.return_error(f'Error listing services: {e}')
        
        return self.return_success(json_out=json_out)
        
    
    def list_services(self,server_name=None,server_folder_name=None,service_types=None,service_name=None):
        logger.debug('Listing services')

        if not service_name:
            server_name = self.service_name
        if not server_folder_name:
            server_folder_name = self.server_folder_name
        if not service_types:
            service_types = self.service_types
        try:
            E = EnterpriseClient.from_vault()
        except Exception as e:
            return self.return_error(f'Error instantiating Enterprise API Client: {e}')
        try:
            json_out = E.list_active_services(
                server_name=server_name,
                server_folders=[server_folder_name],
                service_types=service_types,
                search_string=service_name)
        except Exception as e:
            return self.return_error(f'Error listing services: {e}')
        
        return self.return_success(json_out=json_out)

    def register_table(self):
        logger.debug('Registering table')
        try:
            H = EnterpriseClient.from_vault()
            response = H.register_table(
                table_name=self.table_name)
                #schema_name=self.req_json.get('schemaName', None))
        except Exception as e:
            return self.return_error(f'Error registering table: {e}')
        
        return self.return_success(message=f'Table registered: {response}')

    def sync_map_server(self):
        logger.debug('Synchronizing MapServer')
        
        if not self.datastore_id:
            logger.warning(f'datastoreId missing from request defaulting to default datastore id {DEFAULT_DATASTORE_ID}')
            datastore_id = DEFAULT_DATASTORE_ID
        else:
            datastore_id = self.datastore_id 

        sync_metadata = self.req_json.get('syncMetadata',True)
        wait_on_asynch = self.req_json.get('waitForAsync',True)
        
        try:
            M = MapServer.from_vault(datastore_id=datastore_id,context_name=self.context_name,)
        except Exception as e:
            return self.return_error(f'Could not instantiate MapServer: {e}')
        
        try:
            datastore_exists =  M.item_exists(item_id=datastore_id)
        except Exception as e:
            return self.return_error(f'Error searching for datastore: {e}')
        if not datastore_exists:
            return self.return_error(f'Datastore {datastore_id} does not exist')
        
        try:
            logger.debug(f'Synchronizing MapServer for datastore {datastore_id}')
            M.synchronize_datastore_layers(
                datastore_id=datastore_id,
                sync_metadata=sync_metadata)
        except Exception as e:
            return self.return_error(f'Error during sychronize: {e}')
        
        return self.return_success(message=f'Datastore <{datastore_id}> synchronizing')
    
    def enable_wfs(self):
        logger.debug('Enabling WFS')

        context_name = self.req_json.get('contextName',DEFAULT_VECTOR_CONTEXT_NAME)
        server_folder = self.req_json.get('serverFolder',DEFAULT_DATASTORE_SERVER_FOLDER)
        service_name = self.req_json.get('serviceName',None)

        try:
            M = MapServer.from_vault()
        except Exception as e:
            return self.return_error(f'Could not instantiate MapServer: {e}')
        
        try:
            wfs_url = M.enable_wfs(
                service_name=self.service_name,
                server_folder=server_folder,
                context_name=context_name)
        except Exception as e:
            return self.return_error(f'Error during enable_wfs: {e}')
        
        return self.return_success(message=f'WFS enabled: {wfs_url}',json_out={'wfsUrl':wfs_url})

    def share_all(self):
        logger.debug('Sharing all services')
        
        try:
            M = MapServer.from_vault()
        except Exception as e:
            return self.return_error(f'Could not instantiate MapServer: {e}')
        
        try:
            M.share_all_services()
        except Exception as e:
            return self.return_error(f'Error during share_all: {e}')
        
        return self.return_success(message='All services shared')
    
    def _parse_raster_collection(self,raster_collection=None):
        raster_collection = raster_collection if raster_collection else self.raster_collection
        
        logger.debug(f'Parsing raster collection {raster_collection}')

        split_by = list()
        for delimiter in [',',';','|',' ']:
            if delimiter in self.raster_collection:
                logger.debug(f'Found delimiter {delimiter} in rasterCollection string')
                split_by.append(delimiter)
                    
        if len(split_by) > 1:
            error_message = f'Invalid rasterCollection string: {self.raster_collection} - multiple delimiters found: {split_by}'
            logger.error(error_message)
                
            raise ValueError(error_message)
            
        if len(split_by) == 1:
            logger.debug(f'Splitting rasterCollection string by {split_by[0]}')
                
            raster_collection = [r.strip() for r in raster_collection.split(split_by[0])]
            logger.info(f'Parsed rasterCollection: {self.raster_collection}')
            
            return raster_collection
            
        else:
            error_message = f'Could not parse rasterCollection string: {self.raster_collection} - no delimiters found. Supported delimiters are "," ";" "|" " "'
            logger.error(error_message)
            
            raise ValueError(error_message)
