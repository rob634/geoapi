import azure.functions as func
from api_clients import ImageServer, EnterpriseClient, MapServer
from .base_request import BaseRequest
from utils import *

class EnterpriseRequest(BaseRequest):
    def __init__(self, req: func.HttpRequest,
                 command: str = None,
                 params: dict = None):

        logger.info('Initializing EnterpriseRequest')

        self.params = params if params else {}

        super().__init__(req, use_json=True)
                
        self.container_folder_name = self.json.get(
            'containerFolderName',None)
        
        self.cloudstore_name = self.json.get(
            'cloudstoreName',None)
        
        self.container_name = self.json.get(
            'containerName', DEFAULT_HOSTING_CONTAINER)
        
        self.context_name = self.json.get(
            'contextName',None)
        
        self.datastore_id = self.json.get(
            'datastoreId',DEFAULT_DATASTORE_ID)
                
        self.db_user = self.json.get(
            'dbUser',DEFAULT_DB_USER)
        
        self.raster_name = self.json.get(
            'rasterName',None)
        
        self.service_name = self.json.get(
            'serviceName',None)
        
        self.service_type = self.json.get(
            'serviceType',None)
               
        self.schema_name = self.json.get(
            'schemaName',DEFAULT_DB_USER)
        
        self.server_folder_name = self.json.get(
            'serverFolderName',None)

        self.service_type = self.json.get(
            'serviceType',None)
        
        self.table_name = self.json.get(
            'tableName',None)
        
        self.time_index = self.json.get(
            'timeIndex',None)
        
        self.async_wait = self.json.get(
            'waitForAsync',True)

        
        logger.debug(f'EnterpriseRequest initialized with command {command} request: {self.json}')
        
        self.response = self.hosting_response(command=command)
    
    def hosting_response(self,command:str=None) -> func.HttpResponse:
        logger.debug(f'Handling hosting command: {command}')

        if command == 'publish_raster':
            return self.publish_image_service()
        
        elif command == 'publish_vectors':
            return self.sync_map_server()
        
        elif command == 'enable_wfs':
            return self.enable_wfs()
        
        elif command == 'enable_wcs':
            return self.enable_wcs()
        
        elif command == 'register_table':
            return self._register_table()
        
        elif command == 'share_all':
            return self.share_all()
        
        else:
            return self.return_error(f'Unknown hosting command: {command}')
     
    def _register_table(self):
        
        H = EnterpriseClient()
        try:
            response = H.register_table(
                table_name=self.table_name,
                schema_name=self.json.get('schemaName', None))
        except Exception as e:
            return self.return_error(f'Error registering table: {e}')
        
        return self.return_success(message=f'Table registered: {response}')
        
    def publish_image_service(self):
        logger.debug('Publishing image service')
        self.wcs_url = None
        self.image_service_url = None
        try:
            self.validate_params(method='publish')
        except Exception as e:
            return self.return_error(f'{e}')
        
        try:
            R = ImageServer()
        except Exception as e:
            return self.return_error(f'Could not instantiate ImageServer: {e}')
        
        try:
            self.image_service_url = R.publish_raster(
                raster_name=self.raster_name,
                service_name=self.service_name,
                desc=None,
                cloudstore_name=self.cloudstore_name)
        except Exception as e:
            return self.return_error(f'Error during publish_raster: {e}')
        
        #ping service URL to test

        if self.service_type and any(
            [t in self.service_type for t in ['wcs','WCS']]):
            logger.debug('Enabling WCS')
            try:
                self.wcs_url = R.enable_wcs(
                    service_name=self.service_name,
                    server_folder=self.server_folder_name,
                    context_name=self.context_name)
            except Exception as e:
                return self.return_error(f'Error during enable_wcs: {e}')
        response = {'imageServiceUrl':self.image_service_url}
        message = f'Image service published: {self.image_service_url}'
        if self.wcs_url:
            message += f' WCS enabled: {self.wcs_url}'
            response['wcsUrl'] = self.wcs_url
            
        return self.return_success(message=message, json_out=response)


    def enable_wcs(self):
        logger.debug('Enabling WCS')
        
        try:
            self.validate_params(method='wcs')
        except Exception as e:
            return self.return_error(f'{e}')
        
        try:
            R = ImageServer()
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
    
    def validate_params(self,method=None):
        
        method = method if method else 'publish'
        
        if not self.raster_name and method == 'publish':
            raise ValueError('rasterName missing from request')
        
        if not self.service_name:
            if method == 'wcs':
                raise ValueError('serviceName missing from request')
            elif method == 'publish':
                logger.warning('serviceName missing from request defaulting to filename')
                try:
                    self.service_name = self.raster_name.split('.')[0]
                except Exception as e:
                    raise ValueError(f'Service name not provided and could not get service name from raster name: {e}')
            
        if not self.context_name and method in ['publish','wcs']:
            self.context_name = DEFAULT_IMAGERY_CONTEXT_NAME

    def sync_map_server(self):
        logger.debug('Synchronizing MapServer')

        datastore_id = self.datastore_id if self.datastore_id else DEFAULT_DATASTORE_ID
        sync_metadata = self.json.get('syncMetadata',True)
        wait_on_asynch = self.json.get('waitForAsync',True)
        
        try:
            M = MapServer()
        except Exception as e:
            return self.return_error(f'Could not instantiate MapServer: {e}')
        
        try:
            logger.debug(f'Synchronizing MapServer for datastore {datastore_id}')
            M.synchronize_datastore_layers(
                datastore_id=datastore_id,
                sync_metadata=sync_metadata,
                wait=wait_on_asynch)
        except Exception as e:
            return self.return_error(f'Error during sychronize: {e}')
        
        return self.return_success(message=f'Synchronized MapServer: {datastore_id}')
    
    def enable_wfs(self):
        logger.debug('Enabling WFS')

        context_name = self.json.get('contextName',DEFAULT_VECTOR_CONTEXT_NAME)
        server_folder = self.json.get('serverFolder',DEFAULT_DATASTORE_SERVER_FOLDER)
        service_name = self.json.get('serviceName',None)

        try:
            M = MapServer()
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
            M = MapServer()
        except Exception as e:
            return self.return_error(f'Could not instantiate MapServer: {e}')
        
        try:
            M.share_all_services()
        except Exception as e:
            return self.return_error(f'Error during share_all: {e}')
        
        return self.return_success(message='All services shared')