import json
import requests
import os
import time


#from local_auth import HostingAuth
from authorization import HostingAuth
from utils import *

class HostingServerError(Exception):
    def __init__(self, message):
        super().__init__(message)
        logger.error(message)
        
class EnterpriseClient(HostingAuth):
        
        
    CNAME_URL = f'https://{ENVIRONMENT_CNAME}.worldbank.org'

    
    PORTAL_URL = f'{CNAME_URL}/{DEFAULT_PORTAL_CONTEXT_NAME}'
    IMAGERY_URL = f'{CNAME_URL}/{DEFAULT_IMAGERY_CONTEXT_NAME}'
    HOSTING_URL = f'{CNAME_URL}/{DEFAULT_VECTOR_CONTEXT_NAME}'
    
    TOKEN_URL = f'{PORTAL_URL}/sharing/rest/generatetoken'
    DATASTORES_URL = f'{PORTAL_URL}/sharing/rest/portals/self/datastores'
    
    GPSERVICES_URL = f'{HOSTING_URL}/rest/services/GPServices'

    
    IMAGERY_JOBS_URL = f'{IMAGERY_URL}/admin/system/jobs'
    IMAGERY_SERVICES_URL = f'{IMAGERY_URL}/admin/services'
    RASTER_TOOLS_URL = f'{IMAGERY_URL}/rest/services/System/RasterAnalysisTools/GPServer'
    CLOUDSTORE_CONTENT_URL = f'{RASTER_TOOLS_URL}/ListDatastoreContent'
    BATCH_PUBLISH_RASTER_URL = f'{RASTER_TOOLS_URL}/BatchPublishRaster'


    def __init__(self):
        
        logger.info(f'Initializing EnterpriseClient class')
        
        super().__init__()

        logger.debug(f'EnterpriseClient class initialized')


        try:
            self.token = self._get_hosting_token()
            logger.info(f'EnterpriseClient class initialized with portal: {self.PORTAL_URL}')
        except Exception as e:
            raise HostingServerError(f'Could not get token upon class instantiation: {e}')
            
        
    def _get_hosting_token(self,expiration:int=120):

        payload = {
            'username': self.portal_admin(),
            'password': self.portal_admin_credential(),
            'client': 'referer',
            'referer': self.TOKEN_URL,
            'expiration': expiration,
            'f': 'json'
        }
        try:
            response = requests.post(self.TOKEN_URL, data=payload)
            token = response.json()['token']
        except Exception as e:
            logger.error(f'Error getting token: {e}')
            raise e

        return token

    def json_request(self, url: str= None,
                     params: dict= None,
                     data: dict= None,
                     headers: dict= None,
                     timeout: int= None,
                     method: str= 'GET'):
        
        if not url:
            raise ValueError('URL is required')

        if isinstance(params,dict):
            logger.debug(f'Making JSON request to {url} with params: {params}')
            
            if 'token' in params.keys():
                logger.warning('Token in params - obtaining new token')
                
            params['token'] = self._get_hosting_token()
            params['f'] = 'json'
            
            if isinstance(data,dict):
                logger.warning('Data and JSON Params both provided - data will be ignored')
                data = None
                
        elif isinstance(data,dict):
            logger.debug(f'Making JSON request to {url} with data: {data}')
            if 'token' in data.keys():
                logger.warning('Token in data - obtaining new token')
            data['token'] = self._get_hosting_token()
            data['f'] = 'json'
            
        else:
            if method == 'GET':
                params = {'token':self._get_hosting_token(),'f':'json'}
            elif method == 'POST':
                data = {'token':self._get_hosting_token(),'f':'json'}
                 
        try:
            if method == 'GET':
                response = requests.get(
                    url=url,
                    params=params,
                    headers=headers
                    )
                
            elif method == 'POST':
                response = requests.post(
                    url=url,
                    data=data,
                    headers=headers, 
                    json=params)
            else:
                raise ValueError(f'Invalid HTTP method: {method}')
            
            response.raise_for_status()
            
        except requests.exceptions.RequestException as e:
            logger.error(f'Error in HTTP Request: {e}')
            raise e
        except Exception as e:
            logger.error(f'Error making JSON request: {e}')
            raise e
        
        #logger.debug(f'Response: {response.text}')
        try:
            jobj = response.json()
        except Exception as e:
            logger.error(f'Error parsing JSON response: {e}')
            raise e

        
        return jobj

    def service_name_available(self,service_name:str=None,service_type:str=None):
        
        service_types = ['Feature Service','Map Service']
        if not service_name:
            raise ValueError('Service name is required parameter')
        elif not service_type:
            raise ValueError('Service type is required parameter')
        elif service_type not in service_types:
            raise ValueError(f'Invalid service type: {service_type} must be Map Service or Feature Service')
        
        
        try:
            
            base_url = f'{self.PORTAL_URL}/sharing/rest/portals/0123456789ABCDEF/isServiceNameAvailable'
            params = {
                'name':service_name,
                'type': service_type
                }
            logger.debug(f'Checking service name availability for service {service_name} of type {service_type}')
            response_json = self.json_request(base_url,params,'GET')
            
        except Exception as e:
            raise HostingServerError(f'Error checking service name availability for {service_name} of type {service_type}: {e}')
        
        if response_json:
            if 'available' in response_json.keys():
                logger.info(f'Service name availability response: {response_json}')
                
                if response_json['available']:
                    
                    logger.info(f'Service name {service_name} is available')
                    
                    return True
                
                else:
                    
                    logger.warning(f'Service name {service_name} is not available')
                    
                    return False
                
            else:
                raise HostingServerError(f'Error in response checking for {service_name} of type {service_type}: {response_json}')
        else:
            raise HostingServerError(f'Empty response checking service name availability for {service_name} of type {service_type}')

    def get_item_info(self,item_id:str=None):
        
        
        if not item_id:
            raise ValueError('Item ID is required')
        logger.debug(f'Getting item info for {item_id}')
        
        
        try:
            base_url = f'{self.PORTAL_URL}/sharing/rest/content/items/{item_id}'
            logger.debug(f'Getting item info from {base_url}')
            
            response_json = self.json_request(base_url)
        
        except Exception as e:
            raise HostingServerError(f'Error getting item info for item id {item_id}: {e}')
            
        if response_json:
            
            if 'id' in response_json.keys():
                
                logger.info(f'Item info: {response_json}')
                return response_json
            
            elif 'error' in response_json.keys():
                raise HostingServerError(f'Server returned error searching for item id {item_id}: {response_json}')
                
            else:
                raise HostingServerError(f'No Item Info found in response: {response_json}')

        else:
            raise HostingServerError(f'Empty search results found for: {item_id}')

    def search_items(
        self,
        search:str=None,
        ids_only:bool=True):
        
        if not search:
            raise ValueError('Search string is required')
        
        base_url = f'{self.PORTAL_URL}/sharing/rest/search'
        params = {'q':search}
        
        try:
            
            logger.debug(f'Searching for item in portal: {search}')
            search_results = self.json_request(
                url=base_url,
                params=params,
                method='GET')
            
        except Exception as e:
            raise HostingServerError(f'Error searching for items: {e}')
        
        if search_results:
            
            if 'total' in search_results.keys() and search_results['total'] == 1:
                logger.info(f'Search results: {search_results}')
                
            elif 'total' in search_results.keys() and search_results['total'] > 1:
                logger.warning(f'Multiple items found for search string {search}: {search_results}')
                
            else:
                raise HostingServerError(f'Search string {search} found no items: {search_results}')

        else:
            raise HostingServerError(f'Empty search results found for: {search}')
        
            
        if ids_only:
            
            return [r['id'] for r in search_results['results']]
        
        else:
            
            return search_results['results']

    def get_service_json(
        self,
        context_name:str=None,
        service_name:str=None,
        server_folder:str=None):
        
        if not service_name:
            raise ValueError('Service name is required')
        
        context_name = context_name if context_name else DEFAULT_IMAGERY_CONTEXT_NAME
        
        if context_name == 'imagery':
            server_type = 'ImageServer'
            server_folder = server_folder if server_folder else 'Imagery'
            
        elif context_name == 'hosting':
            server_type = 'MapServer'
            server_folder = server_folder if server_folder else 'Hosted'
        
        else:
            raise ValueError(f'Invalid context name: {context_name}')

        try:
            service_url= f'{self.CNAME_URL}/{context_name}/admin/services/{server_folder}/{service_name}.{server_type}'
            logger.debug(f'Getting service JSON for {service_url}')
            response = self.json_request(url=service_url,method='GET')
            #logger.info(f'Service JSON: {response}')
            
        except Exception as e:
            raise HostingServerError(f'Error getting service JSON: {e}')
        
        return response
    
    def get_service_id(self,context_name:str=None,
                       service_name:str=None,
                       server_folder:str=None):
        
        r = self.get_service_json(context_name,service_name,server_folder)
        return r['portalProperties']['portalItems'][0]['itemID']
    
    def set_sharing(
        self,
        context_name:str=None,
        service_name:str=None,
        server_folder:str=None,
        sharing='public'):
        
        if not service_name:
            raise ValueError('service_name must be provided')
        
        if not context_name:
            raise ValueError('context_name must be provided')
        
        if not server_folder:
            raise ValueError('server_folder must be provided')

        try:
            logger.debug(f'Setting sharing for {service_name} in {server_folder} on /{context_name}')
            service_id = self.get_service_id(
                context_name=context_name,
                service_name=service_name,
                server_folder=server_folder)
            logger.debug(f'Service ID: {service_id}')
        except Exception as e:
            raise e
        
        share_url = f'{self.PORTAL_URL}/sharing/rest/content/users/{self.portal_user}/items/{service_id}/share'
        
        if sharing == 'public':
            payload = {'everyone':True,'org':True}
            try:
                response = self.json_request(
                    url=share_url,
                    data=payload,
                    method='POST')
                if 'notSharedWith' in response.keys() and len(
                    response['notSharedWith']) == 0:
                    
                    logger.info(f'{service_name} shared with everyone: {response}')
                    return True
                else:
                    raise HostingServerError(f'Error sharing service {service_name}: {response}')
                
            except Exception as e: 
                raise HostingServerError(f'Error setting sharing: {e}')
    
    def register_table(
        self,
        table_name,
        schema_name=None):
        
        job_url = f'{self.GPSERVICES_URL}/RegisterTable/GPServer/Register%20Table'

        if not table_name:
            raise ValueError('Table name is required')
        
        if schema_name:
            table_name = f'{schema_name}.{table_name}'
        
        params = {'table':table_name}
        
        try:
            response = self.json_request(
                url=f'{job_url}/submitJob',
                params=params,
                method='GET')
        except Exception as e:
            raise HostingServerError(f'Error submitting GP job: {e}')
        
        try:
            response = self.gp_job(
                job_url,
                params,
                wait=True)
            if response:
                logger.info(f'Table registered: {response}')
                return True
            else:
                raise HostingServerError(f'Error registering table: {response}')
            
        except Exception as e:
            raise HostingServerError(f'Error registering table: {e}')
        
        

    def gp_job(self,job_url,params=None,wait=False):
        
        try:
            response = self.json_request(
                url=f'{job_url}/submitJob',
                params=params,
                method='GET')
        except Exception as e:
            raise HostingServerError(f'Error submitting GP job: {e}')
        
        if 'jobId' in response.keys():
            job_id = response['jobId']
            logger.debug(f'GP Job ID: {job_id}')
             
        else:
            raise HostingServerError(f'Error getting GP job ID: {response}')
        
        while wait:
            
            try:
                job_response = self.json_request(
                url=f'{job_url}/jobs/{job_id}',
                method='GET')
                job_status = job_response['jobStatus']
                #job_response = self.gp_job_status(job_url,job_id)
            except Exception as e:
                raise HostingServerError(f'Error getting GP job status: {e}')
            
            
            
            if 'succeeded' in job_status.lower():
                logger.info(f'GP Job succeeded: {job_status}')
                return True
            
            elif 'failed' in job_status.lower():
                raise HostingServerError(f'GP Job failed: {job_response}')
            
            elif any([s.lower() in job_status.lower() for s in ['executing','submitted']]):
                logger.debug(f'GP Job executing: {job_status}')
                time.sleep(5)
                
            else:
                raise HostingServerError(f'Error getting GP job status: {job_response}')

        return job_id

    def get_server_folder_contents(self,
        server_folder:str=None,
        context_name:str=None,
        server_type:str=None,
        return_list:bool=False):
        
        if not server_folder or not context_name:
            if isinstance(server_type,str):
                if server_type.lower() in ['image','imagery','raster','imageserver']:
                    server_folder = HOSTED_IMAGERY_SERVER_FOLDER
                    context_name = DEFAULT_IMAGERY_CONTEXT_NAME
                elif server_type.lower() in ['hosting','vector','map','mapserver']:
                    server_folder = DEFAULT_DATASTORE_SERVER_FOLDER
                    context_name = DEFAULT_VECTOR_CONTEXT_NAME
                else:
                    raise ValueError(f'Invalid server type: {server_type}')
            else:
                raise ValueError('Server folder or type is required')
        url = f'{self.CNAME_URL}/{context_name}/admin/services/{server_folder}'
        
        try:
            r = self.json_request(url)
        except Exception as e:
            raise HostingServerError(f'Error getting server folder contents: {e}')
        
        if 'services' in r.keys():
            logger.info(f'{len(r["services"])} services found in {server_folder}')
            if return_list:
                return [s['serviceName'] for s in r['services'] if 'serviceName' in s.keys() ]
            else:
            
                return r['services']
        else:
            raise HostingServerError(f'Error getting server folder contents: {r}')

        
  
class MapServer(EnterpriseClient):

    
    def __init__(self,context_name:str=None):   
        super().__init__()
        self.datastore_id = DEFAULT_DATASTORE_ID
        self.datastore_server_folder = DEFAULT_DATASTORE_SERVER_FOLDER
        self.context_name = context_name if context_name else DEFAULT_VECTOR_CONTEXT_NAME
        logger.info('MapServer class initialized')
        
    def list_datastore_layers(
        self,
        datastore_id:str=None):
        
        datastore_id = datastore_id if datastore_id else DEFAULT_DATASTORE_ID
        url = f'{self.DATASTORES_URL}/allDatasets/getLayers'
        params={'datastoreId':datastore_id}
        
        try:
            r = self.json_request(url, data=params,method='POST')
        except Exception as e:
            logger.error(f'Error getting layers and datasets: {e}')
            raise e
        
        if 'layerAndDatasets' in r.keys():
            logger.info(
                f'{len(r["layerAndDatasets"])} layers and datasets found in {datastore_id}')
            
            return r['layerAndDatasets']
        else:
            raise HostingServerError(f'Error getting layers and datasets: {r}')
        
    def get_layer_info(self,
        layer_title:str=None,
        layer_id:str=None,
        dataset_name:str=None,
        datastore_id:str=None,
        service_type:str=None):
        
        results = []
        
        if (layer_title or layer_id) and dataset_name:
            raise ValueError('dataset_name parameter must be used alone')
        
        elif dataset_name:
            logger.debug(f'Searching for layers with dataset: {dataset_name}')
        
        elif layer_title or layer_id:
            logger.debug(f'Searching for layers with title: {layer_title} or id: {layer_id}')
        
        else:
            raise ValueError('At least one of layer_title, layer_id, or dataset_name must be provided')
        
        try:
            r = self.list_datastore_layers(datastore_id=datastore_id)
        except Exception as e:
            logger.error(e)
            raise e
        
        for l in r:
            if layer_title and l['layer']['title'] == layer_title:
                
                if layer_id:
                    if l['layer']['id'] == layer_id:
                        results.append(l)
                else:
                    results.append(l)
                    
            if dataset_name and l['dataset']['name'] == dataset_name:
                results.append(l)
        
        if len(results) == 0:
            raise ValueError('No results found')
        
        elif isinstance(service_type,str):
            
            services = [l for l in results if l['layer']['type'] == service_type]
            
            if len(services) == 0:
                raise ValueError(f'No {service_type} services found')
            
            elif len(services) > 1:
                raise HostingServerError(f'Multiple {service_type} services found {services}')
            
            elif len(services) == 1:
                logger.info(f'{service_type} service found: {services[0]}')
                
                return services[0]
            
        else:
            logger.info(f'{len(results)} results found')
            
            return results

    def synchronize_datastore_layers(
        self,
        datastore_id:str=None,
        sync_metadata=True,
        wait=True):
        
        datastore_id = datastore_id if datastore_id else DEFAULT_DATASTORE_ID
        
        url = f'{self.PORTAL_URL}/sharing/rest/portals/0123456789ABCDEF/datastores/allDatasets/publishLayers'
        
        data = {
            'datastoreId':datastore_id,
            'syncItemInfo':json.dumps(sync_metadata)
            }
        
        try:
            logger.debug(f'Synchronizing layers in datastore: {datastore_id}')
            r = self.json_request(url,data=data,method='POST')
        except Exception as e:
            logger.error(f'Error synchronizing layers: {e}')
            raise e
        
        if 'status' in r.keys():
            logger.debug(f'Synchronization status: {r["status"]}')
            if wait:
                while r['status'].lower() in [
                    'partial',
                    'processing']:
                    
                    time.sleep(5)
                    
                    r = self.check_datastore_status(
                        datastore_id=datastore_id)
                    logger.debug(f'Synchronization status: {r["status"]} {r["statusMessage"]}')
                    
                    if 'jobProgress' in r.keys():
                        logger.debug(f'Job Progress: {r["jobProgress"]}')
                        
                if r['status'].lower() in ['completed','succeeded']:
                    logger.info(f'Synchronization completed: {r}')
                    return r
                else:
                    raise HostingServerError(f'Synchronization failed: {r}')
              
        
        return r
    
    def check_datastore_status(
        self,datastore_id:str=None,
        user:str=None):
        
        datastore_id = datastore_id if datastore_id else DEFAULT_DATASTORE_ID
        user = user if user else self.portal_user
        
        url = f'{self.PORTAL_URL}/sharing/rest/content/users/{user}/items/{datastore_id}/status'
        
        try:
            logger.debug(f'Checking datastore status: {url}')
            r = self.json_request(url,method='GET')
        except Exception as e:
            logger.error(f'Error checking datastore status: {e}')
            raise e
        
        if 'status' in r.keys():
            logger.info(f'Datastore status: {r["status"]}')
            return r
        else:
            raise HostingServerError(f'Error checking datastore status: {r}')
    
    def enable_wfs(
        self,
        service_name: str= None,
        server_folder: str= None,
        context_name: str= None):
        
        
        if not service_name:
            raise ValueError('Service name is required to enable WCS')
        
        server_folder = server_folder if server_folder else self.datastore_server_folder
        
        context_name = context_name if context_name else self.context_name
        
        logger.debug(f'Enabling WFS for {service_name} in {server_folder} on /{context_name}')
        logger.debug('Setting sharing to public')
        
        try:
            self.set_sharing(
                context_name=context_name,
                service_name=service_name,
                server_folder=server_folder,
                sharing='public')
        except Exception as e:
            logger.error(f'Error setting sharing: {e}')
            raise e
        logger.info('Sharing set to public')
        
        edit_url = f'{self.CNAME_URL}/{context_name}/admin/services/{server_folder}/{service_name}.MapServer/edit'
        wfs_url = f'{self.CNAME_URL}/{context_name}/services/{server_folder}/{service_name}/MapServer/WFSServer'
        
        try:
            service_json = self.get_service_json(
                context_name=context_name,
                service_name=service_name,
                server_folder=server_folder)
            #logger.debug(f'Service JSON: {service_json}')
            if 'extensions' not in service_json.keys():
                raise HostingServerError(f'Invalid service JSON: {service_json}')
        except Exception as e:
            logger.error(f'Error getting service JSON: {e}')
            raise e

        extensions = [
            i for i in service_json['extensions'] if i['typeName'] != 'WFSServer'
            ]#remove any existing WFS extension
        
        wfs = self._build_wfs_extension(#builds dict to be passed as JSON to enable WFS
            server_folder=server_folder,
            service_name=service_name,
            context_name=context_name)
        
        extensions.append(wfs)
        
        service_json['extensions'] = extensions
        
        payload = {
            'service':json.dumps(service_json),
            'runAsync':True
            }
        
        try:
            response = self.json_request(
                url=edit_url,
                data=payload,
                method='POST')
            
        except Exception as e:
            logger.error(f'Error enabling WFS using POST: {e}')
            raise e
        
        logger.debug(f'WFS POST succesfull: {response}') 
        
        if 'status' in response.keys(
            ) and response['status'] == 'success':
            
                job_id = response['jobid']
                logger.info(f'WFS async job ID: {job_id}')
                
        else:
            raise HostingServerError(f'Error tasking async job to enable WFS: {response}')
        
        job_url = f'{self.CNAME_URL}/{context_name}/admin/system/jobs/{job_id}'
        
        executing = True
        while executing:
            try:
                response = self.json_request(#IMAGERY_JOBS_URL = f'{IMAGERY_URL}/admin/system/jobs'
                    url=job_url,
                    method='GET')
                status = response['status'].lower()
                
            except Exception as e:
                logger.error(f'Error enabling WFS using POST: {e}')
                raise e
            
            logger.debug(f'Status: {response["status"]}')
            
            if status.lower() in ['completed','succeeded']:
                
                if 'operationResponse' in response.keys(
                    ) and 'status' in response['operationResponse'].keys(
                    ) and response['operationResponse']['status'] == 'error':
                        
                    raise HostingServerError(f'Error enabling WFS: {response}')
                
                logger.info(f'WFS enabled: {response}')
                
                return f'{wfs_url}?SERVICE=WFS&REQUEST=GetCapabilities'
            
            elif status.lower() in ['failed','error']:
                
                raise HostingServerError(f'Error enabling WFS: {response}')

            time.sleep(5)
            
        logger.info(f'Asynch WFS job submitted, job status URL: {job_url}')
        
        return job_url
        
    def _build_wfs_extension(
        self,
        service_name:str=None,
        server_folder:str=None,
        context_name:str=None):
        
        wfs_url = f'{self.CNAME_URL}/{context_name}/services/{server_folder}/{service_name}/MapServer/WFSServer'
        name = f'{server_folder}_{service_name}_WFSServer'
        wfs =         {
            "typeName": "WFSServer",
            "capabilities": None,
            "properties": {
                "name": name,
                "appSchemaURI": wfs_url,
                "role": "",
                "appSchemaPrefix": name,
                "pathToStoredQueryFile": "",
                "enableTransactions": False,
                "title": "",
                "abstract": "",
                "keyword": "",
                "fees": "",
                "serviceType": "",
                "serviceTypeVersion": "",
                "deliveryPoint": "",
                "accessConstraints": "",
                "individualName": "",
                "positionName": "",
                "providerName": "",
                "onlineResource": wfs_url,
                "facsimile": "",
                "phone": "",
                "electronicMailAddress": "",
                "contactInstructions": "",
                "hourOfService": "",
                "providerSite": "",
                "administrativeArea": "",
                "city": "",
                "postalCode": "",
                "country": "",
                "pathToCustomGetCapabilitiesFiles": "",
                "customGetCapabilities": False,
                "transactionsWithoutLocks": False,
                "enableDefMaxFeatures": False,
                "disableStreaming": False,
                "defMaxFeaturesValue": "",
                "axisOrderWFS10": "LongLat",
                "axisOrderWFS11": "LongLat",
                "axisOrderWFS20": "LongLat"
            },
            "enabled": True
        }
        
        return wfs        
            
        
class ImageServer(EnterpriseClient):

    def __init__(self,context_name:str=None):
        super().__init__()
        
        self.cloudstore_server_folder = HOSTED_IMAGERY_SERVER_FOLDER
        self.imagery_context_name = context_name if context_name else DEFAULT_IMAGERY_CONTEXT_NAME
        logger.info('ImageServer class initialized')

    def list_cloudstore_contents(
        self,
        cloudstore_name:str=None,
        filter:str=None):
        
        if not cloudstore_name:
            raise ValueError('Cloudstore name is required')

        list_params = {
                  'dataStoreName':cloudstore_name,
                  'filter':filter}
        
        try:
            logger.debug(f'Submitting ListDatastoreContent job for {cloudstore_name} with params {list_params}')
            response_json = self.json_request(
                url = f'{self.CLOUDSTORE_CONTENT_URL}/submitJob',
                params=list_params,
                method='GET')
        except Exception as e:
            logger.error(f'Error submitting ListDatastoreContent job: {e}')
            raise e
        
        if response_json:
            if 'jobId' in response_json.keys():
                job_id = response_json['jobId']
                logger.info(f'Cloudstore job ID: {job_id}')
            else:
                raise HostingServerError(f'Error getting cloudstore contents: jobId missing: {response_json}')
               
        
        waiting = True
        job_url = f'{self.CLOUDSTORE_CONTENT_URL}/jobs/{job_id}'
        while waiting:
            logger.debug(f'Checking ListDatasoreContent job status: {job_id}')
           
            try:
                response_json = self.json_request(job_url,list_params,'GET')
            except Exception as e:
                logger.error(f'Error getting cloudstore contents: {e}')
                raise e
            
            if 'jobStatus' not in response_json.keys():
                raise HostingServerError(f'Error getting cloudstore contents: jobStatus missing: {response_json}')
            
            if response_json['jobStatus'] == 'esriJobSucceeded':
                logger.debug(f'Job succeeded: {response_json}')
                response_json = self.json_request(
                    url=f'{job_url}/results/contentList',
                    params=list_params,
                    method='GET')
                
                if 'value' in response_json.keys():
                    if isinstance(response_json['value'],dict):
                        logger.info(f'Cloudstore contents: {response_json}')
                        
                        return response_json['value']['contentList'][cloudstore_name]
                    
                    else:
                        raise HostingServerError(f'Error getting cloudstore content is empty with parameters {list_params}: {response_json}')
                else:
                    raise HostingServerError(f'Error getting cloudstore contents: value missing: {response_json}')

            elif response_json['jobStatus'] not in [
                'esriJobExecuting',
                'esriJobSubmitted']:
                raise HostingServerError(f'Error getting cloudstore contents: {response_json}')
            
            elif response_json['jobStatus'] == 'esriJobFailed':
                raise HostingServerError(f'Failure to get cloudstore contents: {response_json}')

            else:
                time.sleep(5)

    def get_cloudstore_item_path(
            self,
            object_name:str=None,
            cloudstore_name:str=None):
        
        cloudstore_name = cloudstore_name if cloudstore_name else f'/cloudStores/{DEFAULT_CLOUDSTORE_PATH}'
        
        if not object_name:
            raise ValueError('Object name required')
        logger.debug(f'Getting cloudstore path for {object_name} in {cloudstore_name}')
        
        try:
            contents = self.list_cloudstore_contents(
                cloudstore_name=cloudstore_name,
                filter=object_name)
            logger.debug(f'Cloudstore contents: {contents}')
            
        except Exception as e:
            logger.error(f'Error getting cloudstore contents: {e}')
            raise e
        
        if isinstance(contents,list) and len(contents) > 0:
            if len(contents) > 1:
                logger.warning(f'Multiple items found for {object_name} in {cloudstore_name}: {contents}')
            logger.info(f'Cloudstore contents: {contents}')
            return contents[0]
        else:
            raise HostingServerError(f'Error getting cloudstore path for {object_name} in {cloudstore_name}: contents: {contents}')

    
    
    def publish_raster(
        self, 
        raster_name: str= None,
        service_name: str= None,
        desc: str= None,
        cloudstore_name: str= None):
        
        if not raster_name:
            raise ValueError('Raster name is required')
        
        service_name = service_name if service_name else raster_name.split('.')[0]
        desc = desc if desc else f'{service_name} hosted on DDH GeoDev'
        cloudstore_name = cloudstore_name if cloudstore_name else f'/cloudStores/{DEFAULT_CLOUDSTORE_PATH}'
        
        try:
            raster_path = self.get_cloudstore_item_path(
                object_name=raster_name,
                cloudstore_name=cloudstore_name)
            
        except Exception as e:
            logger.error(f'Error getting cloudstore path for {raster_name}: {e}')
            raise e
        
        params = {

            'inputServices':
                json.dumps(
                    {
                        "services": [
                            {
                                "serviceConfig": {
                                    "serviceName": service_name,
                                    "type": "ImageServer",
                                    "capabilities": "Image, Metadata, Mensuration",
                                    "provider": "ArcObjectsRasterRendering",
                                    "properties": {
                                        "path": raster_path,
                                        "isManaged": False,
                                        "isCached": False,
                                        "isTiledImagery": "false",
                                        "colormapToRGB": "false",
                                        "description": desc,
                                        "defaultResamplingMethod": 1
                                    }
                                },
                                "itemProperties": {
                                    "folderId": ""
                                }
                            }
                        ]
                    }
                )#,
            #'convertToCRF': 'false',
            #'context': '{}'
        }
        
        try:
            response_json = self.json_request(
                url = f'{self.BATCH_PUBLISH_RASTER_URL}/submitJob',
                params=params,
                method='GET'
            )
        except Exception as e:
            logger.error(f'Error submitting publishing raster job: {e}')
            raise e

        if 'jobId' in response_json.keys():
            job_id = response_json['jobId']
            logger.info(f'Publishing Job ID: {job_id}')
        else:
            logger.error(f'Error getting job ID: {response_json}')
            return None
        
        waiting = True
        while waiting:
            
            try:
                response_json = self.json_request(
                    url=f'{self.BATCH_PUBLISH_RASTER_URL}/jobs/{job_id}',
                    params=None,
                    method='GET'
                )
            except Exception as e:
                logger.error(f'Error getting job status: {e}')
                raise e
            
            logger.debug('Job status: ' + response_json['jobStatus'])
            
            if not 'jobStatus' in response_json.keys():
                raise HostingServerError(f'Error getting job status: {response_json}')
            
            if response_json['jobStatus'] == 'esriJobSucceeded':
                logger.info(response_json['results'])
                results_json = self.json_request(
                    f'{self.BATCH_PUBLISH_RASTER_URL}/jobs/{job_id}/results/outputServices',params,'GET'
                )
                
                if 'value' in results_json.keys() and isinstance(
                    results_json['value'],list
                    ) and len(results_json['value']
                              ) > 0:
                    
                    image_service_url = results_json['value'][0]
                    logger.info(f'Image Service URL: {image_service_url}')
                    return image_service_url
                
                else:
                    raise HostingServerError(f'Error getting image service URL: {results_json}')

            elif response_json['jobStatus'] == 'esriJobFailed':
                logger.error(f'Publishing job: {job_id} failed')
                logger.error(response_json['messages'])
                raise HostingServerError(f'Error publishing raster: {response_json}')
            
            else:
                time.sleep(10)
        
    def enable_wcs(
        self,
        service_name: str= None,
        server_folder: str= None,
        context_name: str= None):
        
        if not service_name:
            raise ValueError('Service name is required to enable WCS')
        
        server_folder = server_folder if server_folder else self.cloudstore_server_folder
        
        context_name = context_name if context_name else self.imagery_context_name
        
        logger.debug(f'Enabling WCS for {service_name} in {server_folder} on {context_name}')
        logger.debug('Setting sharing to public')
        try:
            self.set_sharing(
                context_name=context_name,
                service_name=service_name,
                server_folder=server_folder,
                sharing='public')
        except Exception as e:
            logger.error(f'Error setting sharing: {e}')
            raise e
        
        edit_url = f'{self.IMAGERY_URL}/admin/services/{server_folder}/{service_name}.ImageServer/edit'
        
        try:
            service_json = self.get_service_json(
                context_name=context_name,
                service_name=service_name,
                server_folder=server_folder)
        except Exception as e:
            logger.error(f'Error getting service JSON: {e}')
            raise e
        
        wcs_url = f'{self.CNAME_URL}/{context_name}/services/{server_folder}/{service_name}/ImageServer/WCSServer'

        wcs = {
            'typeName': 'WCSServer',
            'capabilities': None,
            'properties': {
                'name': f'{server_folder}_{service_name}',
                'role': '',
                'title': 'WCS',
                'defaultVersion': '',
                'abstract': '',
                'keywords': '',
                'fees': '',
                'accessConstraints': 'None',
                'responsiblePerson': '',
                'responsiblePosition': '',
                'onlineResource': wcs_url,
                'providerName': '',
                'phone': '',
                'fax': '',
                'contactInstructions': '',
                'email': '',
                'address': '',
                'city': '',
                'province': '',
                'zipcode': '',
                'providerWebsite': '',
                'serviceHour': '',
                'country': '',
                'customGetCapabilities': False,
                'pathToCustomGetCapabilitiesFiles': '',
                'maxImageHeight': '',
                'maxImageWidth': '',
                'SupportEOProfile': False
            },
            'enabled': True
        }
        service_json['extensions'].append(wcs)
        payload = {
            'service':json.dumps(service_json),
            'runAsync':True
            }
        
        try:
            response = self.json_request(
                url=edit_url,
                data=payload,
                method='POST')
            
        except Exception as e:
            logger.error(f'Error enabling WCS using POST: {e}')
            raise e
        
        logger.debug(f'WCS POST succesfull: {response}') 
        
        if 'status' in response.keys(
            ) and response['status'] == 'success':
            
                job_id = response['jobid']
                logger.info(f'WCS async job ID: {job_id}')
                
        else:
            raise HostingServerError(f'Error tasking async job to enable WCS: {response}')
        
        executing = True
        while executing:
            try:
                response = self.json_request(
                    url=f'{self.IMAGERY_JOBS_URL}/{job_id}',
                    method='GET')
                status = response['status'].lower()
                
            except Exception as e:
                logger.error(f'Error enabling WCS using POST: {e}')
                raise e
            
            logger.debug(f'Status: {response["status"]}')
            
            if status == 'completed':
                
                logger.info(f'WCS enabled: {response}')
                
                return f'{wcs_url}?SERVICE=WCS&REQUEST=GetCapabilities'
            
            elif status in ['failed','error']:
                
                raise HostingServerError(f'Error enabling WCS: {response}')

            time.sleep(5)
