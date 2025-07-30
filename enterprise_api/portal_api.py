import json
import requests
import os
import time

from authorization import VaultAuth
from utils import *


class EnterpriseClient:

    CNAME_URL = f"https://{ENVIRONMENT_CNAME}.worldbank.org"
    PORTAL_URL = f"{CNAME_URL}/{DEFAULT_PORTAL_CONTEXT_NAME}"
    PORTAL_ID = '0123456789ABCDEF'
    IMAGERY_URL = f"{CNAME_URL}/{DEFAULT_IMAGERY_CONTEXT_NAME}"
    HOSTING_URL = f"{CNAME_URL}/{DEFAULT_VECTOR_CONTEXT_NAME}"

    DATASTORES_URL = f"{PORTAL_URL}/sharing/rest/portals/self/datastores"
    ITEMS_URL = f"{PORTAL_URL}/sharing/rest/content/items"
    GPSERVICES_URL = f"{HOSTING_URL}/rest/services/GPServices"

    IMAGERY_JOBS_URL = f"{IMAGERY_URL}/admin/system/jobs"
    IMAGERY_SERVICES_URL = f"{IMAGERY_URL}/admin/services"
    RASTER_TOOLS_URL = f"{IMAGERY_URL}/rest/services/System/RasterAnalysisTools/GPServer"

    def __init__(self):

        logger.debug(f"Initializing EnterpriseClient class")
        self.portal_username = DEFAULT_PORTAL_ADMIN_USER
        self.portal_credential = None
        self.token_url = None
        logger.debug(f"EnterpriseClient class initialized")

        if not self.token_url:
            self.token_url = f"{self.PORTAL_URL}/sharing/rest/generatetoken"
    
    @staticmethod    
    def _portal_credential(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            error_message = None
            if hasattr(self, 'portal_credential') and hasattr(self, '_portal_token'):
                if getattr(self, 'portal_credential'):
                    try:
                        
                        getattr(self, '_portal_token')()
                        logger.debug(f"Portal credential test passed")
                    
                        return func(self, *args, **kwargs)
                    
                    except Exception as e:
                        error_message = f"Credential validation failed: Error getting portal credential: {e}"
                else:
                    error_message = f"{self.__class__.__name__} portal_credential is not initialized"
            else:
                error_message = f"{self.__class__.__name__} does not have  portal_credential attribute."
            logger.critical(error_message)
            raise EnterpriseClientError(error_message)
            
        return wrapper  
    
    @_portal_credential
    def rest_api_call(
        self,
        url: str = None,
        params: dict = None,
        headers: dict = None,
        method: str = "GET",
        return_json: bool = True,):
        
        logger.debug(f"Making REST API call to {url} with params: {params} and headers: {headers}")
        
        if params and isinstance(params, dict):
            logger.debug(f"Serializing parameters: {params}")
            # Serialize parameters if not strings
            for param in params.keys():
                if not isinstance(params[param], str):
                    try:
                        param_object = params[param]
                        
                        if isinstance(param_object, set):
                            logger.warning(f"Converting set to list for parameter {param_object}")
                            param_object = list(param_object)

                        params[param] = json.dumps(param_object)
                        logger.debug(f"Serialized parameter {param}: {params[param]}")
                        
                    except SyntaxError as e:
                        logger.error(f"SyntaxError converting parameter {param_object} to JSON: {e}")
                        raise
                    
                    except TypeError as e:
                        logger.error(f"TypeError converting parameter {param_object} to JSON: {e}")
                        raise
                    
                    except Exception as e:
                        logger.error(f"Error converting parameter {param_object} to JSON: {e}")
                        raise
                    
        elif params:
            raise ValueError(f"Invalid parameters: {params}. Must be a dictionary or None.")

        else:
            params = dict()
            
        params["token"] = self._portal_token()
        params["f"] = "json"
        # Make requests
        try:
            if method.lower() == "get":
                response = requests.get(url=url, params=params, headers=headers)

            elif method.lower() == "post":
                response = requests.post(url=url, data=params, headers=headers)
                
            else:
                raise ValueError(f"Invalid HTTP method: {method}")

            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error in HTTP Request: {e}")
            raise
        except Exception as e:
            logger.error(f"Error making JSON request: {e}")
            raise
        
        # Optionally return JSON
        if return_json:
            try:
                jobj = response.json()
                
                return jobj
            
            except requests.exceptions.JSONDecodeError as e:
                logger.error(f"Response did was not of JSON type: {e}")
                logger.error(str(response.content))

                raise
            except Exception as e:
                logger.error(f"Error parsing JSON response: {e}")
                logger.error(str(response.content))

                raise
        else:
            
            return response
    
    @_portal_credential
    def json_request(
        self,
        url: str = None,
        params: dict = None,
        data: dict = None,
        headers: dict = None,
        timeout: int = None,
        method: str = "GET",
    ):
        # ArcGIS REST paramaters should {"arcparam1": json.dumps({"param1": "value1"})}

        if not self.portal_credential:
            error_message = "No portal credential available"
            logger.error(error_message)
            raise EnterpriseClientError(error_message)

        if not url:
            raise ValueError("URL is required")

        if isinstance(params, dict):

            if "token" in params.keys():
                logger.warning("Token in params - obtaining new token")

            params["token"] = self._portal_token()
            params["f"] = "json"

            if isinstance(data, dict):
                logger.warning(
                    "Data and JSON Params both provided - data will be ignored"
                )
                data = None
            logger.debug(
                f"Making JSON <{method}> request to {url} with params: {params}"
            )

        elif isinstance(data, dict):

            if "token" in data.keys():
                logger.warning("Token in data - obtaining new token")
            data["token"] = self._portal_token()
            data["f"] = "json"
            logger.debug(f"Making JSON <{method}> request to {url} with data: {data}")
            # data = json.dumps(data)
            params = None

        else:
            params = {"token": self._portal_token(), "f": "json"}
            logger.debug(
                f"Making JSON <{method}> request to {url} using default params: {params}"
            )
        try:
            if method.lower() == "get":

                logger.debug(url)
                logger.debug(params)
                logger.debug(headers)
                response = requests.get(url=url, params=params, headers=headers)

            elif method.lower() == "post":
                response = requests.post(
                    url=url, data=data, headers=headers, json=params
                )
            else:
                raise ValueError(f"Invalid HTTP method: {method}")

            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error in HTTP Request: {e}")
            raise
        except Exception as e:
            logger.error(f"Error making JSON request: {e}")
            raise

        try:
            jobj = response.json()
        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"Response did was not of JSON type: {e}")
            logger.error(str(response.content))

            raise
        except Exception as e:
            logger.error(f"Error parsing JSON response: {e}")
            logger.error(str(response.content))

            raise

        return jobj

    def check_job(
        self,
        job_id,
        server_name:str='imagery',
        folder_name:str='System',
        gp_service_name:str=None,
        task_name:str=None,
        job_url:str=None,
        
        wait_time:int=5):
        
        response_json = dict()
        if all([server_name, folder_name, 
                gp_service_name, task_name,job_id]):
            
            job_url = f"{self.CNAME_URL}/{server_name}/rest/services/{folder_name}/{gp_service_name}/GPServer/{task_name}/jobs/{job_id}"
            
            logger.debug(f"Using constructed job url: {job_url}")
        elif job_url:
            logger.debug(f"Using job_url parameter: {job_url}")

        waiting = True
        while waiting:
            logger.debug(f"Checking status of {task_name} job {job_id}")

            try:
                response_json = self.rest_api_call(url=job_url, method="GET")
            except Exception as e:
                logger.error(f"Error getting cloudstore contents: {e}")
                raise

            if "jobStatus" not in response_json:
                raise EnterpriseClientError(
                    f"Error getting cloudstore contents: jobStatus missing: {response_json}"
                )

            if response_json["jobStatus"] == "esriJobSucceeded":
                logger.info(f"Job succeeded: {response_json}")
                
                return response_json
                
            elif response_json["jobStatus"] not in [
                "esriJobExecuting",
                "esriJobSubmitted",
            ]:
                raise EnterpriseClientError(
                    f"Error getting cloudstore contents: {response_json}"
                )

            elif response_json["jobStatus"] in ["esriJobFailed", "failed", "error"]:
                raise EnterpriseClientError(
                    f"Failure of {task_name} job: {response_json}"
                )

            else:
                if 'jobStatus' in response_json:
                    logger.debug(f"Job status of {task_name} job {job_id}: {response_json['jobStatus']}")
                time.sleep(wait_time)
            
    def datastore_path_from_id(self, datastore_id: str):
        logger.debug(f"Getting datastore path for {datastore_id}")
        try:
            json_data = self.rest_api_call(
                url=f"{self.ITEMS_URL}/{datastore_id}/data")
            
            if "path" in json_data:
                logger.debug(f"Datastore info: {json_data}") 
                logger.info(f"Datastore path: {json_data['path']}")
                
                return json_data["path"]
            
            elif "error" in json_data:
                logger.error(f"Error getting datastore path for {datastore_id}: {json_data}")
                raise EnterpriseClientError(f"Error getting datastore path for {datastore_id}: {json_data}")
        except Exception as e:
            logger.error(f"Error getting datastore path for {datastore_id}: {e}")
            
    def search(self,search_string:str=None):
        if not search_string:
            raise ValueError("Search string is required")
        base_url = f"{self.PORTAL_URL}/sharing/rest/search"
        params = {"q": search_string}
        try:
            response = self.json_request(url=base_url,params=params,method="GET")
        except Exception as e:
            raise EnterpriseClientError(f"Error searching for items: {e}")
        
        if "total" in response:
            if response["total"] == 0:
                logger.warning(f"No results found for search string: {search_string}")
                return None
            else:
                logger.info(f"Search results: {response['results']}")
                return response['results']

    def item_exists(self,item_id:str):
        try:
            items = self.search(search_string=item_id)
        except Exception as e:
            raise EnterpriseClientError(f"Error searching for item: {e}")
        
        if not items:
            logger.error(f'Item <{item_id}> not found')
            return False

        else:
            logger.debug(f'Item {item_id} found')
            logger.debug(f'Item details: {items}')
            return True
    
    def list_active_services(
        self,
        server_name: str = None,
        server_folders: list = None,
        service_types: list = None,
        search_string: str = None
    ):

        if isinstance(service_types, str):
            service_types = [service_types]
        elif isinstance(service_types, list):
            service_types = service_types
        else:
            service_types = ["FeatureServer", "MapServer", "ImageServer"]

        service_dict = dict()
        services = []
        services_url = f"{self.PORTAL_URL}/sharing/rest/portals/self/servers"

        try:
            servers = self.json_request(services_url).get("servers", [])
        except Exception as e:
            logger.error(f"Error fetching server list: {e}")
            raise EnterpriseClientError(f"Error fetching server list: {e}")

        for server in servers:
            this_server = {
                "url": server["url"],
                "name": server["name"],
                "server_role": server["serverRole"],
                "server_function": server["serverFunction"],
                "server_id": server["id"],
                "folders": dict(),
                "services": [],
            }
            if isinstance(server_name, str):
                if server_name == server["name"]:
                    service_dict[server["name"]] = this_server
                else:
                    continue
            else:
                service_dict[server["name"]] = this_server

            server_services_url = f"{server['url']}/rest/services"
            try:
                server_json = self.json_request(server_services_url)
            except Exception as e:
                error_message = (
                    f"Error fetching server services for {server['name']}: {e}"
                )
                logger.error(error_message)
                service_dict[server["name"]] = error_message
                continue

            root_folders = server_json.get("folders", [])
            # iterate through folders
            if not root_folders or len(root_folders) < 1:
                return service_dict

            for folder in root_folders:
                if isinstance(server_folders, list) and folder not in server_folders:
                    continue
                elif isinstance(server_folders, str) and folder != server_folders:
                    continue

                folder_url = f"{server_services_url}/{folder}"
                try:
                    folder_json = self.json_request(folder_url)
                except Exception as e:
                    error_message = f"Error fetching folder services for {folder} in server <{server}>: {e}"
                    logger.error(error_message)
                    service_dict[server["name"]]["folders"][folder] = error_message
                    continue

                folder_services = folder_json.get("services", [])
                service_dict[server["name"]]["folders"][folder] = {
                    "services": folder_json.get("services", []),
                    "folders": folder_json.get("folders", []),
                    "url": folder_url,
                }
                services = [
                    folder_service
                    for folder_service in folder_services
                    if folder_service["type"] in service_types
                ]

                for service in services:
                    if search_string and not service['name'].endswith(search_string):
                        logger.warning(f"Skipping service {service['name']} - does not match search string {search_string} ")
                        continue
                    else:
                        logger.info(f"Service {search_string} found: {service['name']}")
                    logger.debug(f"Fetching service details for {service['name']}")
                    service["url"] = (
                        f"{server_services_url}/{service['name']}/{service['type']}"
                    )
                    try:
                        logger.debug(f"Fetching service details for {service['name']}")
                        service_json = self.json_request(service["url"])
                    except Exception as e:
                        error_message = (
                            f"Error fetching service details for {service['name']}: {e}"
                        )
                        logger.error(error_message)
                        service["description"] = error_message
                        continue
                    
                    service["item_id"] = service_json.get("serviceItemId", None)

                    layers = service_json.get("layers", [])
                    if len(layers) > 0:
                        service["layers"] = [
                            {"id": l["id"], "name": l["name"]} for l in layers
                        ]
                    for attribute in [
                        "supportedExportFormats",
                        "supportedQueryFormats",
                        "maxRecordCount",
                        "capabilities",
                        "description",
                        "copyrightText",
                        "spatialReference",
                        "serviceItemId",
                    ]:
                        if attribute == "spatialReference":

                            service[attribute] = service_json.get(attribute, {}).get(
                                "wkid", None
                            )

                        else:
                            service[attribute] = service_json.get(attribute, None)

        return service_dict

    def query_datastore_status(self, datastore_id: str = None):
        datastore_id = datastore_id if datastore_id else DEFAULT_DATASTORE_ID
        datastore_url = f"{self.PORTAL_URL}/sharing/rest/content/users/{self.portal_username}/items/{datastore_id}/status"
        try:
            status = self.rest_api_call(url=datastore_url, method="GET")
        except Exception as e:
            raise EnterpriseClientError(
                f"Error querying datastore <{datastore_url}> status: {e}"
            )
        return status

    def service_name_available(
        self, service_name: str = None, service_type: str = None
    ):

        service_types = ["Feature Service", "Map Service"]
        if not service_name:
            raise ValueError("Service name is required parameter")
        elif not service_type:
            raise ValueError("Service type is required parameter")
        elif service_type not in service_types:
            raise ValueError(
                f"Invalid service type: {service_type} must be Map Service or Feature Service"
            )

        try:

            base_url = f"{self.PORTAL_URL}/sharing/rest/portals/0123456789ABCDEF/isServiceNameAvailable"
            params = {"name": service_name, "type": service_type}
            logger.debug(
                f"Checking service name availability for service {service_name} of type {service_type}"
            )
            response_json = self.json_request(base_url, params, "GET")

        except Exception as e:
            raise EnterpriseClientError(
                f"Error checking service name availability for {service_name} of type {service_type}: {e}"
            )

        if response_json:
            if "available" in response_json.keys():
                logger.info(f"Service name availability response: {response_json}")

                if response_json["available"]:

                    logger.info(f"Service name {service_name} is available")

                    return True

                else:

                    logger.warning(f"Service name {service_name} is not available")

                    return False

            else:
                raise EnterpriseClientError(
                    f"Error in response checking for {service_name} of type {service_type}: {response_json}"
                )
        else:
            raise EnterpriseClientError(
                f"Empty response checking service name availability for {service_name} of type {service_type}"
            )

    def get_item_info(self, item_id: str = None):

        if not item_id:
            raise ValueError("Item ID is required")
        logger.debug(f"Getting item info for {item_id}")

        try:
            base_url = f"{self.PORTAL_URL}/sharing/rest/content/items/{item_id}"
            logger.debug(f"Getting item info from {base_url}")

            response_json = self.json_request(base_url)

        except Exception as e:
            raise EnterpriseClientError(
                f"Error getting item info for item id {item_id}: {e}"
            )

        if response_json:

            if "id" in response_json.keys():

                logger.info(f"Item info: {response_json}")
                return response_json

            elif "error" in response_json.keys():
                raise EnterpriseClientError(
                    f"Server returned error searching for item id {item_id}: {response_json}"
                )

            else:
                raise EnterpriseClientError(
                    f"No Item Info found in response: {response_json}"
                )

        else:
            raise EnterpriseClientError(f"Empty search results found for: {item_id}")

    def search_items(self, search: str = None, ids_only: bool = True):

        if not search:
            raise ValueError("Search string is required")

        base_url = f"{self.PORTAL_URL}/sharing/rest/search"
        params = {"q": search}

        try:

            logger.debug(f"Searching for item in portal: {search}")
            search_results = self.json_request(
                url=base_url, params=params, method="GET"
            )

        except Exception as e:
            raise EnterpriseClientError(f"Error searching for items: {e}")

        if search_results:

            if "total" in search_results.keys() and search_results["total"] == 1:
                logger.info(f"Search results: {search_results}")

            elif "total" in search_results.keys() and search_results["total"] > 1:
                logger.warning(
                    f"Multiple items found for search string {search}: {search_results}"
                )

            else:
                raise EnterpriseClientError(
                    f"Search string {search} found no items: {search_results}"
                )

        else:
            raise EnterpriseClientError(f"Empty search results found for: {search}")

        if ids_only:

            return [r["id"] for r in search_results["results"]]

        else:

            return search_results["results"]

    def get_service_json(#This is from ArcGIS Server to make calls through Server Manager
        self,
        context_name: str = None,
        service_name: str = None,
        server_folder: str = None,
    ):

        if not service_name:
            raise ValueError("Service name is required")

        context_name = context_name if context_name else DEFAULT_IMAGERY_CONTEXT_NAME

        if context_name == "imagery":
            server_type = "ImageServer"
            server_folder = server_folder if server_folder else "Imagery"

        elif context_name == "hosting":
            server_type = "MapServer"
            server_folder = server_folder if server_folder else "Hosted"

        else:
            raise ValueError(f"Invalid context name: {context_name}")

        try:
            service_url = f"{self.CNAME_URL}/{context_name}/admin/services/{server_folder}/{service_name}.{server_type}"
            logger.debug(f"Getting service JSON for {service_url}")
            response = self.json_request(url=service_url, method="GET")
            # logger.info(f'Service JSON: {response}')

        except Exception as e:
            raise EnterpriseClientError(f"Error getting service JSON: {e}")

        return response

    def get_service_id(
        self,
        context_name: str = None,
        service_name: str = None,
        server_folder: str = None,
    ):

        r = self.get_service_json(context_name, service_name, server_folder)
        return r["portalProperties"]["portalItems"][0]["itemID"]

    def set_sharing(
        self,
        context_name: str = None,
        service_name: str = None,
        server_folder: str = None,
        sharing="public",
    ):

        if not service_name:
            raise ValueError("service_name must be provided")

        if not context_name:
            raise ValueError("context_name must be provided")

        if not server_folder:
            raise ValueError("server_folder must be provided")

        try:
            logger.debug(
                f"Setting sharing for {service_name} in {server_folder} on /{context_name}"
            )
            service_id = self.get_service_id(
                context_name=context_name,
                service_name=service_name,
                server_folder=server_folder,
            )
            logger.debug(f"Service ID: {service_id}")
        except Exception as e:
            raise

        share_url = f"{self.PORTAL_URL}/sharing/rest/content/users/{self.portal_username}/items/{service_id}/share"

        if sharing == "public":
            payload = {"everyone": True, "org": True}
            try:
                response = self.json_request(url=share_url, data=payload, method="POST")
                if (
                    "notSharedWith" in response.keys()
                    and len(response["notSharedWith"]) == 0
                ):

                    logger.info(f"{service_name} shared with everyone: {response}")
                    return True
                else:
                    raise EnterpriseClientError(
                        f"Error sharing service {service_name}: {response}"
                    )

            except Exception as e:
                raise EnterpriseClientError(f"Error setting sharing: {e}")

    def register_table(self, table_name, schema_name=None):

        job_url = f"{self.GPSERVICES_URL}/RegisterTable/GPServer/Register%20Table"

        if not table_name:
            raise ValueError("Table name is required")

        params = {"table": table_name}

        try:
            response = self.gp_job(job_url, params, wait=True)
            if response:
                logger.info(f"Table registered: {response}")
                return response
            else:
                raise EnterpriseClientError(f"Error registering table: {response}")

        except Exception as e:
            raise EnterpriseClientError(f"Error registering table: {e}")

    def gp_job(self, job_url, params=None, wait=False):

        try:
            response = self.json_request(
                url=f"{job_url}/submitJob", params=params, method="GET"
            )
        except Exception as e:
            raise EnterpriseClientError(f"Error submitting GP job: {e}")

        if "jobId" in response.keys():
            job_id = response["jobId"]
            logger.debug(f"GP Job ID: {job_id}")

        else:
            raise EnterpriseClientError(f"Error getting GP job ID: {response}")

        while wait:

            try:
                job_response = self.json_request(
                    url=f"{job_url}/jobs/{job_id}", method="GET"
                )
                job_status = job_response["jobStatus"]
                # job_response = self.gp_job_status(job_url,job_id)
            except Exception as e:
                raise EnterpriseClientError(f"Error getting GP job status: {e}")

            if "succeeded" in job_status.lower():
                logger.info(f"GP Job succeeded: {job_status}")
                return job_response

            elif "failed" in job_status.lower():
                if "messages" in job_response.keys():
                    raise EnterpriseClientError(
                        f"GP Job failed with messages: {job_response['messages']}"
                    )
                else:
                    raise EnterpriseClientError(f"GP Job failed: {job_response}")

            elif any(
                [s.lower() in job_status.lower() for s in ["executing", "submitted"]]
            ):
                logger.debug(f"GP Job executing: {job_status}")
                time.sleep(5)

            else:
                raise EnterpriseClientError(
                    f"Error getting GP job status: {job_response}"
                )

        return job_id

    def get_server_folder_contents(
        self,
        server_folder: str = None,
        context_name: str = None,
        server_type: str = None,
        return_list: bool = False,
    ):

        if not server_folder or not context_name:
            if isinstance(server_type, str):
                if server_type.lower() in ["image", "imagery", "raster", "imageserver"]:
                    server_folder = HOSTED_IMAGERY_SERVER_FOLDER
                    context_name = DEFAULT_IMAGERY_CONTEXT_NAME
                elif server_type.lower() in ["hosting", "vector", "map", "mapserver"]:
                    server_folder = DEFAULT_DATASTORE_SERVER_FOLDER
                    context_name = DEFAULT_VECTOR_CONTEXT_NAME
                else:
                    raise ValueError(f"Invalid server type: {server_type}")
            else:
                raise ValueError("Server folder or type is required")
        url = f"{self.CNAME_URL}/{context_name}/admin/services/{server_folder}"

        try:
            r = self.json_request(url)
        except Exception as e:
            raise EnterpriseClientError(f"Error getting server folder contents: {e}")

        if "services" in r.keys():
            logger.info(f'{len(r["services"])} services found in {server_folder}')
            if return_list:
                return [
                    s["serviceName"] for s in r["services"] if "serviceName" in s.keys()
                ]
            else:

                return r["services"]
        else:
            raise EnterpriseClientError(f"Error getting server folder contents: {r}")

    def _credential_from_vault(self, vault_name: str = None, secret_name=None):

        secret_name = secret_name if secret_name else SECRET_PORTAL_ADMIN_CREDENTIAL
        auth = VaultAuth(vault_name=vault_name)
        if auth.secret_client:
            try:
                secret_names = auth.secret_client.list_properties_of_secrets()
                names = [secret.name for secret in secret_names]
                logger.debug(f"Secret client test pass - Secrets: " + ", ".join(names))

            except (AzureError, Exception) as e:
                raise AzureError(
                    f"Secret client test fail - error listing secrets: {e}"
                )
            logger.debug(f"Secret client test pass")

            secret = auth.secret_client.get_secret(secret_name)
            if secret and secret.value:
                return secret.value
            else:
                raise EnterpriseClientError(
                    "Failed to retrieve portal credential from vault"
                )

        else:
            raise AzureError(
                f"Azure Secret Client failed to initialize {auth.init_errors}"
            )

    @classmethod
    def from_vault(cls, vault_name: str = None, credential=None,datastore_id: str = None, context_name: str = None):
        instance = cls()
        try:
            instance.portal_credential = instance._credential_from_vault(
                vault_name=vault_name, secret_name=credential
            )
        except Exception as e:
            raise EnterpriseClientError(
                f"Failed to retrieve portal credential from vault: {e}"
            )

        # Test Enterprise connection
        try:
            token = instance._portal_token()
            if token:
                logger.debug(f"Token test passed")
            logger.info(
                f"EnterpriseClient class initialized with portal: {instance.PORTAL_URL}"
            )
        except Exception as e:
            raise EnterpriseClientError(
                f"Could not get token upon class instantiation: {e}"
            )

        return instance

    @classmethod
    def from_params(
        cls,
        portal_username: str = None,
        portal_credential: str = None,
        cname: str = None,
    ):
        logger.debug(
            f"Initializing EnterpriseClient class from parameters {portal_username} {portal_credential}"
        )

        instance = cls()
        if isinstance(cname, str):
            instance.CNAME_URL = f"https://{cname}.worldbank.org"
            instance.PORTAL_URL = f"{instance.CNAME_URL}/{DEFAULT_PORTAL_CONTEXT_NAME}"
            instance.IMAGERY_URL = (
                f"{instance.CNAME_URL}/{DEFAULT_IMAGERY_CONTEXT_NAME}"
            )
            instance.HOSTING_URL = f"{instance.CNAME_URL}/{DEFAULT_VECTOR_CONTEXT_NAME}"

            instance.DATASTORES_URL = (
                f"{instance.PORTAL_URL}/sharing/rest/portals/self/datastores"
            )
            instance.GPSERVICES_URL = f"{instance.HOSTING_URL}/rest/services/GPServices"

            instance.IMAGERY_JOBS_URL = f"{instance.IMAGERY_URL}/admin/system/jobs"
            instance.IMAGERY_SERVICES_URL = f"{instance.IMAGERY_URL}/admin/services"
            instance.RASTER_TOOLS_URL = f"{instance.IMAGERY_URL}/rest/services/System/RasterAnalysisTools/GPServer"

        if isinstance(portal_username, str):
            instance.portal_username = portal_username
        if isinstance(portal_credential, str):
            instance.portal_credential = portal_credential
        elif not instance.portal_credential:
            raise EnterpriseClientError(
                "No portal credential available or provided in parameters"
            )
        logger.debug(
            f"EnterpriseClient class initialized with portal: {instance.PORTAL_URL} {instance.portal_username} {instance.portal_credential}"
        )

        try:
            token = instance._portal_token()
            if token:
                logger.debug(f"Token test passed")
            logger.info(
                f"EnterpriseClient class initialized with portal: {instance.PORTAL_URL}"
            )
        except Exception as e:
            raise EnterpriseClientError(
                f"Could not get token upon class instantiation: {e}"
            )

        return instance

    def _credential_from_params(
        self, portal_username: str = None, portal_credential: str = None
    ):

        if isinstance(portal_credential, str):
            return portal_credential
        else:
            raise EnterpriseClientError("No portal credential available")

    def _portal_token(self, expiration: int = 120):
        if self.portal_credential:
            payload = {
                "username": self.portal_username,
                "password": self.portal_credential,
                "client": "referer",
                "referer": self.token_url,
                "expiration": expiration,
                "f": "json",
            }
            try:
                response = requests.post(self.token_url, data=payload)
                token = response.json()["token"]
                
                return token
            
            except Exception as e:
                logger.error(f"Error getting token: {e}")
                raise
            
        else:
            error_message = "No portal credential available"
            logger.error(error_message)
            raise EnterpriseClientError(error_message)

    def gp_execute(
        self,
        context_name: str = None,
        server_folder_name: str = 'System',
        datastore_id: str = None,
        gp_service_name: str = None,
        task_name: str = None,
        results_path: str = None,
        payload: dict = None,
        wait: bool = True,
    ):
        
        # Pre-execution logic and validation
        task_url = f"{self.CNAME_URL}/{context_name}/rest/services/{server_folder_name}/{gp_service_name}/GPServer/{task_name}"
        
        # Submit job
        try:
            response_json = self.rest_api_call(
                url=f"{task_url}/submitJob",   
                params=payload,
                method="GET",
            )
        except Exception as e:
            logger.error(f"Error submitting publishing raster job: {e}")
            raise

        # Get Job ID
        if isinstance(response_json,dict) and "jobId" in response_json:
            job_id = response_json["jobId"]
            logger.info(f"Publishing Job ID: {job_id}")
        else:
            error_message = f"Error getting job ID: {response_json}"
            logger.error(error_message)
            
            raise EnterpriseClientError(error_message)

        # Check job status
        try:
            job_response_json = self.check_job(
                job_id=job_id,
                server_name=context_name,
                folder_name=server_folder_name, 
                gp_service_name=gp_service_name,
                task_name=task_name,
                )
            logger.info(f'Publishing job: {job_id} succeeded: {response_json}')

        except Exception as e:
            logger.error(f"Error checking job status: {e}")
            raise

        if results_path:
            results_url = f"{task_url}/jobs/{job_id}/results/{results_path}"
            logger.debug(f"Results URL: {results_url}")
            try:
                results_json = self.rest_api_call(url=results_url)
                logger.debug(f"Results JSON: {results_json}")
                
                return results_json
            
            except Exception as e:
                error_message = f"Error getting job results from {results_url}: {e}"
                logger.error(error_message)
                
                raise EnterpriseClientError(error_message)
        else:
            
            return job_response_json

    def get_rest_service_info(self,context_name,server_folder,service_name):
        
        if any(s in context_name for s in ['hosting','feature','vector']):
            service_type = 'ImageServer'
        else:
            service_type = 'FeatureServer'
            
        service_url = f"{self.CNAME_URL}/{context_name}/r/services/{server_folder}/{service_name}/{service_type}"
        try:
            response = self.rest_api_call(url=service_url, method="GET")
            return response
        except Exception as e:
            raise EnterpriseClientError(f"Error getting service info: {e}")