import json
import time

from enterprise_api import EnterpriseClient
from utils import (
    DEFAULT_DATASTORE_ID,
    DEFAULT_VECTOR_CONTEXT_NAME,
    logger,
    EnterpriseClientError
    
)

class MapServer(EnterpriseClient):

    def __init__(self, 
            context_name: str = None, 
            datastore_id: str = None, 
            datastore_server_folder: str = None):
        
        super().__init__()
        
        self.datastore_id = datastore_id if datastore_id else DEFAULT_DATASTORE_ID

        self.context_name = context_name if context_name else DEFAULT_VECTOR_CONTEXT_NAME
        if datastore_server_folder:
            self.datastore_server_folder = datastore_server_folder
        else:
            self.datastore_server_folder = None
            logger.warning("No datastore server folder provided - defaulting to None")
        logger.info("MapServer class initialized")

    @classmethod
    def from_vault(
        cls, vault_name: str = None,
        credential=None, 
        context_name: str = None,
        datastore_server_folder: str = None,
        datastore_id: str = None
    ):

        instance = cls(
            context_name=context_name,
            datastore_id=datastore_id,
            datastore_server_folder=datastore_server_folder)

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

    def list_datastore_layers(self, datastore_id: str = None):

        datastore_id = datastore_id if datastore_id else DEFAULT_DATASTORE_ID
        url = f"{self.DATASTORES_URL}/allDatasets/getLayers"
        params = {"datastoreId": datastore_id}

        try:
            r = self.json_request(url, data=params, method="POST")
        except Exception as e:
            logger.error(f"Error getting layers and datasets: {e}")
            raise e

        if "layerAndDatasets" in r.keys():
            logger.info(
                f'{len(r["layerAndDatasets"])} layers and datasets found in {datastore_id}'
            )

            return r["layerAndDatasets"]
        else:
            raise EnterpriseClientError(f"Error getting layers and datasets: {r}")

    def get_layer_info(
        self,
        layer_title: str = None,
        layer_id: str = None,
        dataset_name: str = None,
        datastore_id: str = None,
        service_type: str = None,
    ):

        results = []

        if (layer_title or layer_id) and dataset_name:
            raise ValueError("dataset_name parameter must be used alone")

        elif dataset_name:
            logger.debug(f"Searching for layers with dataset: {dataset_name}")

        elif layer_title or layer_id:
            logger.debug(
                f"Searching for layers with title: {layer_title} or id: {layer_id}"
            )

        else:
            raise ValueError(
                "At least one of layer_title, layer_id, or dataset_name must be provided"
            )

        try:
            r = self.list_datastore_layers(datastore_id=datastore_id)
        except Exception as e:
            logger.error(e)
            raise e

        for l in r:
            if layer_title and l["layer"]["title"] == layer_title:

                if layer_id:
                    if l["layer"]["id"] == layer_id:
                        results.append(l)
                else:
                    results.append(l)

            if dataset_name and l["dataset"]["name"] == dataset_name:
                results.append(l)

        if len(results) == 0:
            raise ValueError("No results found")

        elif isinstance(service_type, str):

            services = [l for l in results if l["layer"]["type"] == service_type]

            if len(services) == 0:
                raise ValueError(f"No {service_type} services found")

            elif len(services) > 1:
                raise EnterpriseClientError(
                    f"Multiple {service_type} services found {services}"
                )

            elif len(services) == 1:
                logger.info(f"{service_type} service found: {services[0]}")

                return services[0]

        else:
            logger.info(f"{len(results)} results found")

            return results

    def synchronize_datastore_layers(
        self, datastore_id: str = None, sync_metadata=True
    ):

        datastore_id = datastore_id if datastore_id else DEFAULT_DATASTORE_ID

        url = f"{self.PORTAL_URL}/sharing/rest/portals/{self.PORTAL_ID}/datastores/allDatasets/publishLayers"

        data = {"datastoreId": datastore_id, "syncItemInfo": json.dumps(sync_metadata)}

        try:
            logger.debug(f"Synchronizing layers in datastore: {datastore_id}")
            r = self.json_request(url, data=data, method="POST")
        except Exception as e:
            logger.error(f"Error synchronizing layers: {e}")
            raise e

        if 'success' in r:
            logger.debug(f'Synchronization success: {r["success"]}')
            if r['success'] or r['success'] == 'True':
                logger.info(f"Synchronization call executed {r}")
                return r
        
        if "status" in r.keys():
            logger.debug(f'Synchronization status: {r["status"]}')
                
            if r["status"].lower() in ["completed", "succeeded", "partial", "processing"]:
                logger.info(f"Synchronization call executed {r}")
                
                return r
            else:
                raise EnterpriseClientError(f"Synchronization failed: {r}")
        else:   
            raise EnterpriseClientError(f"Invalid response from server: {r}")

    def check_datastore_status(self, datastore_id: str = None, user: str = None):

        datastore_id = datastore_id if datastore_id else DEFAULT_DATASTORE_ID
        user = user if user else self.portal_username

        url = f"{self.PORTAL_URL}/sharing/rest/content/users/{user}/items/{datastore_id}/status"

        try:
            logger.debug(f"Checking datastore status: {url}")
            r = self.json_request(url, method="GET")
        except Exception as e:
            logger.error(f"Error checking datastore status: {e}")
            raise e

        if "status" in r.keys():
            logger.info(f'Datastore status: {r["status"]}')
            return r
        else:
            raise EnterpriseClientError(f"Error checking datastore status: {r}")

    def enable_wfs(
        self,
        service_name: str = None,
        server_folder: str = None,
        context_name: str = None,
    ):

        if not service_name:
            raise ValueError("Service name is required to enable WCS")

        server_folder = server_folder if server_folder else self.datastore_server_folder

        context_name = context_name if context_name else self.context_name

        logger.debug(
            f"Enabling WFS for {service_name} in {server_folder} on /{context_name}"
        )
        logger.debug("Setting sharing to public")

        try:
            self.set_sharing(
                context_name=context_name,
                service_name=service_name,
                server_folder=server_folder,
                sharing="public",
            )
        except Exception as e:
            logger.error(f"Error setting sharing: {e}")
            raise e
        logger.info("Sharing set to public")

        edit_url = f"{self.CNAME_URL}/{context_name}/admin/services/{server_folder}/{service_name}.MapServer/edit"
        wfs_url = f"{self.CNAME_URL}/{context_name}/services/{server_folder}/{service_name}/MapServer/WFSServer"

        try:
            service_json = self.get_service_json(
                context_name=context_name,
                service_name=service_name,
                server_folder=server_folder,
            )
            # logger.debug(f'Service JSON: {service_json}')
            if "extensions" not in service_json.keys():
                raise EnterpriseClientError(f"Invalid service JSON: {service_json}")
        except Exception as e:
            logger.error(f"Error getting service JSON: {e}")
            raise e

        extensions = [
            i for i in service_json["extensions"] if i["typeName"] != "WFSServer"
        ]  # remove any existing WFS extension

        wfs = (
            self._build_wfs_extension(  # builds dict to be passed as JSON to enable WFS
                server_folder=server_folder,
                service_name=service_name,
                context_name=context_name,
            )
        )

        extensions.append(wfs)

        service_json["extensions"] = extensions

        payload = {"service": json.dumps(service_json), "runAsync": True}

        try:
            response = self.json_request(url=edit_url, data=payload, method="POST")

        except Exception as e:
            logger.error(f"Error enabling WFS using POST: {e}")
            raise e

        logger.debug(f"WFS POST succesfull: {response}")

        if "status" in response.keys() and response["status"] == "success":

            job_id = response["jobid"]
            logger.info(f"WFS async job ID: {job_id}")

        else:
            raise EnterpriseClientError(
                f"Error tasking async job to enable WFS: {response}"
            )

        job_url = f"{self.CNAME_URL}/{context_name}/admin/system/jobs/{job_id}"

        executing = True
        while executing:
            try:
                response = self.json_request(  # IMAGERY_JOBS_URL = f'{IMAGERY_URL}/admin/system/jobs'
                    url=job_url, method="GET"
                )
                status = response["status"].lower()

            except Exception as e:
                logger.error(f"Error enabling WFS using POST: {e}")
                raise e

            logger.debug(f'Status: {response["status"]}')

            if status.lower() in ["completed", "succeeded"]:

                if (
                    "operationResponse" in response.keys()
                    and "status" in response["operationResponse"].keys()
                    and response["operationResponse"]["status"] == "error"
                ):

                    raise EnterpriseClientError(f"Error enabling WFS: {response}")

                logger.info(f"WFS enabled: {response}")

                return f"{wfs_url}?SERVICE=WFS&REQUEST=GetCapabilities"

            elif status.lower() in ["failed", "error"]:

                raise EnterpriseClientError(f"Error enabling WFS: {response}")

            time.sleep(5)

        logger.info(f"Asynch WFS job submitted, job status URL: {job_url}")

        return job_url

    def _build_wfs_extension(
        self,
        service_name: str = None,
        server_folder: str = None,
        context_name: str = None,
    ):

        wfs_url = f"{self.CNAME_URL}/{context_name}/services/{server_folder}/{service_name}/MapServer/WFSServer"
        name = f"{server_folder}_{service_name}_WFSServer"
        wfs = {
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
                "axisOrderWFS20": "LongLat",
            },
            "enabled": True,
        }

        return wfs


