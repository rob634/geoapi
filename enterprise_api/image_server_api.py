import json
import os
import time

from enterprise_api import EnterpriseClient
from utils import (
    HOSTED_IMAGERY_SERVER_FOLDER,
    DEFAULT_IMAGERY_CONTEXT_NAME,
    logger,
    DEFAULT_CLOUDSTORE_PATH,
    EnterpriseClientError,
    DEFAULT_CLOUDSTORE_ID,
    DEFAULT_EPSG_CODE
)

# time series in mosaic dataset
# consuming 

class ImageServer(EnterpriseClient):

    def __init__(
        self, 
        context_name: str = None, 
        server_folder: str = None,
        cloudstore_name: str = None,
        cloudstore_id: str = None,
        ):
        
        super().__init__()
        self.server_folder = (
            server_folder if server_folder else HOSTED_IMAGERY_SERVER_FOLDER
        )
        self.context_name = (
            context_name if context_name else DEFAULT_IMAGERY_CONTEXT_NAME
        )
        
        self.cloudstore_id = cloudstore_id if cloudstore_id else DEFAULT_CLOUDSTORE_ID
        self.cloudstore_contents = list()
        self.cloudstore_dict = dict()
        logger.info("ImageServer class initialized")
    
    # List contents of cloud datastore -> list of filepaths
    def list_cloudstore_contents(
        self, 
        cloudstore_name: str = None, 
        cloudstore_id: str = None,
        filter: str = None, 
        ext: str = "tif"
    ):
        task_name = 'ListDatastoreContent'
        cloudstore_id = cloudstore_id if cloudstore_id else DEFAULT_CLOUDSTORE_ID
        cloudstore_name = self.datastore_path_from_id(datastore_id=cloudstore_id)

        list_params = {"dataStoreName": cloudstore_name}
        if isinstance(filter, str):
            list_params["filter"] = filter

        try:
            logger.debug(
                f"Submitting {task_name} job for {cloudstore_name} with params {list_params}"
            )

            response_json = self.gp_execute(
                context_name=self.context_name,
                server_folder_name="System",
                datastore_id=cloudstore_id,
                gp_service_name="RasterAnalysisTools",
                task_name=task_name,
                results_path="contentList",
                payload=list_params,
            )
            
        except Exception as e:
            logger.error(f"Error submitting {task_name} job: {e}")
            raise e

        if "value" in response_json:
            if isinstance(response_json["value"], dict):
                logger.info(f"Cloudstore contents: {response_json}")

                return response_json["value"]["contentList"][cloudstore_name]

            else:
                raise EnterpriseClientError(
                    f"Error getting cloudstore content is empty with parameters {list_params}: {response_json}"
                )
        else:
            raise EnterpriseClientError(
                f"Error getting cloudstore contents: value missing: {response_json}")

    # Create service into which to add raster collection -> json with itemId and serviceUrl
    def create_raster_collection_service(
        self,
        service_name: str = None,
        desc: str = None,
    ):

        url = f"{self.PORTAL_URL}/sharing/rest/content/users/{self.portal_username}/createService"

        payload = {
            "outputType": "imageService",
            "title": service_name,
            "tags": "Image Service",
            "description" : desc,
            "createParameters":
                {
                    "name": service_name,
                    "capabilities": "Image, Catalog, Mensuration, Metadata",
                    "cacheControlMaxAge": "43200",
                    "provider": "ArcObjectsRasterRendering",
                    "properties": {
                        "isManaged": True,
                        "isCached": False,
                        "esriImageServiceSourceType": "esriImageServiceSourceTypeMosaicDataset",
                        "isTiledImagery": False,
                        "colormapToRGB": False,
                        "description": desc,
                        "defaultResamplingMethod": 1,
                    },
                    "copyData": False,
                }
            }

        try:
            logger.debug(f"Creating image collection: {service_name}")
            response = self.rest_api_call(url=url, params=payload, method="POST")
            logger.debug(f"Response: {response}")
        except Exception as e:
            error_message = f"Error creating image collection when calling {url}: {e}"
            logger.error(error_message)
            raise EnterpriseClientError(error_message)

        if "success" in response:
            if response["success"]:
                if "itemId" in response and "serviceurl" in response:
                    item_id = response["itemId"]
                    service_url = response["serviceurl"]
                    logger.info(f"Image collection created: {item_id} {service_url}")
                    
                    return {"item_id": item_id, "service_url": service_url}

                else:
                    error_message = (
                        f"Error creating image collection: itemID not found in response {response}"
                    )
                    logger.error(error_message)
                    
                    raise EnterpriseClientError(error_message)
            else:
                error_message = f"Error creating image collection: {response}"
                logger.error(error_message)
                
                raise EnterpriseClientError(error_message)

        else:
            error_message = f"Error creating image collection: Invalid response from server: {response}"
            logger.error(error_message)
            raise EnterpriseClientError(error_message)

    # Publish individual raster -> image service URL
    def publish_raster(
        self,
        raster_name: str,
        service_name: str = None,
        desc: str = None,
        cloudstore_name: str = None,
        cloudstore_id: str = None,
    ):
        logger.debug(f"Publishing individual raster: {raster_name}")
        service_name = service_name if service_name else raster_name.split(".")[0]
        cloudstore_id = cloudstore_id if cloudstore_id else DEFAULT_CLOUDSTORE_ID
        desc = desc if desc else f"{service_name} hosted on DDH GeoDev"
        
        if not self.cloudstore_dict:
            self._get_cloudstore_contents()

        if raster_name in self.cloudstore_dict:
            raster_path = self.cloudstore_dict[raster_name]
            logger.debug(f"Raster path: {raster_path}")
        else:
            error_message = f"Raster {raster_name} not found in cloudstore: {list(self.cloudstore_dict.keys())}"
            logger.error(error_message)
            
            raise ValueError(error_message)

        payload = {
            "inputServices":
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
                                    "isTiledImagery": False,
                                    "colormapToRGB": False,
                                    "description": desc,
                                    "defaultResamplingMethod": 1,
                                },
                            },
                            "itemProperties": {"folderId": ""},
                        }
                    ]
                }
        }
        # Submit job
        gp_service_name = "RasterAnalysisTools"
        task_name = "BatchPublishRaster"
        results_path = "outputServices"
        try:
            logger.debug(f"Submitting {task_name} job for {service_name} with params {payload}")
            results_json = self.gp_execute(
                context_name= self.context_name,
                server_folder_name = 'System',
                datastore_id = cloudstore_id,
                gp_service_name = gp_service_name,
                task_name = task_name,
                results_path = results_path,
                payload= payload,)
            logger.debug(f"Response: {results_json}")
        except Exception as e:
            error_message = f"Error submitting {task_name} job: {e}"
            logger.error(error_message)
            
            raise EnterpriseClientError(error_message)

        # Get URL from result json
        if isinstance(results_json, list) and results_json:
            results_json = results_json[0]
            logger.debug(f"Results from list: {results_json}")

        if isinstance(results_json, dict) and "value" in results_json:

            if isinstance(results_json["value"], list) and results_json["value"]:

                image_service_url = results_json["value"][0]
                logger.info(f"Image Service URL: {image_service_url}")
                
                return image_service_url

            elif isinstance(results_json["value"], dict
                            ) and "url" in results_json["value"]:
                
                image_service_url = results_json["value"]["url"]
                logger.info(f"Image Service URL: {image_service_url}")
                
                return image_service_url
                
            else:
                raise EnterpriseClientError(
                    f"No image service results found: {results_json}"
                )
        else:
            raise EnterpriseClientError(
                f"Error getting image service URL: {results_json}"
            )

    # Publish raster collection -> image service URL
    def publish_raster_collection(
        self,
        raster_names: list = None,
        service_name: str = None,
        desc: str = None,
        cloudstore_name: str = None,
        cloudstore_id: str = None,):

        logger.debug(f"Publishing raster collection: {raster_names}")
        try:
            logger.debug(
                f"Creating image collection service: {service_name} in {cloudstore_id}"
            )
            new_service_json = self.create_raster_collection_service(
                service_name=service_name,
                desc=desc
            )
            item_id = new_service_json["item_id"]
            service_url = new_service_json["service_url"]
            logger.info(f"Image collection service URL: {service_url}")
            
        except Exception as e:
            error_message = (
                f"Error creating image collection for {service_name}: {e}"
            )
            logger.error(error_message)
            raise EnterpriseClientError(error_message)
        
        if not self.cloudstore_dict:
            self._get_cloudstore_contents()
            
        raster_uris = []
        for _name in raster_names:
            if _name in self.cloudstore_dict:
                logger.debug(f"Raster path: {self.cloudstore_dict[_name]}")
                raster_uris.append(self.cloudstore_dict[_name])
            else:
                error_message = f"Raster {_name} not found in cloudstore: {list(self.cloudstore_dict.keys())}"
                logger.error(error_message)
                
                raise ValueError(error_message)
            
        if len(raster_uris) == 0:
            error_message = (
                f"Could not find rasters <{raster_names}> in cloudstore: {list(self.cloudstore_dict.keys())}"
            )
            logger.error(error_message)
            raise ValueError(error_message)
        
        logger.debug(f"Raster URIs: {raster_uris}")
  
        payload = {
            "inputRasters":
                {
                    "uris": raster_uris, 
                    "byref": True
                    },
            "rasterType":
                {
                    "rasterTypeName": "Raster Dataset",
                    "rasterTypeParameters": None,
                },
            "imageCollection":
                {"itemId": item_id},
            "context": 
                {
                    "outSR": {"wkid": DEFAULT_EPSG_CODE},
                    "buildFootprints": False,
                    "buildOverview": True,
                },
            }
        
        # Submit job
        gp_service_name = "RasterAnalysisTools"
        task_name = "CreateImageCollection"         
        results_path = "result"
        try:
            logger.debug(f"Submitting {task_name} job for {service_name} with params {payload}")
            results_json = self.gp_execute(
                context_name= self.context_name,
                server_folder_name = 'System',
                datastore_id = cloudstore_id,
                gp_service_name = gp_service_name,
                task_name = task_name,
                results_path = results_path,
                payload= payload,)
            logger.debug(f"Response: {results_json}")
        except Exception as e:
            error_message = f"Error submitting {task_name} job: {e}"
            logger.error(error_message)
            
            raise EnterpriseClientError(error_message)

        if 'value' in results_json and 'url' in results_json['value'] and 'itemId' in results_json['value']:
            logger.info(f"Image Service: {results_json['value']}")
            
            return results_json['value']['url']
            
        else:
            error_message = f"Invalid response from server: {results_json}"
            logger.error(error_message)

            raise EnterpriseClientError(error_message)
            
    def enable_wcs(
        self,
        service_name: str = None,
        server_folder: str = None,
        context_name: str = None,
    ):

        if not service_name:
            raise ValueError("Service name is required to enable WCS")

        server_folder = server_folder if server_folder else self.server_folder

        context_name = context_name if context_name else self.context_name

        logger.debug(
            f"Enabling WCS for {service_name} in {server_folder} on {context_name}"
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

        edit_url = f"{self.CNAME_URL}/{context_name}/admin/services/{server_folder}/{service_name}.ImageServer/edit"

        try:
            service_json = self.get_service_json(
                context_name=context_name,
                service_name=service_name,
                server_folder=server_folder,
            )
        except Exception as e:
            logger.error(f"Error getting service JSON: {e}")
            raise e

        wcs_url = f"{self.CNAME_URL}/{context_name}/services/{server_folder}/{service_name}/ImageServer/WCSServer"

        wcs = {
            "typeName": "WCSServer",
            "capabilities": None,
            "properties": {
                "name": f"{server_folder}_{service_name}",
                "role": "",
                "title": "WCS",
                "defaultVersion": "",
                "abstract": "",
                "keywords": "",
                "fees": "",
                "accessConstraints": "None",
                "responsiblePerson": "",
                "responsiblePosition": "",
                "onlineResource": wcs_url,
                "providerName": "",
                "phone": "",
                "fax": "",
                "contactInstructions": "",
                "email": "",
                "address": "",
                "city": "",
                "province": "",
                "zipcode": "",
                "providerWebsite": "",
                "serviceHour": "",
                "country": "",
                "customGetCapabilities": False,
                "pathToCustomGetCapabilitiesFiles": "",
                "maxImageHeight": "",
                "maxImageWidth": "",
                "SupportEOProfile": False,
            },
            "enabled": True,
        }
        service_json["extensions"].append(wcs)
        payload = {"service": service_json, "runAsync": True}

        try:
            logger.debug(f"Enabling WCS for {service_name} with params {payload}")
            response_json = self.rest_api_call(url=edit_url, params=payload, method="POST")

        except Exception as e:
            logger.error(f"Error enabling WCS using POST: {e}")
            raise e

        logger.debug(f"WCS POST succesfull: {response_json}")

        if isinstance(response_json,dict) and "status" in response_json and response_json["status"] == "success":

            job_id = response_json["jobid"]
            logger.info(f"WCS async job ID: {job_id}")

        else:
            raise EnterpriseClientError(
                f"Error tasking async job to enable WCS: {response_json}"
            )

        #try:
        #    self.check_job(job_url= f"{self.CNAME_URL}/{context_name}/admin/system/jobs/{job_id}")
        #except Exception as e:
        #    logger.error(f"Error checking job status: {e}")
        #    raise e
        
        executing = True
        while executing:
            try:
                response = self.rest_api_call(
                    url= f'{self.CNAME_URL}/{context_name}/admin/system/jobs/{job_id}'
                )
                status = response["status"].lower()

            except Exception as e:
                logger.error(f"Error enabling WCS using POST: {e}")
                raise e

            logger.debug(f'Status: {response["status"]}')

            if status == "completed":

                logger.info(f"WCS enabled: {response}")

                return f"{wcs_url}?SERVICE=WCS&REQUEST=GetCapabilities"

            elif status in ["failed", "error"]:

                raise EnterpriseClientError(f"Error enabling WCS: {response}")

            time.sleep(5)
            
    def _get_cloudstore_contents(self):

        try:
            self.cloudstore_contents = self.list_cloudstore_contents(cloudstore_id=self.cloudstore_id)
            self.cloudstore_dict = dict((os.path.basename(i), i) for i in self.cloudstore_contents if os.path.basename(i))
        except Exception as e:
            logger.error(f"Error getting cloudstore contents: {e}")
            raise e
        
        return 

    def get_thumbnail(
        self,
        service_name: str = None,
        server_folder: str = None,
        context_name: str = None,
    ):
        if not service_name:
            raise ValueError("Service name is required to get thumbnail")
        fullExtent= {
            "xmin": -61.94400787353515,
            "ymin": 16.97668009136554,
            "xmax": -61.611327723113476,
            "ymax": 17.21229954797059,
            "spatialReference": {
            "wkid": DEFAULT_EPSG_CODE,
            "latestWkid": DEFAULT_EPSG_CODE
            }
        }
        server_folder = server_folder if server_folder else self.server_folder
        context_name = context_name if context_name else self.context_name

        logger.debug(
            f"Getting thumbnail for {service_name} in {server_folder} on {context_name}"
        )

        try:
            service_json = self.get_service_json(
                context_name=context_name,
                service_name=service_name,
                server_folder=server_folder,
            )
            logger.debug(f"Service JSON: {service_json}")
            
            return service_json["thumbnail"]
            
        except Exception as e:
            logger.error(f"Error getting thumbnail: {e}")
            raise e
        

    @classmethod
    def from_params(
        cls,
        portal_username: str = None,
        portal_credential: str = None,
        server_folder: str = None,
        context_name: str = None,
        cloudstore_id: str = None,
    ):
        instance = cls(
            context_name=context_name,
            server_folder=server_folder,
            cloudstore_id=cloudstore_id,)
        instance.portal_username = portal_username
        instance.portal_credential = portal_credential
        
        try:
            instance._portal_token()
            logger.debug(f"Token test passed")
            logger.info(
                f"EnterpriseClient class initialized with portal: {instance.PORTAL_URL}"
            )
            
        except Exception as e:
            raise EnterpriseClientError(
                f"Could not get token upon class instantiation: {e}"
            )
        try:
            instance._get_cloudstore_contents()
            logger.debug(f"Cloudstore contents: {instance.cloudstore_contents}")
            
            return instance
        
        except Exception as e:
            raise EnterpriseClientError(
                f"Could not get cloudstore contents upon class instantiation: {e}"
            )

        
    @classmethod
    def from_vault(
        cls,
        vault_name: str = None,
        context_name: str = None,
        server_folder: str = None,
        secret_name: str = None,
        cloudstore_id: str = None,
    ):

        instance = cls(
            context_name=context_name,
            server_folder=server_folder,
            cloudstore_id=cloudstore_id,)

        try:
            instance.portal_credential = instance._credential_from_vault(
                vault_name=vault_name, secret_name=secret_name
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

        try:
            instance._get_cloudstore_contents()
            logger.debug(f"Cloudstore contents: {instance.cloudstore_contents}")
            
            return instance
        
        except Exception as e:
            raise EnterpriseClientError(
                f"Could not get cloudstore contents upon class instantiation: {e}"
            )