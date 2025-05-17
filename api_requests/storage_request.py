import azure.functions as func

from api_clients import StorageHandler
from .base_request import BaseRequest
from utils import *


class StorageRequest(BaseRequest):

    def __init__(
        self,
        req: func.HttpRequest,
        use_json: bool = True,
        command: str = None,
        params: dict = None,
    ):

        logger.debug("Initializing StorageRequest")

        self.params = params if params else {}

        super().__init__(req, use_json=use_json)

        self.default_container = DEFAULT_WORKSPACE_CONTAINER
        self.default_target_container = DEFAULT_HOSTING_CONTAINER

        self.response = self.storage_command(command=command)

    def storage_command(self, command: str = None) -> func.HttpResponse:
        logger.debug(f"Handling storage command: {command}")

        if not isinstance(self.req_json, dict):
            
            return self.return_error(f"StorageRequest Error: request json not found")

        if command == "copy":

            if not self.req_json.get("objectNameIn"):

                return self.return_error("Error: objectNameIn missing from request")
            
            object_name_in = self.req_json.get("objectNameIn", None)
            input_container = self.req_json.get("inputContainer", self.default_container)
            object_name_out = self.req_json.get("objectNameOut", object_name_in)
            output_container = self.req_json.get("outputContainer", self.default_target_container)
            wait = self.req_json.get("wait", True)
            overwrite = self.req_json.get("overwrite", False)
            
            try:
                storage = StorageHandler(workspace_container_name=input_container)
                
            except Exception as e:

                return self.return_error(
                    f"Error: could not instantiate storage handler: {e}"
                )
            try:
                logger.debug(
                    f"Copying {object_name_in} from {input_container} to {output_container} as {object_name_out}"
                )
                copy_result = storage.copy_blob(
                    
                    source_blob_name= object_name_in,
                    source_container_name= input_container,
                    dest_blob_name= object_name_out,
                    dest_container_name= output_container,
                    wait_on_status= wait,
                    overwrite= overwrite,
                )
                
                logger.debug(f"Copy result: {copy_result}")
                
            except Exception as e:
                
                return self.return_error(f"Error during copy operation: {e}")

            if isinstance(copy_result, str):

                return self.return_success(
                    message=f"{object_name_in} copied from {input_container} to {output_container} as {object_name_out}",
                    json_out={
                        "copy_result": copy_result,
                        "object_name_out": object_name_out,
                        "output_container": output_container,
                    },
                )

            elif isinstance(copy_result, dict):

                return self.return_success(
                    message=f"Copy operation started for {object_name_in} from {input_container} to {output_container} as {object_name_out}",
                    json_out={"copy_result": copy_result},
                )

            else:
                return self.return_error(f"Unknown error during copy operation")

        elif command == "list_containers":
            try:
                storage = StorageHandler()
                containers = storage.list_containers()

                return self.return_success(
                    message=f"Containers: {containers}",
                    json_out={"containers": containers},
                )

            except Exception as e:

                return self.return_error(f"Could not list containers {e}")

        elif command == "list_container_contents":
            if self.req_json.get("containerName"):
                container_name = self.req_json.get("containerName")
                logger.debug(f"Container name: {container_name}")
            else:
                logger.warning(f"Container name not found in request, using default: {self.default_container}")
                container_name = self.default_container
                
            try:
                storage = StorageHandler(workspace_container_name=container_name)
                contents = storage.list_container_blobs(container_name)

                return self.return_success(
                    message=f"Contents of {container_name}: {contents}",
                    json_out={"container_name": container_name, "contents": contents},
                )

            except Exception as e:
                return self.return_error(
                    f"Could not list contents for container {container_name}: {e}"
                )
        else:
            return self.return_error(f"Unknown storage command: {command}")
