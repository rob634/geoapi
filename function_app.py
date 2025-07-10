import azure.functions as func
from json import dumps

from api_requests import *
from utils import logger

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route='test_pulse', methods=['GET', 'POST'])
def test_pulse(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Azure Function recieved a test request')
    response_body = dumps({'message' :'Pulse test successful'})
    return func.HttpResponse(body=response_body,status_code=200)

###Storage functions

@app.route(route='upload', methods=['GET', 'POST'])
def upload(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Upload function recieved a request')
    handler = UploadRequest(req)
    return handler.response

@app.route(route='copy', methods=['GET', 'POST'])
def copy(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Copy function recieved a request')
    handler = StorageRequest(req,command='copy')
    return handler.response

@app.route(route='list_containers', methods=['GET', 'POST'])
def list_containers(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('List containers function recieved a request')
    handler = StorageRequest(req,command='list_containers')
    return handler.response

@app.route(route='list_container_contents', methods=['GET', 'POST'])
def list_contents(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('List container contents function recieved a request')
    handler = StorageRequest(req,command='list_container_contents')
    return handler.response

###Raster functions
@app.route(route='stage_raster', methods=['GET', 'POST'])
def stage_raster(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Stage raster function recieved a request')
    handler = RasterRequest(req,'stage')
    return handler.response

@app.route(route='publish_raster', methods=['GET', 'POST'])
def publish_raster(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Publish raster function recieved a request')
    handler = EnterpriseRequest(req,'publish_raster')
    return handler.response

@app.route(route='publish_raster_collection', methods=['GET', 'POST'])
def publish_raster_collection(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Publish raster collection function recieved a request')
    handler = EnterpriseRequest(req,'publish_raster_collection')
    return handler.response

###Vector functions
@app.route(route='stage_vector', methods=['GET', 'POST'])
def stage_vector(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Stage vector function recieved a request')
    handler = VectorRequest(req,'stage')
    return handler.response

@app.route(route='list_tables', methods=['GET', 'POST'])
def list_tables(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('List tables function recieved a request')
    handler = VectorRequest(req,command='list_tables')
    return handler.response

@app.route(route='register_table', methods=['GET', 'POST'])
def register_table(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Register table function recieved a request')
    handler = EnterpriseRequest(req,command='register_table')
    return handler.response

@app.route(route='publish_vectors', methods=['GET', 'POST'])
def publish_vectors(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Publish vectors function recieved a request')
    handler = EnterpriseRequest(req,command='publish_vectors')
    return handler.response

@app.route(route='query_datastore_status', methods=['GET', 'POST'])
def query_datastore_status(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Query datastore status function recieved a request')
    handler = EnterpriseRequest(req,command='query_datastore_status')
    return handler.response

@app.route(route='enable_wfs', methods=['GET', 'POST'])
def enable_wfs(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Enable WFS function recieved a request')
    handler = EnterpriseRequest(req,command='enable_wfs')
    return handler.response

@app.route(route='share_all', methods=['GET', 'POST'])
def share_all(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Share all function recieved a request')
    handler = EnterpriseRequest(req,command='share_all')
    return handler.response

@app.route(route='list_services', methods=['GET', 'POST'])
def list_services(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('List services function recieved a request')
    handler = EnterpriseRequest(req,command='list_services')
    return handler.response

