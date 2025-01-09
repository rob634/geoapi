import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueServiceClient
#from api_requests import BaseRequest
from utils import *


def json_from_request(req:func.HttpRequest):

    try:
        content_type = req.headers.get('Content-Type')
        logger.debug(f'Content-Type: {content_type}')
        if not content_type:
            raise Exception('No Content-Type found in request headers')
    except Exception as e:
        logger.error(f'Could not get content type from request headers {e}')
        raise e

    if 'application/json' in content_type:
        logger.debug('application/json found, parsing JSON')
        try:
            req_json = req.get_json()
            if req_json:
                logger.debug(f'JSON: {req_json}')
                
                return req_json
            
            else:
                raise Exception('No JSON found in request with application/json content type')
        except Exception as e:
            logger.error(f'Error: could not get JSON from request: {e}')
            raise e
    else:
        message_out = 'Content-Type must be application/json'
        logger.error(message_out)
        raise Exception(message_out)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.queue_trigger(
    arg_name="azqueue",
    queue_name="rmhazureqstorage",
    connection="AzureWebJobsStorage") 

def queue_trigger1(azqueue: func.QueueMessage):
    logger.info('Python Queue trigger processed a message: %s',azqueue.get_body().decode('utf-8'))

@app.route(route='test_pulse', methods=['GET', 'POST'])
def test_pulse(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Azure Function recieved a test_pulse request')
    message = 'Pulse test successful'
    
    return func.HttpResponse(body=message,status_code=200)


@app.route(route='test_q', methods=['GET', 'POST'])
def test_q(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Azure Function recieved a test_q request')
       
    try:
        req_json = json_from_request(req)
        message = req_json.get('message','EMPTY MESSAGE')
        logger.info(f'Message: {message}')
    except Exception as e:
        
        return func.HttpResponse(body=f'Error: {e}',status_code=500)
    
    try:
        queue_service_client = QueueServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.queue.core.windows.net",
            credential=DefaultAzureCredential()
        )        
    except Exception as e:
        
        return func.HttpResponse(body=f'Error: {e}',status_code=500)

    try:
        q_client = queue_service_client.get_queue_client(STORAGE_QUEUE_NAME)
        q_result = q_client.send_message(message)
        
        return func.HttpResponse(body=q_result,status_code=200)
    
    except Exception as e:
        
        return func.HttpResponse(body=f'Error: {e}',status_code=500)
    

