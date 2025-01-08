import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.queue import QueueServiceClient
from api_requests import BaseRequest
from utils import *

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


#@app.queue_trigger(arg_name="azqueue",
#                   queue_name="rmhazureqstorage",
#                   connection="rmhazureqstrorage_STORAGE")

#def geoapi(azqueue: func.QueueMessage):
#    logger.info('Python Queue trigger processed a message: %s',
#                azqueue.get_body().decode('utf-8'))


@app.route(route='test_pulse', methods=['GET', 'POST'])

def test_pulse(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Azure Function recieved a test_pulse request')
    message = 'Pulse test successful'
    return func.HttpResponse(body=message,status_code=200)


@app.route(route='test_q', methods=['GET', 'POST'])

def test_q(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Azure Function recieved a test_q request')
    
    B = BaseRequest(req)
    if B.response:
        return B.response
    
    try:
        message = B.json.get('message','EMPTY MESSAGE')
        logger.info(message)
    except Exception as e:
        return B.return_error(f'Error: {e}')
    
    try:
        queue_service_client = QueueServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.queue.core.windows.net",
            credential=DefaultAzureCredential()
        )

        q_client = queue_service_client.get_queue_client(STORAGE_QUEUE_NAME)
        q_client.send_message(message)
    except Exception as e:
        return B.return_error(f'Error: {e}')
    
    return B.return_success(f'Message "{message}" sent to Queue',return_log=True)

  
