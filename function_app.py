import azure.functions as func

from api_requests import BaseRequest
from utils import logger

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


@app.route(route='test_base', methods=['GET', 'POST'])

def test_base(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Azure Function recieved a test_base request')
    
    B = BaseRequest(req)
    if B.response:
        return B.response
    else:
        return B.return_success('BaseRequest initialized',return_log=True)
    
