import azure.functions as func
from json import dumps

#from api_requests import *
from utils import logger

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route='test_pulse', methods=['GET', 'POST'])
def test_pulse(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Azure Function recieved a test request')
    response_body = dumps({'message' :'Pulse test successful'})
    return func.HttpResponse(body=response_body,status_code=200)



