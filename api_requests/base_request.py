import azure.functions as func
from datetime import datetime
import json
from utils import *


class BaseRequest:
    def __init__(self, req:func.HttpRequest, use_json:bool=True):

        logger.debug('Initializing BaseRequest')
        self.response = None
        self._log_init()
        self._req_init(req)

        self.log_obj['request']['content_type'] = self.content_type

        if use_json:
            self._json_init()

        logger.info('BaseRequest initialized')
    
    def _req_init(self,req:func.HttpRequest) -> None:
        
        self.req = req
        try:
            self.content_type = self.req.headers.get('Content-Type')
            logger.debug(f'Content-Type: {self.content_type}')
        except Exception as e:
            message_out = f'Could not get content type from request headers {e}'
            self.response = self.return_error(message_out)
        return
    
    def _log_init(self) -> None:
        logger.debug('Initializing log object')
        self.log_obj = {
            'init_time':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'request':{},
            'logs':{}
                        }
        self.log_index = 0
        return
    
    def _json_init(self) -> None:
        
        if self.content_type and 'application/json' in self.content_type:
            logger.debug('application/json found, parsing JSON')
            try:
                self.json = self.req.get_json()
                logger.debug(f'JSON: {self.json}')
                self.log_obj['request']['json'] = self.json
            except Exception as e:
                message_out = f'Error: could not get JSON from request {e}'
                self.response = self.return_error(message_out)
        else:
            message_out = 'Content-Type must be application/json'
            self.response = self.return_error(message_out)
            
        return
                
    def return_error(self,
                    message:str=None,
                    code:int=400,
                    json_out:dict=None,
                    headers:dict=None,
                    return_log:bool=False,
                    ) -> func.HttpResponse:

        json_response = {}
        message = message if message else 'General Error'
        logger.error(f'Returning error! - {message} \n code: {code}')
        #Append request class json and error response to json_response
        if isinstance(self.json,dict):
            json_response['request_class'] = self.json
        if isinstance(json_out,dict):
            json_response['error_response'] = json_out
        logger.error(self.json)

        return self.respond(code=code, return_log=return_log, json_out=json_out, message=message, headers=headers)

    def return_success(self,
                       message:str=None,
                       code:int=200,
                       json_out:dict=None,
                       headers:dict=None,
                       return_log:bool=False,
                       ) -> func.HttpResponse:
    
        message = message if message else 'Success'
        
        logger.debug(f'Returning success - message: {message} \n code: {code}')

        return self.respond(code=code, return_log=return_log, json_out=json_out, message=message, headers=headers)
     
    def respond(
        self,
        code:int,
        json_out:dict=None,
        message:str=None,
        headers:dict=None,
        return_log:bool=False
        ) -> func.HttpResponse:

        if isinstance(headers,dict):
            headers['Content-Type'] = 'application/json'
        else:
            headers = {'Content-Type':'application/json'}
        
        json_body = {}
        if isinstance(json_out,dict):
            json_body['response'] = json_out
        if isinstance(message,str):
            json_body['message'] = message
        if return_log:
            json_body['log'] = self.log_obj

        return func.HttpResponse(body=json.dumps(json_body),status_code=code,headers=headers)
    
    
    def log_dict(self,obj) -> None:

        if type(obj) in [int,float,str,list,tuple,dict]:
            now = datetime.now()
            logger.debug(f'Request Class logging: {obj}')
            self.log_obj['logs'][self.log_index] = {
                'timestamp':now.strftime('%Y-%m-%d %H:%M:%S'),
                'content':obj}
            self.log_index += 1
        else:
            logger.error(f'Error: invalid object type for logging: {type(obj)}')
