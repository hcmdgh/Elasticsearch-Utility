import pprint 
from typing import Any 

__all__ = [
    'ESError', 
    'IndexNotExistError', 
    'UnknownError',
    'IndexAlreadyExistError', 
    'AuthenticationError', 
]


class ESError(RuntimeError):
    pass 


class UnknownError(ESError):
    def __init__(self, 
                 resp_json: dict[str, Any]):
        msg = f"Unknown ERROR:\n{pprint.pformat(resp_json)}"
                 
        super().__init__(msg) 


class IndexNotExistError(ESError):
    pass 


class IndexAlreadyExistError(ESError):
    pass 


class AuthenticationError(ESError):
    pass 
