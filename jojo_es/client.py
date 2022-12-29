from .index import * 
from .error import * 
from .util import * 

import requests 
from typing import Optional 

__all__ = [
    'ESClient', 
]


class ESClient:
    def __init__(self,
                 host: str,
                 port: int = 9200,
                 username: str = 'elastic',
                 password: Optional[str] = None):
        self.host = host 
        self.port = port 
        
        if password:
            self.auth = (username, password)
        else:
            self.auth = None  
        
    def get_index(self,
                  index_name: str) -> ESIndex:
        return ESIndex(
            client_config = dict(
                host = self.host, 
                port = self.port, 
                auth = self.auth, 
            ), 
            index_name = index_name, 
        )

    def test_connection(self):
        resp = requests.get(
            url = f"http://{self.host}:{self.port}", 
            auth = self.auth, 
        )
        
        if resp.status_code != 200:
            resp_json = resp.json()
            raise UnknownError(resp_json)
