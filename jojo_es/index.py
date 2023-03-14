from .error import * 
from .es_type import * 
from .util import * 

import requests 
from pprint import pprint 
import json 
from tqdm import tqdm 
from typing import Optional, Any 
from collections.abc import Iterable 

__all__ = [
    'ESIndex', 
]


class ESIndex:
    def __init__(self,
                 host: str, 
                 port: int,
                 auth: Optional[tuple], 
                 index_name: str,
                 type_name: str = '_doc') -> None:
        self.host = host  
        self.port = port 
        self.auth = auth 
        self.index_name = index_name 
        self.type_name = type_name 

    def count(self) -> int:
        resp = requests.get(
            url = f"http://{self.host}:{self.port}/{self.index_name}/_count", 
            auth = self.auth, 
        )
        resp_json = resp.json() 
        
        if 'count' in resp_json:
            return int(resp_json['count'])
        elif explore_dict(resp_json, 'error/type') == 'index_not_found_exception':
            raise IndexNotExistError
        else:
            raise UnknownError(resp_json)

    def create_mapping(self,
                       mapping: dict[str, int],
                       dynamic: bool = False):
        properties = dict() 
        
        for k, v in mapping.items():
            if v == KEYWORD:
                properties[k] = { 
                    'type': 'keyword',
                    'ignore_above': 512, 
                }
            elif v == TEXT:
                properties[k] = { 'type': 'text' }
            elif v == LONG:
                properties[k] = { 'type': 'long' }
            elif v == INTEGER:
                properties[k] = { 'type': 'integer' }
            elif v == DOUBLE:
                properties[k] = { 'type': 'double' }
            elif v == KEYWORD_TEXT:
                properties[k] = { 
                    'type': 'keyword',
                    'ignore_above': 512, 
                    'fields': {
                        'text': {
                            'type': 'text', 
                        },
                    },
                }
            elif v == BOOLEAN:
                properties[k] = { 'type': 'boolean' }
            elif v == DENSE_VECTOR_768:
                properties[k] = { 
                    'type': 'dense_vector', 
                    'dims': 768,
                    'index': True, 
                    'similarity': 'l2_norm',
                }
            else:
                raise TypeError

        resp = requests.put(
            url = f"http://{self.host}:{self.port}/{self.index_name}",
            json = {
                'mappings': {
                    'properties': properties, 
                    'dynamic': 'true' if dynamic else 'strict', 
                },
            },
            auth = self.auth, 
        )
        resp_json = resp.json() 
        
        if resp_json.get('acknowledged') == True:
            pass 
        elif 'already exists' in explore_dict(resp_json, path='error/reason', default=''):
            raise IndexAlreadyExistError
        else:
            raise UnknownError(resp_json)
    
    def delete_index(self) -> bool:
        resp = requests.delete(
            url = f"http://{self.host}:{self.port}/{self.index_name}",
            auth = self.auth, 
        )           
        resp_json = resp.json()
        
        if resp_json.get('acknowledged') == True:
            return True 
        elif 'no such index' in explore_dict(resp_json, path='error/reason', default=''):
            return False 
        else:
            raise UnknownError(resp_json)
    
    def insert(self,
               document: dict[str, Any]) -> str:
        if '_id' in document:
            _id = str(document.pop('_id'))
            
            resp = requests.put(
                url = f"http://{self.host}:{self.port}/{self.index_name}/{self.type_name}/{_id}",
                json = document, 
                auth = self.auth, 
            )           
            resp_json = resp.json()
            
            if resp_json.get('result') == 'created':
                return _id  
            else:
                raise UnknownError(resp_json)

        else:
            resp = requests.post(
                url = f"http://{self.host}:{self.port}/{self.index_name}/{self.type_name}",
                json = document, 
                auth = self.auth, 
            )           
            resp_json = resp.json()

            if resp_json.get('result') == 'created':
                return resp_json['_id'] 
            else:
                raise UnknownError(resp_json)
    
    def query_by_id(self,
                    id: Any) -> Optional[dict[str, Any]]:
        resp = requests.get(
            url = f"http://{self.host}:{self.port}/{self.index_name}/{self.type_name}/{id}",
            auth = self.auth, 
        )           
        resp_json = resp.json()
        
        if resp_json.get('found') == True:
            return resp_json['_source']
        elif resp_json.get('found') == False:
            return None 
        else:
            raise UnknownError(resp_json)
        
    def query_id_in_x(self, 
                      x: Iterable[Any]) -> list[dict[str, Any]]:
        resp = requests.get(
            url = f"http://{self.host}:{self.port}/{self.index_name}/{self.type_name}/_search",
            auth = self.auth, 
            json = {
                'query': {
                    'ids': {
                        'values': list(x)
                    }
                }    
            },
        )           
        resp_json = resp.json()
        
        if 'hits' in resp_json:
            entry_list = [] 
            
            for item in resp_json['hits']['hits']:
                entry = item['_source']
                entry['_id'] = item['_id']
                entry_list.append(entry)
                
            return entry_list
        else:
            raise UnknownError(resp_json)
        
    def query_X_eq_x(self,
                     X: str,
                     x: Any,
                     limit: int = 10000) -> list[dict[str, Any]]:
        resp = requests.get(
            url = f"http://{self.host}:{self.port}/{self.index_name}/{self.type_name}/_search",
            auth = self.auth, 
            json = {
                'query': {
                    'match': {
                        X: x, 
                    }
                },
                'size': limit, 
            },
        )           
        resp_json = resp.json()
        
        if 'hits' in resp_json:
            entry_list = [] 
            
            for item in resp_json['hits']['hits']:
                entry = item['_source']
                entry['_id'] = item['_id']
                entry_list.append(entry)
                
            return entry_list
        else:
            raise UnknownError(resp_json)
        
    def query_X_eq_x_and_Y_eq_y(self,
                                X: str,
                                x: Any,
                                Y: str,
                                y: Any,
                                limit: int = 10000) -> list[dict[str, Any]]:
        resp = requests.get(
            url = f"http://{self.host}:{self.port}/{self.index_name}/{self.type_name}/_search",
            auth = self.auth, 
            json = {
                'query': {
                    'bool': {
                        'must': [
                            { 'match': { X: x } }, 
                            { 'match': { Y: y } }, 
                        ]
                    }
                },
                'size': limit, 
            },
        )           
        resp_json = resp.json()
        
        if 'hits' in resp_json:
            entry_list = [] 
            
            for item in resp_json['hits']['hits']:
                entry = item['_source']
                entry['_id'] = item['_id']
                entry_list.append(entry)
                
            return entry_list
        else:
            raise UnknownError(resp_json)
        
    def query_X_in_x_and_Y_eq_y(self,
                                X: str,
                                x: Any,
                                Y: str,
                                y: Any,
                                limit: int = 10000) -> list[dict[str, Any]]:
        resp = requests.get(
            url = f"http://{self.host}:{self.port}/{self.index_name}/{self.type_name}/_search",
            auth = self.auth, 
            json = {
                'query': {
                    'bool': {
                        'must': [
                            { 'terms': { X: list(x) } }, 
                            { 'match': { Y: y } }, 
                        ]
                    }
                },
                'size': limit, 
            },
        )           
        resp_json = resp.json()
        
        if 'hits' in resp_json:
            entry_list = [] 
            
            for item in resp_json['hits']['hits']:
                entry = item['_source']
                entry['_id'] = item['_id']
                entry_list.append(entry)
                
            return entry_list
        else:
            raise UnknownError(resp_json)
    
    def query_X_in_x(self,
                     X: str,
                     x: Iterable[Any],
                     limit: int = 10000) -> list[dict[str, Any]]:
        resp = requests.get(
            url = f"http://{self.host}:{self.port}/{self.index_name}/{self.type_name}/_search",
            auth = self.auth, 
            json = {
                'query': {
                    'terms': {
                        X: list(x), 
                    }
                },
                'size': limit, 
            },
        )           
        resp_json = resp.json()
        
        if 'hits' in resp_json:
            entry_list = [] 
            
            for item in resp_json['hits']['hits']:
                entry = item['_source']
                entry['_id'] = item['_id']
                entry_list.append(entry)
                
            return entry_list
        else:
            raise UnknownError(resp_json)
        
    def scroll(self,
               scroll_size: int = 1000,
               scroll_time: str = '5m',
               log_scroll_id: bool = False) -> Iterable[dict[str, Any]]:
        query = { 'query': { 'match_all': {} } }
        search_url = f"http://{self.host}:{self.port}/{self.index_name}/_search?scroll={scroll_time}&size={scroll_size}"
        scroll_url = f"http://{self.host}:{self.port}/_search/scroll?scroll={scroll_time}"
        
        resp = requests.post(url=search_url, json=query)
        resp_json = resp.json()

        scroll_id = resp_json['_scroll_id'].strip() 

        while True:
            hits = resp_json["hits"]["hits"]

            if not hits:
                break 
            
            for hit in hits:
                entry = hit['_source']
                entry['_id'] = hit['_id']
                
                yield entry 

            resp = requests.post(url=scroll_url, json={ 'scroll_id': scroll_id })
            resp_json = resp.json() 

            scroll_id = resp_json['_scroll_id'].strip()
            
            if log_scroll_id:
                print(f"scroll id: {scroll_id}") 

    def bulk_insert(self,
                    entry_list: list[dict[str, Any]]):
        if not entry_list:
            return 
        
        batch_json = ''
        
        for entry in entry_list:
            if '_id' in entry: 
                _id = str(entry.pop('_id')) 
            else:
                _id = None 
            
            if _id:
                if self.type_name == '_doc':
                    batch_json += json_dump({ 'index': { '_index': self.index_name, '_id': _id } }) + '\n'
                else:
                    batch_json += json_dump({ 'index': { '_index': self.index_name, '_type': self.type_name, '_id': _id } }) + '\n'
            else:
                if self.type_name == '_doc':
                    batch_json += json_dump({ 'index': { '_index': self.index_name } }) + '\n'
                else:
                    batch_json += json_dump({ 'index': { '_index': self.index_name, '_type': self.type_name } }) + '\n'
                
            batch_json += json_dump(entry) + '\n'
            
        resp = requests.post(
            url = f"http://{self.host}:{self.port}/_bulk",
            headers = { 'Content-Type': 'application/json' }, 
            data = batch_json.encode('utf-8'), 
            auth = self.auth, 
        )           
        resp_json = resp.json()
        
        if resp_json.get('errors') == False:
            pass 
        else:
            raise UnknownError(resp_json)
    
    def bulk_insert_old(self,
                    entry_sequence: Iterable[dict[str, Any]],
                    batch_size: int = 10000,
                    use_tqdm: bool = True,
                    total: Optional[int] = None):
        def send_batch_json(batch_json: str):
            resp = requests.post(
                url = f"http://{self.host}:{self.port}/_bulk",
                headers = { 'Content-Type': 'application/json' }, 
                data = batch_json.encode('utf-8'), 
                auth = self.auth, 
            )           
            resp_json = resp.json()
            
            if resp_json.get('errors') == False:
                pass 
            else:
                raise UnknownError(resp_json)
        
        batch_json = ''
        batch_cnt = 0 
        
        for entry in tqdm(entry_sequence, desc='Bulk Inserting', disable=not use_tqdm, total=total):
            if '_id' in entry: 
                _id = str(entry.pop('_id')) 
            else:
                _id = None 
            
            if _id:
                batch_json += json_dump({ 'index': { '_index': self.index_name, '_id': _id } }) + '\n'
            else:
                batch_json += json_dump({ 'index': { '_index': self.index_name } }) + '\n'
                
            batch_json += json_dump(entry) + '\n'
            batch_cnt += 1 
            
            if batch_cnt >= batch_size:
                send_batch_json(batch_json)        
            
                batch_cnt = 0 
                batch_json = ''
                
        if batch_cnt > 0:
            send_batch_json(batch_json)
