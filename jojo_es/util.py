import json 
from typing import Any 

__all__ = [
    'explore_dict', 
    'json_dump', 
]


def explore_dict(dict_: dict[str, Any],
                 path: str,
                 default: Any = None) -> Any:
    value = dict_ 
    
    try:
        for key in path.split('/'):
            value = value[key]

        return value 
    except Exception:
        return default


def json_dump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False).strip() 
