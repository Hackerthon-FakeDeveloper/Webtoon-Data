from typing import Dict
from typing import Any

def get_my_database() -> Dict[str, Any]:
    database: Dict[str, Any] = {
        'host': '101.79.9.196',
        'user': 'root',
        'password': 'throeld!@#',
        'db': 'WEBTOON',
        'port': 3306,
        'charset': 'utf8',
        'use_unicode': True 
    }

    return database
