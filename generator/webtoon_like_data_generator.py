import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))) 
import database._database as db
from random import randint
import pandas as pd
from typing import List, Set, Tuple

EXCEL_PATH: str = '.\\data\\webtoon_like_data.xlsx'

def generate() -> pd.DataFrame:
    webtoon_num = db.count_rows(db_key='test', table='webtoon')
    users_num = db.count_rows(db_key='test', table='users')

    webtoon_seq_index_list: List[int] = []
    user_seq_index_list: List[int] = []

    liked: Set[Tuple[int, int]] = set()
    for _ in range(webtoon_num * 10):
        webtoon_seq_index = randint(0, webtoon_num - 1)
        user_seq_index = randint(0, users_num - 1)

        while (webtoon_seq_index, user_seq_index) in liked:
            webtoon_seq_index = randint(0, webtoon_num - 1)
            user_seq_index = randint(0, users_num - 1)

        liked.add((webtoon_seq_index, user_seq_index))
        webtoon_seq_index_list.append(webtoon_seq_index)
        user_seq_index_list.append(user_seq_index)

    df = pd.DataFrame({
        'webtoon_seq_index': webtoon_seq_index_list,
        'user_seq_index': user_seq_index_list
    })
    df.to_excel(EXCEL_PATH)

    return df