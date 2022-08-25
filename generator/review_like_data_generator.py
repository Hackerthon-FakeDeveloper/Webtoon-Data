import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))) 
import database._database as db
from random import randint
import pandas as pd
from typing import List, Set, Tuple

EXCEL_PATH: str = '.\\data\\review_like_data.xlsx'

def generate() -> pd.DataFrame:
    review_num = db.count_rows(db_key='test', table='review')
    users_num = db.count_rows(db_key='test', table='users')

    review_seq_index_list: List[int] = []
    user_seq_index_list: List[int] = []

    liked: Set[Tuple[int, int]] = set()
    for _ in range(review_num*2):
        review_seq_index = randint(0, review_num - 1)
        user_seq_index = randint(0, users_num - 1)

        while (review_seq_index, user_seq_index) in liked:
            review_seq_index = randint(0, review_num - 1)
            user_seq_index = randint(0, users_num - 1)
        
        liked.add((review_seq_index, user_seq_index))
        review_seq_index_list.append(review_seq_index)
        user_seq_index_list.append(user_seq_index)

    df = pd.DataFrame({
        'review_seq_index': review_seq_index_list,
        'user_seq_index': user_seq_index_list
    })
    df.to_excel(EXCEL_PATH)

    return df