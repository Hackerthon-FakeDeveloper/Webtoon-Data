import pandas as pd
from typing import List
from random import randint
from math import ceil

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))) 
import database._database as db


EXCEL_PATH: str = '.\\data\\rating_data.xlsx'


def generate() -> pd.DataFrame:
    content_list: List[str] = []
    score_first_list: List[str] = []
    score_second_list: List[str] = []
    score_third_list: List[str] = []
    user_seq_list: List[str] = []
    webtoon_seq_list: List[str] = []

    webtoon_num = db.count_rows(db_key='test', table='webtoon')
    users_num = db.count_rows(db_key='test', table='users')

    content = {
        1: '최악임',
        2: '별로임',
        3: '그냥 그럼',
        4: '나쁘지 않음',
        5: '\"명작은 그 전개와 결말을 알고서도 다시 찾게 만든다\"',
    }

    reviewed = set()
    for _ in range(int(users_num*0.7)):
        score_first = randint(1, 5)
        score_first_list.append(score_first)
        
        score_second = randint(1, 5)
        score_second_list.append(score_second)

        score_third = randint(1, 5)
        score_third_list.append(score_third)

        score_avg = ceil((score_first + score_second + score_third)/3)
        content_list.append(content[score_avg])

        user_seq = randint(1, users_num)
        webtoon_seq = randint(1, webtoon_num)
        while (user_seq, webtoon_seq) in reviewed:
            user_seq = randint(1, users_num)
            webtoon_seq = randint(1, webtoon_num)
        reviewed.add((user_seq, webtoon_seq)) 

        user_seq_list.append(user_seq)
        webtoon_seq_list.append(webtoon_seq)

    df = pd.DataFrame({
        'content': content_list,
        'score_first': score_first_list,
        'score_second': score_second_list,
        'score_third': score_third_list,
        'user_seq': user_seq_list,
        'webtoon_seq': webtoon_seq_list
    })

    df.to_excel(EXCEL_PATH)

    return df

