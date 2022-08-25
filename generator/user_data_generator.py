from random import randint
from typing import List
import pandas as pd

'''
Columns
-------
created_date
now()

last_modified_date
now()

nickname: str
adjectives + nouns

role: str
'USER'

age: int

gender
'F', 'M' or 'N'
'F': Female
'M': Male
'N': No info

'''

EXCEL_PATH: str = '.\\data\\user_data.xlsx'

#created_date, last_modified_date, nickname, role, age, gender
def generate() -> pd.DataFrame:
    adjectives: List[str] = [
        '전투용',
        '숨겨진',
        '유령',
        '행복한',
        '교활한',
        '귀염둥이',
        '어지러운',
        '기운찬',
        '별난',
        '톡쏘는',
        '무시무시한',
        '야릇한',
        '고약한'
    ]
    nouns: List[str] = [
        '바지',
        '대머리',
        '기사',
        '거북이',
        '곰',
        '개구리',
        '발바닥',
        '고양이',
        '늑대',
        '호랑이',
        '코끼리',
        '자객',
        '도적',
        '영웅'
    ]

    nickname_list: List[str] = []
    role_list: List[str] = []
    age_list: List[int] = []
    gender_list: List[str] = []
    for adj in adjectives:
        for noun in nouns:
            nickname = adj + noun
            gender = 'F' if randint(1, 1000) <= 500 else 'M'

            nickname_list.append(nickname)
            role_list.append('USER')
            age_list.append(randint(10, 39))
            gender_list.append(gender)

    df = pd.DataFrame({
        'nickname': nickname_list,
        'role': role_list,
        'age': age_list,
        'gender': gender_list
    }).sort_values(by=['age'])

    df.to_excel(EXCEL_PATH)
    #df.to_json(JSON_PATH, orient='records', indent=4, force_ascii=False)

    return df