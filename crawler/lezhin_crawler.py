import pandas as pd

'''
key of json file
_id: identifier of each comic
additional: {'new': <bool>, 'adult': <bool>, 'rest': <bool>, 'up': <bool>}
week: [0], [1], ..., [7], 7 means 'finished'
service: providing platform
img: URL of thumbnail
url: URL of webtoon
author: author-1,author-2,...,author-n
title: title
'''
JSON_PATH: str = '.\\data\\lezhin_json_data.json'
EXCEL_PATH: str = '.\\data\\lezhin_excel_data.xlsx'

try:
    df =  pd.read_excel(EXCEL_PATH)
    df.to_json(JSON_PATH, orient='records', indent=4, force_ascii=False)
except:
    print('Error')
