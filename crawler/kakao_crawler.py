import json
from typing import List
import pandas as pd
import requests
from bs4 import BeautifulSoup
import winsound
from typing import Dict, Any

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
JSON_PATH: str = '.\\data\\kakao_json_data.json'
EXCEL_PATH: str = '.\\data\\kakao_excel_data.xlsx'

def crawl() -> pd.DataFrame:
    ENCODING: str = 'utf-8'
    
    SERIALIZING: int = 0
    SUSPEND: int = 1
    COMPLELTE: int = 2

    html: str = requests.get('https://korea-webtoon-api.herokuapp.com/kakao').text
    with open(JSON_PATH, 'wt', encoding=ENCODING) as f:
        f.write(html)
        f.flush()
        f.close()

    id_list: List[str] = []
    description_list: List[str] = []
    is_adult_list: List[str] = [] # if webtoon is adult-only 1 else 0
    platform_list: List[str] = []
    start_date_list: List[str] = []
    thumbnail_list: List[str] = [] # URL of thumbnail
    title_list: List[str] = []
    url_list: List[str] = [] # URL of webtoon
    serial_status_list: List[str] = [] # int

    author_list: List[str] = [] # 'author1/author2/author3...', to 'author' table
    tag_list: List[str] = [] #'tag1,tag2,tag3...', to 'tag' table
    genre_list: List[str] = []

    ################################################################# 모든 웹툰의 제목, 작가, 연재일, 상태, 청불 여부, 썸네일 url, 웹툰 url
    ##### 장르, 스토리, 형식 정보 없음
    with open(file=JSON_PATH, mode='r', encoding=ENCODING) as f:
        data: List[Dict[str, Any]] = json.load(f) #<class 'list'> of json objects
        for obj in data: # for each json object--<class 'dict'>--,
            # https://webtoon.kakao.com/content/동천-만물수리점/2767
            
            url = str(obj['url'])
            #url = 'https://webtoon.kakao.com/content/미생/818'

            # crawl description
            headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'}
            html = requests.get(url, headers=headers).text
            soup = BeautifulSoup(html, 'html.parser')
            description = soup.find('head', attrs=None).find('meta', {'name': 'description'})
            if description != None:
                description = description['content']

                # get id
                id_list.append(url[(url.rindex('/') + 1):])

                # get description
                description_list.append(description)   

                # get is_adult
                is_adult_list.append(int(obj['additional'].get('adult'))) #0, 1

                # get platform
                platform_list.append('kakao')

                # get start_date
                start_date_list.append('')

                # get thumbnail url
                thumbnail_list.append(obj['img'])

                # get title
                title_list.append(str(obj['title']))
                
                # get url
                url_list.append(url)

                # get serial status
                week = obj['week'][0]
                serial_status = SERIALIZING
                if week == 7:
                    serial_status = COMPLELTE
                elif obj['additional'].get('rest'):
                    serial_status = SUSPEND                
                serial_status_list.append(serial_status)

                # get author
                author_list.append(str(obj['author']).replace(',', '/'))
                
                # get genre
                genre_list.append(' ')
                
                # get tag
                tag_list.append(' ')
                

    print(f'id: {len(id_list)}')
    print(f'description: {len(description_list)}')
    print(f'is_adult: {len(is_adult_list)}')
    print(f'platform: {len(platform_list)}')
    print(f'start_date: {len(start_date_list)}')
    print(f'thumbnail: {len(thumbnail_list)}')
    print(f'title: {len(title_list)}')
    print(f'url: {len(url_list)}')
    print(f'author: {len(author_list)}')
    print(f'genre: {len(genre_list)}')
    print(f'tag: {len(tag_list)}')

    # construct truct data frame
    df = pd.DataFrame({
        'id': id_list,
        'description': description_list,
        'is_adult': is_adult_list,
        'platform': platform_list,
        'start_date': start_date_list,
        'thumbnail': thumbnail_list,
        'title': title_list,
        'url': url_list,
        'serial_status': serial_status_list,
        
        'author': author_list,
        'genre': genre_list,
        'tag': tag_list,
    }).set_index('id')

    print(df.head(5))

    df.to_excel(EXCEL_PATH, encoding='utf-8')
    df.to_json(JSON_PATH, orient='records', indent=4, force_ascii=False)

    winsound.Beep(1000, 1000)

    return df
