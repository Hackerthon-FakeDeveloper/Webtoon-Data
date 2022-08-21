import json
from typing import List
import pandas as pd
import requests
from bs4 import BeautifulSoup


def crawl() -> str:
    '''
    key of json file
    _id: identifier of each comic
    additional: {'new': <bool>, 'adult': <bool>, 'rest': <bool>, 'up': <bool>}
    week: [0], [1], ..., [7], 7 means 'finished'
    service: providing platform
    img: URL of thumbnail
    url: URL of webtoon(online comic)
    author: author-1,author-2,...,author-n
    title: title
    '''
    JSON_FILE_PATH: str = 'naver_data.json'
    ENCODING: str = 'utf-8'
    NO_INFO: str = 'no_info'
    WEEKS: List[str] = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
    NAVER_INFO_FILE: str = 'naver_webtoon_info.xlsx'

    SERIALIZING: int = 0
    SUSPEND: int = 1
    COMPLELTE: int = 2

    html:str = requests.get('https://korea-webtoon-api.herokuapp.com/naver').text
    with open(JSON_FILE_PATH, 'wt', encoding=ENCODING) as f:
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
    url_list: List[str] = [] # URL of webtoon(online comic)
    serial_status_list: List[str] = [] # int

    author_list: List[str] = [] # 'author1/author2/author3...', to 'author' table
    tag_list: List[str] = [] #'tag1,tag2,tag3...', to 'tag' table
    day_list: List[str] = []  # 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'fin'
    genre_list: List[str] = []

    #################################################################
    # 모든 웹툰의 제목, 작가, 연재 요일, 상태, 청불 여부, 썸네일 url, 웹툰 url
    # 
    ##### 장르, 스토리, 형식, 연재 시작일 정보 없음
    with open(file=JSON_FILE_PATH, mode='r', encoding=ENCODING) as f:
        data: List = json.load(f) #<class 'list'> of json objects
        for obj in data: # for each json object (<class 'dict'>)
            url: str = str(obj['url']).replace('m.comic.naver.com', 'comic.naver.com')

            # check if adult only
            html = requests.get(url).text
            soup = BeautifulSoup(html, 'html.parser')
            detail = soup.find('p', {'class': 'detail_info'})
            age = detail.find('span', {'class': 'age'})
            age: str = '전체연령가' if age == None else age.text

            if '18' not in age:
                # get id
                id = url[url.index('=') + 1: url.index('&')] if '&' in url else url[url.index('=') + 1:]
                id_list.append(id)
                # get title
                title = obj['title']
                title_list.append(title)

                # get author
                author = obj['author'].replace(',', '/')
                author_list.append(author)

                # get uploading day of week
                week = obj['week'][0]
                day_list.append(week)

                # get description
                description = soup.select_one('.comicinfo > .detail').select_one('p')
                if description != None:
                    description = str(description).replace('<p>', '')
                    description = description.replace('</p>', '')
                    description = description.replace('&nbsp;', ' ')
                    description = description.replace('<br/>', '\n')                
                else:
                    description = NO_INFO
                description_list.append(description)

                # get genre
                genre = detail.find('span', {'class': 'genre'})
                genre = genre.text if genre != None else NO_INFO
                genre_list.append(genre.split(',')[-1])

                # get url to get start_date, tag
                url = f'https://comic.naver.com/webtoon/detail?titleId={id}&no=1'
                html = requests.get(url).text
                soup = BeautifulSoup(html, 'html.parser')
                try: # get start_date
                    start_date = \
                    soup.find('div', {'id': 'container'}) \
                        .find('div', {'class': 'section_cont wide', 'id': 'sectionContWide'}) \
                            .find('div', {'class': 'tit_area'}) \
                                .find('div', {'class': 'vote_lst'}) \
                                    .find('dl', {'class': 'rt'}) \
                                        .find('dd', {'class': 'date'}) \
                                            .text.replace('.', '-')
                except:
                    start_date = '0000-00-00'
                start_date_list.append(start_date)

                # get tag
                a = soup.find('div', {'id': 'content'})
                if a != None:
                    a = a.find('div', {'class': 'view_area'}) 
                    a = a.find('div', {'class': 'view_sub'})
                    a = a.find('div', {'class': 'tag'})
                    a = a.find_all('a', attrs=None)
                    tag = ''.join([t.text for t in a])
                else:
                    tag = NO_INFO
                tag_list.append(tag)

                # get serial status
                serial_status = SERIALIZING
                if week == 7:
                    serial_status = COMPLELTE
                elif obj['additional'].get('rest'):
                    serial_status = SUSPEND                
                serial_status_list.append(serial_status)

                # the adult-only have already filtered.
                is_adult_list.append(0)

                # get thumbnail url
                thumbnail = obj['img']
                thumbnail_list.append(thumbnail)

                if week < 7: # if not finished
                    url = url + f'&weekday={WEEKS[week]}'
                url_list.append(url)

                # crawls from only naver webtoon.
                platform_list.append('naver')

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

    df.to_excel(NAVER_INFO_FILE)
    #return NAVER_INFO_FILE

    return df
