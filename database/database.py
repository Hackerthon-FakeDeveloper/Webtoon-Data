from typing import Dict
from typing import Any
from pymysql.cursors import Cursor # for typing

import pandas as pd
import pymysql
from datetime import datetime

# add parent path
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))) 

from crawler.naver_crawler import EXCEL_PATH, JSON_TO
from crawler.naver_crawler import crawl

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

def get_test_database() -> Dict[str, Any]:
    database: Dict[str, Any] = {
        'host': '101.79.9.196',
        'user': 'root',
        'password': 'throeld!@#',
        'db': 'WEBTOON',
        'port': 53306,
        'charset': 'utf8',
        'use_unicode': True 
    }
    return database


'''
Parameters
----------
key: str
    'excel', 'json' or 'crawl'
    default: 'excel'

path: str
    .xlsx file or .json file path to read
    default: 'EXCEL_PATH'
'''
def get_data_frame(key='excel', path=EXCEL_PATH) -> pd.DataFrame:
    if key == 'json':
        if path == EXCEL_PATH or path == None:
            df = pd.read_json(JSON_TO)
        else:
            df = pd.read_json(path)
    elif key == 'crawl':
        df = crawl()
    elif key == 'excel':
        if path == None:
            df = pd.read_excel(EXCEL_PATH)
        else:
            df = pd.read_excel(path)
    else:
        df = pd.read_excel(EXCEL_PATH)

    return df

# establish connection
'''
Parameters
----------
key: str
    'my' or 'test'
    default: 'test'
'''
def get_connection(key: str) -> pymysql.connections.Connection:
    _database: Dict[str, Any] = \
        get_my_database() if key == 'my' else get_test_database()

    # connect to DB
    conn = pymysql.connect(
        host=_database['host'],
        user=_database['user'],
        password=_database['password'],
        db=_database['db'],
        port=_database['port'],
        charset=_database['charset'],
        use_unicode=_database['use_unicode'] # default is True
    )

    return conn

# close connection
def close_connection(conn: pymysql.connections.Connection) -> None:
    return conn.close()

# get cursor
def get_cursor(conn: pymysql.connections.Connection) -> Cursor:
    return conn.cursor()

# close cursor
def close_cursor(cursor: Cursor) -> None:
    return cursor.close()


'''
# insert all authors into 'author' table
get_authors_query = \
    'SELECT name FROM author'
cursor.execute(query=get_authors_query)
authors = [author[0] for author in cursor.fetchall()]
for author in df['author'].unique():
    author = str(author)
    # for author in authors.split('/'):
    if author not in authors:
        author_query = \
            f'INSERT INTO author( \
                description, \
                name \
            ) \
            VALUES( \
                \'\', \
                \'{author}\')'
        
        affected_rows_num = cursor.execute(query=author_query)
        if affected_rows_num == 1:
            conn.commit()
'''

# insert into 'webtoon' table
def insert_webtoons(
    conn: pymysql.connections.Connection,
    cursor: Cursor,
    df: pd.DataFrame
):
    # check if there is duplication by URL
    get_url_query = 'SELECT url FROM webtoon'
    cursor.execute(get_url_query)
    urls = [str(url[0])for url in cursor.fetchall()]
    for i in df.index:
        row = df.loc[i]

        if row['url'] in urls: # skip if duplication is on DB
            continue

        author_name = row["author"].replace('\'', '\\\'')
        get_author_seq_query = \
            f'SELECT author_seq \
            FROM author \
            WHERE name=\'{author_name}\''
        affected_rows_num = cursor.execute(query=get_author_seq_query)
        if affected_rows_num == 0: # if row["author"] is not in 'author' table
            insert_author_query = \
                f'INSERT INTO author( \
                    description, \
                    name \
                ) \
                VALUES( \
                    \'\', \
                    \'{author_name}\')'
            affected_rows_num = cursor.execute(query=insert_author_query)
            if affected_rows_num == 1:
                conn.commit()
            
                cursor.execute(query=get_author_seq_query)
            else:
                continue
        
        author_seq = int(cursor.fetchone()[0]) # 1, 2, 3, ... (auto_increment)
        now = str(datetime.now().date()) # yyyy-MM-dd
        description = str(row["description"]).replace('\'', '\\\'')
        insert_webtoon_query = \
            f'INSERT INTO webtoon( \
                created_date, \
                last_modified_date, \
                description, \
                is_adult, \
                platform, \
                start_date, \
                thumbnail, \
                title, \
                url, \
                author_seq, \
                serial_status) \
            VALUES( \
                \'{now}\', \
                \'{now}\', \
                \'{description}\', \
                {int(row["is_adult"])}, \
                \'{row["platform"]}\', \
                \'{row["start_date"]}\', \
                \'{row["thumbnail"]}\', \
                \'{row["title"]}\', \
                \'{row["url"]}\',\
                {author_seq}, \
                {int(row["serial_status"])})'

        affected_rows_num = cursor.execute(query=insert_webtoon_query)
        if affected_rows_num == 1:
            conn.commit()

# insert into 'genre' table
def insert_genres(
    conn: pymysql.connections.Connection,
    cursor: Cursor,
    df: pd.DataFrame
):
    get_genres_query = 'SELECT genre FROM genre'
    cursor.execute(query=get_genres_query)
    genres = [str(g[0]) for g in cursor.fetchall()]
    for genre in df['genre'].unique():
        if genre not in genres:
            genre_query = f'INSERT INTO genre(genre) VALUES(\'{genre}\')'
            affected_rows_num = cursor.execute(genre_query)
            if affected_rows_num == 1:
                conn.commit()


# insert into 'tag' table
def insert_tags(
    conn: pymysql.connections.Connection,
    cursor: Cursor,
    df: pd.DataFrame
):
    # constuct tag set
    tag_set = set()
    for tags in df['tag']:
        tags = str(tags)
        for tag in tags.split(','):
            tag_set.add(tag)

    get_tags_query = 'SELECT name FROM tag'
    cursor.execute(query = get_tags_query)
    tags = [str(t[0]) for t in cursor.fetchall()]
    for tag in tag_set:
        if tag not in tags:
            tag_query =  f'INSERT INTO tag(name) VALUES(\'{tag}\')'
            affected_rows_num = cursor.execute(tag_query)
            if affected_rows_num == 1:
                conn.commit()
