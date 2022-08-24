from typing import Dict
from typing import Any
from pymysql.cursors import Cursor # for typing

import pandas as pd
import pymysql
from datetime import datetime

# add parent path
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))) 

import crawler.naver_crawler as nc
import crawler.kakao_crawler as kc
import crawler.lezhin_crawler as lc

import generator.user_data_generator as udg
import generator.rating_data_generator as rdg


def __get_my_database() -> Dict[str, Any]:
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

def __get_test_database() -> Dict[str, Any]:
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
    'excel' or 'json'
    default: 'excel'

platform: str
    'naver, 'kakao', or 'lezhin'
    default: 'naver'

crawl_from: str
'''
def __get_data_frame(
    key: str='excel',
    platform: str='naver'
) -> pd.DataFrame:
    excel_paths = {
        'naver': nc.EXCEL_PATH,
        'kakao': kc.EXCEL_PATH,
        'lezhin': lc.EXCEL_PATH        
    }
    json_paths = {
        'naver': nc.JSON_PATH,
        'kakao': kc.JSON_PATH,
        'lezhin': lc.JSON_PATH        
    }

    if key == 'json':
        df = pd.read_json(json_paths[platform])
    elif key == 'excel':
        df = pd.read_excel(excel_paths[platform])
    else:
        df = None

    return df

# establish connection
'''
Parameters
----------
key: str
    'my' or 'test'
    default: 'test'
'''
def __get_connection(key: str='test') -> pymysql.connections.Connection:
    _database: Dict[str, Any] = \
        __get_my_database() if key == 'my' else __get_test_database()

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
def __close_connection(conn: pymysql.connections.Connection) -> None:
    return conn.close()

# get cursor
def __get_cursor(conn: pymysql.connections.Connection) -> Cursor:
    return conn.cursor()

# close cursor
def __close_cursor(cursor: Cursor) -> None:
    return cursor.close()

'''
Parameters
----------
table: str
    table name to count rows
    'webtoon' or 'users'
    default: 'webtoon'

db_key: str
    'test' or 'my'
    default: 'test
'''
# get counts of table
def count_rows(db_key: str='test', table: str='webtoon') -> int:
    if table != 'webtoon' and table != 'users':
        return 0

    conn = __get_connection(key=db_key)
    cursor = __get_cursor(conn=conn)

    query = \
        f'SELECT COUNT(*) \
        FROM {table}'
    affected_rows_num = cursor.execute(query)
    count = cursor.fetchone()[0]

    __close_cursor(cursor=cursor)
    __close_connection(conn=conn)

    return count if affected_rows_num == 1 else 0


# insert into 'webtoon' table
def __insert_webtoon(
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

        author_name = str(row["author"]).replace('\'', '\\\'')
        get_author_seq_query = \
            f'SELECT author_seq \
            FROM author \
            WHERE name=\'{author_name}\''
        affected_rows_num = cursor.execute(query=get_author_seq_query)
        if affected_rows_num == 0: # if row["author"] is not in 'author' table
            print(f'No such author in DB: {row["author"]}')

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
            else:
                print(f'insert author error: {author_name}')
                continue

        cursor.execute(query=get_author_seq_query)
        author_seq = int(cursor.fetchone()[0]) # 1, 2, 3, ... (auto_increment)
        now = str(datetime.now().date()) # yyyy-MM-dd
        description = str(row["description"]).replace('\'', '\\\'').strip()
        is_adult = int(row["is_adult"])
        platform = str(row['platform']).strip()
        start_date = str(row["start_date"]).strip()
        thumbnail = str(row["thumbnail"])
        title = str(row['title']).replace('\'', '\\\'').strip()
        serial_status = int(row["serial_status"])
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
                {is_adult}, \
                \'{platform}\', \
                \'{start_date}\', \
                \'{thumbnail}\', \
                \'{title}\', \
                \'{row["url"]}\',\
                {author_seq}, \
                {serial_status})'

        affected_rows_num = cursor.execute(query=insert_webtoon_query)
        if affected_rows_num == 1:
            conn.commit()
        else:
            print(f'insert webtoon error: {row["id"]}')

# insert into 'genre' table
def __insert_genre(
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
            else:
                print(f'insert genre error: {genre}')

# insert into 'tag' table
def __insert_tag(
    conn: pymysql.connections.Connection,
    cursor: Cursor,
    df: pd.DataFrame
):
    # constuct tag set
    tag_list = []
    for tags in df['tag']:
        tags = str(tags).replace('.', ',')
        for tag in tags.split(','):
            tag_list.append(tag.strip())

    get_tags_query = 'SELECT name FROM tag'
    cursor.execute(query = get_tags_query)
    tags = [str(t[0]) for t in cursor.fetchall()]
    for tag in tag_list:
        if tag not in tags:
            insert_tag_query = \
                f'INSERT \
                INTO tag(name) \
                VALUES(\'{tag}\')'
            affected_rows_num = cursor.execute(insert_tag_query)
            if affected_rows_num == 1:
                conn.commit()

'''
Parameters
----------
key: str
    'excel' or 'json'
    default: 'excel'

platform: str
    'naver, 'kakao', or 'lezhin'
    default: 'naver'

db_key: str
    'test' or 'my'
    default: 'test
'''
def insert_webtoon(
    df_key: str='excel',
    platform: str='naver',
    db_key: str='test'
):
    df = __get_data_frame(key=df_key, platform=platform)

    conn = __get_connection(key=db_key)
    cursor = __get_cursor(conn=conn)

    __insert_webtoon(conn=conn, cursor=cursor, df=df)
    __insert_genre(conn=conn, cursor=cursor, df=df)
    __insert_tag(conn=conn, cursor=cursor, df=df)
    __close_cursor(cursor=cursor)
    __close_connection(conn=conn)


# insert into 'users' table
def __insert_users(
    conn: pymysql.connections.Connection,
    cursor: Cursor,
    df: pd.DataFrame
):
    get_nicknames_query = 'SELECT nickname FROM users'
    cursor.execute(get_nicknames_query)
    nicknames = [str(n[0]) for n in cursor.fetchall()]
    for i in df.index:
        row = df.loc[i]
        nickname = str(row['nickname'])
        if nickname not in nicknames:
            now = str(datetime.now().date())
            role = str(row['role'])
            age = int(row['age'])
            gender = str(row['gender'])
            insert_nickname_query = \
            f'INSERT INTO users( \
                created_date, \
                last_modified_date, \
                nickname, \
                role, \
                age, \
                gender) \
            VALUES( \
                \'{now}\', \
                \'{now}\', \
                \'{nickname}\', \
                \'{role}\', \
                {age}, \
                \'{gender}\')'
            affected_rows_num = cursor.execute(insert_nickname_query)

            if affected_rows_num == 1:
                conn.commit()
            else:
                print(f'insert user error: {row["nickname"]}')

'''
Parameters
----------
db_key: str
    'test' or 'my'
    default: 'test

method: str
    'file' or 'new'
    'file' -> read excel file
    'new' -> generate new data
'''
def insert_users(db_key: str='test', method: str='file'):
    if method=='file':
        try:
            df = pd.read_excel(udg.EXCEL_PATH)
        except:
            return None
    else:
        df = udg.generate()

    conn = __get_connection(key=db_key)
    cursor = __get_cursor(conn=conn)

    __insert_users(conn=conn, cursor=cursor, df=df)

    __close_cursor(cursor=cursor)
    __close_connection(conn=conn)


def __insert_review(
    conn: pymysql.connections.Connection,
    cursor: Cursor,
    df: pd.DataFrame
):
    get_seqs_query = \
        'SELECT user_seq, webtoon_seq \
        FROM review'
    cursor.execute(get_seqs_query)
    seqs = cursor.fetchall()
    for i in df.index:
        row = df.loc[i]
        user_seq = int(row['user_seq'])
        webtoon_seq = int(row['webtoon_seq'])
        if (user_seq, webtoon_seq) not in seqs:
            now = str(datetime.now().date())
            content = str(row['content'])
            score_first = int(row['score_first'])
            score_second = int(row['score_second'])
            score_third = int(row['score_third'])

            insert_nickname_query = \
            f'INSERT INTO review( \
                created_date, \
                last_modified_date, \
                content, \
                score_first, \
                score_second, \
                score_third, \
                user_seq, \
                webtoon_seq) \
            VALUES( \
                \'{now}\', \
                \'{now}\', \
                \'{content}\', \
                {score_first}, \
                {score_second}, \
                {score_third}, \
                {user_seq}, \
                {webtoon_seq})'
            affected_rows_num = cursor.execute(insert_nickname_query)

            if affected_rows_num == 1:
                conn.commit()
            else:
                print(f'insert user error: {row["nickname"]}')
    
'''
Parameters
----------
db_key: str
    'test' or 'my'
    default: 'test

method: str
    'file' or 'new'
    'file' -> read excel file
    'new' -> generate new data
'''
def insert_review(db_key: str='test', method: str='file'):
    if method=='file':
        try:
            df = pd.read_excel(rdg.EXCEL_PATH)
        except:
            return
    else:
        df = udg.generate()
    conn = __get_connection(key=db_key)
    cursor = __get_cursor(conn=conn)

    __insert_review(conn=conn, cursor=cursor, df=df)

    __close_cursor(cursor=cursor)
    __close_connection(conn=conn)
