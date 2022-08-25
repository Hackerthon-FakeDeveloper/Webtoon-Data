from typing import Dict, List, Any
from pymysql.cursors import Cursor # for typing

import pandas as pd
import pymysql
from datetime import datetime
from random import randrange
from datetime import timedelta

# add parent path
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))) 

import crawler.naver_crawler as nc
import crawler.kakao_crawler as kc
import crawler.lezhin_crawler as lc

import generator.user_data_generator as udg
import generator.review_data_generator as rdg
import generator.review_like_data_generator as rldg
import generator.webtoon_like_data_generator as wldg


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
    __database: Dict[str, Any] = \
        __get_my_database() if key == 'my' else __get_test_database()

    # connect to DB
    conn = pymysql.connect(
        host=__database['host'],
        user=__database['user'],
        password=__database['password'],
        db=__database['db'],
        port=__database['port'],
        charset=__database['charset'],
        use_unicode=__database['use_unicode'] # default is True
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
    'webtoon', 'users' or 'review'
    default: 'webtoon'

db_key: str
    'test' or 'my'
    default: 'test
'''
# get counts of table
def count_rows(db_key: str='test', table: str='webtoon') -> int:
    if table != 'webtoon' and table != 'users' and table != 'review':
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

    get_webtoon_seq_query = \
        f'SELECT webtoon_seq \
        FROM webtoon'
    cursor.execute(get_webtoon_seq_query)
    webtoon_seqs = [int(seq[0]) for seq in cursor.fetchall()]

    for i in df.index:
        row = df.loc[i]
        user_seq = int(row['user_seq'])
        webtoon_seq = webtoon_seqs[int(row['webtoon_seq_index'])]
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
    if method == 'file':
        try:
            df = pd.read_excel(rdg.EXCEL_PATH)
        except:
            return
    else:
        df = rdg.generate()
    conn = __get_connection(key=db_key)
    cursor = __get_cursor(conn=conn)

    __insert_review(conn=conn, cursor=cursor, df=df)

    __close_cursor(cursor=cursor)
    __close_connection(conn=conn)


def __insert_webtoon_tag(
    conn: pymysql.connections.Connection,
    cursor: Cursor,
    df: pd.DataFrame
):
    select_webtoon_tag_query = \
    f'SELECT tag_seq, webtoon_seq \
    FROM webtoon_tag'
    affected_rows_num = cursor.execute(select_webtoon_tag_query)
    if affected_rows_num < 0:
        print('select webtoon_tag error')
        return
    webtoon_tags = cursor.fetchall()
    for i in df.index:
        row = df.loc[i]
        get_webtoon_seq_query = \
            f'SELECT webtoon_seq \
            FROM webtoon \
            WHERE url=\'{str(row["url"])}\''
        affected_rows_num = cursor.execute(get_webtoon_seq_query)
        if affected_rows_num != 1:
            print('select webtoon_seq error')
            continue

        webtoon_seq = int(cursor.fetchone()[0])
        tags = str(row['tag']).replace('.', ',').split(',')
        for tag in tags:
            get_tag_seq_query = \
                f'SELECT tag_seq \
                FROM tag \
                WHERE name=\'{tag.strip()}\''
            affected_rows_num = cursor.execute(get_tag_seq_query)
            if affected_rows_num != 1:
                print(get_tag_seq_query)
                print(f'select tag_seq error: {tag}, {affected_rows_num}')
                continue
            tag_seq = int(cursor.fetchone()[0])

            if (tag_seq, webtoon_seq) not in webtoon_tags:
                insert_webtoon_tag_query = \
                    f'INSERT INTO \
                    webtoon_tag( \
                        tag_seq, \
                        webtoon_seq) \
                    VALUES( \
                        {tag_seq}, \
                        {webtoon_seq})'
                affected_rows_num = cursor.execute(insert_webtoon_tag_query)
                if affected_rows_num != 1:
                    print(f'insert webtoon_tag error: ({tag_seq}, {webtoon_seq})')
                else:
                    conn.commit()

'''
Parameters
----------
df_key: str
    'excel' or 'json'
    default: 'excel'

platform: str
    'naver, 'kakao', or 'lezhin'
    default: 'naver'

db_key: str
    'test' or 'my'
    default: 'test
'''
def insert_webtoon_tag(
    df_key: str='excel',
    platform: str='naver',
    db_key: str='test'
):
    df = __get_data_frame(key=df_key, platform=platform)

    conn = __get_connection(key=db_key)
    cursor = __get_cursor(conn=conn)

    __insert_webtoon_tag(conn=conn, cursor=cursor, df=df)

    __close_cursor(cursor=cursor)
    __close_connection(conn=conn)


def __insert_review_like(
    conn: pymysql.connections.Connection,
    cursor: Cursor,
    df: pd.DataFrame
):
    get_seq_query = \
        'SELECT review_seq, user_seq \
        FROM review_like'
    cursor.execute(get_seq_query)
    seqs = cursor.fetchall()

    get_review_seq_query = \
        'SELECT review_seq \
        FROM review'
    cursor.execute(get_review_seq_query)
    review_seqs = cursor.fetchall()

    get_user_seq_query = \
        'SELECT user_seq \
        FROM users'
    cursor.execute(get_user_seq_query)
    user_seqs = cursor.fetchall()

    seq_set = set()
    for review_seq_index, user_seq_index in zip(df['review_seq_index'], df['user_seq_index']):
        review_seq = review_seqs[review_seq_index][0]
        user_seq = user_seqs[user_seq_index][0]
        if (review_seq, user_seq) not in seqs \
        and (review_seq, user_seq) not in seq_set:
            seq_set.add((review_seq, user_seq))
            now = str(datetime.now().date())
            insert_review_like_query = \
                f'INSERT INTO \
                review_like( \
                    review_seq, \
                    user_seq, \
                    created_date, \
                    last_modified_date) \
                VALUES( \
                    {review_seq}, \
                    {user_seq}, \
                    \'{now}\', \
                    \'{now}\')'
            
            affected_rows_num = cursor.execute(insert_review_like_query)
            if affected_rows_num != 1:
                print('insert review_like error')
            else:
                conn.commit()

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
def insert_review_like(db_key: str='test', method: str='file'):
    if method == 'file':
        try:
            df = pd.read_excel(rldg.EXCEL_PATH)
        except:
            return
    else:
        df = rldg.generate()

    conn = __get_connection(key=db_key)
    cursor = __get_cursor(conn=conn)

    __insert_review_like(conn=conn, cursor=cursor, df=df)

    __close_cursor(cursor=cursor)
    __close_connection(conn=conn)


def __insert_webtoon_like(
    conn: pymysql.connections.Connection,
    cursor: Cursor,
    df: pd.DataFrame
):
    get_seq_query = \
        'SELECT webtoon_seq, user_seq \
        FROM webtoon_like'
    cursor.execute(get_seq_query)
    seqs = cursor.fetchall()

    get_webtoon_seq_query = \
        'SELECT webtoon_seq \
        FROM webtoon'
    cursor.execute(get_webtoon_seq_query)
    webtoon_seqs = cursor.fetchall()

    get_user_seq_query = \
        'SELECT user_seq \
        FROM users'
    cursor.execute(get_user_seq_query)
    user_seqs = cursor.fetchall()

    seq_set = set()
    for webtoon_seq_index, user_seq_index in zip(df['webtoon_seq_index'], df['user_seq_index']):
        webtoon_seq = webtoon_seqs[webtoon_seq_index][0]
        user_seq = user_seqs[user_seq_index][0]
        if (webtoon_seq, user_seq) not in seqs \
        and (webtoon_seq, user_seq) not in seq_set:
            seq_set.add((webtoon_seq, user_seq))
            
            get_start_date_query = \
                f'SELECT start_date \
                FROM webtoon \
                WHERE webtoon_seq={webtoon_seq}'
            cursor.execute(get_start_date_query)

            start_date = cursor.fetchone()[0]
            now = datetime.now().date()

            delta = now - start_date
            int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
            random_second = randrange(int_delta)
            date = start_date + timedelta(seconds=random_second)

            insert_review_like_query = \
                f'INSERT INTO \
                webtoon_like( \
                    user_seq, \
                    webtoon_seq, \
                    created_date, \
                    last_modified_date) \
                VALUES( \
                    {user_seq}, \
                    {webtoon_seq}, \
                    \'{date}\', \
                    \'{date}\')'

            affected_rows_num = cursor.execute(insert_review_like_query)
            if affected_rows_num != 1:
                print('insert review_like error')
            else:
                conn.commit()

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
def insert_webtoon_like(db_key: str='test', method: str='file'):
    if method == 'file':
        try:
            df = pd.read_excel(wldg.EXCEL_PATH)
        except:
            return
    else:
        df = wldg.generate()

    conn = __get_connection(key=db_key)
    cursor = __get_cursor(conn=conn)

    __insert_webtoon_like(conn=conn, cursor=cursor, df=df)

    __close_cursor(cursor=cursor)
    __close_connection(conn=conn)
