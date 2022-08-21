from typing import Dict
import pandas as pd
import pymysql
from pymysql.cursors import Cursor # for typing
from datetime import datetime
from database_info import get_my_database
# from naver_crawler import crawl

def get_data_frame() -> pd.DataFrame:
    path: str = '.\\naver_webtoon_info.xlsx'
    df = pd.read_excel(path)
    # or df = crawl()

    return df

# establish connection
def get_connection() -> pymysql.connections.Connection:
    # informations to connect to database
    _database: Dict[str, str] = get_my_database()

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


df = get_data_frame()
# n = 3; df = df.head(n)
conn = get_connection()
cursor = get_cursor(conn=conn)

# insert into 'webtoon' table
# check if there is duplication by URL
get_url_query = 'SELECT url FROM webtoon'
cursor.execute(get_url_query)
urls = [str(url[0])for url in cursor.fetchall()]
for i in df.index:
    row = df.loc[i]

    if row['url'] in urls: # skip if duplication is on DB
        continue

    get_author_seq_query = \
        f'SELECT author_seq \
        FROM author \
        WHERE name=\'{row["author"]}\''
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
                \'{row["author"]}\')'
        affected_rows_num = cursor.execute(query=insert_author_query)
        if affected_rows_num == 1:
            conn.commit()
        
            cursor.execute(query=get_author_seq_query)
        else:
            print(f'insert author error: {row["author"]}')
            continue
    
    author_seq = int(cursor.fetchone()[0]) # 1, 2, 3, ... (auto_increment)
    now = str(datetime.now().date()) # yyyy-MM-dd
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
            \'{row["description"]}\', \
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
    else:
        print(f'insert webtoon error: {row["id"]}')

# insert into 'genre' table
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


close_cursor(cursor=cursor)
close_connection(conn=conn)
