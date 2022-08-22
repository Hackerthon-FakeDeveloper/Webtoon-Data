from database import database as db

df = db.get_data_frame(key='excel')

conn = db.get_connection(key='my')
cursor = db.get_cursor(conn=conn)

db.insert_webtoons(conn=conn, cursor=cursor, df=df)
db.insert_genres(conn=conn, cursor=cursor, df=df)
db.insert_tags(conn=conn, cursor=cursor, df=df)
db.close_cursor(cursor=cursor)
db.close_connection(conn=conn)

