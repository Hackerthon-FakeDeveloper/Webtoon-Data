import database._database as db

db.insert_webtoon(df_key='excel', platform='naver', db_key='my')
db.insert_users(db_key='my', method='file')
db.insert_review(db_key='my', method='file')
