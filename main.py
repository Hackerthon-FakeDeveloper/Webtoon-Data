import database._database as db
from visualization.group_by_gender import get_by_gender
from visualization.group_by_age import get_by_age

db.insert_webtoon(df_key='excel', platform='naver', db_key='my')
db.insert_webtoon(df_key='excel', platform='kakao', db_key='my')
db.insert_webtoon(df_key='excel', platform='lezhin', db_key='my')
db.insert_webtoon_tag(df_key='excel', platform='naver', db_key='my')
db.insert_webtoon_tag(df_key='excel', platform='kakao', db_key='my')
db.insert_webtoon_tag(df_key='excel', platform='lezhin', db_key='my')
db.insert_users(db_key='my', method='file')
db.insert_webtoon_like(db_key='my', method='file')
db.insert_review(db_key='my', method='new')
db.insert_review_like(db_key='my', method='new')

get_by_gender()
get_by_age()
