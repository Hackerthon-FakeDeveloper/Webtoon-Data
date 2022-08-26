import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))) 
import database._database as db
import pandas as pd

def get_by_age():
    df_10, df_20, df_30 = db.get_review_group_by_age(db_key='my')
    '''df_10, \
    df_20, \
    df_30 = \
        pd.read_excel('.\\data\\10_review.xlsx'), \
        pd.read_excel('.\\data\\20_review.xlsx'), \
        pd.read_excel('.\\data\\30_review.xlsx')'''
    df_10 = df_10.groupby('webtoon_seq').mean()
    df_10['score_mean'] = (df_10.loc[:, 'score_first':'score_third']).mean(axis=1)
    df_10 = df_10.drop(columns=['score_first', 'score_second', 'score_third', 'user_seq', 'age'])
    df_10.to_json('.\\data\\10_review.json', orient='index', index=True, indent=4, force_ascii=False)

    df_20 = df_20.groupby('webtoon_seq').mean()
    df_20['score_mean'] = (df_20.loc[:, 'score_first':'score_third']).mean(axis=1)
    df_20 = df_20.drop(columns=['score_first', 'score_second', 'score_third', 'user_seq', 'age'])
    df_20.to_json('.\\data\\20_review.json', orient='index', index=True, indent=4, force_ascii=False)

    df_30 = df_30.groupby('webtoon_seq').mean()
    df_30['score_mean'] = (df_30.loc[:, 'score_first':'score_third']).mean(axis=1)
    df_30 = df_30.drop(columns=['score_first', 'score_second', 'score_third', 'user_seq', 'age'])
    df_30.to_json('.\\data\\30_review.json', orient='index', index=True, indent=4, force_ascii=False)

