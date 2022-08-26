import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))) 
import database._database as db

def get_by_gender():
    m_df, f_df = db.get_review_group_by_gender(db_key='my')
    #m_df, f_df = pd.read_excel('.\\data\\m_review.xlsx'), pd.read_excel('.\\data\\f_review.xlsx')
    m_df = m_df.groupby('webtoon_seq').mean()
    m_df['score_mean'] = (m_df.loc[:, 'score_first':'score_third']).mean(axis=1)
    m_df = m_df.drop(columns=['score_first', 'score_second', 'score_third', 'user_seq'])
    m_df.to_json('.\\data\\m_review.json', orient='index', index=True, indent=4, force_ascii=False)

    f_df = f_df.groupby('webtoon_seq').mean()
    f_df['score_mean'] = (f_df.loc[:, 'score_first':'score_third']).mean(axis=1)
    f_df = f_df.drop(columns=['score_first', 'score_second', 'score_third', 'user_seq'])
    f_df.to_json('.\\data\\f_review.json', orient='index', index=True, indent=4, force_ascii=False)
