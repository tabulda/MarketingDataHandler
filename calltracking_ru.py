#%%
import requests
import pandas as pd
from datetime import datetime, date
from datetime import timedelta
from config import ACC_SETTINGS
# %%
def get_call_df(_date):
    # YESTERDAY = (date.today() - timedelta(14)).strftime('%Y-%m-%d') # Поменять 0 на 1
    # YESTERDAY = '2024-04-18'
    date_for_download = _date.strftime('%Y-%m-%d')
    API_URL = 'https://calltracking.ru/api/get_data.php'
    API_PARAM = {
        "project":"10382",
        "dimensions":"ct:dcampaign, ct:recordlink, ct:cid, ct:caller",
        "metrics":"ct:calls, ct:duration, ct:answer_time",
        "start-date":date_for_download,
        "end-date":date_for_download,
        "max-results":"100",
        "start-index":"0",
        "auth": ACC_SETTINGS['Interfix']['calltracking_ru_token'],
        "view-type":"list",
        "scope-unique":0
        }

    response = requests.get(API_URL,params=API_PARAM)
    result = response.json()
    df_calls = pd.DataFrame(result['data'])
    if df_calls.empty:
        return df_calls
    df_calls.rename({'dcampaign':'campaign'},inplace=True, axis=1)
    df_calls['duration'] = (df_calls['duration'].apply(lambda x: int(float(x))) 
                            - df_calls['answer_time'].apply(lambda x: int(float(x))))
    df_calls['calls'] = df_calls['calls'].apply(lambda x: int(x))
    df_calls.drop('answer_time', axis=1, inplace=True)
    df_calls['date'] = _date
    return df_calls
_date = date.today() - timedelta(2)
df = get_call_df(_date)
df