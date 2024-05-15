#%%
# Импортируем библиотеку requests 
import requests
import pandas as pd
from datetime import datetime, date, timedelta

from config import ACC_SETTINGS
#%%
URL_PARAM = "https://api-metrika.yandex.net/stat/v1/data"
# _date = date.today() - timedelta(1)

def get_metrika_df(_date):
    header_params = {
        'GET': '/management/v1/counters HTTP/1.1',
        'Host': 'api-metrika.yandex.net',
        'Authorization': 'OAuth ' + ACC_SETTINGS['Interfix']['metrika_token'],
        'Content-Type': 'application/x-yametrika+json'
        }
    api_param = {
            "metrics":'ym:ev:expenses<currency>, ym:ev:expenseClicks',
            "dimensions":'ym:ev:<attribution>ExpenseCampaign',
            "date1":_date,
            "date2":_date,
            "accuracy":"full",
            "limit":100000,
            "direct_client_logins":"accountdirect-198096-hy10",
            'ids':ACC_SETTINGS['Interfix']['metrika_id']
            }  

    result = (requests.get(URL_PARAM,params=api_param,headers=header_params)).json()
    if len(result['data'])==0:
        return pd.DataFrame()
    df = pd.DataFrame(result['data'])
    df['campaign'] = df['dimensions'].apply(lambda x:x[0]['name'])
    df['cost'] = df['metrics'].apply(lambda x:x[0])
    df['clicks'] = df['metrics'].apply(lambda x:x[1])
    df.drop(['dimensions', 'metrics'], inplace=True, axis=1)
    df['clicks'] = df['clicks'].apply(int)
    df['date'] = _date
    df = df.query('cost!=0 & clicks!=0')
    return df
# df = get_metrika_df(_date)
# df