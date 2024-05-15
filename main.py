#%%
from calltracking_ru import get_call_df
from y_metrika import get_metrika_df
from datetime import date, timedelta
from sqlalchemy import create_engine
import pandas as pd
import telebot
from config import SQL_PASS, BOT_TOKEN, MY_TG_ID, TEST_CHANNEL_TG_ID
from postgresql import get_last_day_main_metrics
# %%
bot = telebot.TeleBot(BOT_TOKEN, parse_mode='HTML')
engine = create_engine(f'postgresql://postgres:{SQL_PASS}@localhost:5432/SEM')
yesterday = date.today() - timedelta(5)

df_metrika = get_metrika_df(yesterday)
df_calls = get_call_df(yesterday)
if df_calls.empty:
    df_metrika[['calls', 'duration']] = 0
    df_merge = df_metrika
else:
    df_calls_grouped = df_calls.groupby(['campaign', 'date'], as_index=False)[['calls', 'duration']].sum()
    df_merge = df_metrika.merge(df_calls_grouped, on=['campaign', 'date'], how='outer')
    df_calls['key_column'] = df_calls['campaign'] + df_merge['date'].apply(lambda x: x.strftime('_%Y_%m_%d'))
    try:
        df_calls.to_sql('call_records', engine, index=False, if_exists='append')
    except Exception as e: 
        bot.send_message(MY_TG_ID, 'В SQL БД уже есть записи звонков которые скрипт пытается выгрузить')
        
df_merge['key_column'] = df_merge['campaign'] + df_merge['date'].apply(lambda x: x.strftime('_%Y_%m_%d'))
try:
    df_merge.to_sql('main_metrics', engine, index=False, if_exists='append')
except:
    bot.send_message(MY_TG_ID, 'В SQL БД уже есть записи основных метрик которые скрипт пытается выгрузить')
#%%
df_report = get_last_day_main_metrics()

msg = f'__________________________\n <b>Отчет за {yesterday.strftime('%d.%m.%Y')}</b> \n \n \n '
for index, row in df_report.iterrows():
    msg += (f"{index+1}. {row['campaign']} - {row['cost']}руб.({"{:+}".format(row['cost_diff'])}%)" 
          +f" // {row['clicks']}клк.({"{:+}".format(row['clicks_diff'])}%)" 
          +f" // {row['calls']}звн.({"{:+}".format(row['calls_diff'])}%)" 
          +"\n \n")

if not df_calls.empty:
    msg += f'\n Записи звонков: \n \n '
    for index, row in df_calls.iterrows():
        msg += (f"{index+1}. {row['caller']} - {row['duration']}сек."
                f", <a href='{row['recordlink']}'> запись звонка</a>"
                +"\n \n")

bot.send_message(TEST_CHANNEL_TG_ID, msg, parse_mode='HTML')
#%%
df_report