#%%
from sqlalchemy import create_engine
import pandas as pd
from config import SQL_PASS
#%%
engine = create_engine(f'postgresql://postgres:{SQL_PASS}@localhost:5432/SEM')
def get_last_day_main_metrics():
    query = '''
            with last_2_date as (
            SELECT DISTINCT date 
            FROM main_metrics
            ORDER BY date DESC
            LIMIT 2),
            last_date as (
            SELECT DISTINCT date 
            FROM main_metrics
            ORDER BY date DESC
            LIMIT 1)

            SELECT * 
            FROM 
            (SELECT date, campaign, cost::int, 
                (((cost - LAG(cost,1) OVER(partition by campaign order by date)) 
                        / cost::float)*100) as cost_diff,
                clicks,
                (((clicks - LAG(clicks,1) OVER(partition by campaign order by date)) 
                        / clicks::float)*100) as clicks_diff,
                calls,
                (((calls - LAG(calls,1) OVER(partition by campaign order by date)) 
                        / calls::float)*100) as calls_diff
            FROM main_metrics
            WHERE date IN (SELECT * FROM last_2_date)
            ORDER BY campaign, date desc)
            WHERE date=(SELECT * FROM last_date)
            '''
    df_main_report = pd.read_sql(query, engine)
    df_main_report['calls'] = df_main_report['calls'].fillna(0).apply(int)
    df_main_report['cost_diff'] = df_main_report['cost_diff'].fillna(0).apply(int)
    df_main_report['clicks_diff'] = df_main_report['clicks_diff'].fillna(0).apply(int)
    df_main_report['calls_diff'] = df_main_report['calls_diff'].fillna(0).apply(int)
    df_main_report = df_main_report.fillna(0)
    return df_main_report