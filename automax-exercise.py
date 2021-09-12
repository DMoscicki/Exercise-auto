import psycopg2
import pandas.io.sql as pds
import pandas as pd
from pandas import DataFrame


connection = psycopg2.connect(host='analytics.maximum-auto.ru', user='dmitry_mastitskiy', port='15432',
                              password='h5gvfF0NuxqF', dbname='data')

cursor = connection.cursor()

#-------------------------------------------------A-------------------------------------------------------------
cursor.execute('''
SELECT
    communications.visitor_id,
    communications.site_id,
    communications.date_time as communication_date_time,
    communications.communication_id,
    sessions.visitor_session_id,
    sessions.campaign_id,
    sessions.site_id,
    sessions.visitor_id,
    sessions.date_time,
    row_number() over (partition by sessions.visitor_id, sessions.site_id order by sessions.date_time) as row_n,
    lag(sessions.date_time) over (partition by communications.visitor_id, communications.site_id order by sessions.date_time) as row_nn,
	last_value(sessions.visitor_session_id) over (partition by communications.communication_id order by sessions.date_time)
FROM
    communications
left JOIN
    sessions
ON sessions.visitor_id = communications.visitor_id''')

dfsql = DataFrame(cursor.fetchall())

pd.set_option('display.expand_frame_repr', False)

print(dfsql)

#-------------------------------------------------B-------------------------------------------------------------

communications  = pds.read_sql("SELECT * FROM communications;", connection)
sessions = pds.read_sql("select * from sessions;", connection)

df1 = pd.DataFrame(communications)
df2 = pd.DataFrame(sessions)

df = df1.merge(df2, on='visitor_id', how='left')
df['row_n'] = df.sort_values(['visitor_id','date_time_y'], ascending=[False, True]).groupby(['visitor_id', 'site_id_y'])\
                  .cumcount()+1 # нумерация сессий по дате в рамках сайта
df['row_nn'] = df.groupby(['visitor_id','site_id_x'])['date_time_y'].shift(1) # последняя сессия на момент обращения

pd.set_option('display.expand_frame_repr', False)

print(df)

connection.close()