from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection1 = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20221220',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }
connection2 = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }
# Дефолтные параметры, которые прокидываются в таски

default_args = {
'owner': 'p-erofeev',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2023, 1, 29),
}

# Интервал запуска DAG

schedule_interval = '0 17 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_etl_erofeev():

    @task()
    def extract_actions():
        q1 = """
        SELECT
        user_id,
        toDate(time) as event_date,
        os,
        gender,
        age,
        countIf(action = 'like') AS likes,
        countIf(action = 'view') AS views
        FROM simulator_20221220.feed_actions
        WHERE event_date=today()-1
        GROUP BY
        user_id,
        os,
        gender,
        age,
        event_date"""
        df_cube_actions = ph.read_clickhouse(q1, connection=connection1)
        return df_cube_actions

    @task()
    def extract_messages():
        query2 = """
        SELECT user_id, event_date, os, gender, age, messages_received, messages_sent, users_received, users_sent 
        FROM 
            (SELECT user_id, toDate(time) AS event_date, os, gender, age,
            COUNT(reciever_id) AS messages_sent,
            COUNT(distinct reciever_id) AS users_sent
            FROM simulator_20221220.message_actions
            WHERE event_date=today()-1
            GROUP BY user_id, os, gender, age, event_date) AS sent
        INNER JOIN
            (SELECT reciever_id,
            COUNT(user_id) AS messages_received,
            COUNT(distinct user_id) AS users_received
            FROM simulator_20221220.message_actions
            WHERE toDate(time)=today()-1
            GROUP BY reciever_id) received
        ON received.reciever_id=sent.user_id
        """
        df_cube_messages = ph.read_clickhouse(query2, connection=connection1)
        return df_cube_messages

    @task
    def transform_merge(df_cube_actions, df_cube_messages):
        df_cube_all = df_cube_actions.merge(df_cube_messages, how='outer', on=['user_id', 'event_date', 'age', 'os',  'gender'])
        return df_cube_all

    @task
    def transform_os(df_cube_all):
        df_os=df_cube_all.groupby(['os','event_date'])\
        [['messages_received','messages_sent','users_received','users_sent','likes','views']].sum().reset_index()
        df_os.rename(columns = {'os' : 'dimension_value'}, inplace = True)
        df_os['dimension'] = 'os'
        return df_os

    @task
    def transform_gender(df_cube_all):
        df_gender=df_cube_all.groupby(['gender','event_date'])\
        [['messages_received','messages_sent','users_received','users_sent','likes','views']].sum().reset_index()
        df_gender.rename(columns = {'gender' : 'dimension_value'}, inplace = True)
        df_gender['dimension'] = 'gender'
        return df_gender

    @task
    def transform_age(df_cube_all):
        df_cube_all['age'] = pd.cut(df_cube_all['age'], bins=[0, 18, 30, 50, 100], labels=['до 18', '18-30', '30-50', '50+'])
        df_age = df_cube_all.groupby(['age', 'event_date'])\
        [['messages_received', 'messages_sent', 'users_received', 'users_sent', 'likes', 'views']].sum().reset_index()
        df_age.rename(columns = {'age' : 'dimension_value'}, inplace = True)
        df_age['dimension'] = 'age'
        return df_age

    @task
    def transform_union(df_gender, df_os, df_age):
        df_union = pd.concat([df_gender, df_os, df_age]).reset_index()
        df_union = df_union.drop(['index'], axis=1)
        df_union['messages_received'] = df_union['messages_received'].astype('int')
        df_union['messages_sent'] = df_union['messages_sent'].astype('int')
        df_union['users_received'] = df_union['users_received'].astype('int')
        df_union['users_sent'] = df_union['users_sent'].astype('int')
        df_union['likes'] = df_union['likes'].astype('int')
        df_union['views'] = df_union['views'].astype('int')
        return df_union

    @task
    def load_union(df_union):
        
        create = """CREATE TABLE IF NOT EXISTS test.erofeev_etl_1
            (
            event_date Date,
            dimension String,
            dimension_value String,
            views UInt64,
            likes UInt64,
            messages_received UInt64,
            messages_sent UInt64,
            users_received UInt64,
            users_sent UInt64
            )
            ENGINE = MergeTree()
            ORDER BY event_date
            """
        ph.execute(create, connection = connection2)
        ph.to_clickhouse(df_union, 'erofeev_etl_1', index = False, connection = connection2)


    df_cube_messages = extract_messages()
    df_cube_actions = extract_actions()
    df_cube_all = transform_merge(df_cube_actions, df_cube_messages)
    df_os = transform_os(df_cube_all)
    df_gender = transform_gender(df_cube_all)
    df_age = transform_age(df_cube_all)
    df_union = transform_union(df_gender, df_os, df_age)
    load_union(df_union)


dag_etl_erofeev=dag_etl_erofeev()