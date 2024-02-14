from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


#параметры подключения к CH
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20231113'
}



# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'k-prohodtsev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 12, 13),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'



@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_kprohodtsev_test():

    # вытаскиваем данные из feed_actions
    @task()
    def extract_fa():
        query_fa = """SELECT yesterday() as event_date, 
                        user_id, countIf(action = 'view') as views, 
                        countIf(action = 'like') as likes,   
                        if(max(gender)=1, 'male', 'female') as gender,
                        max(age) as age,
                        max(os) as os
                FROM  simulator_20231113.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY user_id"""
        df_fa = ph.read_clickhouse(query=query_fa, connection=connection)
        return df_fa

    
    # вытаскиваем данные из message_actions
    @task()
    def extract_msg():
        query_msg = """select yesterday() as event_date,
                        user_id,
                        count(receiver_id) as messages_sent,
                        countIf(user_id in  (select receiver_id from simulator_20231113.message_actions
                        where toDate(time) = yesterday())
                        ) as messages_received,
                        count(distinct receiver_id) as users_sent,
                        countIf(user_id in  (select distinct receiver_id from simulator_20231113.message_actions
                        where toDate(time) = yesterday())
                        ) as users_received,
                        if(max(gender)=1, 'male', 'female') as gender,
                        max(age) as age,
                        max(os) as os
                from simulator_20231113.message_actions
                where toDate(time) = yesterday()
                group by user_id"""
        df_msg = ph.read_clickhouse(query=query_msg, connection=connection)
        return df_msg
        
    
    
    # объединяем message_actions и feed_actions
    @task
    def merge_tables(df_fa, df_msg):
        df_glbl = df_msg.merge(df_fa, how='outer', on= ['event_date', 'user_id', 'gender', 'age', 'os'])
        return df_glbl
    
    
    
    # делаем срез для OS
    @task
    def transfrom_os(df_glbl):
        os_srez = df_glbl.groupby(['event_date','os'],as_index=False)\
                .agg(views = ('views', 'sum'),
                    likes = ('likes', 'sum'),
                     messages_received = ('messages_received', 'sum'),
                     messages_sent = ('messages_sent', 'sum'),
                     users_received = ('users_received', 'sum'),
                     users_sent = ('users_sent', 'sum'),
                    )\
                .rename(columns ={'os':'dimension_value'})\
        
        os_srez.insert( 1 ,'dimension', value = 'os')
        return os_srez

    
    # делаем срез для GENDER
    @task
    def transfrom_gender(df_glbl):
        gender_srez = df_glbl.groupby(['event_date','gender'],as_index=False)\
                .agg(views = ('views', 'sum'),
                    likes = ('likes', 'sum'),
                     messages_received = ('messages_received', 'sum'),
                     messages_sent = ('messages_sent', 'sum'),
                     users_received = ('users_received', 'sum'),
                     users_sent = ('users_sent', 'sum'),
                    )\
                .rename(columns ={'gender':'dimension_value'})\
        
        gender_srez.insert( 1 ,'dimension', value = 'gender')
        return gender_srez

    
    # делаем срез для AGE
    @task
    def transfrom_age(df_glbl):
        age_srez = df_glbl.groupby(['event_date','age'],as_index=False)\
                .agg(views = ('views', 'sum'),
                    likes = ('likes', 'sum'),
                     messages_received = ('messages_received', 'sum'),
                     messages_sent = ('messages_sent', 'sum'),
                     users_received = ('users_received', 'sum'),
                     users_sent = ('users_sent', 'sum'),
                    )\
                .rename(columns ={'age':'dimension_value'})\
        
        age_srez.insert( 1 ,'dimension', value = 'age')
        return age_srez
    
    
    
    #объединяем все сразы и подготавливает к загрузке на СН
    @task
    def transform_union(os_srez, gender_srez, age_srez):
        df_all = pd.concat([os_srez, gender_srez, age_srez]).reset_index()
        df_all = df_all.drop(['index'], axis=1)
        df_all = df_all.astype({'likes': 'int64', 'views': 'int64', 'messages_received': 'int64', 'messages_sent': 'int64',  
                                'users_received': 'int64', 'users_sent': 'int64'})
        df_all = (df_all[['dimension', 'dimension_value', 'event_date', 'likes', 'views', 'messages_received','messages_sent', 'users_received', 'users_sent']])
        return df_all

    
    
    # создаем таблицу kprohodtsev_L8_test и загружаем туда данные
    @task
    def load(df_all):
        connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'}
        
        query_test ="""CREATE TABLE IF NOT EXISTS test.kprohodtsev_L8_test
                (event_date Date,
                dimension String,
                dimension_value String,
                likes UInt64,
                views UInt64,
                messages_received UInt64,
                messages_sent UInt64,
                users_received UInt64,
                users_sent UInt64)
                ENGINE = MergeTree()
                ORDER BY event_date"""

        
        ph.execute(query = query_test, connection = connection_test)
        ph.to_clickhouse(df=df_all, table='kprohodtsev_L8_test', index = False, connection = connection_test)

    
        

    df_fa = extract_fa()
    df_msg = extract_msg()
    df_glbl = merge_tables(df_fa, df_msg)
    os_srez = transfrom_os(df_glbl)
    gender_srez = transfrom_gender(df_glbl)
    age_srez = transfrom_age(df_glbl)
    
    df_all = transform_union(os_srez, gender_srez, age_srez)
    
    load(df_all)
    

dag_kprohodtsev_test = dag_kprohodtsev_test()
