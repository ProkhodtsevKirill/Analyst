#импорт библиотек
import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import io
import telegram

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
    'start_date': datetime(2023, 12, 17),
}

# Отчет должен приходить ежедневно в 11:00
schedule_interval = '0 11 * * *'



@dag(default_args=default_args, 
     schedule_interval=schedule_interval, 
     catchup=False)

def dag_bot_by_kprohodtsev_test():
    
    # вытаскиваем данные из feed_actions
    @task()
    def extract_fa():
        query_data = """
                select toDate(time) as date , 
                count (distinct user_id) as DAU, 
                countIf(action='like') as likes, 
                countIf(action='view') as views,
                likes/views as CTR
                FROM simulator_20231113.feed_actions 
                where toDate(time) >= today()-7 and toDate(time) <= yesterday()
                group by date
                """

        df_data = ph.read_clickhouse(query = query_data, connection=connection)
        return df_data
    
    
    

    # создаем метрики и текст с ними
    @task()
    def create_metrics(df_data):
        df_yesterday = df_data[df_data.date == df_data.date.max()]
        yesterday = df_yesterday.date.iloc[0]
        yesterday = str(datetime.date(yesterday))
        DAU = df_yesterday.DAU.iloc[0]
        likes = df_yesterday.likes.iloc[0]
        views = df_yesterday.views.iloc[0]
        ctr = df_yesterday.CTR.iloc[0]
        CTR = str(ctr.round(6)*100)+'%'
        text = f'Ключевые метрики за {yesterday}:\nDAU: {DAU}\nПросмотры: {views}\nЛайки: {likes}\nCTR: {CTR}'
        
        return text
    
    
    
    # создаем граффики и кладем их в буффер
    @task()
    def graph(df_data, metric):
        sns.set_style("darkgrid")
        plt.figure(figsize=(8, 4))
        pic = sns.lineplot(x=df_data['date'], y=df_data[metric])
        plt.title(f'{metric} за предыдущие 7 дней')
        pic.set(xlabel=None, ylabel=None)

        buffer = io.BytesIO()                    #создали буффер куда будем сохранять граффики
        plt.savefig(buffer, format='png')        #сохраняем наш граффик
        buffer.seek(0)                           #передвинули курсор в начало файлового объекта

        plt.clf()
        plt.close()

        return buffer
    
    
    #параметры бота и чата, отправка изображений из буффера
    @task
    def send_message(text, photos):
        my_token = '6782704467:AAE1GY-vF9TlKYDAalS-UmsOxGMhA0rX7bo'
        chat_id=-938659451                      #'ОТЧЕТЫ | KC Симулятор Аналитика'
        bot = telegram.Bot(token=my_token)  
        # updates = bot.getUpdates() этим методом мы определили наш чат ид.
        # print(updates[-1]) ....'chat': {'id': 355098206....  может быть с минусом, его тоже нужно брать

        bot.sendMessage(chat_id=chat_id, text=text)
        for photo in photos:
            bot.sendPhoto(chat_id=chat_id, photo=photo)

            
    df_data = extract_fa()
    text = create_metrics(df_data)
    photo_DAU = graph(df_data, metric = 'DAU')
    photo_likes = graph(df_data, metric = 'likes')
    photo_views = graph(df_data, metric = 'views')
    photo_CTR = graph(df_data, metric = 'CTR')
    photos = [photo_DAU, photo_likes, photo_views, photo_CTR]
    send_message(text, photos)


dag_bot_by_kprohodtsev_test = dag_bot_by_kprohodtsev_test()








