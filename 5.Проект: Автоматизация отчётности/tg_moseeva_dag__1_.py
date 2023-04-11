#!/usr/bin/env python
# coding: utf-8

# In[4]:


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

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20221220'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-moseeva-12',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 01, 25),
}

schedule_interval = '0 11 * * *' # отчет приходит каждый день в 11 утра

my_token = 'ххххххххххххххххххххххххххххх'
bot = telegram.Bot(token=my_token)

chat_id = 1787919430

@dag(default_args=default_args, schedule_interval='0 11 * * *', catchup=False)
def task7_moseeva_feed_report():

    
    @task
    # Загружаем данные за прошлый день
    def extract_yesterday():
        q = """
        SELECT max(toDate(time)) as day, 
        count(DISTINCT user_id) as DAU, 
        sum(action = 'like') as likes,
        sum(action = 'view') as views, 
        likes/views as CTR
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) = yesterday()
        """

        report_last_day = ph.read_clickhouse(q, connection=connection)
        return report_last_day
    
    @task
    # Загружаем данные за прошлую неделею
    def extract_last_week():
        
        q_2 = """
        SELECT toDate(time) as day, 
        count(DISTINCT user_id) as DAU, 
        sum(action = 'like') as likes,
        sum(action = 'view') as views, 
        likes/views as CTR
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) > today()-8 AND toDate(time) < today()
        GROUP BY day
        """

        report_week = ph.read_clickhouse(q_2, connection=connection)
        return report_week

    @task()
    def send_message_last_day(report_last_day, chat_id):
        dau = report_last_day['DAU'].sum()
        views = report_last_day['views'].sum()
        likes = report_last_day['likes'].sum()
        ctr = report_last_day['CTR'].sum()

        m_1 = '-' * 20 + '\n\n' + f'Статистика ленты новостей за вчера:\n\nDAU: {dau}\nПросмотры: {views}\nЛайки: {likes}\nCTR: {ctr:.2f}\n' + '-' * 20 + '\n'

        bot.sendMessage(chat_id=chat_id, text=msg)
        

        
        
    @task
    def send_photo_week(report_week, chat_id):
        fig, axes = plt.subplots(2, 2, figsize=(20, 14))

        fig.suptitle('Динамика показателей за последние 7 дней', fontsize=30)

        sns.lineplot(ax = axes[0, 0], data = report_week, x = 'event_date', y = 'DAU')
        axes[0, 0].set_title('DAU')
        axes[0, 0].grid()

        sns.lineplot(ax = axes[0, 1], data = report_week, x = 'event_date', y = 'CTR')
        axes[0, 1].set_title('CTR')
        axes[0, 1].grid()

        sns.lineplot(ax = axes[1, 0], data = report_week, x = 'event_date', y = 'views')
        axes[1, 0].set_title('Просмотры')
        axes[1, 0].grid()

        sns.lineplot(ax = axes[1, 1], data = report_week, x = 'event_date', y = 'likes')
        axes[1, 1].set_title('Лайки')
        axes[1, 1].grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Stats.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        send_message_yesterday(report_last_day, group_chat_id)
        send_photo_week(report_week, group_chat_id)
        

task7_moseeva_feed_report = task7_moseeva_feed_report()
