from datetime import datetime, timedelta, date
import pandas as pd
import pandahouse as ph
import numpy as np
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'e-moseeva-12',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
    'start_date': datetime(2023, 1, 25),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

# связки с clickHouse
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database': 'simulator_20221220',
                      'user': 'student',
                      'password': 'dpo_python_2020'
                      }

yes = date.today() - timedelta(days=1)
start_date = date.today() - timedelta(days=14)

# запросы для вчерашних метрик

# запрос на формирование DAU и daily actions per user - среднее количество действий (просмотр, лайк, сообщение) на одного пользователя, округлённое до целого, - за последние 14 дней

q1 = '''
    SELECT
      d.event_date,
      d.DAU,
      ROUND(t.actions / d.DAU) AS daily_actions
    FROM
      (
        SELECT
          event_date,
          count(DISTINCT user_id) AS DAU
        FROM
          (
            SELECT
              DISTINCT user_id,
              toStartOfDay(toDateTime(time)) AS event_date
            FROM
              simulator_20221220.feed_actions
            WHERE
              toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 13, yesterday())
              AND yesterday()
            UNION ALL
            SELECT
              DISTINCT user_id,
              toStartOfDay(toDateTime(time)) AS event_date
            FROM
              simulator_20221220.message_actions
            WHERE
              toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 13, yesterday())
              AND yesterday()
          )
        GROUP BY
          event_date
        ORDER BY
          event_date
      ) d
      JOIN (
        SELECT
          event_date,
          COUNT(user_id) AS actions
        FROM
          (
            SELECT
              toStartOfDay(toDateTime(time)) AS event_date,
              user_id
            FROM
              simulator_20221220.feed_actions
            WHERE
              toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 13, yesterday())
              AND yesterday()
            UNION ALL
            SELECT
              toStartOfDay(toDateTime(time)) AS event_date,
              user_id
            FROM
              simulator_20221220.message_actions
            WHERE
              toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 13, yesterday())
              AND yesterday()
          ) a
        GROUP BY
          event_date
        ORDER BY
          event_date,
          actions DESC
      ) t USING event_date
    ORDER BY
      event_date
'''

# запрос на CTR / likes / views / количество сообщений с разбивкой по полу и платформе за последние 14 дней

q2 = '''
    SELECT
      toDate(t.event_date) AS event_date,
      t.os,
      CASE
        WHEN t.gender = 1 THEN 'male'
        ELSE 'female'
      END AS gender,
      t2.likes,
      t2.views,
      t2.ctr,
      t.messages_sent
    FROM
      (
        SELECT
          toStartOfDay(toDateTime(time)) as event_date,
          os,
          gender,
          COUNT(user_id) AS messages_sent
        FROM
          simulator_20221220.message_actions
        WHERE
          toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 13, yesterday())
          AND yesterday()
        GROUP BY
          toStartOfDay(toDateTime(time)),
          os,
          gender
        ORDER BY
          event_date,
          os,
          gender
      ) AS t
      JOIN (
        SELECT
          toStartOfDay(toDateTime(time)) as event_date,
          os,
          gender,
          countIf(action = 'like') AS likes,
          countIf(action = 'view') AS views,
          ROUND(
            countIf(action = 'like') / countIf(action = 'view'),
            3
          ) AS ctr
        FROM
          simulator_20221220.feed_actions
        WHERE
          toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 13, yesterday())
          AND yesterday()
        GROUP BY
          toStartOfDay(toDateTime(time)),
          os,
          gender
        ORDER BY
          event_date,
          os,
          gender
      ) AS t2 ON t.event_date = t2.event_date
      AND t.gender = t2.gender
      AND t.os = t2.os
    ORDER BY
      event_date,
      os,
      gender
'''

# Retention Rate за последние 14 дней
q3 = """
    SELECT
      toString(start) AS start_day,
      toString(dates) AS day,
      count(user_id) AS users
    FROM
      (
        SELECT
          user_id,
          min(toDate(time)) AS start
        FROM
          (
            SELECT
              DISTINCT user_id,
              toDate(time) AS time
            FROM
              simulator_20221220.feed_actions
            UNION ALL
            SELECT
              DISTINCT user_id,
              toDate(time) AS time
            FROM
              simulator_20221220.message_actions
          ) AS t
        GROUP BY
          user_id
        ORDER BY
          start
      ) t1
      JOIN (
        SELECT
          DISTINCT user_id,
          toDate(time) AS dates
        FROM
          (
            SELECT
              DISTINCT user_id,
              toDate(time) AS time
            FROM
              simulator_20221220.feed_actions
            UNION ALL
            SELECT
              DISTINCT user_id,
              toDate(time) AS time
            FROM
              simulator_20221220.message_actions
          ) AS d
      ) AS t2 USING user_id
    WHERE
      start BETWEEN timestamp_sub(DAY, 13, yesterday())
      AND yesterday()
      AND dates BETWEEN timestamp_sub(DAY, 13, yesterday())
      AND yesterday()
    GROUP BY
      start_day,
      day
    ORDER BY
      start_day,
      day
"""

# запрос количество совершенных действий с разбивкой по странам за последние 14 дней

q4 = """
    SELECT
      event_date,
      country,
      COUNT(user_id) AS actions
    FROM
      (
        SELECT
          toStartOfDay(toDateTime(time)) AS event_date,
          user_id,
          country
        FROM
          simulator_20221220.feed_actions
        WHERE
          toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 13, yesterday())
          AND yesterday()
        UNION ALL
        SELECT
          toStartOfDay(toDateTime(time)) AS event_date,
          user_id,
          country
        FROM
          simulator_20221220.message_actions
        WHERE
          toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 13, yesterday())
          AND yesterday()
      ) a
    GROUP BY
      event_date,
      country
    ORDER BY
      event_date,
      actions DESC
"""

# инфа для телеги
my_token = 'ссссссссссссссссссссссссссссссссссс'
bot = telegram.Bot(token=my_token)
chat_id = 1787919430

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def e_moseeva_dag_lesson_7_2():
    
    # получаем метрики DAU и daily actions per user за последние 14 дней - и отправляем в телегу инфо и графики
    @task()
    def get_DAU_and_actions():
        msg = f"""Metrics over the last two weeks ({start_date} - {yes}):\n(for more detailed info go to the dashboard: https://superset.lab.karpov.courses/superset/dashboard/2506/):"""
        bot.sendMessage(chat_id=chat_id, text=msg)
        d1 = ph.read_clickhouse(q1, connection=connection)
        msg = f"""DAU & daily actions per user for {yes.strftime('%d-%m-%Y')}:
            DAU  -  {d1[d1.columns[1]].iloc[-1]}
            daily actions per user  -  {d1[d1.columns[2]].iloc[-1]}\nDAU & daily actions per user over the last two weeks ({start_date} - {yes}):"""
        bot.sendMessage(chat_id=chat_id, text=msg)
        
        # графики - DAU
        plt.figure(figsize=(16, 8))
        sns.lineplot(d1.event_date, d1.DAU, marker='o')
        plt.title('DAU', fontsize=20)
        plt.grid(True)
        plt.xlabel('Day')
        plt.ylabel('Active Unique Users')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'DAU_last_two_weeks.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        # графики - daily actions per user
        plt.figure(figsize=(16, 8))
        sns.lineplot(d1.event_date, d1.daily_actions, marker='o')
        plt.title('Daily Actions per User', fontsize=20)
        plt.grid(True)
        plt.xlabel('Day')
        plt.ylabel('Actions per User')
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Daily_Actions_per_User_last_two_weeks.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        passed1 = True
        return passed1

    # получаем метрики CTR / likes / views / количество сообщений с разбивкой по полу и платформе за последние 14 дней - и отправляем в телегу csv
    @task()
    def get_actions_os_gender(passed1=None):
        if passed1:
            msg = f"""Likes & Views & CTR & Messages sent by gender and os over the last two weeks ({start_date} - {yes}) in the CSV file:"""
            bot.sendMessage(chat_id=chat_id, text=msg)
            d2 = ph.read_clickhouse(q2, connection=connection)
            file_object = io.StringIO()
            d2.to_csv(file_object)
            file_object.name = 'actions_by_os_gender_last_two_weeks.csv'
            file_object.seek(0)
            bot.sendDocument(chat_id=chat_id, document=file_object)
            passed2 = True
            return passed2
        
    # получаем тепловую карту с Retention Rate за последние 14 дней - и отправляем в телегу график
    @task()
    def get_ret_rate(passed2=None):
        if passed2:
            msg = f"""User Retention Rate over the last two weeks ({start_date} - {yes}):"""
            bot.sendMessage(chat_id=chat_id, text=msg)
            d3 = ph.read_clickhouse(q3, connection=connection)
            # создадим функцию, которая будет считать retention пользователей
            def calculate_retention_rate(df):
                grouped = df.groupby(['start_day', 'day'])
                cohorts = grouped.agg({'users': 'sum'})
                
                def cohort_period(df):
                    df['day'] = np.arange(len(df))
                    return df

                cohorts = cohorts.groupby(level=0).apply(cohort_period)
                cohort_group_size = cohorts['users'].groupby(level=0).first()
                user_retention = cohorts['users'].unstack(0).divide(cohort_group_size, axis=1)
                return user_retention
            # heatmap
            d_ur = calculate_retention_rate(d3)
            sns.set(style='ticks')
            plt.figure(figsize=(20, 8))
            plt.title('Heatmap of User Retention Rate', fontsize=20)
            plt.xlabel('Day')
            plt.ylabel('Registration Date')
            sns.heatmap(d_ur.T, mask=d_ur.T.isnull(), annot=True, fmt='.1%', linewidths=0.1, cbar=False)
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'Heatmap_of_User_Retention_Rate_last_two_weeks.png'
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            passed3 = True
            return passed3

    # получаем тепловую карту с Retention Rate за последние 14 дней - и отправляем в телегу график
    @task()
    def get_actions_country(passed3=None):
        if passed3:
            d4 = ph.read_clickhouse(q4, connection=connection)
            d4_yes = d4[d4.event_date.dt.date == yes]
            msg = f"""Total actions (messages / likes / views) by country for {yes}:"""
            bot.sendMessage(chat_id=chat_id, text=msg)      
            msg = ''
            for row in d4_yes.itertuples(index=False):
                msg = msg + str(row[1]) + ': ' + str(row[2]) + '\n'
            bot.sendMessage(chat_id=chat_id, text=msg)
            msg = f"""Total actions (messages / likes / views) by country over the last two weeks ({start_date} - {yes}) in the CSV file:"""
            bot.sendMessage(chat_id=chat_id, text=msg)
            file_object = io.StringIO()
            d4.to_csv(file_object)
            file_object.name = 'total_actions_by_country_last_two_weeks.csv'
            file_object.seek(0)
            bot.sendDocument(chat_id=chat_id, document=file_object)
            
    passed1 = get_DAU_and_actions()
    passed2 = get_actions_os_gender(passed1)
    passed3 = get_ret_rate(passed2)
    get_actions_country(passed3)
    

e_moseeva_dag_lesson_7_2 = e_moseeva_dag_lesson_7_2()
