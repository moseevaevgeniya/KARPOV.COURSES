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

# запросы для вчерашних метрик

# DAU

q1 = """
    SELECT
      toStartOfDay(toDateTime(time)) AS event_date,
      count(DISTINCT user_id) AS DAU
    FROM
      simulator_20221220.feed_actions
    WHERE
      toStartOfDay(toDateTime(time)) = yesterday()
    GROUP BY
      toStartOfDay(toDateTime(time))
"""

# views

q2 = """
    SELECT
      toStartOfDay(toDateTime(time)) AS event_date,
      countIf(action = 'view') AS views
    FROM
      simulator_20221220.feed_actions
    WHERE
      toStartOfDay(toDateTime(time)) = yesterday()
    GROUP BY
      toStartOfDay(toDateTime(time))
"""

#likes

q3 = """
    SELECT
      toStartOfDay(toDateTime(time)) AS event_date,
      countIf(action = 'like') AS likes
    FROM
      simulator_20221220.feed_actions
    WHERE
      toStartOfDay(toDateTime(time)) = yesterday()
    GROUP BY
      toStartOfDay(toDateTime(time))
"""

#ctr

q4 = """
    SELECT
      toStartOfDay(toDateTime(time)) AS event_date,
      ROUND(countIf(action = 'like') / countIf(action = 'view'), 3) AS ctr
    FROM
      simulator_20221220.feed_actions
    WHERE
      toStartOfDay(toDateTime(time)) = yesterday()
    GROUP BY
      toStartOfDay(toDateTime(time))
"""

# запросы для метрик прошлой недели

# DAU

q5 = """
    SELECT
      toStartOfDay(toDateTime(time)) AS event_date,
      count(DISTINCT user_id) AS DAU
    FROM
      simulator_20221220.feed_actions
    WHERE
      toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 6, yesterday())
      AND yesterday()
    GROUP BY
      toStartOfDay(toDateTime(time))
    ORDER BY
      event_date
"""

# views

q6 = """
    SELECT
      toStartOfDay(toDateTime(time)) AS event_date,
      countIf(action = 'view') AS views
    FROM
      simulator_20221220.feed_actions
    WHERE
      toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 6, yesterday())
      AND yesterday()
    GROUP BY
      toStartOfDay(toDateTime(time))
    ORDER BY
      event_date
"""

#likes

q7 = """
    SELECT
      toStartOfDay(toDateTime(time)) AS event_date,
      countIf(action = 'like') AS likes
    FROM
      simulator_20221220.feed_actions
    WHERE
      toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 6, yesterday())
      AND yesterday()
    GROUP BY
      toStartOfDay(toDateTime(time))
    ORDER BY
      event_date
"""

#ctr

q8 = """
    SELECT
      toStartOfDay(toDateTime(time)) AS event_date,
      ROUND(countIf(action = 'like') / countIf(action = 'view'), 3) AS ctr
    FROM
      simulator_20221220.feed_actions
    WHERE
      toStartOfDay(toDateTime(time)) BETWEEN timestamp_sub(DAY, 6, yesterday())
      AND yesterday()
    GROUP BY
      toStartOfDay(toDateTime(time))
    ORDER BY
      event_date
"""

# инфа для телеги
my_token = 'фффффффффффф'
bot = telegram.Bot(token=my_token)
chat_id = 1787919430

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def e_moseeva_dag_lesson_7_1():
    
    # получаем метрики и отправляем в чат
    @task()
    def get_and_send_metrics_info():
        yes_DAU = ph.read_clickhouse(q1, connection=connection).DAU[0]
        yes_views = ph.read_clickhouse(q2, connection=connection).views[0]
        yes_likes = ph.read_clickhouse(q3, connection=connection).likes[0]
        yes_ctr = ph.read_clickhouse(q4, connection=connection).ctr[0]
        yes = date.today() - timedelta(days=1)
        msg1 = f"""Key metrics for {yes.strftime('%d-%m-%Y')}:
                     DAU  -  {yes_DAU}
                     views  -  {yes_views}
                     likes  -  {yes_likes}
                     ctr  -  {yes_ctr}"""
        bot.sendMessage(chat_id=chat_id, text=msg1)
        start_date = date.today() - timedelta(days=7)
        end_date = date.today() - timedelta(days=1)
        msg2 = f"Key metrics for the last week ({start_date.strftime('%d-%m-%Y')} - {end_date.strftime('%d-%m-%Y')})"
        bot.sendMessage(chat_id=chat_id, text=msg2)
        for q in [q5, q6, q7, q8]:
            df = ph.read_clickhouse(q, connection=connection)
            plt.figure(figsize=(12, 6))
            plt.rcParams['font.size'] = '10'
            sns.lineplot(df.event_date, df[df.columns[1]])
            plt.title(f'{df.columns[1]}', fontsize=20)
            plt.xlabel('Day')
            plt.ylabel(f'{df.columns[1]}')
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = f'{df.columns[1]}_lw.png'
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    get_and_send_metrics_info()

e_moseeva_dag_lesson_7_1 = e_moseeva_dag_lesson_7_1()
