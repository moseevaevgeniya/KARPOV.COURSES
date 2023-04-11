# Напишем систему алертов для мобильного приложения VK

# Описание задачи:

# Система должна с периодичность каждые 15 минут проверять ключевые метрики, такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений. 

# Изучим поведение метрик и подберём наиболее подходящий метод для детектирования аномалий. 

# В случае обнаружения аномального значения, в чат должен отправиться алерт - сообщение со следующей информацией: метрика, ее значение, величина отклонения.

# импортируем библиотеки
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram as tg
import pandahouse as ps
from datetime import date
import io
from airflow.decorators import dag, task 
from airflow.operators.python import get_current_context


my_token = '5973082967:AAFmnT1Fl6AG0-1Wjnm2DWz9K-5orYL2U_w' 
# токен бота
chat_id = 1787919430
# чат id

#  опишем запрос к БД ленты новостей:
q1 = """
SELECT
toStartOfFifteenMinutes(time) as ts,
toStartOfDay(time) as date,
formatDateTime(ts, '%R') as hm,
uniqExact(user_id) as users_feed,
countIf(action='view') as views,
countIf(action='like') as likes,
likes / views as ctr
FROM simulator_20221220.feed_actions
WHERE ts >= today() - 1 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm 
ORDER BY ts
"""
#  опишем запрос к БД сообщений:
q2 ="""
SELECT
toStartOfFifteenMinutes(time) as ts,
toStartOfDay(time) as date,
formatDateTime(ts, '%R') as hm,
uniqExact(user_id) as act_mes,
count(user_id) as messages
FROM simulator_20221220.message_actions
WHERE ts >= today() - 1 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm
ORDER BY ts
"""

#подключимся к БД:
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20221220',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

# аргументы для подключения к БД:
default_args = {
    'owner': 'e-moseeva-12',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 27),
}

metrics1 = ['users_feed', 'views','likes', 'ctr']
#список метрик для данных ленты новостей
metrics2 = ['act_mes', 'messages']
#список метрик для данных мессенджера

schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_emoseeva_alerts():

    @task
    def extract_feed():
        # подключимся к базе данных и получим данные в датафрейм из ленты новостей
        dffeed = ps.read_clickhouse(q1, connection=connection)
        return dffeed
    
    @task
    def extract_mes():
        dfmes = ps.read_clickhouse(q2, connection=connection)
        # подключимся к базе данных и получим данные в датафрейм из мессенджера
        return dfmes
    def check_anomaly(df, metric, a=4, n=5):
        # функция, в которой опишем сам алгоритм поиска аномалий, и будем применять к данным
        #  в нашем случае это межквартильный размах:
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        # вычисляем 25 квантиль и сглаживаем функцией rolling
        # функцией shift сдвигаем окно на один период назад, тк текущая 15-ти минутка может быть не закончена
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
         # вычисляем 75 квантиль и сглаживаем функцией rolling
        df['iqr'] = df['q75'] - df['q25'] 
        # значения межвантильного размаха
        df['up'] = df['q75'] + a*df['iqr'] 
        #  рассчитываем значение верхней границы
        df['low'] = df['q25'] - a*df['iqr']
        #  рассчитываем значение нижней границы

        
        df['up'] = df['up'].rolling(window=n, center=True, min_periods = 1).mean()
        df['low'] = df['low'].rolling(window=n, center=True, min_periods = 1).mean()

        # рассчитаем условия флага алерт:
        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, df
    
    
    @task
    def check_an(df3, metrics):
    # функция, которая будет запускать нашу систему алертов т.е. подключаться к базе,
    # доставать данные из базы, будем формировать сам алерт, если будет такая необходимость:
        for metric in metrics:
            df = df3[['ts', 'date', 'hm', metric]].copy()
            is_alert, df2 = check_anomaly(df, metric)

            if is_alert == 1:
                msg = '''Метрика {metric}:\nТеущее значение {current_val:.2f}:\nОтклонение {last_val_diff:.2%}'''\
                        .format(metric=metric, current_val=df2[metric].iloc[-1],\
                                last_val_diff=abs(1-df2[metric].iloc[-1]/df[metric].iloc[-2]))
                # формируем текстовый шаблон нашего алерта
                bot.sendMessage(chat_id=chat_id, text=msg)
                # отправляем сообщение в TELEGRAMM

                sns.set(rc={'figure.figsize': (16, 10)})
                plt.tight_layout()
                # построим график

                ax = sns.lineplot(x=df2['ts'], y=df2[metric], label=metric)
                ax = sns.lineplot(x=df2['ts'], y=df2['up'], label='up')
                ax = sns.lineplot(x=df2['ts'], y=df2['low'], label='low')
                # построим линий границ и зададим параметры для осей координат  X,Y

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 15 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                #  зададим подписи тиков - каждый второй

                ax.set_title(metric)
                ax.set(ylim=(0, None))
                # зададим заголовок нашего графика

                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.name = '{0}.png'.format(metric)
                plot_object.seek(0)
                plt.close()
                # готовим полученный график для отправки в TELEGRAMM
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                #  отправляем график в TELEGRAMM
    
        
    
    dffed = extract_feed()
    dfmes = extract_mes()
    check_an(dffed, metrics1)
    check_an(dfmes, metrics2)
    
    
dag_emoseeva_alerts = dag_emoseeva_alerts()    
