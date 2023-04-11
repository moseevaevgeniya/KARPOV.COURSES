from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection_main = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database': 'simulator_20221220',
                  'user': 'student',
                  'password': 'dpo_python_2020'}


connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                   'database':'test',
                   'user':'student-rw', 
                   'password':'656e2b0c9c'}


# Функция для чтения данных из ClickHouse
def ch_get_df(query):
    result = ph.read_clickhouse(query, connection=connection_main)
    return result

# Параметры, которые прокидываются в таски по умолчанию
default_args = {
    'owner': 'e-moseeva-12',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2023, 1, 21),
}

# Интервал запуска DAG = 10:00 каждый день
schedule_interval = '0 10 * * *'

filter_by_yesterday = 'toDate(time) = toDate(now()) - 1'
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def moseeva_dag_ETL():

    @task
    def extract_feed_data(filter='1 = 1'):
        feed_data_query = """
            select 
                user_id,
                countIf(action='like') as likes,
                countIf(action='view') as views,
                max(age) as age,
                max(gender) as gender,
                max(os) as os,
                max(toDate(time)) as event_date
            from
                simulator_20221220.feed_actions
            where """ \
            + filter \
            + """
            group by user_id
        """
        df_cube = ch_get_df(query=feed_data_query)
        return df_cube

    @task
    def extract_message_data(filter='1 = 1'):
        message_data_query = """
            with senders as (select 
                user_id,
                count(*) as total_messages_sent,
                count(distinct reciever_id) as num_of_receivers
            from 
                simulator_20221220.message_actions
            where """ \
            + filter \
            + """
            group by user_id),
            recievers as (select 
                reciever_id,
                count(*) as total_messages_received,
                count(distinct user_id) as num_of_senders
            from 
                simulator_20221220.message_actions
            where """ \
            + filter \
            + """
            group by reciever_id)
            select coalesce(s.user_id, r.reciever_id) as user_id,
            coalesce(s.total_messages_sent, 0) as messages_sent,
            coalesce(s.num_of_receivers, 0) as users_sent,
            coalesce(r.total_messages_received, 0) as messages_received,
            coalesce(r.num_of_senders, 0) as users_received
            from senders s
            full join recievers r
            on s.user_id = r.reciever_id
        """
        df_cube = ch_get_df(query=message_data_query)
        return df_cube

    # Объединяем обе таблицы (лента и сообщения)
    @task
    def transfrom_feed_join_messages(df_cube1, df_cube2):
        joined_df_cube = pd.merge(df_cube1, df_cube2, on='user_id', how='left').copy()
        joined_df_cube['messages_sent'] = joined_df_cube['messages_sent'].fillna(0)
        joined_df_cube['messages_received'] = joined_df_cube['messages_received'].fillna(0)
        joined_df_cube['users_sent'] = joined_df_cube['users_sent'].fillna(0)
        joined_df_cube['users_received'] = joined_df_cube['users_received'].fillna(0)
        return joined_df_cube

    # Создаем срез по операционной системе
    @task
    def transfrom_df_os(joined_df_cube):
        df_os = joined_df_cube.groupby(['os', 'event_date'])[['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].sum().reset_index()
        df_os.rename(columns = {'os' : 'dimension_value'}, inplace = True)
        df_os.insert(0, 'dimension', 'os')
        return df_os

    # Создаем срез по полу
    @task
    def transfrom_df_gender(joined_df_cube):
        df_gender = joined_df_cube.groupby(['gender', 'event_date'])[['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].sum().reset_index()
        df_gender.rename(columns = {'gender' : 'dimension_value'}, inplace = True)
        df_gender.insert(0, 'dimension', 'gender')
        return df_gender

    # Создаем срез по возрастным группам
    @task
    def transfrom_df_age(joined_df_cube):
        prep_df_age = joined_df_cube.drop(columns=['age']).copy()
        prep_df_age['age_group'] = pd.cut(joined_df_cube['age'], bins = [0, 17 , 25, 35, 45, 100], labels =['0-17', '18-25', '26-35', '36-45', '45+'])
        df_age = prep_df_age.groupby(['age_group', 'event_date'])[['likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].sum().reset_index()
        df_age.rename(columns = {'age_group' : 'dimension_value'}, inplace = True)
        df_age.insert(0, 'dimension', 'age')
        return df_age

    # Объединяем все срезы в одну таблицу
    @task
    def transfrom_df_contact(df1, df2, df3):
        combined_df = pd.concat([df1, df2, df3]).reset_index(drop=True)
        combined_df['messages_received'] = combined_df['messages_received'].astype('int')
        combined_df['messages_sent'] = combined_df['messages_sent'].astype('int')
        combined_df['users_received'] = combined_df['users_received'].astype('int')
        combined_df['users_sent'] = combined_df['users_sent'].astype('int')
        return combined_df
    
    @task
    def load(df):
        creat_table_query = """
        CREATE TABLE IF NOT EXISTS test.ea_moseeva
        (
        dimension String,
        dimension_value String,
        event_date Date,
        likes UInt64,
        views UInt64,
        messages_received UInt64,
        messages_sent UInt64,
        users_received UInt64,
        users_sent UInt64
        )
        ENGINE = MergeTree()
        ORDER BY event_date
        """
        ph.execute(creat_table_query, connection=connection_test)
        ph.to_clickhouse(df, 'ea_moseeva', index=False, connection=connection_test)

    feed_df_cube = extract_feed_data(filter=filter_by_yesterday)
    messages_df_cube = extract_message_data(filter=filter_by_yesterday)
    joined_df_cube = transfrom_feed_join_messages(feed_df_cube, messages_df_cube)
    df_os_slice = transfrom_df_os(joined_df_cube)
    df_gender_slice = transfrom_df_gender(joined_df_cube)
    df_age_slice = transfrom_df_age(joined_df_cube)
    df_to_load = transfrom_df_contact(df_os_slice, df_gender_slice, df_age_slice)
    load(df_to_load)

moseeva_dag_ETL = moseeva_dag_ETL()
