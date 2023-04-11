## Проект-4: Построение ETL-Пайплайна  

### 1. Описание проекта:  

В нашем проекте мы будем решать задачу на ETL схему и как это можно сделать при помощи библиотеки Airflow.  

В целом Airflow — это удобный инструмент для решения ETL-задач  

Мы напишем task-и на python, которые Airflow будет исполнять. Ожидается, что на выходе будет DAG в Airflow, который будет считаться каждый день за вчера.  

**Наша задача:**  

- Параллельно будем обрабатывать две таблицы. В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка должна быть в отдельном таске.
- Далее объединяем две таблицы в одну.
- Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.
- И финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse.
- Каждый день таблица должна дополняться новыми данными.


**Структура финальной таблицы должна быть такая:**

- Дата - `event_date`  
- Название среза - `dimension`  
- Значение среза - `dimension_value`  
- Число просмотров - `views`  
- Числой лайков - `likes`  
- Число полученных сообщений - `messages_received`  
- Число отправленных сообщений - `messages_sent`  
- От скольких пользователей получили сообщения - `users_received`  
- Скольким пользователям отправили сообщение - `users_sent`  
- Срез - это `os`, gender и `age`  


**Нашу таблицу необходимо загрузить в схему test.**    

### 2. Вот что у нас получилось:  

[dag_ea_moseeva_12.py](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/a57e2f0e952acc73018f437dd0666859874076e1/4.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%9F%D0%BE%D1%81%D1%82%D1%80%D0%BE%D0%B5%D0%BD%D0%B8%D0%B5%20ETL-%D0%BF%D0%B0%D0%B9%D0%BF%D0%BB%D0%B0%D0%B9%D0%BD%D0%B0/dag_ea_moseeva_12.py)  
[dag Airflow.pnj](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/566945939953b7e7eabedd758fe1bacc40f2f979/4.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%9F%D0%BE%D1%81%D1%82%D1%80%D0%BE%D0%B5%D0%BD%D0%B8%D0%B5%20ETL-%D0%BF%D0%B0%D0%B9%D0%BF%D0%BB%D0%B0%D0%B9%D0%BD%D0%B0/dag_airflow.png)  
[схема test в Redash]()  
[DAG запустили в Airflow]()     


### 3. Теги:  

- ClickHouse  
- SQL Lab   
- JUPYTERHUB  
- Redash  
- Airflow  
- Python  
- библиотеки: pandas, pandahouse, datetime, timedelta, numpy  
- для создания DAG-а и его «содержания»: airflow, DAG,task,  airflow.operators.python_operator, PythonOperator, datetime, airflow.decorators.  
