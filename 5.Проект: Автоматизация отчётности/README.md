## Проект-5: Автоматизация отчётности  

Здесь мы будем автоматизировать базовую отчетность нашего приложения.  Наладим автоматическую отправку аналитической сводки в телеграм каждое утро!  

### 1. Наша задача:  


##### Создадим своего телеграм-бота с помощью @botfather. Чтобы получить chat_id, воспользуемся ссылкой https://api.telegram.org/bot<токен_вашего_бота>/getUpdates  или методом bot.getUpdates()  


##### Напишем скрипт для сборки отчета по ленте новостей. Отчет должен состоять из двух частей:  

- Текст с информацией о значениях ключевых метрик за предыдущий день  
- График с значениями метрик за предыдущие 7 дней 


##### Отобразим в отчете следующие ключевые метрики:  

- DAU  
- Просмотры  
- Лайки  
- CTR  


##### Автоматизируем отправку отчета с помощью Airflow. Код для сборки отчета разместим в GitLab

Отчет должен приходить ежедневно в 11:00 в свой чат.


### 2. Вот что у нас получилось:

- [task_6_1_dag.py](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/6b41964c50d740effeeca5aba723904f84cac519/5.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%90%D0%B2%D1%82%D0%BE%D0%BC%D0%B0%D1%82%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D1%8F%20%D0%BE%D1%82%D1%87%D1%91%D1%82%D0%BD%D0%BE%D1%81%D1%82%D0%B8/task6_1_dag__1___1_.py)  
- [task_6_2_dag.py](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/68a965c072e7a6ff2db329be80d96fcd730ab4c8/5.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%90%D0%B2%D1%82%D0%BE%D0%BC%D0%B0%D1%82%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D1%8F%20%D0%BE%D1%82%D1%87%D1%91%D1%82%D0%BD%D0%BE%D1%81%D1%82%D0%B8/task_6_2_dag__1___1_.py)  
- tg_moseeva_dag.py  
- В Airflow DAG-и:  
  - Airflow_task_6_1_значения_метрик.png  
  - Airflow_task_6_2_значения_метрик.png  
- В Telegram:  
  - Отчёт в Telegram  
  - Отчёт в Telegram-продолжение  


### 3. Автоматизировали отправку отчета с помощью Airflow, для этого:

- Клонировали репозиторий  
- В локальной копии внутри папки dags создали свою папку — она должна совпадать по названию с моим именем пользователя, которое через @ в профиле GitLab  
- Создали там DAG — он должен быть в файле с форматом .py  
- Запушивали результат  
- Включили DAG, когда он появился в Airflow включили DAG в Airflow  


### 4. Теги:

- ClickHouse  
- SQL Lab  
- JUPYTERHUB  
- Redash  
- Airflow  
- Python  
- библиотеки: pandas, pandahouse, datetime, timedelta, numpy, telegram, matplotlib.pyplot, seaborn, io,  
- для создания DAG-а и его «содержания»: airflow, DAG,task,  airflow.operators.python_operator, PythonOperator, datetime, airflow.decorators, get_current_context  
