# KARPOV.COURSES
<h1 align="center">Привет, меня зовут Евгения
<img src="https://github.com/blackcater/blackcater/raw/main/images/Hi.gif" height="32"/></h1>
<h3 align="center">   

Здесь собраны 6 проектов, реализованных в рамках программы "Симулятор аналитика" в KARPOV.COURSES.
## Описание проектов:  
|Номер проекта| Наименование проекта                      | Описание проекта                                            |   Стек                                          проекта  |
| ----------- | ----------------------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------------------- |  
| 1. | Построение дашбордов в Superset для приложения VK| Строительство дашбордов в Superset и Redash, используя БД ClickHouse.|ClickHouse,Apache Superset,SQL Lab, Dashboards |
| 2. |  Анализ продуктовых метрик|Визуализация метрик в [Superset](http://superset.lab.karpov.courses/r/3398) и [Redash](http://redash.lab.karpov.courses/public/dashboards/I4AR7pdf0DfSYP5ZEzdRNgE1tRMclKAFqihTv8aa?org_slug=default), используя БД ClickHouse. |ClickHouse,Apache Superset,SQL Lab, Redash, Dashboards |
| 3. |  Планирование и запуск А/В тестирования|[Проведение АА-тестов на пользователях ленты новостей](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/125e2039b435b9d81dcf19f367eed96732099558/3.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%D0%9F%D0%BB%D0%B0%D0%BD%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5%20%D0%B8%20%D0%B7%D0%B0%D0%BF%D1%83%D1%81%D0%BA%20%D0%90%D0%92%20%D1%82%D0%B5%D1%81%D1%82%D0%B0/Task_1_AA_test__1_.ipynb). [Оценка результатов проведения А/В тестирования данных пользователей ленты новостей.](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/c4c8cbbe0ada8927ef56f37904340c3d4ebcf22d/3.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%D0%9F%D0%BB%D0%B0%D0%BD%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5%20%D0%B8%20%D0%B7%D0%B0%D0%BF%D1%83%D1%81%D0%BA%20%D0%90%D0%92%20%D1%82%D0%B5%D1%81%D1%82%D0%B0/AB_test_task_2__1_.ipynb)[Анализ АВ теста по метрике линеаризованных лайков](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/2b2d030358037a083609305c1c37dc9d2e6e3a55/3.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%D0%9F%D0%BB%D0%B0%D0%BD%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5%20%D0%B8%20%D0%B7%D0%B0%D0%BF%D1%83%D1%81%D0%BA%20%D0%90%D0%92%20%D1%82%D0%B5%D1%81%D1%82%D0%B0/AB_test_linea.ipynb) |ClickHouse, SQL Lab, JUPYTERHUB, Python, библиотеки: pandas, pandahouse, numpy, matplotlib.pyplot,seaborn, scipy,stats,tqdm.auto,tqdm |
| 4. |  Построение ETL-пайплайна|Напишем task-и на python, которые Airflow будет исполнять. Ожидается, что на выходе будет [DAG в Airflow](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/6577cf0a0be6058edee010af70f88da1cb00a816/4.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%9F%D0%BE%D1%81%D1%82%D1%80%D0%BE%D0%B5%D0%BD%D0%B8%D0%B5%20ETL-%D0%BF%D0%B0%D0%B9%D0%BF%D0%BB%D0%B0%D0%B9%D0%BD%D0%B0/dag_ea_moseeva_12.py), который будет считаться каждый день за вчера. Для этого:  1. Параллельно будем обрабатывать две таблицы. В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка должна быть в отдельном таске.   2. Объединим две таблицы в одну.   3. Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.  4. Финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse.  5. Каждый день таблица должна дополняться новыми данными.  6. Нашу таблицу загрузим в схему [test](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/e4a809e3a68709c01e47b81673b7f7f44500cc18/4.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%9F%D0%BE%D1%81%D1%82%D1%80%D0%BE%D0%B5%D0%BD%D0%B8%D0%B5%20ETL-%D0%BF%D0%B0%D0%B9%D0%BF%D0%BB%D0%B0%D0%B9%D0%BD%D0%B0/test_redash.png). |ClickHouse, SQL Lab, JUPYTERHUB, Redash, Airflow, Python, библиотеки: pandas, pandahouse, datetime, timedelta, numpy, для создания DAG-а и его «содержания»: airflow, DAG,task,  airflow.operators.python_operator, PythonOperator, datetime, airflow.decorators.|  
| 5. |Автоматизация отчетности используя связку Python + CI/CD + BI + telegram|1. Создадим своего телеграм-бота 2. Напишем [скрипт](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/d1a48dc0ee928983fee44c4e83f2d5e9d5955397/5.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%90%D0%B2%D1%82%D0%BE%D0%BC%D0%B0%D1%82%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D1%8F%20%D0%BE%D1%82%D1%87%D1%91%D1%82%D0%BD%D0%BE%D1%81%D1%82%D0%B8/tg_moseeva_dag__1_.py) для сборки отчета по ленте новостей. Отчет должен состоять из двух частей:текст с информацией о значениях [ключевых метрик](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/139fb78f2a0085b87c3d419016516a91a587942c/5.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%90%D0%B2%D1%82%D0%BE%D0%BC%D0%B0%D1%82%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D1%8F%20%D0%BE%D1%82%D1%87%D1%91%D1%82%D0%BD%D0%BE%D1%81%D1%82%D0%B8/tg_moseeva_dag__1_.py) за предыдущий день, [график](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/20c544a46b8f5133cb36338073dae4cf8990a942/5.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%90%D0%B2%D1%82%D0%BE%D0%BC%D0%B0%D1%82%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D1%8F%20%D0%BE%D1%82%D1%87%D1%91%D1%82%D0%BD%D0%BE%D1%81%D1%82%D0%B8/task_6_2_dag__1___1_.py) с значениями метрик за предыдущие 7 дней. 3.Отобразим в отчете ключевые метрики: DAU,Просмотры,Лайки,CTR. 4. Автоматизируем отправку отчета с помощью Airflow. Код для сборки отчета разместим в [GitLab](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/12ca521a11ef7db2a4420519802f14f6b67d1144/5.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%90%D0%B2%D1%82%D0%BE%D0%BC%D0%B0%D1%82%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D1%8F%20%D0%BE%D1%82%D1%87%D1%91%D1%82%D0%BD%D0%BE%D1%81%D1%82%D0%B8/raw.txt). 5. Отчет должен приходить ежедневно в 11:00 в свой чат.  |ClickHouse, SQL Lab, JUPYTERHUB, Redash, Airflow, Python, библиотеки: pandas, pandahouse, datetime, timedelta, numpy, telegram, matplotlib.pyplot, seaborn, io, для создания DAG-а и его «содержания»: airflow, DAG,task,  airflow.operators.python_operator, PythonOperator, datetime, airflow.decorators, get_current_context |
| 6. | Поиск аномалий. Система алертов.|Реализация автоматической [алерт](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/4aaf0c126e6eded5f954706e08684ba0a3485a8c/6.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%20%D0%9F%D0%BE%D0%B8%D1%81%D0%BA%20%D0%B0%D0%BD%D0%BE%D0%BC%D0%B0%D0%BB%D0%B8%D0%B9.%20%D0%A1%D0%B8%D1%81%D1%82%D0%B5%D0%BC%D0%B0%20%D0%B0%D0%BB%D0%B5%D1%80%D1%82%D0%BE%D0%B2./task_7_alert_dag__1_.py) системы по поиску аномалий, с алертом в telegram для нашего приложения VK. |ClickHouse, SQL Lab, JUPYTERHUB, Redash, Airflow, Python, библиотеки: pandas, pandahouse, datetime, timedelta, numpy, telegram, matplotlib.pyplot, seaborn, io, для создания DAG-а и его «содержания»: airflow, DAG, task, airflow.operators.python_operator, PythonOperator, datetime, airflow.decorators, get_current_context|
| 7. | Сертификат KARPOV.COURSES| [Обучение по специализации «Симулятор аналитика»](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/27fa39495f2c3f3af16f2655d737389849192e7e/%D0%9A%D0%B0%D1%80%D0%BF%D0%BE%D0%B2_%D1%80%D1%83%D1%81%D1%81%D0%BA%D0%B8%D0%B9%20(1).pdf)|Номер сертификата: №AC32520 20/02/23 |
