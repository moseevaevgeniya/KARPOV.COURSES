# KARPOV.COURSES
<h1 align="center">Привет, меня зовут Евгения
<img src="https://github.com/blackcater/blackcater/raw/main/images/Hi.gif" height="32"/></h1>
<h3 align="center">   

Здесь собраны реализованные проекты в рамках программы "Симулятор аналитика" в KARPOV.COURSES.
## Описание проектов:  
|Номер проекта| Наименование проекта                      | Описание проекта                                            |   Стек                                          проекта  |
| ----------- | ----------------------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------------------- |  
| 1. | Построение дашбордов в Superset для приложения VK| Строительство дашбордов в Superset и Redash, используя БД ClickHouse.|ClickHouse,Apache Superset,SQL Lab, Dashboards |
| 2. |  Анализ продуктовых метрик|Визуализация метрик в Superset и Redash, используя БД ClickHouse. |ClickHouse,Apache Superset,SQL Lab, Redash, Dashboards |
| 3. |  Планирование и запуск А/В тестирования|[Проведение АА-тестов на пользователях ленты новостей](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/125e2039b435b9d81dcf19f367eed96732099558/3.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%D0%9F%D0%BB%D0%B0%D0%BD%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5%20%D0%B8%20%D0%B7%D0%B0%D0%BF%D1%83%D1%81%D0%BA%20%D0%90%D0%92%20%D1%82%D0%B5%D1%81%D1%82%D0%B0/Task_1_AA_test__1_.ipynb). [Оценка результатов проведения А/В тестирования данных пользователей ленты новостей.](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/c4c8cbbe0ada8927ef56f37904340c3d4ebcf22d/3.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%D0%9F%D0%BB%D0%B0%D0%BD%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5%20%D0%B8%20%D0%B7%D0%B0%D0%BF%D1%83%D1%81%D0%BA%20%D0%90%D0%92%20%D1%82%D0%B5%D1%81%D1%82%D0%B0/AB_test_task_2__1_.ipynb)[Анализ АВ теста по метрике линеаризованных лайков](https://github.com/moseevaevgeniya/Project_in_Karpov.courses/blob/2b2d030358037a083609305c1c37dc9d2e6e3a55/3.%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82:%D0%9F%D0%BB%D0%B0%D0%BD%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5%20%D0%B8%20%D0%B7%D0%B0%D0%BF%D1%83%D1%81%D0%BA%20%D0%90%D0%92%20%D1%82%D0%B5%D1%81%D1%82%D0%B0/AB_test_linea.ipynb) |ClickHouse, SQL Lab, JUPYTERHUB, Python, библиотеки: pandas, pandahouse, numpy, matplotlib.pyplot,seaborn, scipy,stats,tqdm.auto,tqdm |
| 4. |  Построение ETL-пайплайна|Напишем task-и на python, которые Airflow будет исполнять. Ожидается, что на выходе будет DAG в Airflow, который будет считаться каждый день за вчера. Для этого:  
 - Параллельно будем обрабатывать две таблицы. В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка должна быть в отдельном таске.
 - Объединяем две таблицы в одну.
 - Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.
 - Финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse.
 - Каждый день таблица должна дополняться новыми данными. 
 - Нашу таблицу необходимо загрузить в схему test. |ClickHouse, SQL Lab, JUPYTERHUB, Redash, Airflow, Python, библиотеки: pandas, pandahouse, datetime, timedelta, numpy, для создания DAG-а и его «содержания»: airflow, DAG,task,  airflow.operators.python_operator, PythonOperator, datetime, airflow.decorators.|  
