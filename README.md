

## Общая информация о проекте
   Проект предусматривает разработку прототипа системы управления данными (Data Pipeline), которые в дальнейшем будут собираться и храниться интернет-магазином. Нам предоставлены архивные данные о клиентах и их транзакциях, которые требуется сохранить и перенести в новую систему. Для этого необходимо разработать структуры OLTP базы данных на основе исходных данных, разработать структуры баз данных для дальнейшего хранения и анализа данных в ClickHouse. Также необходимо проверить работоспособность Pipeline со сторонними источниками данных, такими как API. Все процессы автоматизируются с использованием Airflow. 

## Используемые инструменты
PostgreSQL, ClickHouse, Airflow, Pandas, Flask, Docker-Compose.
    
## Структура файлов проекта
В папке "csv_files" находятся исходные исторические данные. Папки с префиксами "app_" содержат Docker-файлы и скрипты для создания и инициализации приложений с использованием Docker-Compose. Папка "dags" содержит исходный код Airlow DAGs и исполняемые Python скрипты. Файл .airflowignore необходим для исключения скриптов Python, отличных от DAGs, из планировщика Airflow.

## Настройка проекта

 * Клонировать репозиторий.
 
 * Произвести первичную настройку Airflow:
    https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
    Выполнить только пункты, указанные в "Setting the right Airflow user". Дальнейшая настройка будет производиться через Docker-Compose.
        
 * Из папки проекта, запустить следующие команды:
    ```
    docker-compose -f airflow_compose.yaml up airflow-init
    docker-compose -f airflow_compose.yaml -f db_compose.yaml up
    ```
 
## Краткое описание проекта

1. На первом этапе составляется подходящая схема данных для СУБД Postgres. Учитывается структура исходных данных, создаётся нормализованная схема отношений БД.
[Описание схемы данных](#Описание-схемы-данных), [ссылка на DDL-операторы Postgres](https://github.com/vlkudriashev/data_engineering_project/blob/main/initdb_postgres/create_source.sql)
2. Исходные (архивные) данные из CSV-файлов переносятся в OLTP систему управления базами данных (СУБД) Postgres. Пропуски в значениях редактируются при помощи Pandas с целью поддержки ссылочной целостности БД при импорте данных. В завершение, данные записываются в СУБД при помощи коннектора Psycopg. [Подробное описание ДАГа №1](#dag-1-csv---source), [скрипт трансформации и записи данных](https://github.com/vlkudriashev/data_engineering_project/blob/main/dags/scripts/python/csv_to_oltp.py)
3. Формируются базовые принципы построения ДАГов, а именно: идемпотентность задач, возможность заполнения значений за предыдущие промежутки времени (Backfilling). [Особенности реализации Pipeline.](#особенности-реализации-pipeline)
4.  Создаётся единый ДАГ для транспортировки данных из из исходного (Source) слоя СУБД Postgres в промежуточный (Staging) и целевой (Target) слои СУБД ClickHouse. Для этих целей используется оператор из ClickHouse Plugin.  [Ссылка на код ДАГа №2](https://github.com/vlkudriashev/data_engineering_project/blob/main/dags/2_DAG_oltp_pipeline.py), [Подробные сведения о реализации задач ДАГа №2.](#описание-операторов-и-задач-дага)

    4.1 В первой часть ДАГа OLTP данные загружаются из исходного слоя в промежуточный (Staging). [Скрипт создания Staging таблиц в ClickHouse](https://github.com/vlkudriashev/data_engineering_project/blob/main/initdb_click/create_all_stage.sql).
    
    4.2 Во второй части данные перемещаются в конечный слой, решаются проблемы дедуплицирования данных при помощи движка Replacing Merge Tree. Также реализуется SCD для таблицы Products.

5. Проверяется работоспособность Pipeline со сторонними API. На Flask API создаётся тестовый API для получения данных о кредитном рейтинге продавцов e-commerce площадки. Данные из API вводятся в Staging и Targer слои СУБД ClickHouse при помощи PythonOperator и ClickHouse Driver. В Staging слое данные переводятся из формата API (JSON) в Nested формат СУБД ClickHouse.
[Код API приложения](https://github.com/vlkudriashev/data_engineering_project/blob/main/sellers_api/app/main.py), [код ДАГа №3](https://github.com/vlkudriashev/data_engineering_project/blob/main/dags/3_DAG_api_pipeline.py), [код скрипта ввода данных](https://github.com/vlkudriashev/data_engineering_project/blob/main/dags/scripts/python/api_to_stage.py), [подробное описание ДАГа](31-api---staging)
    
## Описание схемы данных

### 1. Source DB - Postgres
Файл «orders.csv» содержит данные о купленных товарах и их количестве. Значения этих полей представлены в виде массивов. Так как магазин планирует добавлять новые данные о заказах в OLTP-базу данных в нормализованном виде. Данные приводятся к нормализованному виду форматированием. Затем создаются отношения на их основе. Отношение "orders", содержащее массивы приводятся к третьей нормальной форме разбиением на «orders» и «order_items». Конечная схема:
 
<img src="https://github.com/vlkudriashev/data_engineering_project/blob/main/pics/csv_to_source.png" width=50%>
    

### 2. Staging DB - Clickhouse
В СУБД Clickhouse создаётся Staging слой для временного хранения и обработки данных, поступающих из различных внутренних и внешних источников. Такой слой позволяет не нагружать сервер исходной OLTP СУБД преобразованием данных, а также делает возможным проверку качества поступающих данных до переноса в основное хранилище.
    
### 3. Target DB - Clickhouse
Target слой создаётся для длительного хранения данных и дальнейшей работы с очищенными данными. На его основе могут создаваться витрины данных.
    
Общая структура проекта:
    
<img src="https://github.com/vlkudriashev/data_engineering_project/blob/main/pics/project_structure.png" width=75%>
    
## Особенности реализации pipeline
 ### 1. Идемпотентность задач
Известно, что задачи по работе с данными могут выполниться некорректно при потере соединения с СУБД. Если часть данных всё же была доставлена в целевую систему, но задача не завершилась корректно, то при её повторном запуске в данных могут появиться дубликаты.

Поэтому при построении систем передачи данных применяется DELETE-WRITE паттерн. В нем для определенных временных интервалов данные удаляются, а затем записываются новые. Важно, чтобы обе операции выполнялись в одном ДАГ-таске, чтобы избежать дублирования записей в целевой СУБД в случае ошибок или перезапусков задач. 

Применение ALTER DELETE в ClickHouse может сказаться на стабильности системы управления базами данных. Частые удаления и вставки данных в Lightweight delete могут привести к непредвиденным последствиям. Для избежания дублирования данных при перезапуске задач используется DELETE-WRITE с разбиением на партиции. В Staging таблицах создаются дневные партиции, которые перезаписываются при перезапуске задач. В Таргет таблицах используется движок ReplacingMergeTree, так как в них будет производиться вставка обновленных данных. Такие таблицы также будут обеспечивать идемпотентность задач.

### 2. Возможность backfilling
Все ДАГи поддерживают возможность заполнения значений за прошедшие периоды времени. Это возможно благодаря тому, что в коде SQL-запросов используются динамические даты из jinja-шаблонов. 
       
## Детали выполнения проекта

### 0.1. Расписание задач ДАГов
В большинстве дагов указан режим выполнения "@once" для облегчения их первичной отладки и проверки. Дата начала исполнения ДАГов указана как дата самого раннего события (действия пользователей) в исходных данных. Дата окончания исполнения ДАГов указана, чтобы избежать множественного исполнения дагов при тестировании. Режим выполнения ДАГов возможно изменить согласно необходимости.

### 0.2. Режим загрузки данных
В приведенном Pipeline тестируется возможность инкрементальной загрузки данных и возможность загрузки данных за определенный период времени. Не выполняется стадия полной загрузки данных в Stage и Target, хотя ДАГи легко модифицируются под эти цели.
  
 ### DAG 1: CSV -> Source
 
! Так как проект имеет учебный характер, способ ввода данных был выбран для изучения работы коннектора Psycopg и обработки данных в Pandas.
 
#### 1.1. Подготовка данных к импорту

ДАГ №1 загружает данные из CSV-файлов в "source" схему СУБД PostgreSQL. Файлы могут содержать пропуски в значениях, которые далее используются как первичные ключи таблиц (например, user_id, order_id). Внешние ключи в других таблицах могут ссылаться на пропущенные значения. Если пропуски не устранить, это может привести к нарушению ссылочной целостности и невозможности импорта данных. Используя Pandas, мы заполняем пропуски в порядковых значениях для первичных ключей в таблицах. 

#### 1.2.  Импорт данных

Все данные, включая отсутствующие значения (NULL), импортируются из датафрейма Pandas и преобразуются в типы данных, поддерживаемые коннектором "Psycopg". В скрипте импорта данных "csv_to_oltp" создаётся одно соединение для импорта всех файлов. Запись производится методом "execute_batch", а сами данные вводятся при помощи итератора. Это позволяет процессу работать быстро и занимать меньше места в памяти.
    
#### 1.3. Альтернативный импорт данных
    
Альтернативным решением ввода данных является их копирование при помощи функции COPY СУБД Postgres. Такой способ предполагает редактирование данных уже внутри БД и последующую модификацию исходных таблиц. Также возможно редактирование набора данных отдельно до их импорта и импорт в готовые таблицы также при помощи COPY. Вышеприведенные способы импорта следует рассматривать при большом размере исходных файлов.

    
### DAG 2: Source -> Staging -> Target
В рамках ДАГа №2, мы сначала осуществляем перемещение всех OLTP данных из исходного (Source) слоя в промежуточный (Staging) слой. Затем, данные проходят процесс трансформации и перемещаются в конечный (Target) слой.

Для ввода данных из Postgres в Clickhouse используется плагин "clickhouse-plugin" https://github.com/bryzgaloff/airflow-clickhouse-plugin. Он устанавливается в окружение Airflow из requirements на этапе cборки Docker-образа. Код содержит sql-операторы, которым нужны одни и те же значения параметров. Для удобства форматирования кода используется метод .format.

Визуализация зависимостей задач ДАГа:

<img src="https://github.com/vlkudriashev/data_engineering_project/blob/main/pics/airflow_tasks.jpg" width=50%>

### Описание операторов и задач ДАГа
#### 2.1. Задачи  "insert_stg_all_", "insert_tgt_simple_", "insert_tgt_orders"

При помощи оператора "insert_stg_all" для каждой таблицы из Source слоя динамически создаётся задача по переносу данных в Staging слой без изменений. К данным добавляется поле "insertion_ts", чтобы знать время их загрузки. В дальнейшем это позволит идентифицировать значения для загрузки в Target слой. Данные загружаются в партиции по дням.

В задаче "insert_tgt_simple" выполняется перенос данных таблиц, где не требуется трансформация или изменение данных. В "insert_tgt_orders" производится преобразование  данных таблиц "orders" и "order_items" обратно в массивы для эффективного хранения

Проблема дедуплицирования записей появляется в случаях, где требуется замена значений на более актуальные. Например, при обновлении данных пользователя в исходной системе необходимость перезаписать данные в целевой системе для пользователя с таким же ID. В традиционных СУБД эта проблема решается при помощи UPSERT (Update-Insert) операции. В таблицах Target слоя ClickHouse для этого используется движок ReplacingMergeTree. 
    
#### 2.2. Задача "insert_tgt_products"

Для таблицы с ценами товаров реализуется Slowly Changing Dimensions второго типа (SCD Type 2), так как при построении аналитики обычно требуется цена товара на конкретный момент времени. Реализация SCD позволит отслеживать изменение цены с нужной гранулярностью (раз в день, два раза в день, раз в шесть часов и так далее), если настроить выгрузку данных данные с нужной периодичностью.

Для реализации SCD в таблице "products" в "target_db" вводятся такие поля как "start_ts" и "end_ts", отражающие сроки начала и конца действия записи. Текущему (актуальному) значению мы присваиваем "end_ts" = '2100-01-01 00:00:00', что практически равно максимальной дате в DateTime типе ClickHouse. Прошлые максимальные значения для записей с одним и тем же идентификатором будут позже удалены движком ReplacingMergeTree Clickhouse. На место прошлых "максимумов" записываются значения, где датой окончания является дата начала новой записи с тем же идентификатором. Альтернативой такой замены значений для классических СУБД (например, Postgres) является Upsert (Insert - Update). 

Больше о движке RMT можно можно узнать в статье: [ClickHouse ReplacingMergeTree Explained: The Good, The Bad, and The Ugly | Altinity Blog](https://altinity.com/blog/clickhouse-replacingmergetree-explained-the-good-the-bad-and-the-ugly), где говорится, что при использовании параметра "do_not_merge_across_partitions_select_final=1" для таблицы в 900 млн строк можно получить скорость выполнения запросов всего на 30% медленнее, чем без использования FINAL. Известно, что если данные находятся в разных партициях, то замена значений на более новые может не произойти. В нашем примере, данные можно разбить на партиции по дате начала действия записи. Тогда данные об одной записи, подлежащей изменению, всегда (или почти всегда) будут иметь одну дату начала действия записи. В таком случае удаление дубликатов при запросах будет работать и корректно и быстро.

На этом работа по загрузке OLTP данных завершается.

Эволюция схемы данных таблицы "products":

<img src="https://github.com/vlkudriashev/data_engineering_project/blob/main/pics/products_tbl_evolution.png" width=50%>
   
    
### DAG 3. Работа с данными АПИ
В ДАГе №3 тестируется способность pipeline корректно обрабатывать данные, полученные из API-источников.

#### 3.1. API -> Staging
Здесь происходит обращение к запущенному в докере FastAPI приложению (как локальному аналогу удаленного API) и запрашивается некоторое число записей. К записям добавляется идентификатор времени загрузки данных, после чего данные записываются в Staging базу данных в "Nested" формате для их компактного хранения.

#### 3.2. API (Staging) -> Target
В операторе производится преобразование данных из Nested формата в формат обычной строковой записи. Эта операция должна облегчить понимание данных конечными пользователями и позволит удобно производить их дальнейшую обработку. Для записи значений используется коннектор ClickHouse Driver и PythonOperator ДАГа.

 ### Дальнейшие улучшения
* В некоторых операторах присутствует выборка всех полей таблиц (SELECT *). Явное указание колонок использовалось там, где происходила трансформация данных. Хорошей практикой считается явное задание полей таблиц, с тем, чтобы при изменении схем данных таблиц Pipeline работал стабильно. Явное указание колонок для произвольного числа таблиц можно реализовать, используя конфигурационный файл.
* Сейчас данные об исполнении задач хранятся лишь в логах Airflow. В дальнейшем можно реализовать запись таких данных во внутреннюю СУБД.
* Возможно проработать механизм проверки качества вводимых данных и сбора статистики по по записанным значениям.

### Результаты выполнения проекта
* Разработан Pipeline для управления данными с использованием Airflow;
* Реализована идемпотентность задач Pipeline;
*  В СУБД ClickHouse реализована дедупликация данных при вводе и обработка данных из API;
* Реализована концепция медленно меняющихся измерений (SCD) для сохранения истории изменения данных;
* Протестирован ввод данных в СУБД ClickHouse и Postgres коннекторами ClickHouse Driver, Psycopg;
* Проведена работа по трансформации данных с использованием инструмента Pandas.

