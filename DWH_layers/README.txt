Разрабатываем прототип хранилища данных. 

Технический стек: SQL/ PostgreSQL / Python/ Airflow.

1)  Источник данных – генератор синтетических данных, который мы выполнили в тренировочном кейсе.
2)  Всё наше решение выполняется в рамках одной базы данных. Слои хранилища данных представлены в одной схеме, генератор синтетических данных в своей схеме.

Перечень схем – слоёв: (делаем так, в реальности система источник это отдельная СУБД, REST, либо брокер сообщений, а слои хранилища это отдельные СУБД)

a.  Схема «oltp_src_system» - в ней укладываются таблицы, функции, триггеры системы генерации синтетических данных (для демонстрации достаточно одной таблицы, но возможно кому то будет интересно сделать несколько таблиц в источнике);
b.  Схема «oltp_cdc_src_system» - здесь храниться лог изменений CDC –  строится на триггерах таблицы источника «src_system», каждая строка таблица обогащена полями operation_type и updated_dttm (время обновления);
c.  Схема «dwh_stage» - первый слой прототипа хранилища данных. Получает данные из источника схемы «oltp_cdc_src_system».;
d.  Схема «dwh_ods» - детальный слой, где мы храним все версии всех строк таблиц, дополнительные технические поля valid_from_dttm, valid_to_dttm, deleted_flg, deleted_dttm; История хранится непрерывно.
e. Схема «report» - слой для материализации отчета (витрины данных) для последующего использования.

3) В каждый слой прототипа должны входить следующие таблицы и функции:
a.  Схема «oltp_src_system» - таблицы данных, функции для заполнения, триггер ;
b.  Схема «oltp_cdc_src_system» - таблицы – лог CDC изменений источника данных;
c.  Схема «dwh_stage» - таблицы и функции для заполнения данных этого слоя;
d.  Схема «dwh_ods» - таблицы и функции для заполнения этого слоя, тут же будут храниться аналитический справочник;
e.  Схема «report» - функции для расчетов таблиц для отчетов.

4) В слое схема «dwh_ods» предусмотреть создание аналитического справочника календарь 
(он должен содержать следующие поля CREATE TABLE Dim_Date (date_key INT PRIMARY KEY,   date_actual DATE NOT NULL,   year INT NOT NULL,   quarter INT NOT NULL,   month INT NOT NULL,   week INT NOT NULL,   day_of_month INT NOT NULL,
   day_of_week INT NOT NULL,   is_weekday BOOLEAN NOT NULL,   is_holiday BOOLEAN NOT NULL,   fiscal_year INT NOT NULL
);).

5) В слое Схема «report» необходимо будет подготовить представление:
a. по которому можно получить крайнее актуальное состояние данных в системе источнике;
b. Несколько витрин данных по Вашим бизнес метрикам, одна – две витрины должны быть сделаны использованием календаря, подготовленного в схеме dwh_ods;

6) Запуск логики упаковать в DAG Python; Вся логика должна быть на стороне PostgreSQL. Airflow нужен только для запуска всех функций в нужном порядке.

Требования к ответу
По всем созданным объектам в базе данных, предоставить:

Описание и назначение полей;
Описание алгоритмов захвата и формирования данных;
Нарисовать схему решения в любом виде ;
Маппинги S2T;
Для витринного слоя написать задание на формирование каждого отчета, описание итоговых таблиц и алгоритмов формирования;
Описать шаги повторения, которые нужно произвести при добавлении дополнительных таблиц – источников в слое oltp_src_system – указать какие таблицы нужно указать с какими техническими полями на каких шагах;
Подготовить тест-кейсы для прототипа, запустить их, описать.