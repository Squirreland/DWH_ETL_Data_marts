#Student 71 Серина М.С.
# Dag для генерации данных, перемещения истории их изменения в хранилище и построение отчетов в рамках Кейс 3

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
import psycopg2
import psycopg2.extras as extras
import numpy as np
import pandas as pd
import datetime

#подключение к БД
conn_pg_params = {
    'host': '10.4.49.51',
    'database': 'student71_serina_ms',
    'user': 'Student71',
    'password': 'Rt30K44Y85D99FyD',
}
with DAG(
        'st71_control_сase_3',
        default_args={
            'depends_on_past': False,
            # 'email': ['developer@yandex.ru'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 0,
            'retry_delay': datetime.timedelta(minutes=5),
            # 'queue': 'bash_queue',
            # 'pool': 'backfill',
            # 'priority_weight': 10,
            # 'end_date': datetime(2016, 1, 1),
            # 'wait_for_downstream': False,
            # 'sla': timedelta(hours=2),
            # 'execution_timeout': timedelta(seconds=300),
            # 'on_failure_callback': some_function,
            # 'on_success_callback': some_other_function,
            # 'on_retry_callbak': another_function,
            # 'sla_miss_callback': yet_another_function,
            # 'trigger_rule': 'all_success'
        },
        description='',
        schedule_interval="@hourly",
        start_date=datetime.datetime(2023, 5, 13),
        catchup=False,
        max_active_runs=1,
        tags=['case_3_control'],
) as dag:

    def connect(params_dic):
        # NOTE подключение к серверу
        conn = None
        try:
            conn = psycopg2.connect(**params_dic)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            exit(1)
        return conn

    # группа генерации и изменения даных в источнике
    with TaskGroup(group_id="gr_generate_source_data") as gr_generate_source_data:
        def generate_new_tasks(**kwargs):
            conn_pg = connect(conn_pg_params)
            conn_pg.cursor().execute('select oltp_src_system.create_tasks();')
            conn_pg.commit()
            conn_pg.close()
        task_1_generate_new_tasks = PythonOperator(
            task_id='task_1_generate_new_tasks',
            python_callable=generate_new_tasks,
            op_kwargs={},
        )
        def update_exists_tasks(**kwargs):
            conn_pg = connect(conn_pg_params)
            conn_pg.cursor().execute('select oltp_src_system.delete_existed_task();')
            conn_pg.commit()
            conn_pg.close()
        task_2_update_exists_tasks = PythonOperator(
            task_id='task_2_update_exists_tasks',
            python_callable=update_exists_tasks,
            op_kwargs={},
        )
        def delete_random_task(**kwargs):
            conn_pg = connect(conn_pg_params)
            conn_pg.cursor().execute('select oltp_src_system.update_existed_task();')
            conn_pg.commit()
            conn_pg.close()

        task_3_delete_tasks = PythonOperator(
            task_id='task_3_delete_random_task',
            python_callable=delete_random_task,
            op_kwargs={},
        )

        task_1_generate_new_tasks >> task_2_update_exists_tasks >> task_3_delete_tasks

    # группа переноса данных
    with TaskGroup(group_id="gr_load_data") as gr_load_data:
        def load_from_oltp_cdc_src_system_to_dwh_stage(**kwargs):
            conn_pg = connect(conn_pg_params)
            conn_pg.cursor().execute('select dwh_stage.load_from_cdc_src_system_card_application_data_to_stage();')
            conn_pg.commit()
            conn_pg.close()
        # Из логов cdc из oltp_src_system в stage хранилища
        # select dwh_stage.load_from_cdc_src_system_card_application_data_to_stage()
        task_1_load_from_oltp_cdc_src_system_to_dwh_stage = PythonOperator(
            task_id='load_from_oltp_cdc_src_system_to_dwh_stage',
            python_callable=load_from_oltp_cdc_src_system_to_dwh_stage,
            op_kwargs={},
        )

        def load_from_dwh_stage_to_dwh_ods(**kwargs):
            conn_pg = connect(conn_pg_params)
            conn_pg.cursor().execute('select dwh_ods.load_card_application_data_hist()')
            conn_pg.commit()
            conn_pg.close()
        # Перенос в детальный слой хранилища
        # select dwh_ods.load_card_application_data_hist()
        task_2_load_from_dwh_stage_to_dwh_ods = PythonOperator(
            task_id='load_from_dwh_stage_to_dwh_ods',
            python_callable=load_from_dwh_stage_to_dwh_ods,
            op_kwargs={},
        )
        task_1_load_from_oltp_cdc_src_system_to_dwh_stage >> Label("Increment")>> task_2_load_from_dwh_stage_to_dwh_ods

    # группа построения представления и витрин (отчетов)
    with TaskGroup(group_id="gr_reports") as gr_reports:
        def tasks_start_in_state_count(**kwargs):
            conn_pg = connect(conn_pg_params)
            conn_pg.cursor().execute('select report.tasks_start_in_state_count()')
            conn_pg.commit()
            conn_pg.close()
        # Количество задач, получивших данный статус в данный день (даты без учета времени,
        # задача может в один день начатся и перейти в другие состояния, и будет учтена в подсчете для каждого из статусов)
        # итоговая таблица report.tasks_start_in_state_count
        # Данная витрина строится без связи с аналитическим календарем
        # select tasks_start_in_state_count()
        task_1_tasks_start_in_state_count = PythonOperator(
            task_id='tasks_start_in_state_count',
            python_callable=tasks_start_in_state_count,
            op_kwargs={},
        )

        def tasks_in_state_count(**kwargs):
            conn_pg = connect(conn_pg_params)
            conn_pg.cursor().execute('select report.tasks_in_state_count()')
            conn_pg.commit()
            conn_pg.close()
        # Количество задач, имеющих данный статус в данный день (даты без учета времени,
        # задача может в один день начатся и перейти в другие состояния, и будет учтена в подсчете для каждого из статусов)
        # итоговая таблица report.tasks_in_state_count
        # Данная витрина строится по связи с аналитическим календарем
        # select tasks_in_state_count()
        task_2_tasks_in_state_count = PythonOperator(
            task_id='tasks_in_state_count',
            python_callable=tasks_in_state_count,
            op_kwargs={},
        )

        def deleted_tasks_count(**kwargs):
            conn_pg = connect(conn_pg_params)
            conn_pg.cursor().execute('select report.deleted_tasks_count()')
            conn_pg.commit()
            conn_pg.close()
        # Количество задач, которые удалили в данном статусе в данный день
        # итоговая таблица report.deleted_tasks_count
        # Данная витрина строится по связи с аналитическим календарем
        # select report.deleted_tasks_count()
        task_3_deleted_tasks_count = PythonOperator(
            task_id='deleted_tasks_count',
            python_callable=deleted_tasks_count,
            op_kwargs={},
        )

        def deleted_tasks_in_state(**kwargs):
            conn_pg = connect(conn_pg_params)
            conn_pg.cursor().execute('select report.deleted_tasks_in_state()')
            conn_pg.commit()
            conn_pg.close()
        # Идентификаторы задач, их статус, в котором их удалили в данный день
        # итоговая таблица report.deleted_tasks_in_state
        # Данная витрина строится по связи с аналитическим календарем
        # select report.deleted_tasks_in_state()
        task_4_deleted_tasks_in_state = PythonOperator(
            task_id='deleted_tasks_in_state',
            python_callable=deleted_tasks_in_state,
            op_kwargs={},
        )



    run_f = DummyOperator(task_id='run_flg_dummy')
    last_step = DummyOperator(task_id='last_run_flg_dummy')
    run_f >>gr_generate_source_data >> Label("CDC") >> gr_load_data >> Label("For Report") >> gr_reports >> last_step

