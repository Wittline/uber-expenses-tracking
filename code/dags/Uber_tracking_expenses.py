import logging
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
import sql_statements

dag = DAG(
    'Uber_tracking_expenses',
     description = 'Uber tracking expenses',
     start_date = datetime.datetime.now(),
     schedule_interval= '@weekly',
     tags=['UBER']
)

def fixing_locations():
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.fixing_locations
    redshift_hook.run(sql_stmt)
    print(f"Locations fixed successfully.")



def data_quality_checks(tables):
    tables = tables.split(',')
    redshift_hook = PostgresHook("redshift")
    for table in tables:
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")


def cleaning_stagings():
    redshift_hook = PostgresHook("redshift")
    sql_stmt = sql_statements.drop_staging
    redshift_hook.run(sql_stmt)
    print(f"Staging tables dropped successfully.")



def loading_table(table):

    redshift_hook = PostgresHook("redshift")
    
    if table == 'dim_users':
        sql_stmt = sql_statements.load_dim_users
    elif table == 'dim_times':
        sql_stmt = sql_statements.load_dim_times
    elif table == 'dim_products':
        sql_stmt = sql_statements.load_dim_products
    elif table == 'dim_products_order':
        sql_stmt = sql_statements.load_dim_products_order
    elif table == 'dim_restaurants':
        sql_stmt = sql_statements.load_dim_restaurants
    elif table == 'dim_locations':
        sql_stmt = sql_statements.load_dim_locations
    elif table == 'dim_weekday':
        sql_stmt = sql_statements.load_dim_weekday
    elif table == 'dim_month':
        sql_stmt= sql_statements.load_dim_month
    elif table == 'dim_year':
        sql_stmt = sql_statements.load_dim_year
    elif table == 'dim_hour':
        sql_stmt = sql_statements.load_dim_hour
    elif table == 'fact_rides':
        sql_stmt = sql_statements.load_fact_rides
    else:
        sql_stmt = sql_statements.load_fact_eats

    redshift_hook.run(sql_stmt)
    print(f"Table {table} was loaded successfully.")



def create_table(table):

    redshift_hook = PostgresHook("redshift")
    if table == 'staging_eats':
        sql_stmt = sql_statements.create_staging_eats
    elif table == 'staging_rides':
        sql_stmt = sql_statements.create_staging_rides
    elif table == 'staging_eats_items':
        sql_stmt = sql_statements.create_staging_eats_items       
    elif table == 'dim_users':
        sql_stmt = sql_statements.create_dim_users
    elif table == 'dim_times':
        sql_stmt = sql_statements.create_dim_times
    elif table == 'dim_products':
        sql_stmt = sql_statements.create_dim_products
    elif table == 'dim_products_order':
        sql_stmt = sql_statements.create_dim_products_order
    elif table == 'dim_restaurants':
        sql_stmt = sql_statements.create_dim_restaurants
    elif table == 'dim_locations':
        sql_stmt = sql_statements.create_dim_locations
    elif table == 'dim_weekday':
        sql_stmt = sql_statements.create_dim_weekday
    elif table == 'dim_month':
        sql_stmt= sql_statements.create_dim_month
    elif table == 'dim_year':
        sql_stmt = sql_statements.create_dim_year
    elif table == 'dim_hour':
        sql_stmt = sql_statements.create_dim_hour
    elif table == 'fact_rides':
        sql_stmt = sql_statements.create_fact_rides
    else:
        sql_stmt = sql_statements.create_fact_eats

    redshift_hook.run(sql_stmt)
    print(f"Table {table} was created successfully.")



def staging_eats_to_redshift(*args, **kwargs):
    aws_hook = AwsHook(aws_conn_id ="aws_credentials", client_type ='s3')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    bucket = str(Variable.get('s3_bucket'))    
    sql_stmt = sql_statements.COPY_ALL_EATS_SQL.format(
        bucket,
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)
    print(f"Table staging_eats was loaded successfully.") 

def staging_rides_to_redshift(*args, **kwargs):
    aws_hook = AwsHook(aws_conn_id ="aws_credentials", client_type ='s3')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    bucket = str(Variable.get('s3_bucket'))
    sql_stmt = sql_statements.COPY_ALL_RIDES_SQL.format(
        bucket,
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)
    print(f"Table staging_rides was loaded successfully.") 

def staging_items_to_redshift(*args, **kwargs):
    aws_hook = AwsHook(aws_conn_id ="aws_credentials", client_type ='s3')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    bucket = str(Variable.get('s3_bucket'))
    sql_stmt = sql_statements.COPY_ALL_EATS_ITEMS_SQL.format(
        bucket,
        credentials.access_key,
        credentials.secret_key,
    )
    redshift_hook.run(sql_stmt)
    print(f"Table staging_items was loaded successfully.") 

def processing_rides_receipts(rides):

    import eml_parser
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.models import Variable
    from  data_receipts import data_receipts
 
    print("Processing Uber Rides Receipts")

    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = 'uber-tracking-expenses-bucket-s3-' + str(Variable.get('s3_bucket'))

    replace_chars = ["[","]","'"]

    for r in replace_chars:
        rides = rides.replace(r, '')
    
    rides = rides.split(',')
    all_receipts = []
    dr = None

    for ride in range(0, len(rides)):
        print("Processing receipt:", rides[ride])
        obj = hook.get_key(rides[ride].strip(), bucket)
        bt = obj.get()['Body'].read()
        eml = eml_parser.eml_parser.decode_email_b(bt,True,True)        
        dr = data_receipts('rides', eml, rides[ride].strip(), ride)
        result = dr.get_data()                
        all_receipts.append(result)

    print("SAVING ALL.........")
    dr.save_as_csv(all_receipts, bucket, '' )       

def processing_eats_receipts(eats):
    
    import eml_parser
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.models import Variable    
    from  data_receipts import data_receipts

    print("Processing Uber Eats Receipts")

    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = 'uber-tracking-expenses-bucket-s3-' + str(Variable.get('s3_bucket'))

    replace_chars = ["[","]","'"]

    for r in replace_chars:
        eats = eats.replace(r, '')
    
    eats = eats.split(',')
    all_receipts = []
    all_items = []

    dr = None
    
    for eat in range(0, len(eats)):
        print("Processing receipt:", eats[eat])
        obj = hook.get_key(eats[eat].strip(),  bucket)
        bt = obj.get()['Body'].read()
        eml = eml_parser.eml_parser.decode_email_b(bt,True,True)
        dr = data_receipts('eats', eml, eats[eat].strip(), eat)
        receipts, items = dr.get_data()
        all_receipts.append(receipts)

        for i in items:
            all_items.append(i)

    print("SAVING ALL.........")
    dr.save_as_csv(all_receipts, bucket,  '')
    dr.save_as_csv(all_items, bucket, 'items_')


def Start_UBER_Business(**kwargs):

    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = 'uber-tracking-expenses-bucket-s3-' + str(Variable.get('s3_bucket'))
    prefix = 'unprocessed_receipts'

    print(bucket, prefix)
    
    keys = hook.list_keys(bucket, prefix=prefix)

    exclude_receipts = ['canceled', 'failed']
    uber_eats = []
    uber_rides = []

    keys = [key for key in keys if not any(k in key.lower() for k in exclude_receipts)]

    for key in keys:
        if 'eats' in key.lower():
            uber_eats.append(key)
        elif 'trip' in key.lower() or 'viaje' in key.lower():
            uber_rides.append(key)
        else:
            continue
    
    kwargs['ti'].xcom_push(key='uber_eats', value = uber_eats )
    kwargs['ti'].xcom_push(key='uber_rides', value = uber_rides )


# /// DATA QUALITY CHECKS AND CLEANING ///

running_cleaning_task = PythonOperator(
    task_id='cleaning_staging',
    dag=dag,
    python_callable=cleaning_stagings,
)

data_quality_checks_task = PythonOperator(
    task_id='data_quality_checks',
    dag=dag,
    python_callable=data_quality_checks,    
    op_kwargs={
        'tables': 'fact_eats,fact_rides',
    }
)

# /// LOADING TABLES ///

loading_fact_rides_task = PythonOperator(
    task_id='loading_fact_eats',
    dag=dag,
    op_kwargs={'table': 'fact_eats'},
    python_callable=loading_table,
)

loading_fact_eats_task = PythonOperator(
    task_id='loading_fact_rides',
    dag=dag,
    op_kwargs={'table': 'fact_rides'},
    python_callable=loading_table,
)

loading_dim_products_order_task = PythonOperator(
    task_id='loading_dim_products_order',
    dag=dag,
    op_kwargs={'table': 'dim_products_order'},
    python_callable=loading_table,
)

loading_dim_products_task = PythonOperator(
    task_id='loading_dim_products',
    dag=dag,
    op_kwargs={'table': 'dim_products'},
    python_callable=loading_table,
)

loading_dim_restaurants_task = PythonOperator(
    task_id='loading_dim_restaurants',
    dag=dag,
    op_kwargs={'table': 'dim_restaurants'},
    python_callable=loading_table,
)

loading_dim_locations_task = PythonOperator(
    task_id='loading_dim_locations',
    dag=dag,
    op_kwargs={'table': 'dim_locations'},
    python_callable=loading_table,
)

loading_dim_users_task = PythonOperator(
    task_id='loading_dim_users',
    dag=dag,
    op_kwargs={'table': 'dim_users'},
    python_callable=loading_table,
)

loading_dim_month_task = PythonOperator(
    task_id='loading_dim_month',
    dag=dag,
    op_kwargs={'table': 'dim_month'},
    python_callable=loading_table,
)

loading_dim_weekday_task = PythonOperator(
    task_id='loading_dim_weekday',
    dag=dag,
    op_kwargs={'table': 'dim_weekday'},
    python_callable=loading_table,
)


loading_dim_hour_task = PythonOperator(
    task_id='loading_dim_hour',
    dag=dag,
    op_kwargs={'table': 'dim_hour'},
    python_callable=loading_table,
)

loading_dim_year_task = PythonOperator(
    task_id='loading_dim_year',
    dag=dag,
    op_kwargs={'table': 'dim_year'},
    python_callable=loading_table,
)

loading_dim_times_task = PythonOperator(
    task_id='loading_dim_times',
    dag=dag,
    op_kwargs={'table': 'dim_times'},
    python_callable=loading_table,
)

# //// FIXING LOCATIONS ////

fixing_locations_task = PythonOperator(
    task_id='fixing_locations',
    dag=dag,
    python_callable=fixing_locations,
)

# /// CREATING TABLES ///

creating_dim_hour_task = PythonOperator(
    task_id='dim_hour_table',
    dag=dag,
    op_kwargs={'table': 'dim_hour'},
    python_callable=create_table,
)

creating_dim_month_task = PythonOperator(
    task_id='dim_month_table',
    dag=dag,
    op_kwargs={'table': 'dim_month'},
    python_callable=create_table,
)

creating_dim_year_task = PythonOperator(
    task_id='dim_year_table',
    dag=dag,
    op_kwargs={'table': 'dim_year'},
    python_callable=create_table,
)

creating_dim_weekday_task = PythonOperator(
    task_id='dim_weekday_table',
    dag=dag,
    op_kwargs={'table': 'dim_weekday'},
    python_callable=create_table,
)

creating_fact_rides_task = PythonOperator(
    task_id='fact_rides_table',
    dag=dag,
    op_kwargs={'table': 'fact_rides'},
    python_callable=create_table,
)

creating_fact_eats_task = PythonOperator(
    task_id='fact_eats_table',
    dag=dag,
    op_kwargs={'table': 'fact_eats'},
    python_callable=create_table,
)

creating_dim_locations_task = PythonOperator(
    task_id='dim_locations_table',
    dag=dag,
    op_kwargs={'table': 'dim_locations'},
    python_callable=create_table,
)


creating_dim_restaurants_task = PythonOperator(
    task_id='dim_restaurants_table',
    dag=dag,
    op_kwargs={'table': 'dim_restaurants'},
    python_callable=create_table,
)

creating_dim_products_task = PythonOperator(
    task_id='dim_products_table',
    dag=dag,
    op_kwargs={'table': 'dim_products'},
    python_callable=create_table,
)

creating_dim_products_order_task = PythonOperator(
    task_id='dim_products_order_table',
    dag=dag,
    op_kwargs={'table': 'dim_products_order'},
    python_callable=create_table,
)

creating_dim_users_task = PythonOperator(
    task_id='dim_users_table',
    dag=dag,
    op_kwargs={'table': 'dim_users'},
    python_callable=create_table,
)

creating_dim_times_task = PythonOperator(
    task_id='dim_times_table',
    dag=dag,
    op_kwargs={'table': 'dim_times'},
    python_callable=create_table,
)

creating_staging_eats_table_task = PythonOperator(
    task_id='staging_eats_table',
    dag=dag,
    op_kwargs={'table': 'staging_eats'},
    python_callable=create_table,
)

creating_staging_items_table_task = PythonOperator(
    task_id='staging_items_table',
    dag=dag,
    op_kwargs={'table': 'staging_eats_items'},
    python_callable=create_table,
)

creating_staging_rides_table_task = PythonOperator(
    task_id='staging_rides_table',
    dag=dag,
    op_kwargs={'table': 'staging_rides'},
    python_callable=create_table,
)


uber_expenses_dwh_ready_task = DummyOperator(
    task_id = 'UBER_expenses_ready'
)

fact_tables_ready_task = DummyOperator(
    task_id = 'fact_tables_ready'
)

dimensions_tables_ready_task = DummyOperator(
    task_id = 'dimension_tables_ready'
)

tables_created_redshift_task = DummyOperator(
    task_id = 'tables_created_in_redshift'
)

staging_redshift_ready_task = DummyOperator(
    task_id = 's3_to_redshift_ready'
)

s3_receipts_ready_task = DummyOperator(
    task_id = 's3_receipts_ready'
)


staging_eats_to_redshift_task = PythonOperator(
    task_id='s3_staging_eats_to_redshift',
    dag=dag,
    python_callable=staging_eats_to_redshift,
)

staging_items_to_redshift_task = PythonOperator(
    task_id='s3_staging_items_to_redshift',
    dag=dag,
    python_callable=staging_items_to_redshift,
)

staging_rides_to_redshift_task = PythonOperator(
    task_id='s3_staging_rides_to_redshift',
    dag=dag,
    python_callable=staging_rides_to_redshift,
)

rides_receipts_to_s3_task = PythonVirtualenvOperator(
        task_id = 'rides_receipts_to_s3',
        python_callable = processing_rides_receipts,
        requirements=["fsspec == 0.8.7", "s3fs == 0.5.2", "bs4==0.0.1", "eml-parser==1.14.4"],
        system_site_packages=True,
        op_kwargs={'rides': " {{ ti.xcom_pull(task_ids='start_UBER_receipts_processing', key='uber_rides') }}"},  
        dag = dag
)

eats_receipts_to_s3_task = PythonVirtualenvOperator(
        task_id = 'eats_receipts_to_s3',
        python_callable = processing_eats_receipts,
        requirements=["fsspec == 0.8.7", "s3fs == 0.5.2", "bs4==0.0.1", "eml-parser==1.14.4"],
        system_site_packages=True,
        op_kwargs={'eats': " {{ ti.xcom_pull(task_ids='start_UBER_receipts_processing', key='uber_eats') }}"},  
        dag = dag
)

start_UBER_receipts_processing_task = PythonOperator(
        task_id= 'start_UBER_receipts_processing',
        python_callable = Start_UBER_Business,
        dag = dag
)

start_UBER_receipts_processing_task >> [rides_receipts_to_s3_task, eats_receipts_to_s3_task]

[rides_receipts_to_s3_task, eats_receipts_to_s3_task] >> s3_receipts_ready_task


s3_receipts_ready_task >> [creating_staging_eats_table_task, 
                           creating_staging_items_table_task, 
                           creating_staging_rides_table_task, 
                           creating_dim_users_task, 
                           creating_dim_times_task,
                           creating_dim_products_order_task,
                           creating_dim_products_task,
                           creating_dim_locations_task,
                           creating_dim_restaurants_task,
                           creating_fact_eats_task,
                           creating_fact_rides_task,
                           creating_dim_hour_task,
                           creating_dim_month_task,
                           creating_dim_year_task,
                           creating_dim_weekday_task]

[creating_staging_eats_table_task, 
creating_staging_items_table_task, 
creating_staging_rides_table_task, 
creating_dim_users_task, 
creating_dim_times_task,
creating_dim_products_order_task,
creating_dim_products_task,
creating_dim_locations_task,
creating_dim_restaurants_task,
creating_fact_eats_task,
creating_fact_rides_task,
creating_dim_hour_task,
creating_dim_month_task,
creating_dim_year_task,
creating_dim_weekday_task] >> tables_created_redshift_task

tables_created_redshift_task >> [staging_rides_to_redshift_task, staging_items_to_redshift_task, staging_eats_to_redshift_task]


[staging_rides_to_redshift_task, staging_items_to_redshift_task, staging_eats_to_redshift_task] >> staging_redshift_ready_task


staging_redshift_ready_task >> [loading_dim_users_task, 
                                loading_dim_times_task,
                                loading_dim_year_task,
                                loading_dim_month_task,
                                loading_dim_weekday_task,
                                loading_dim_hour_task,
                                fixing_locations_task,                                
                                loading_dim_products_task]

fixing_locations_task >> loading_dim_locations_task >> loading_dim_restaurants_task

loading_dim_products_task >> loading_dim_products_order_task


[loading_dim_users_task, 
loading_dim_times_task,
loading_dim_year_task,
loading_dim_month_task,
loading_dim_weekday_task,
loading_dim_hour_task,
loading_dim_restaurants_task,
loading_dim_products_order_task] >> dimensions_tables_ready_task


dimensions_tables_ready_task >> [loading_fact_rides_task, loading_fact_eats_task]
[loading_fact_rides_task, loading_fact_eats_task] >> fact_tables_ready_task
fact_tables_ready_task >> data_quality_checks_task >> running_cleaning_task >> uber_expenses_dwh_ready_task
