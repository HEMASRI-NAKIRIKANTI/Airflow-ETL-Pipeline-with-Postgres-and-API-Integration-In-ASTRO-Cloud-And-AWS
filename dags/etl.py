from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow import DAG
from airflow.decorators import  task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

with DAG(
    dag_id="Nasa_code_postgres",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # step-1 Table creation if that doesn't exist
    
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS apod_data(
        id SERIAL PRIMARY KEY,
        title VARCHAR(255),
        explanation TEXT,
        url TEXT,
        date DATE,
        media_type VARCHAR(255)
        ); """

        postgres_hook.run(create_table_query)

    # Step-2 Extract the data from the NASA Data - Creating a extract pipeline
    #   planetary/apod?api_key=9W5sO2FJHymEwVqQeaDmSLR2Pm0q3kyyeJar8twQ
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',  ## Connection ID Defined In Airflow For NASA API
        endpoint='planetary/apod', ## NASA API enpoint for APOD
        method='GET',
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"}, ## USe the API Key from the connection
        response_filter=lambda response:response.json(), ## Convert response to json
    )
   
    # step-3 Transform the data (pick the information that we want to save)

    @task
    def transform_data(response):
        print(response)
        apod_data={
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')

        }
        return apod_data

    # Step-4 Load the data into the postgres
    @task
    def  load_data_to_postgres(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        insert_query = f"""
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%(title)s, %(explanation)s, %(url)s, %(date)s, %(media_type)s)
        """
        postgres_hook.run(insert_query, parameters=apod_data)
    # Step-5 DB viewer to visualize the data



    # Step-6 Create a trigger to automatically run the DAG every day and create the dependencies
    #Extract
    create_table() >> extract_apod
    api_response = extract_apod.output
    # Transform
    transform_data_task = transform_data(api_response)
    # Load
    load_data_to_postgres(transform_data_task)