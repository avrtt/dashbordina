from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import json
import os

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Helper functions
def extract_events_data(**kwargs):
    """Extract user events data from PostgreSQL"""
    try:
        # Define time window for data extraction
        execution_date = kwargs['execution_date']
        start_date = execution_date - timedelta(hours=1)
        end_date = execution_date
        
        # SQL query to extract data
        query = f"""
        SELECT * 
        FROM raw.user_events 
        WHERE event_timestamp >= '{start_date}' 
        AND event_timestamp < '{end_date}'
        """
        
        # Execute query using Postgres hook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = pg_hook.get_conn()
        
        # Load data into a pandas DataFrame
        df = pd.read_sql(query, connection)
        
        # Save to a temporary CSV file
        temp_path = f"/tmp/user_events_{execution_date.strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(temp_path, index=False)
        
        # Push file path to XCom for the next task
        return temp_path
        
    except Exception as e:
        logging.error(f"Error extracting events data: {e}")
        raise

def transform_events_data(**kwargs):
    """Transform user events data for analytics"""
    try:
        # Get file path from XCom
        ti = kwargs['ti']
        file_path = ti.xcom_pull(task_ids='extract_events_data')
        
        # Load data from CSV
        df = pd.read_csv(file_path)
        
        # Convert timestamp strings to datetime objects
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
        
        # Create date and hour columns for easier aggregation
        df['event_date'] = df['event_timestamp'].dt.date
        df['event_hour'] = df['event_timestamp'].dt.hour
        
        # Group by campaign, channel, and date+hour
        hourly_events = df.groupby(['campaign_id', 'channel_id', 'event_date', 'event_hour', 'event_name']).agg({
            'user_id': 'nunique',  # Unique users
            'event_id': 'count',   # Total events
        }).reset_index()
        
        hourly_events.rename(columns={
            'user_id': 'unique_users',
            'event_id': 'event_count'
        }, inplace=True)
        
        # Save transformed data
        transformed_path = f"/tmp/transformed_events_{kwargs['execution_date'].strftime('%Y%m%d_%H%M%S')}.csv"
        hourly_events.to_csv(transformed_path, index=False)
        
        # Clean up the original file
        if os.path.exists(file_path):
            os.remove(file_path)
            
        return transformed_path
        
    except Exception as e:
        logging.error(f"Error transforming events data: {e}")
        raise

def load_events_data(**kwargs):
    """Load transformed data into PostgreSQL analytics tables"""
    try:
        # Get file path from XCom
        ti = kwargs['ti']
        file_path = ti.xcom_pull(task_ids='transform_events_data')
        
        # Load transformed data
        df = pd.read_csv(file_path)
        
        # Get PostgreSQL hook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Prepare data for insertion into hourly_events table
        # First check if the table exists, create if not
        pg_hook.run("""
        CREATE TABLE IF NOT EXISTS analytics.hourly_events (
            id SERIAL PRIMARY KEY,
            campaign_id INTEGER,
            channel_id INTEGER,
            event_date DATE,
            event_hour INTEGER,
            event_name VARCHAR(100),
            unique_users INTEGER,
            event_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Insert data row by row (can be optimized with COPY for larger datasets)
        for _, row in df.iterrows():
            pg_hook.run(f"""
            INSERT INTO analytics.hourly_events
            (campaign_id, channel_id, event_date, event_hour, event_name, unique_users, event_count)
            VALUES
            ({row['campaign_id'] if pd.notna(row['campaign_id']) else 'NULL'},
             {row['channel_id'] if pd.notna(row['channel_id']) else 'NULL'},
             '{row['event_date']}',
             {row['event_hour']},
             '{row['event_name']}',
             {row['unique_users']},
             {row['event_count']})
            """)
            
        # Clean up the transformed file
        if os.path.exists(file_path):
            os.remove(file_path)
            
        return "Data loaded successfully"
        
    except Exception as e:
        logging.error(f"Error loading events data: {e}")
        raise

def calculate_metrics(**kwargs):
    """Calculate marketing metrics and update materialized views"""
    try:
        # Get PostgreSQL hook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Refresh materialized views to ensure they contain the latest data
        pg_hook.run("REFRESH MATERIALIZED VIEW analytics.daily_campaign_performance")
        pg_hook.run("REFRESH MATERIALIZED VIEW analytics.daily_channel_performance")
        pg_hook.run("REFRESH MATERIALIZED VIEW analytics.segment_performance")
        
        # Calculate additional metrics like CAC, CLV, ROAS
        pg_hook.run("""
        -- Calculate Customer Acquisition Cost (CAC) by segment
        CREATE OR REPLACE VIEW analytics.segment_cac AS
        SELECT
            s.segment_id,
            s.segment_name,
            DATE(c.conversion_timestamp) AS date,
            SUM(ca.spend_to_date) / NULLIF(COUNT(DISTINCT c.user_id), 0) AS cac
        FROM 
            analytics.conversions c
        JOIN 
            analytics.user_segments us ON c.user_id = us.user_id
        JOIN 
            analytics.segments s ON us.segment_id = s.segment_id
        JOIN 
            analytics.campaigns ca ON c.campaign_id = ca.campaign_id
        WHERE 
            c.conversion_type = 'new_customer'
        GROUP BY 
            s.segment_id, s.segment_name, DATE(c.conversion_timestamp);
            
        -- Calculate Return on Ad Spend (ROAS) by channel
        CREATE OR REPLACE VIEW analytics.channel_roas AS
        SELECT
            ch.channel_id,
            ch.channel_name,
            DATE(c.conversion_timestamp) AS date,
            SUM(c.conversion_value) / NULLIF(SUM(ca.spend_to_date), 0) AS roas
        FROM 
            analytics.conversions c
        JOIN 
            analytics.campaigns ca ON c.campaign_id = ca.campaign_id
        JOIN 
            analytics.channels ch ON ca.channel_id = ch.channel_id
        GROUP BY 
            ch.channel_id, ch.channel_name, DATE(c.conversion_timestamp);
        """)
        
        return "Metrics calculated successfully"
        
    except Exception as e:
        logging.error(f"Error calculating metrics: {e}")
        raise

def archive_daily_data(**kwargs):
    """Archive daily data snapshots"""
    try:
        execution_date = kwargs['execution_date']
        yesterday = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Get PostgreSQL hook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Create daily snapshot tables if they don't exist
        pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS archive.daily_events_{yesterday.replace('-', '_')} AS
        SELECT * FROM raw.user_events 
        WHERE DATE(event_timestamp) = '{yesterday}';
        
        CREATE TABLE IF NOT EXISTS archive.daily_conversions_{yesterday.replace('-', '_')} AS
        SELECT * FROM analytics.conversions 
        WHERE DATE(conversion_timestamp) = '{yesterday}';
        """)
        
        return f"Daily data archived for {yesterday}"
        
    except Exception as e:
        logging.error(f"Error archiving daily data: {e}")
        raise

# Define the DAG
with DAG(
    'marketing_etl_hourly',
    default_args=default_args,
    description='Hourly ETL for marketing data',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # Task to extract events data
    extract_task = PythonOperator(
        task_id='extract_events_data',
        python_callable=extract_events_data,
        provide_context=True,
    )
    
    # Task to transform events data
    transform_task = PythonOperator(
        task_id='transform_events_data',
        python_callable=transform_events_data,
        provide_context=True,
    )
    
    # Task to load events data
    load_task = PythonOperator(
        task_id='load_events_data',
        python_callable=load_events_data,
        provide_context=True,
    )
    
    # Task to calculate metrics
    metrics_task = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics,
        provide_context=True,
    )
    
    # Define task dependencies
    extract_task >> transform_task >> load_task >> metrics_task


# Daily archiving DAG
with DAG(
    'marketing_daily_archive',
    default_args=default_args,
    description='Daily archiving of marketing data',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as archive_dag:
    
    # Task to archive daily data
    archive_task = PythonOperator(
        task_id='archive_daily_data',
        python_callable=archive_daily_data,
        provide_context=True,
    )
    
    # Optional task to clean up old data if needed
    cleanup_sql = """
    -- This SQL would delete data older than X days if required
    -- DELETE FROM raw.user_events WHERE event_timestamp < NOW() - INTERVAL '90 days';
    """
    
    cleanup_task = PostgresOperator(
        task_id='cleanup_old_data',
        postgres_conn_id='postgres_default',
        sql=cleanup_sql,
    )
    
    archive_task >> cleanup_task 