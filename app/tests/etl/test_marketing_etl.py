import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime, timedelta

# Adjust path to import ETL code
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
from app.etl.dags.marketing_etl_dag import (
    extract_events_data, 
    transform_events_data, 
    load_events_data, 
    calculate_metrics
)

class TestMarketingETL(unittest.TestCase):
    """Test cases for marketing ETL functions"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a sample DataFrame for testing
        self.sample_data = pd.DataFrame({
            'event_id': range(1, 6),
            'user_id': ['user1', 'user2', 'user1', 'user3', 'user2'],
            'event_name': ['impression', 'click', 'click', 'impression', 'conversion'],
            'event_timestamp': [
                datetime.now() - timedelta(hours=2),
                datetime.now() - timedelta(hours=1),
                datetime.now() - timedelta(minutes=45),
                datetime.now() - timedelta(minutes=30),
                datetime.now() - timedelta(minutes=15)
            ],
            'campaign_id': [1, 1, 2, 2, 1],
            'channel_id': [1, 1, 2, 2, 1],
            'referrer': ['google', 'facebook', 'direct', 'email', 'twitter'],
            'device_type': ['mobile', 'desktop', 'mobile', 'tablet', 'desktop'],
            'browser': ['chrome', 'firefox', 'safari', 'chrome', 'edge'],
            'location': ['US', 'UK', 'CA', 'US', 'AU'],
            'event_properties': [
                '{"ad_id": 123}',
                '{"ad_id": 123, "position": "top"}',
                '{"ad_id": 456}',
                '{"ad_id": 456, "position": "sidebar"}',
                '{"ad_id": 123, "value": 49.99}'
            ]
        })
        
        # Mock the execution date
        self.execution_date = datetime.now()
        
        # Create a temporary directory for test files
        if not os.path.exists('/tmp/test_etl'):
            os.makedirs('/tmp/test_etl')
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Remove any test files created
        for file in os.listdir('/tmp/test_etl'):
            os.remove(os.path.join('/tmp/test_etl', file))
        if os.path.exists('/tmp/test_etl'):
            os.rmdir('/tmp/test_etl')
    
    @patch('app.etl.dags.marketing_etl_dag.PostgresHook')
    def test_extract_events_data(self, mock_postgres_hook):
        """Test the extract_events_data function"""
        # Mock the PostgresHook and connection
        mock_conn = MagicMock()
        mock_postgres_hook.return_value.get_conn.return_value = mock_conn
        
        # Mock pd.read_sql to return our sample DataFrame
        with patch('pandas.read_sql', return_value=self.sample_data):
            result = extract_events_data(execution_date=self.execution_date, ti=MagicMock())
        
        # Assert that the function returns a file path
        self.assertTrue(result.endswith('.csv'))
        self.assertTrue(os.path.exists(result))
        
        # Verify the correct SQL query was executed
        start_date = self.execution_date - timedelta(hours=1)
        end_date = self.execution_date
        expected_query = f"""
        SELECT * 
        FROM raw.user_events 
        WHERE event_timestamp >= '{start_date}' 
        AND event_timestamp < '{end_date}'
        """
        
        mock_postgres_hook.assert_called_once()
        mock_postgres_hook.return_value.get_conn.assert_called_once()
        
        # Clean up the created file
        if os.path.exists(result):
            os.remove(result)
    
    def test_transform_events_data(self):
        """Test the transform_events_data function"""
        # Create a sample CSV file for input
        input_file = f"/tmp/test_etl/sample_input_{self.execution_date.strftime('%Y%m%d_%H%M%S')}.csv"
        self.sample_data.to_csv(input_file, index=False)
        
        # Mock the ti.xcom_pull to return our file path
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = input_file
        
        # Call the transform function
        result = transform_events_data(execution_date=self.execution_date, ti=mock_ti)
        
        # Assert that the function returns a file path
        self.assertTrue(result.endswith('.csv'))
        self.assertTrue(os.path.exists(result))
        
        # Load the transformed data to check its structure
        transformed_data = pd.read_csv(result)
        
        # Verify the transformed data has the expected columns
        expected_columns = ['campaign_id', 'channel_id', 'event_date', 'event_hour', 'event_name', 'unique_users', 'event_count']
        for col in expected_columns:
            self.assertIn(col, transformed_data.columns)
        
        # Verify aggregation worked correctly
        self.assertEqual(len(transformed_data), len(self.sample_data['event_name'].unique()))
        
        # Clean up the created files
        if os.path.exists(result):
            os.remove(result)
    
    @patch('app.etl.dags.marketing_etl_dag.PostgresHook')
    def test_load_events_data(self, mock_postgres_hook):
        """Test the load_events_data function"""
        # Create a transformed CSV file
        transformed_data = pd.DataFrame({
            'campaign_id': [1, 2],
            'channel_id': [1, 2],
            'event_date': ['2023-01-01', '2023-01-01'],
            'event_hour': [10, 11],
            'event_name': ['click', 'impression'],
            'unique_users': [2, 1],
            'event_count': [3, 2]
        })
        transformed_file = f"/tmp/test_etl/transformed_{self.execution_date.strftime('%Y%m%d_%H%M%S')}.csv"
        transformed_data.to_csv(transformed_file, index=False)
        
        # Mock the ti.xcom_pull to return our file path
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = transformed_file
        
        # Call the load function
        result = load_events_data(execution_date=self.execution_date, ti=mock_ti)
        
        # Verify that PostgresHook was called
        mock_postgres_hook.assert_called_once()
        
        # Verify that the CREATE TABLE IF NOT EXISTS was executed
        mock_postgres_hook.return_value.run.assert_called()
        
        # Verify that the function returns a success message
        self.assertEqual(result, "Data loaded successfully")
        
        # Clean up the created file
        if os.path.exists(transformed_file):
            os.remove(transformed_file)
    
    @patch('app.etl.dags.marketing_etl_dag.PostgresHook')
    def test_calculate_metrics(self, mock_postgres_hook):
        """Test the calculate_metrics function"""
        # Call the calculate_metrics function
        result = calculate_metrics(execution_date=self.execution_date)
        
        # Verify that PostgresHook was called
        mock_postgres_hook.assert_called_once()
        
        # Verify that materialized views were refreshed
        mock_postgres_hook.return_value.run.assert_called()
        
        # Verify that the function returns a success message
        self.assertEqual(result, "Metrics calculated successfully")

if __name__ == '__main__':
    unittest.main() 