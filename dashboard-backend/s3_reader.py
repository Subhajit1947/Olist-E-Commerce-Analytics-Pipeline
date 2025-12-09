"""
S3 Data Reader Module
Handles reading CSV files from AWS S3
"""
import boto3
import pandas as pd
from io import StringIO
from functools import lru_cache
import time
import logging
from .config import Config

logger = logging.getLogger(__name__)

class S3DataReader:
    """Reader for S3 CSV files"""
    
    def __init__(self, bucket_name,aws_access_key_id=None, aws_secret_access_key=None):
        """
        Initialize S3 reader
        
        Args:
            bucket_name: S3 bucket name
            region_name: AWS region
            aws_access_key_id: AWS access key (optional - uses IAM role if not provided)
            aws_secret_access_key: AWS secret key (optional)
        """
        self.bucket_name = bucket_name
        
        # Initialize S3 client
        if aws_access_key_id and aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
        else:
            # Use IAM role or default credentials
            self.s3_client = boto3.client('s3')
        
        logger.info(f"S3 Reader initialized for bucket: {bucket_name}")
    
    def test_connection(self):
        """Test S3 connection"""
        try:
            self.s3_client.list_buckets()
            logger.info("S3 connection test successful")
            return True
        except Exception as e:
            logger.error(f"S3 connection failed: {str(e)}")
            raise
    
    @lru_cache(maxsize=100)
    def read_business_answer(self, question_num):
        """
        Read CSV data for a specific business question from S3
        
        Args:
            question_num: Question number (1-10)
            refresh_cache: Force refresh cache
            
        Returns:
            pandas.DataFrame with the data
        """
        try:
            # Construct S3 path
            prefix = f"{Config.S3_BUSINESS_RES}/Q{question_num}/"
            
            logger.info(f"Reading data from S3: {self.bucket_name}/{prefix}")
            
            # List objects in the prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response or len(response['Contents']) == 0:
                logger.warning(f"No files found at {prefix}")
                return pd.DataFrame()
            
            # Find CSV files
            csv_files = []
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.csv') and not key.endswith('/'):
                    csv_files.append(key)
            
            if not csv_files:
                logger.warning(f"No CSV files found at {prefix}")
                return pd.DataFrame()
            
            # Read the first CSV file (assuming one file per question)
            csv_key = csv_files[0]
            logger.info(f"Reading CSV file: {csv_key}")
            
            # Get object from S3
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=csv_key)
            csv_content = obj['Body'].read().decode('utf-8')
            
            # Parse CSV
            df = pd.read_csv(StringIO(csv_content))
            
            logger.info(f"Successfully loaded {len(df)} rows from {csv_key}")
            logger.info(f"Columns: {list(df.columns)}")
            
            return df
            
        except self.s3_client.exceptions.NoSuchBucket:
            logger.error(f"Bucket not found: {self.bucket_name}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error reading from S3: {str(e)}")
            return pd.DataFrame()
    
