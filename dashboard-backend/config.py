import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration settings for Flask application"""
    
    # Flask settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    # Server settings
    HOST = os.getenv('HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', 5000))
    
    # AWS Configuration
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
    # AWS_REGION = os.getenv('AWS_REGION', 'ap-south-1')
    S3_BUCKET = os.getenv('S3_BUCKET', 'analytic-olist')
    S3_BUSINESS_RES=os.getenv("S3_BUSINESS_RES","analytic-olist11")
    # Cache settings
    CACHE_TIMEOUT = int(os.getenv('CACHE_TIMEOUT', 300))  # 5 minutes
    
    # CORS settings
    CORS_ORIGINS = os.getenv('CORS_ORIGINS', '*').split(',')
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        missing = []
        
        if not cls.AWS_ACCESS_KEY_ID:
            missing.append('AWS_ACCESS_KEY')
        if not cls.AWS_SECRET_ACCESS_KEY:
            missing.append('AWS_SECRET_KEY')
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        print(f"Configuration loaded successfully")
        print(f"  S3 Bucket: {cls.S3_BUCKET}")
        # print(f"  AWS Region: {cls.AWS_REGION}")
        print(f"  Server: {cls.HOST}:{cls.PORT}")