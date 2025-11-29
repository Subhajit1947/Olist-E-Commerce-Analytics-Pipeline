from main.utils.s3_client_object import *
import os
from dotenv import load_dotenv

load_dotenv("/opt/app/.env")
s3_client_provider = S3ClientProvider(aws_access_key=os.getenv('AWS_ACCESS_KEY'),aws_secret_key=os.getenv('AWS_SECRET_KEY'))
s3_client = s3_client_provider.get_client()


def upload_to_s3(s3_directory, s3_bucket, local_file_path):
    s3_prefix = f"{s3_directory}"
    try:
        for root, dirs, files in os.walk(local_file_path):
            for file in files:
                print(file)
                local_file_path = os.path.join(root,file)
                s3_key = f"{s3_prefix}{file}"
                s3_client.upload_file(local_file_path, s3_bucket, s3_key)
    except Exception as e:
        raise e

local_file_path = f"/opt/app/data/"
upload_to_s3(os.getenv("S3_DIRECTORY"), os.getenv("S3_BUCKET"), local_file_path)