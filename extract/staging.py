import boto3
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()
bucket_name = os.getenv('BUCKET_NAME')
staging_prefix = os.getenv('STAGING_PREFIX')

extract_dir = Path(__file__).resolve().parent / "extracted_data"

s3 = boto3.client('s3')

for file_path in extract_dir.glob("*.parquet"):
    file_stem = file_path.stem
    s3_key = f"{staging_prefix}{file_stem}/{file_path.name}"
    s3.upload_file(str(file_path), bucket_name, s3_key)
    print(f"Uploaded {file_path.name} to s3://{bucket_name}/{s3_key}")