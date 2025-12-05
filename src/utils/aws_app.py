"""
AWS S3 Storage Utility Module
Handles all S3 operations for the utility billing application.
"""

import os
import json
import tempfile
import logging
from pathlib import Path
from io import BytesIO
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = os.getenv("Secret_access_key")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "utility-billing-data")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1").strip()

# Clean up region name - extract just the region code if it contains extra text
if " " in AWS_REGION:
    AWS_REGION = AWS_REGION.split()[-1]  # Get last part (e.g., "us-east-2" from "US East (Ohio) us-east-2")

logger.info(f"AWS Configuration: region={AWS_REGION}, bucket={BUCKET_NAME}")

# Verify credentials are set
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    logger.error("AWS credentials not found in environment variables")
    s3_client = None
else:
    # Initialize S3 client
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        logger.info(f"✅ S3 client initialized successfully with region: {AWS_REGION}")
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        s3_client = None


# ==================== UPLOAD FUNCTIONS ====================

def upload_file_to_s3(file_path, s3_key):
    """
    Upload a local file to S3.
    
    Args:
        file_path: Local file path (str or Path)
        s3_key: S3 key (e.g., "data/raw/bill.pdf")
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not s3_client:
        logger.error("S3 client not initialized")
        return False
    
    try:
        s3_client.upload_file(str(file_path), BUCKET_NAME, s3_key)
        logger.info(f"Uploaded {file_path} to s3://{BUCKET_NAME}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {file_path}: {e}")
        return False


def upload_fileobject_to_s3(file_object, s3_key):
    """
    Upload a file-like object (e.g., from Streamlit file_uploader) to S3.
    
    Args:
        file_object: File-like object with read() method
        s3_key: S3 key (e.g., "data/raw/bill.pdf")
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not s3_client:
        logger.error("S3 client not initialized")
        return False
    
    try:
        # Reset file pointer if needed
        if hasattr(file_object, 'seek'):
            file_object.seek(0)
        
        s3_client.upload_fileobj(file_object, BUCKET_NAME, s3_key)
        logger.info(f"Uploaded file object to s3://{BUCKET_NAME}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload file object: {e}")
        return False


def upload_json_to_s3(data, s3_key):
    """
    Upload JSON data to S3.
    
    Args:
        data: Python dict or list to serialize as JSON
        s3_key: S3 key (e.g., "data/processed/results.json")
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not s3_client:
        logger.error("S3 client not initialized")
        return False
    
    try:
        json_data = json.dumps(data, indent=2)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"Uploaded JSON to s3://{BUCKET_NAME}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload JSON: {e}")
        return False


# ==================== DOWNLOAD FUNCTIONS ====================

def download_file_from_s3(s3_key, local_path):
    """
    Download a file from S3 to local path.
    
    Args:
        s3_key: S3 key (e.g., "data/raw/bill.pdf")
        local_path: Local file path to save (str or Path)
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not s3_client:
        logger.error("S3 client not initialized")
        return False
    
    try:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(BUCKET_NAME, s3_key, str(local_path))
        logger.info(f"Downloaded s3://{BUCKET_NAME}/{s3_key} to {local_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to download {s3_key}: {e}")
        return False


def download_json_from_s3(s3_key):
    """
    Download and parse JSON from S3.
    
    Args:
        s3_key: S3 key (e.g., "data/processed/results.json")
    
    Returns:
        dict or list: Parsed JSON data, or None if failed
    """
    if not s3_client:
        logger.error("S3 client not initialized")
        return None
    
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        logger.info(f"Downloaded JSON from s3://{BUCKET_NAME}/{s3_key}")
        return data
    except Exception as e:
        logger.error(f"Failed to download JSON {s3_key}: {e}")
        return None


def get_file_content_from_s3(s3_key):
    """
    Get file content as bytes from S3.
    
    Args:
        s3_key: S3 key (e.g., "data/raw/bill.pdf")
    
    Returns:
        bytes: File content, or None if failed
    """
    if not s3_client:
        logger.error("S3 client not initialized")
        return None
    
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        content = response['Body'].read()
        logger.info(f"Retrieved content from s3://{BUCKET_NAME}/{s3_key}")
        return content
    except Exception as e:
        logger.error(f"Failed to get content {s3_key}: {e}")
        return None


# ==================== UTILITY FUNCTIONS ====================

def file_exists_in_s3(s3_key):
    """
    Check if a file exists in S3.
    
    Args:
        s3_key: S3 key to check
    
    Returns:
        bool: True if exists, False otherwise
    """
    if not s3_client:
        return False
    
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        return True
    except ClientError:
        return False


def list_files_in_s3(prefix):
    """
    List all files in S3 with a given prefix.
    
    Args:
        prefix: S3 prefix (e.g., "data/raw/")
    
    Returns:
        list: List of S3 keys, or empty list if failed
    """
    if not s3_client:
        logger.error("S3 client not initialized")
        return []
    
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' in response:
            keys = [obj['Key'] for obj in response['Contents']]
            logger.info(f"Found {len(keys)} files with prefix {prefix}")
            return keys
        return []
    except Exception as e:
        logger.error(f"Failed to list files with prefix {prefix}: {e}")
        return []


def list_files_in_s3_with_meta(prefix):
    """
    List files in S3 with their last modified timestamp.

    Args:
        prefix: S3 prefix (e.g., "data/raw/")

    Returns:
        list[dict]: Each dict has keys "Key" and "LastModified". Empty list on error.
    """
    if not s3_client:
        logger.error("S3 client not initialized")
        return []
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' in response:
            items = [
                {"Key": obj['Key'], "LastModified": obj.get('LastModified')}
                for obj in response['Contents']
            ]
            logger.info(f"Found {len(items)} files with prefix {prefix} (with metadata)")
            return items
        return []
    except Exception as e:
        logger.error(f"Failed to list files with meta for prefix {prefix}: {e}")
        return []


def delete_file_from_s3(s3_key):
    """
    Delete a file from S3.
    
    Args:
        s3_key: S3 key to delete
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not s3_client:
        logger.error("S3 client not initialized")
        return False
    
    try:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
        logger.info(f"Deleted s3://{BUCKET_NAME}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to delete {s3_key}: {e}")
        return False


# ==================== HELPER FUNCTIONS ====================

def get_s3_key(folder_type, filename):
    """
    Generate S3 key from folder type and filename.
    
    Args:
        folder_type: "raw", "processed", "output", or "incoming"
        filename: File name
    
    Returns:
        str: S3 key (e.g., "data/raw/bill.pdf")
    """
    return f"data/{folder_type}/{filename}"


def download_to_temp(s3_key):
    """
    Download S3 file to a temporary file and return the path.
    
    Args:
        s3_key: S3 key to download
    
    Returns:
        str: Path to temporary file, or None if failed
    """
    if not s3_client:
        return None
    
    try:
        # Create temp file with same extension
        suffix = Path(s3_key).suffix
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        temp_path = temp_file.name
        temp_file.close()
        
        if download_file_from_s3(s3_key, temp_path):
            logger.info(f"Downloaded {s3_key} to temp file {temp_path}")
            return temp_path
        return None
    except Exception as e:
        logger.error(f"Failed to download to temp: {e}")
        return None