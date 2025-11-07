# ------------------------------------------------------------------
# file_upload_handler.py
# Manages file uploads, validation, and database record creation.
# ------------------------------------------------------------------

import os
import shutil
from datetime import datetime
from pathlib import Path

from src.utils.logger import get_logger
from src.database.db_utils import insert_raw_document

logger = get_logger(__name__)

# ------------------------------------------------------------------
# FILE UPLOAD HANDLER
# ------------------------------------------------------------------

def get_file_extension(file_name: str) -> str:
    """
    🔍 Extracts file extension from file name.
    
    Example:
      get_file_extension("bill.pdf") -> "pdf"
      get_file_extension("usage.xlsx") -> "xlsx"
    """
    return Path(file_name).suffix.lstrip(".").lower()


def validate_file_extension(file_name: str, allowed_extensions: list) -> bool:
    """
    ✅ Validates if file has an allowed extension.
    
    Parameters:
      file_name: Name of the file to validate.
      allowed_extensions: List of allowed extensions (e.g., ["pdf", "xlsx", "csv"]).
    
    Returns:
      True if valid, False otherwise.
    """
    ext = get_file_extension(file_name)
    is_valid = ext in allowed_extensions
    
    if not is_valid:
        logger.warning(f"⚠️ Invalid file extension: {ext}. Allowed: {allowed_extensions}")
    
    return is_valid


def get_file_size_mb(file_path: str) -> float:
    """
    📊 Gets file size in megabytes.
    
    Parameters:
      file_path: Full path to the file.
    
    Returns:
      File size in MB.
    """
    if os.path.exists(file_path):
        size_bytes = os.path.getsize(file_path)
        size_mb = size_bytes / (1024 * 1024)
        return round(size_mb, 2)
    else:
        logger.warning(f"⚠️ File not found: {file_path}")
        return 0.0


def move_uploaded_file(source_path: str, destination_folder: str) -> str:
    """
    📁 Moves uploaded file from source to destination folder.
    
    Parameters:
      source_path: Full path to the uploaded file.
      destination_folder: Target folder (e.g., "data/incoming/").
    
    Returns:
      Full path to the file in the new location.
      None if move failed.
    """
    try:
        # Create destination folder if it doesn't exist
        os.makedirs(destination_folder, exist_ok=True)
        
        # Get file name
        file_name = os.path.basename(source_path)
        destination_path = os.path.join(destination_folder, file_name)
        
        # Move file
        shutil.move(source_path, destination_path)
        logger.info(f"📂 File moved: {source_path} → {destination_path}")
        
        return destination_path
    except Exception as e:
        logger.error(f"❌ Error moving file: {e}")
        return None


def save_uploaded_file(file_obj, destination_folder: str) -> str:
    """
    💾 Saves uploaded file object to destination folder.
    
    Parameters:
      file_obj: File object from upload (has .filename and .file attributes).
      destination_folder: Target folder (e.g., "data/incoming/").
    
    Returns:
      Full path to the saved file.
      None if save failed.
    """
    try:
        # Create destination folder if needed
        os.makedirs(destination_folder, exist_ok=True)
        
        file_name = file_obj.filename
        file_path = os.path.join(destination_folder, file_name)
        
        # Save file
        with open(file_path, "wb") as f:
            f.write(file_obj.file.read())
        
        logger.info(f"💾 File saved: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"❌ Error saving uploaded file: {e}")
        return None


def register_uploaded_file(
    file_name: str,
    file_type: str,
    file_path: str,
    source: str = "Client Upload",
    additional_metadata: dict = None
) -> bool:
    """
    📋 Registers uploaded file in the database (insert_raw_document).
    
    This function creates a database record for the uploaded file,
    storing metadata like file name, type, upload date, source, and status.
    
    Parameters:
      file_name: Name of the file (e.g., "bill.pdf").
      file_type: Type of file (e.g., "PDF", "Excel", "CSV").
      file_path: Full path to the file for size calculation.
      source: Source of upload (default: "Client Upload").
      additional_metadata: Dictionary of extra fields to store.
    
    Returns:
      True if registration successful, False otherwise.
    """
    try:
        # Get file size
        file_size_mb = get_file_size_mb(file_path)
        
        # Build metadata record
        record = {
            "file_name": file_name,
            "file_type": file_type,
            "upload_date": str(datetime.now().date()),
            "upload_time": str(datetime.now().time()),
            "source": source,
            "file_size_mb": file_size_mb,
            "status": "uploaded",  # Initial status
            "file_path": file_path  # Store for later reference
        }
        
        # Add any additional metadata
        if additional_metadata:
            record.update(additional_metadata)
        
        # Insert into database
        insert_raw_document(record)
        
        logger.info(f"✅ Registered file in DB: {file_name} ({file_size_mb} MB)")
        return True
    
    except Exception as e:
        logger.error(f"❌ Error registering file: {e}")
        return False


def handle_file_upload(file_obj, file_type: str, destination_folder: str = "data/incoming/") -> dict:
    """
    🚀 Complete file upload workflow:
      1️⃣ Validate file extension.
      2️⃣ Save file to destination folder.
      3️⃣ Register file in database.
    
    Parameters:
      file_obj: File object from upload (FastAPI UploadFile or similar).
      file_type: Type of file to store in DB (e.g., "PDF", "Excel").
      destination_folder: Where to save the file.
    
    Returns:
      Dictionary with:
        - success: True/False
        - file_name: Name of the file
        - file_path: Full path to saved file
        - file_size_mb: Size of the file
        - message: Success or error message
    """
    try:
        file_name = file_obj.filename
        
        # Step 1: Validate extension
        allowed_ext = ["pdf", "xlsx", "xls", "csv"]
        if not validate_file_extension(file_name, allowed_ext):
            return {
                "success": False,
                "file_name": file_name,
                "message": f"❌ Invalid file type. Allowed: {allowed_ext}"
            }
        
        # Step 2: Save file
        file_path = save_uploaded_file(file_obj, destination_folder)
        if not file_path:
            return {
                "success": False,
                "file_name": file_name,
                "message": "❌ Failed to save file"
            }
        
        # Step 3: Register in database
        success = register_uploaded_file(
            file_name=file_name,
            file_type=file_type,
            file_path=file_path,
            source="Client Upload"
        )
        
        if success:
            file_size = get_file_size_mb(file_path)
            return {
                "success": True,
                "file_name": file_name,
                "file_path": file_path,
                "file_size_mb": file_size,
                "message": f"✅ File uploaded and registered: {file_name}"
            }
        else:
            return {
                "success": False,
                "file_name": file_name,
                "file_path": file_path,
                "message": "❌ File saved but failed to register in database"
            }
    
    except Exception as e:
        logger.error(f"❌ Upload failed: {e}")
        return {
            "success": False,
            "file_name": getattr(file_obj, "filename", "unknown"),
            "message": f"❌ Upload error: {str(e)}"
        }
