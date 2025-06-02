"""
Document Margin Analyzer - AWS S3 Handler
Copyright (C) 2024 Noa J Oliver
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program.  If not, see https://www.gnu.org/licenses/.
"""

import os
import tempfile
import logging
from typing import List, Dict, Any, Optional, Tuple, Callable
from dataclasses import dataclass
from urllib.parse import urlparse
import threading
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import NoCredentialsError, ClientError, BotoCoreError
    from botocore.config import Config
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False
    boto3 = None

logger = logging.getLogger(__name__)


@dataclass
class S3Config:
    """Configuration for S3 operations"""
    profile_name: Optional[str] = None
    region_name: Optional[str] = 'us-east-1'
    max_concurrent_downloads: int = 10
    multipart_threshold: int = 8 * 1024 * 1024  # 8MB
    multipart_chunksize: int = 8 * 1024 * 1024  # 8MB
    use_ssl: bool = True
    verify: bool = True
    
    
class S3FileHandler:
    """Handles S3 file operations for the Document Analyzer"""
    
    def __init__(self, config: Optional[S3Config] = None):
        """
        Initialize S3 handler
        
        Args:
            config: S3 configuration options
        """
        if not S3_AVAILABLE:
            raise ImportError("boto3 is not installed. Install with: pip install boto3")
            
        self.config = config or S3Config()
        self._client = None
        self._resource = None
        self._temp_dir = None
        self._download_cache = {}  # Maps S3 keys to local paths
        self._lock = threading.Lock()
        
    @property
    def client(self):
        """Lazy-load S3 client"""
        if self._client is None:
            self._client = self._create_client()
        return self._client
        
    @property 
    def resource(self):
        """Lazy-load S3 resource"""
        if self._resource is None:
            self._resource = self._create_resource()
        return self._resource
        
    def _create_client(self):
        """Create S3 client with configuration"""
        session_kwargs = {}
        if self.config.profile_name:
            session_kwargs['profile_name'] = self.config.profile_name
            
        session = boto3.Session(**session_kwargs)
        
        client_config = Config(
            region_name=self.config.region_name,
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )
        
        return session.client(
            's3',
            config=client_config,
            use_ssl=self.config.use_ssl,
            verify=self.config.verify
        )
        
    def _create_resource(self):
        """Create S3 resource with configuration"""
        session_kwargs = {}
        if self.config.profile_name:
            session_kwargs['profile_name'] = self.config.profile_name
            
        session = boto3.Session(**session_kwargs)
        
        return session.resource(
            's3',
            region_name=self.config.region_name,
            use_ssl=self.config.use_ssl,
            verify=self.config.verify
        )
        
    def parse_s3_path(self, s3_path: str) -> Tuple[str, str]:
        """
        Parse S3 path into bucket and key
        
        Args:
            s3_path: S3 path in format s3://bucket/key or bucket/key
            
        Returns:
            Tuple of (bucket_name, key)
        """
        if s3_path.startswith('s3://'):
            parsed = urlparse(s3_path)
            bucket = parsed.netloc
            key = parsed.path.lstrip('/')
        else:
            parts = s3_path.split('/', 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ''
            
        return bucket, key
        
    def list_objects(self, s3_path: str, file_extensions: Optional[List[str]] = None,
                    recursive: bool = True, progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """
        List objects in S3 path
        
        Args:
            s3_path: S3 path to list (s3://bucket/prefix)
            file_extensions: List of file extensions to filter (e.g., ['.pdf', '.tiff'])
            recursive: Whether to list recursively
            progress_callback: Callback for progress updates
            
        Returns:
            List of dictionaries with file information
        """
        bucket_name, prefix = self.parse_s3_path(s3_path)
        
        if file_extensions:
            # Normalize extensions
            file_extensions = [ext.lower() if ext.startswith('.') else f'.{ext.lower()}' 
                             for ext in file_extensions]
        
        objects = []
        
        try:
            paginator = self.client.get_paginator('list_objects_v2')
            
            # Configure pagination parameters
            pagination_config = {
                'Bucket': bucket_name,
                'Prefix': prefix,
            }
            
            if not recursive:
                pagination_config['Delimiter'] = '/'
                
            page_iterator = paginator.paginate(**pagination_config)
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # Skip if it's a directory marker
                        if key.endswith('/'):
                            continue
                            
                        # Apply extension filter if specified
                        if file_extensions:
                            if not any(key.lower().endswith(ext) for ext in file_extensions):
                                continue
                                
                        file_info = {
                            'Key': key,
                            'Size': obj['Size'],
                            'LastModified': obj['LastModified'],
                            'ETag': obj['ETag'],
                            'S3Path': f"s3://{bucket_name}/{key}",
                            'FileName': os.path.basename(key)
                        }
                        
                        objects.append(file_info)
                        
                        if progress_callback:
                            progress_callback(f"Found: {key}")
                            
        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            raise
            
        return objects
        
    def download_file(self, s3_path: str, local_path: Optional[str] = None,
                     progress_callback: Optional[Callable] = None) -> str:
        """
        Download file from S3
        
        Args:
            s3_path: S3 path to download from
            local_path: Local path to save to (if None, uses temp directory)
            progress_callback: Callback for progress updates
            
        Returns:
            Local path of downloaded file
        """
        bucket_name, key = self.parse_s3_path(s3_path)
        
        # Check cache first
        with self._lock:
            if s3_path in self._download_cache:
                cached_path = self._download_cache[s3_path]
                if os.path.exists(cached_path):
                    logger.debug(f"Using cached file: {cached_path}")
                    return cached_path
                    
        # Determine local path
        if local_path is None:
            if self._temp_dir is None:
                self._temp_dir = tempfile.mkdtemp(prefix="s3_cache_")
            local_path = os.path.join(self._temp_dir, os.path.basename(key))
            
        # Ensure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        try:
            # Download with progress callback if provided
            if progress_callback:
                file_size = self.client.head_object(Bucket=bucket_name, Key=key)['ContentLength']
                
                def download_callback(bytes_transferred):
                    progress = (bytes_transferred / file_size) * 100
                    progress_callback(f"Downloading {os.path.basename(key)}: {progress:.1f}%")
                    
                self.client.download_file(
                    bucket_name, key, local_path,
                    Callback=download_callback
                )
            else:
                self.client.download_file(bucket_name, key, local_path)
                
            # Cache the download
            with self._lock:
                self._download_cache[s3_path] = local_path
                
            logger.info(f"Downloaded {s3_path} to {local_path}")
            return local_path
            
        except ClientError as e:
            logger.error(f"Error downloading file from S3: {e}")
            raise
            
    def download_files_batch(self, s3_paths: List[str], 
                           progress_callback: Optional[Callable] = None) -> Dict[str, str]:
        """
        Download multiple files from S3 in parallel
        
        Args:
            s3_paths: List of S3 paths to download
            progress_callback: Callback for progress updates
            
        Returns:
            Dictionary mapping S3 paths to local paths
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = {}
        
        def download_single(s3_path):
            try:
                local_path = self.download_file(s3_path, progress_callback=progress_callback)
                return s3_path, local_path
            except Exception as e:
                logger.error(f"Failed to download {s3_path}: {e}")
                return s3_path, None
                
        with ThreadPoolExecutor(max_workers=self.config.max_concurrent_downloads) as executor:
            futures = [executor.submit(download_single, path) for path in s3_paths]
            
            for future in as_completed(futures):
                s3_path, local_path = future.result()
                if local_path:
                    results[s3_path] = local_path
                    
        return results
        
    def upload_file(self, local_path: str, s3_path: str,
                   progress_callback: Optional[Callable] = None) -> bool:
        """
        Upload file to S3
        
        Args:
            local_path: Local file path to upload
            s3_path: S3 destination path
            progress_callback: Callback for progress updates
            
        Returns:
            True if successful
        """
        bucket_name, key = self.parse_s3_path(s3_path)
        
        try:
            file_size = os.path.getsize(local_path)
            
            # Configure multipart upload for large files
            transfer_config = boto3.s3.transfer.TransferConfig(
                multipart_threshold=self.config.multipart_threshold,
                multipart_chunksize=self.config.multipart_chunksize
            )
            
            # Upload with progress callback if provided
            if progress_callback:
                def upload_callback(bytes_transferred):
                    progress = (bytes_transferred / file_size) * 100
                    progress_callback(f"Uploading {os.path.basename(local_path)}: {progress:.1f}%")
                    
                self.client.upload_file(
                    local_path, bucket_name, key,
                    Config=transfer_config,
                    Callback=upload_callback
                )
            else:
                self.client.upload_file(
                    local_path, bucket_name, key,
                    Config=transfer_config
                )
                
            logger.info(f"Uploaded {local_path} to {s3_path}")
            return True
            
        except ClientError as e:
            logger.error(f"Error uploading file to S3: {e}")
            return False
            
    def verify_credentials(self) -> Tuple[bool, Optional[str]]:
        """
        Verify S3 credentials are configured
        
        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Try to list buckets as a simple check
            self.client.list_buckets()
            return True, None
        except NoCredentialsError:
            return False, "No AWS credentials found. Please configure AWS credentials."
        except ClientError as e:
            return False, f"AWS client error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"
            
    def get_bucket_list(self) -> List[str]:
        """
        Get list of accessible S3 buckets
        
        Returns:
            List of bucket names
        """
        try:
            response = self.client.list_buckets()
            return [bucket['Name'] for bucket in response['Buckets']]
        except Exception as e:
            logger.error(f"Error listing buckets: {e}")
            return []
            
    def cleanup(self):
        """Clean up temporary files and connections"""
        if self._temp_dir and os.path.exists(self._temp_dir):
            import shutil
            try:
                shutil.rmtree(self._temp_dir)
                logger.info(f"Cleaned up temp directory: {self._temp_dir}")
            except Exception as e:
                logger.error(f"Error cleaning up temp directory: {e}")
                
        self._download_cache.clear()
        
        if self._client:
            self._client = None
        if self._resource:
            self._resource = None