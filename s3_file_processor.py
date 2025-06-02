"""
Document Margin Analyzer - S3 File Processor Integration
Copyright (C) 2024 Noa J Oliver
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
"""

import os
import logging
from typing import List, Dict, Any, Optional, Callable, Set
from dataclasses import dataclass
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from s3_handler import S3FileHandler, S3Config, S3_AVAILABLE
from sampling import FileProcessor, SamplingParameters, SamplingCalculator

logger = logging.getLogger(__name__)


@dataclass
class S3ProcessingOptions:
    """Options for S3 file processing"""
    download_batch_size: int = 10
    delete_after_processing: bool = True
    use_multiprocessing: bool = True
    cache_downloads: bool = True
    

class S3FileProcessor:
    """Integrates S3 file operations with the document analyzer's file processing"""
    
    def __init__(self, s3_handler: S3FileHandler, options: Optional[S3ProcessingOptions] = None):
        """
        Initialize S3 file processor
        
        Args:
            s3_handler: S3FileHandler instance
            options: Processing options
        """
        self.s3_handler = s3_handler
        self.options = options or S3ProcessingOptions()
        self._local_cache = {}  # Maps S3 paths to local paths
        self._temp_dir = None
        
    def get_s3_file_list(self, s3_path: str, include_pdfs: bool, include_images: bool,
                        supported_formats: Set[str],
                        progress_callback: Optional[Callable[[str], None]] = None) -> List[Dict[str, Any]]:
        """
        Get list of files from S3 matching criteria
        
        Args:
            s3_path: S3 path (s3://bucket/prefix)
            include_pdfs: Whether to include PDF files
            include_images: Whether to include image files
            supported_formats: Set of supported image extensions
            progress_callback: Progress callback function
            
        Returns:
            List of file info dictionaries with S3 metadata
        """
        # Build list of extensions to filter
        extensions = []
        if include_pdfs:
            extensions.append('.pdf')
        if include_images:
            extensions.extend(list(supported_formats))
            
        # List objects in S3
        s3_objects = self.s3_handler.list_objects(
            s3_path,
            file_extensions=extensions,
            recursive=True,
            progress_callback=progress_callback
        )
        
        # Sort by key (path)
        s3_objects.sort(key=lambda x: x['Key'])
        
        return s3_objects
        
    def prepare_file_list_with_sampling(self, s3_path: str, settings: 'AnalysisSettings',
                                      supported_formats: Set[str]) -> List[Dict[str, Any]]:
        """
        Prepare S3 file list with optional sampling
        
        Args:
            s3_path: S3 path to process
            settings: Analysis settings including sampling parameters
            supported_formats: Set of supported image file extensions
            
        Returns:
            List of S3 file info dictionaries to process
        """
        # Get complete file list from S3
        files = self.get_s3_file_list(
            s3_path,
            settings.include_pdfs,
            settings.include_images,
            supported_formats
        )
        
        total_files = len(files)
        if total_files == 0:
            return []
            
        # Apply sampling if enabled
        if settings.use_sampling:
            params = SamplingParameters(
                confidence_level=settings.confidence_level,
                margin_of_error=settings.margin_of_error,
                population_size=total_files
            )
            
            sample_size = SamplingCalculator.calculate_sample_size(params)
            settings.sample_size = sample_size
            settings.total_files = total_files
            
            # Use the same random sampling as local files
            return SamplingCalculator.select_random_files(files, sample_size)
            
        return files
        
    def download_and_process_batch(self, s3_files: List[Dict[str, Any]], 
                                 processor: Callable[[str], Dict[str, Any]],
                                 progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """
        Download a batch of S3 files and process them
        
        Args:
            s3_files: List of S3 file info dictionaries
            processor: Function to process each file
            progress_callback: Progress callback
            
        Returns:
            List of processing results
        """
        results = []
        
        # Create temp directory if needed
        if self._temp_dir is None:
            self._temp_dir = tempfile.mkdtemp(prefix="s3_docs_")
            
        # Download files
        s3_paths = [f['S3Path'] for f in s3_files]
        download_map = self.s3_handler.download_files_batch(s3_paths, progress_callback)
        
        # Process each downloaded file
        for s3_file in s3_files:
            s3_path = s3_file['S3Path']
            local_path = download_map.get(s3_path)
            
            if local_path and os.path.exists(local_path):
                try:
                    # Process the file
                    result = processor(local_path)
                    
                    # Update result with S3 information
                    if isinstance(result, list):
                        # Multi-page result (PDF or multi-page TIFF)
                        for page_result in result:
                            if page_result:
                                page_result['OriginalPath'] = s3_path
                                page_result['S3Metadata'] = {
                                    'Bucket': s3_file.get('Bucket'),
                                    'Key': s3_file['Key'],
                                    'Size': s3_file['Size'],
                                    'LastModified': s3_file['LastModified']
                                }
                        results.extend(result)
                    elif result:
                        # Single result
                        result['OriginalPath'] = s3_path
                        result['S3Metadata'] = {
                            'Bucket': s3_file.get('Bucket'),
                            'Key': s3_file['Key'],
                            'Size': s3_file['Size'],
                            'LastModified': s3_file['LastModified']
                        }
                        results.append(result)
                        
                    # Optionally delete local file after processing
                    if self.options.delete_after_processing and not self.options.cache_downloads:
                        try:
                            os.remove(local_path)
                        except Exception as e:
                            logger.warning(f"Failed to delete temp file {local_path}: {e}")
                            
                except Exception as e:
                    logger.error(f"Error processing S3 file {s3_path}: {e}")
                    # Add error result
                    error_result = {
                        'File': s3_path,
                        'OriginalPath': s3_path,
                        'Page': 1,
                        'Content Status': 'Processing Failed',
                        'Type': 'Unknown',
                        'Error': str(e),
                        'Error Severity': 'ERROR',
                        'S3Metadata': {
                            'Key': s3_file['Key'],
                            'Size': s3_file['Size']
                        }
                    }
                    results.append(error_result)
            else:
                # Download failed
                error_result = {
                    'File': s3_path,
                    'OriginalPath': s3_path,
                    'Page': 1,
                    'Content Status': 'Processing Failed',
                    'Type': 'Unknown',
                    'Error': 'Failed to download from S3',
                    'Error Severity': 'ERROR',
                    'S3Metadata': {
                        'Key': s3_file['Key'],
                        'Size': s3_file['Size']
                    }
                }
                results.append(error_result)
                
        return results
        
    def process_s3_files_parallel(self, s3_files: List[Dict[str, Any]],
                                processor: Callable[[str], Dict[str, Any]],
                                max_workers: int,
                                progress_callback: Optional[Callable] = None) -> List[Dict[str, Any]]:
        """
        Process S3 files in parallel with batched downloads
        
        Args:
            s3_files: List of S3 file info dictionaries
            processor: Function to process each file
            max_workers: Maximum number of worker threads
            progress_callback: Progress callback
            
        Returns:
            List of all processing results
        """
        all_results = []
        total_files = len(s3_files)
        processed = 0
        
        # Process in batches
        batch_size = self.options.download_batch_size
        
        for i in range(0, total_files, batch_size):
            batch = s3_files[i:i + batch_size]
            
            # Download and process batch
            batch_results = self.download_and_process_batch(
                batch, processor, progress_callback
            )
            
            all_results.extend(batch_results)
            
            processed += len(batch)
            if progress_callback:
                progress_callback(f"Processed {processed}/{total_files} files")
                
        return all_results
        
    def cleanup(self):
        """Clean up temporary files"""
        if self._temp_dir and os.path.exists(self._temp_dir):
            import shutil
            try:
                shutil.rmtree(self._temp_dir)
                logger.info(f"Cleaned up temp directory: {self._temp_dir}")
            except Exception as e:
                logger.error(f"Error cleaning up temp directory: {e}")
                
                
def create_s3_processor(config: Optional[S3Config] = None,
                       options: Optional[S3ProcessingOptions] = None) -> Optional[S3FileProcessor]:
    """
    Create S3 file processor if S3 support is available
    
    Args:
        config: S3 configuration
        options: Processing options
        
    Returns:
        S3FileProcessor instance or None if S3 not available
    """
    if not S3_AVAILABLE:
        logger.warning("S3 support not available - boto3 not installed")
        return None
        
    try:
        s3_handler = S3FileHandler(config)
        # Verify credentials
        success, error = s3_handler.verify_credentials()
        if not success:
            logger.error(f"S3 credentials verification failed: {error}")
            return None
            
        return S3FileProcessor(s3_handler, options)
        
    except Exception as e:
        logger.error(f"Failed to create S3 processor: {e}")
        return None