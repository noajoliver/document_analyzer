"""
Document Margin Analyzer - S3 Output Wrapper
Copyright (C) 2024 Noa J Oliver
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
"""

import os
import tempfile
import logging
from typing import List, Dict, Any, Optional

from output_handlers import OutputHandler
from s3_handler import S3FileHandler

logger = logging.getLogger(__name__)


class S3OutputWrapper:
    """Wraps an output handler to upload results to S3"""
    
    def __init__(self, output_handler: OutputHandler, s3_handler: S3FileHandler, 
                 s3_output_path: str):
        """
        Initialize S3 output wrapper
        
        Args:
            output_handler: The actual output handler (CSV, Parquet, SQLite)
            s3_handler: S3 file handler for uploads
            s3_output_path: S3 destination path
        """
        self.output_handler = output_handler
        self.s3_handler = s3_handler
        self.s3_output_path = s3_output_path
        self.temp_files = []
        
        # Modify output handler to use temp directory
        self.temp_dir = tempfile.mkdtemp(prefix="s3_output_")
        base_name = os.path.basename(output_handler.output_path)
        self.local_path = os.path.join(self.temp_dir, base_name)
        
        # Update output handler's path
        self.output_handler.output_path = self.local_path
        
    def write_batch(self, batch: List[Dict[str, Any]], is_final: bool = False) -> Optional[str]:
        """Write batch and upload to S3 if final"""
        # Write to local temp file
        local_result = self.output_handler.write_batch(batch, is_final)
        
        if local_result and local_result not in self.temp_files:
            self.temp_files.append(local_result)
            
        if is_final:
            # Upload all files to S3
            self.upload_to_s3()
            
        return self.s3_output_path if is_final else None
        
    def upload_to_s3(self):
        """Upload all output files to S3"""
        logger.info(f"Uploading output files to S3: {self.s3_output_path}")
        
        for temp_file in self.temp_files:
            if os.path.exists(temp_file):
                # Determine S3 key
                base_name = os.path.basename(temp_file)
                if self.s3_output_path.endswith(base_name):
                    s3_path = self.s3_output_path
                else:
                    # Handle multi-part files
                    s3_path = self.s3_output_path.rsplit('.', 1)[0]
                    if '__' in base_name:  # Multi-part file
                        parts = base_name.split('__')
                        s3_path = f"{s3_path}__{parts[1]}"
                    else:
                        s3_path = self.s3_output_path
                        
                # Upload file
                success = self.s3_handler.upload_file(
                    temp_file, 
                    s3_path,
                    progress_callback=lambda msg: logger.info(msg)
                )
                
                if success:
                    logger.info(f"Successfully uploaded {base_name} to {s3_path}")
                else:
                    logger.error(f"Failed to upload {base_name} to {s3_path}")
                    
        # Also upload metadata files if they exist
        self.upload_metadata_files()
                    
    def upload_metadata_files(self):
        """Upload associated metadata files"""
        # Check for metadata JSON file
        metadata_path = f"{os.path.splitext(self.local_path)[0]}_metadata.json"
        if os.path.exists(metadata_path):
            s3_metadata_path = f"{os.path.splitext(self.s3_output_path)[0]}_metadata.json"
            self.s3_handler.upload_file(metadata_path, s3_metadata_path)
            
        # Check for column descriptions file
        desc_path = f"{os.path.splitext(self.local_path)[0]}_column_descriptions.txt"
        if os.path.exists(desc_path):
            s3_desc_path = f"{os.path.splitext(self.s3_output_path)[0]}_column_descriptions.txt"
            self.s3_handler.upload_file(desc_path, s3_desc_path)
            
    def cleanup(self):
        """Clean up temp files and call handler cleanup"""
        self.output_handler.cleanup()
        
        # Clean up temp directory
        if self.temp_dir and os.path.exists(self.temp_dir):
            import shutil
            try:
                shutil.rmtree(self.temp_dir)
                logger.info(f"Cleaned up temp directory: {self.temp_dir}")
            except Exception as e:
                logger.error(f"Error cleaning up temp directory: {e}")
                
    def get_metadata_dict(self) -> Dict[str, Any]:
        """Get metadata from wrapped handler"""
        return self.output_handler.get_metadata_dict()