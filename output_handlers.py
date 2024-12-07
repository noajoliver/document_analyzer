import os
import time
import json
import sqlite3
import csv
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime


@dataclass
class AnalysisMetadata:
    """Metadata for analysis results"""
    created_at: str
    threshold: float
    format_version: str = "1.0"
    sampling_method: str = "none"  # "none", "statistical", or "random_n"
    sampling_enabled: bool = False
    confidence_level: Optional[float] = None
    margin_of_error: Optional[float] = None
    sample_size: Optional[int] = None
    random_n_size: Optional[int] = None  # Single field for random N sampling
    total_files: Optional[int] = None

    def __post_init__(self):
        """Validate metadata after initialization"""
        if self.sampling_method not in ["none", "statistical", "random_n"]:
            raise ValueError("Invalid sampling method")

        if self.sampling_method == "statistical":
            if self.confidence_level is None or self.margin_of_error is None:
                raise ValueError("Statistical sampling requires confidence level and margin of error")

        if self.sampling_method == "random_n":
            if self.random_n_size is None:
                raise ValueError("Random N sampling requires sample size")


class OutputHandler:
    """Base class for handling different output formats"""

    def __init__(self, output_path: str, settings: 'AnalysisSettings'):
        self.output_path = output_path
        self.settings = settings
        self.metadata = AnalysisMetadata(
            created_at=datetime.now().isoformat(),
            threshold=settings.threshold,
            sampling_method="random_n" if settings.use_random_n else "statistical" if settings.use_sampling else "none",
            sampling_enabled=settings.use_sampling or settings.use_random_n,
            confidence_level=settings.confidence_level if settings.use_sampling else None,
            margin_of_error=settings.margin_of_error if settings.use_sampling else None,
            sample_size=settings.sample_size if settings.use_sampling else None,
            random_n_size=settings.random_n_size if settings.use_random_n else None,
            total_files=settings.total_files
        )

    def write_batch(self, batch: List[Dict[str, Any]], is_final: bool = False) -> Optional[str]:
        """Write a batch of results to the output"""
        raise NotImplementedError()

    def cleanup(self):
        """Perform any necessary cleanup"""
        pass

    def get_metadata_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary format"""
        return asdict(self.metadata)


class CSVOutputHandler(OutputHandler):
    """Handles output in CSV format with file splitting"""

    def __init__(self, output_path: str, settings: 'AnalysisSettings'):
        super().__init__(output_path, settings)
        self.current_file_number = 1
        self.total_rows_written = 0

        # Write metadata to separate JSON file
        self.write_metadata()

    def write_metadata(self):
        """Write metadata to a separate JSON file"""
        metadata_path = f"{os.path.splitext(self.output_path)[0]}_metadata.json"
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(self.get_metadata_dict(), f, indent=2)

    def get_next_filename(self) -> str:
        """Generate next filename for split files"""
        if self.current_file_number == 1:
            return os.path.abspath(self.output_path)

        abs_path = os.path.abspath(self.output_path)
        base, ext = os.path.splitext(abs_path)
        return f"{base}__{self.current_file_number}{ext}"

    def write_batch(self, batch: List[Dict[str, Any]], is_final: bool = False) -> Optional[str]:
        """Write a batch of results to CSV file(s)"""
        if not batch and not is_final:
            return None

        # Process each result to extract only needed fields
        processed_results = []
        for result in batch:
            if self.settings.minimal_output:
                # For minimal output, only include specified fields
                processed_result = {
                    'File': os.path.abspath(result['File']),
                    'Page': result.get('Page', 1),
                    'Content Status': result['Content Status']
                }
            else:
                # For full output, include all fields
                processed_result = result.copy()
                if 'File' in processed_result:
                    processed_result['File'] = os.path.abspath(processed_result['File'])

            processed_results.append(processed_result)

        # Check if we need a new file
        current_batch_size = len(processed_results)
        if self.total_rows_written + current_batch_size > self.settings.max_rows_per_file:
            self.current_file_number += 1
            self.total_rows_written = 0

        output_file = self.get_next_filename()
        write_header = not os.path.exists(output_file) or self.total_rows_written == 0

        # Convert to DataFrame
        df_batch = pd.DataFrame(processed_results)

        # For Page column, ensure it's properly typed
        if 'Page' in df_batch.columns:
            df_batch['Page'] = df_batch['Page'].astype('Int64')

        # Write to CSV
        df_batch.to_csv(
            output_file,
            mode='a' if not write_header else 'w',
            header=write_header,
            index=False,
            encoding='utf-8',
            quoting=csv.QUOTE_MINIMAL
        )

        self.total_rows_written += current_batch_size
        return output_file


class ParquetOutputHandler(OutputHandler):
    """Handles output in Parquet format with support for nested data structures"""

    def __init__(self, output_path: str, settings: 'AnalysisSettings'):
        super().__init__(output_path, settings)
        self.output_path = f"{os.path.splitext(output_path)[0]}.parquet"
        self.schema = None
        self.writer = None
        self.row_group_size = 100000
        self.temp_batches = []
        self.batch_size = 10000

    def _flatten_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten nested dictionary structures and handle special fields"""
        flattened = {}

        for key, value in record.items():
            if key == 'Analysis Details':
                if isinstance(value, dict):
                    # Flatten nested Analysis Details structure
                    for category, details in value.items():
                        if isinstance(details, dict):
                            for detail_key, detail_value in details.items():
                                flat_key = f"{category}_{detail_key}".replace(" ", "_")
                                flattened[flat_key] = detail_value
                        else:
                            flattened[category] = str(details)
            else:
                # Handle other fields
                if isinstance(value, (str, int, float, bool, type(None))):
                    flattened[key] = value
                else:
                    flattened[key] = str(value)

        return flattened

    def _flatten_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Flatten entire batch of records"""
        return [self._flatten_record(record) for record in batch]

    def write_batch(self, batch: List[Dict[str, Any]], is_final: bool = False) -> Optional[str]:
        """Write a batch of results to Parquet file"""
        if not batch and not is_final:
            return None

        try:
            # Flatten the batch data
            flattened_batch = self._flatten_batch(batch)

            # Convert to DataFrame
            df = pd.DataFrame(flattened_batch)

            # Convert any remaining object columns to string
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str)

            # Special handling for Page column
            if 'Page' in df.columns:
                df['Page'] = pd.to_numeric(df['Page'], errors='coerce').astype('Int32')

            # Write to parquet
            if not os.path.exists(self.output_path):
                # First write - create new file
                df.to_parquet(
                    self.output_path,
                    engine='pyarrow',
                    compression='snappy',
                    index=False
                )
            else:
                # Append to existing file
                existing_df = pd.read_parquet(self.output_path)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.to_parquet(
                    self.output_path,
                    engine='pyarrow',
                    compression='snappy',
                    index=False
                )

            if is_final:
                self._write_metadata()

            return self.output_path if is_final else None

        except Exception as e:
            raise IOError(f"Error writing Parquet batch: {str(e)}")

    def _write_metadata(self):
        """Write analysis metadata to companion JSON file"""
        try:
            metadata_path = f"{os.path.splitext(self.output_path)[0]}_metadata.json"
            metadata = self.get_metadata_dict()

            # Add Parquet-specific metadata
            metadata.update({
                'row_group_size': self.row_group_size,
                'compression': 'snappy',
                'created_at': datetime.now().isoformat()
            })

            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)

        except Exception as e:
            print(f"Warning: Failed to write metadata file: {str(e)}")

    def cleanup(self):
        """Clean up resources"""
        pass  # No cleanup needed for this implementation


class SQLiteOutputHandler(OutputHandler):
    """Handles output in SQLite format"""

    def __init__(self, output_path: str, settings: 'AnalysisSettings'):
        super().__init__(output_path, settings)
        self.output_path = f"{os.path.splitext(output_path)[0]}.db"
        self.conn = sqlite3.connect(self.output_path)
        self.create_tables()

    def create_tables(self):
        """Create necessary database tables"""
        with self.conn:
            # Create results table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS analysis_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT NOT NULL,
                    page_number INTEGER,
                    content_status TEXT,
                    text_status TEXT,
                    image_status TEXT,
                    file_type TEXT,
                    processing_error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create metadata table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS analysis_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create analysis details table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS analysis_details (
                    result_id INTEGER,
                    detail_type TEXT,
                    detail_key TEXT,
                    detail_value TEXT,
                    FOREIGN KEY(result_id) REFERENCES analysis_results(id)
                )
            ''')

            # Store metadata
            metadata = self.get_metadata_dict()
            self.conn.executemany(
                'INSERT OR REPLACE INTO analysis_metadata (key, value) VALUES (?, ?)',
                [(k, json.dumps(v)) for k, v in metadata.items()]
            )

    def write_batch(self, batch: List[Dict[str, Any]], is_final: bool = False) -> Optional[str]:
        """Write a batch of results to SQLite database"""
        if not batch:
            return None

        with self.conn:
            # Insert main results
            cursor = self.conn.executemany(
                '''INSERT INTO analysis_results 
                   (file_name, page_number, content_status, text_status, 
                    image_status, file_type, processing_error)
                   VALUES (?, ?, ?, ?, ?, ?, ?)''',
                [(
                    r['File'],
                    int(r.get('Page', 1)) if r.get('Page') is not None else None,  # Ensure integer
                    r['Content Status'],
                    r.get('Text Status', ''),
                    r.get('Image Status', ''),
                    r.get('Type', 'Unknown'),
                    r.get('Error', None)
                ) for r in batch]
            )

            # Insert detailed analysis results if present
            for result in batch:
                if 'Analysis Details' in result:
                    result_id = cursor.lastrowid
                    details = result['Analysis Details']
                    detail_records = []

                    for detail_type, type_details in details.items():
                        for key, value in type_details.items():
                            detail_records.append((
                                result_id,
                                detail_type,
                                key,
                                str(value)
                            ))

                    if detail_records:
                        self.conn.executemany(
                            '''INSERT INTO analysis_details 
                               (result_id, detail_type, detail_key, detail_value)
                               VALUES (?, ?, ?, ?)''',
                            detail_records
                        )

        return self.output_path

    def cleanup(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None


def create_output_handler(output_format: str, output_path: str, settings: 'AnalysisSettings') -> OutputHandler:
    """Factory function to create appropriate output handler"""
    handlers = {
        'csv': CSVOutputHandler,
        'parquet': ParquetOutputHandler,
        'sqlite': SQLiteOutputHandler
    }

    handler_class = handlers.get(output_format.lower())
    if not handler_class:
        raise ValueError(f"Unsupported output format: {output_format}")

    return handler_class(output_path, settings)
