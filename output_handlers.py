import os
import time
import json
import sqlite3
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
    sampling_enabled: bool = False
    confidence_level: Optional[float] = None
    margin_of_error: Optional[float] = None
    sample_size: Optional[int] = None
    total_files: Optional[int] = None


class OutputHandler:
    """Base class for handling different output formats"""

    def __init__(self, output_path: str, settings: 'AnalysisSettings'):
        self.output_path = output_path
        self.settings = settings
        self.metadata = AnalysisMetadata(
            created_at=datetime.now().isoformat(),
            threshold=settings.threshold,
            sampling_enabled=settings.sample_size is not None,
            confidence_level=settings.confidence_level,
            margin_of_error=settings.margin_of_error,
            sample_size=settings.sample_size,
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
        """Generate the next filename for split files"""
        if self.current_file_number == 1:
            return self.output_path
        base, ext = os.path.splitext(self.output_path)
        return f"{base}__{self.current_file_number}{ext}"

    def write_batch(self, batch: List[Dict[str, Any]], is_final: bool = False) -> Optional[str]:
        """Write a batch of results to CSV file(s)"""
        if not batch and not is_final:
            return None

        current_batch_size = len(batch)
        if self.total_rows_written + current_batch_size > self.settings.max_rows_per_file:
            self.current_file_number += 1
            self.total_rows_written = 0

        output_file = self.get_next_filename()
        write_header = not os.path.exists(output_file) or self.total_rows_written == 0

        # Convert to DataFrame and specify dtype for Page column
        df_batch = pd.DataFrame(batch)
        if 'Page' in df_batch.columns:
            df_batch['Page'] = df_batch['Page'].astype('Int64')  # Use nullable integer type

        df_batch.to_csv(
            output_file,
            mode='a' if not write_header else 'w',
            header=write_header,
            index=False,
            encoding='utf-8'
        )

        self.total_rows_written += current_batch_size
        return output_file


class ParquetOutputHandler(OutputHandler):
    """Handles output in Parquet format"""

    def __init__(self, output_path: str, settings: 'AnalysisSettings'):
        super().__init__(output_path, settings)
        self.output_path = f"{os.path.splitext(output_path)[0]}.parquet"
        self.all_data = []

    def write_batch(self, batch: List[Dict[str, Any]], is_final: bool = False) -> Optional[str]:
        """Accumulate data and write to Parquet file on final batch"""
        self.all_data.extend(batch)

        if is_final:
            df = pd.DataFrame(self.all_data)
            if 'Page' in df.columns:
                df['Page'] = df['Page'].astype('Int64')  # Use nullable integer type

            table = pa.Table.from_pandas(df)

            # Convert metadata to strings for Parquet compatibility
            metadata = {k: str(v) for k, v in self.get_metadata_dict().items()}

            pq.write_table(
                table,
                self.output_path,
                metadata=metadata
            )
            return self.output_path
        return None


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