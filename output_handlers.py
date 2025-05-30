"""
Document Margin Analyzer
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

import csv
import json
import os
import sqlite3
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from threading import Lock
from output_schema import COLUMN_DESCRIPTIONS, get_column_description
from typing import List, Dict, Any, Optional

import pandas as pd


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
        self._descriptions_written = False # New flag
        self.logger = logging.getLogger(__name__) # Assuming logger is available
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
        self._ensure_descriptions_written() # New call

    def _ensure_descriptions_written(self):
        if not self._descriptions_written:
            self._write_column_descriptions_file()
            self._descriptions_written = True

    def _write_column_descriptions_file(self):
        if not self.output_path: # Should not happen if constructor is used properly
            self.logger.warning("Output path not set, cannot write column descriptions file.")
            return

        base, _ = os.path.splitext(self.output_path)
        desc_file_path = base + "_column_descriptions.txt"

        try:
            with open(desc_file_path, "w", encoding="utf-8") as f:
                f.write("Output Column Descriptions:\n\n")
                for col_name, desc in COLUMN_DESCRIPTIONS.items():
                    f.write(f"Column Name: {col_name}\n")
                    f.write(f"Description: {desc}\n\n")
            self.logger.info(f"Successfully wrote column descriptions to {desc_file_path}")
        except IOError as e:
            self.logger.error(f"Failed to write column descriptions file to {desc_file_path}: {e}", exc_info=True)

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
    def __init__(self, output_path: str, settings: 'AnalysisSettings'):
        super().__init__(output_path, settings)
        self.current_file_number = 1
        self.total_rows_written_for_current_file = 0 # Renamed for clarity
        self._fieldnames = []
        self.csv_file = None
        self.writer = None
        self._csv_header_comments_written_for_current_file = False
        # _ensure_descriptions_written is called by super().__init__

    def _open_new_csv_part(self):
        """Closes existing CSV part if open, and opens a new one."""
        if self.csv_file:
            self.csv_file.close()
        
        output_file_path = self.get_next_filename()
        self.logger.info(f"Opening new CSV part: {output_file_path}")
        self.csv_file = open(output_file_path, 'w', newline='', encoding='utf-8')
        # Fieldnames should be set before calling this if possible,
        # or DictWriter will be created/recreated when first batch for this part is written.
        if self._fieldnames:
            self.writer = csv.DictWriter(self.csv_file, fieldnames=self._fieldnames, lineterminator='\n')
        else:
            self.writer = None # Will be created once fieldnames are known
        self._csv_header_comments_written_for_current_file = False
        self.total_rows_written_for_current_file = 0


    def write_metadata(self):
        """Write metadata to a separate JSON file, including margin info."""
        metadata_path = f"{os.path.splitext(self.output_path)[0]}_metadata.json"
        # 1) Get base metadata from parent
        meta = self.get_metadata_dict()

        # Inject margin fields from the user’s final settings:
        meta["top_margin_percent"] = self.settings.top_margin_percent
        meta["bottom_margin_percent"] = self.settings.bottom_margin_percent

        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(meta, f, indent=2)

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
        if not processed_results and not is_final: # No data to write, not the end
            return None

        # Determine fieldnames from the first record if not already set
        if not self._fieldnames and processed_results:
            self._fieldnames = list(processed_results[0].keys())
            # If writer was None because fieldnames weren't known, create it now
            if self.csv_file and not self.writer:
                 self.writer = csv.DictWriter(self.csv_file, fieldnames=self._fieldnames, lineterminator='\n')


        # Handle file splitting
        # If current file is None (first batch ever) or max rows will be exceeded
        if self.csv_file is None or \
           (self.total_rows_written_for_current_file + len(processed_results) > self.settings.max_rows_per_file and self.settings.max_rows_per_file > 0):
            if self.csv_file: # Close previous part if it exists
                 self.csv_file.close()
            if self.csv_file is not None: # Increment file number only if it's not the very first opening
                self.current_file_number += 1
            self._open_new_csv_part() # This sets self.csv_file, self.writer, resets counters

        current_output_file_path = self.get_next_filename()

        # Write comments and header if it's a new file part and headers haven't been written for it
        if not self._csv_header_comments_written_for_current_file and self._fieldnames:
            if self.csv_file: # Should always be true if _open_new_csv_part was called
                self.csv_file.write("# CSV Column Descriptions (for full details, see the accompanying _column_descriptions.txt file):\n")
                for fieldname in self._fieldnames:
                    description = get_column_description(fieldname)
                    brief_desc = description.split('.')[0] + "." if '.' in description else description
                    self.csv_file.write(f"# {fieldname}: {brief_desc}\n")
                self.csv_file.write("\n") # Blank line after comments
                
                if not self.writer and self._fieldnames: # Ensure writer is created if it wasn't (e.g. first batch was empty)
                    self.writer = csv.DictWriter(self.csv_file, fieldnames=self._fieldnames, lineterminator='\n')
                
                if self.writer:
                    self.writer.writeheader()
                self._csv_header_comments_written_for_current_file = True

        # Write actual data
        if self.writer and processed_results:
            try:
                self.writer.writerows(processed_results)
                self.total_rows_written_for_current_file += len(processed_results)
                if self.csv_file:
                    self.csv_file.flush() # Ensure data is written to disk
            except Exception as e:
                self.logger.error(f"Error writing CSV rows: {e}", exc_info=True)
                # Decide if we should re-raise or handle
        
        if is_final:
            if self.csv_file:
                self.csv_file.close()
                self.csv_file = None
                self.writer = None
            self.write_metadata() # Write metadata at the very end

        return current_output_file_path if processed_results or is_final else None

    def cleanup(self):
        """Perform any necessary cleanup"""
        if self.csv_file:
            self.csv_file.close()
            self.csv_file = None
        self.writer = None
        self.logger.info("CSVOutputHandler cleaned up.")


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

            # 1) Insert the user-selected margin values
            metadata["top_margin_percent"] = self.settings.top_margin_percent
            metadata["bottom_margin_percent"] = self.settings.bottom_margin_percent

            # 2) Add Parquet-specific metadata
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
    """Handles output in SQLite format with thread-safe operations"""

    def __init__(self, output_path: str, settings: 'AnalysisSettings'):
        super().__init__(output_path, settings)
        self.output_path = f"{os.path.splitext(output_path)[0]}.db"
        self.batch_size = 1000 # Batch size for SQLite inserts
        self.row_count = 0
        self.conn = self._get_connection() # Persistent connection for the lifetime of the handler
        self.connection_lock = Lock() # To ensure thread-safe operations on self.conn
        self._metadata_table_created = False
        self.setup_database() # Initial schema setup
        self._create_and_populate_column_descriptions_table() # Create/populate descriptions table

    def _get_connection(self) -> sqlite3.Connection:
        """Create or return a database connection with appropriate settings"""
        conn = sqlite3.connect(self.output_path, timeout=60)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = -2000")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def setup_database(self):
        """Initialize database schema, excluding column descriptions table which is handled separately."""
        try:
            with self.connection_lock, self.conn as conn: # Use the persistent connection
                # Main results table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS analysis_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        file_path TEXT NOT NULL,
                        page_number INTEGER,
                        content_status TEXT NOT NULL,
                        text_status TEXT,
                        image_status TEXT,
                        file_type TEXT NOT NULL,
                        error_message TEXT,
                        error_severity TEXT,
                        relative_path TEXT,
                        file_size INTEGER,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        batch_id INTEGER
                    )
                ''')

                # Analysis details table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS analysis_details (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        result_id INTEGER NOT NULL,
                        category TEXT NOT NULL,
                        detail_type TEXT NOT NULL,
                        detail_value TEXT,
                        numeric_value REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(result_id) REFERENCES analysis_results(id) ON DELETE CASCADE
                    )
                ''')

                # Processing stats table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS processing_stats (
                        batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        records_processed INTEGER,
                        success_count INTEGER,
                        error_count INTEGER
                    )
                ''')

                # Metadata table
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS analysis_metadata (
                        key TEXT NOT NULL,
                        value TEXT,
                        version INTEGER DEFAULT 1,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (key, version)
                    )
                ''')

                # Create initial indexes
                conn.execute('CREATE INDEX IF NOT EXISTS idx_file_path ON analysis_results(file_path)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_content_status ON analysis_results(content_status)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_details_result ON analysis_details(result_id)')

                # Store initial metadata
                self._store_metadata(conn) # Store other metadata

        except sqlite3.Error as e:
            self.logger.error(f"Failed to initialize SQLite database schema: {e}", exc_info=True)
            raise IOError(f"Failed to initialize SQLite database schema: {str(e)}")

    def _create_and_populate_column_descriptions_table(self):
        if self._metadata_table_created:
            return

        descriptions_table_name = "output_column_descriptions"
        try:
            with self.connection_lock, self.conn as conn: # Use the persistent connection
                cursor = conn.cursor()
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {descriptions_table_name} (
                        column_name TEXT PRIMARY KEY,
                        description TEXT
                    )
                ''')
                
                for col_name, desc in COLUMN_DESCRIPTIONS.items():
                    cursor.execute(f'''
                        INSERT OR IGNORE INTO {descriptions_table_name} (column_name, description)
                        VALUES (?, ?)
                    ''', (col_name, desc))
                
                conn.commit()
            self._metadata_table_created = True
            self.logger.info(f"Ensured '{descriptions_table_name}' table exists and is populated.")
        except sqlite3.Error as e:
            self.logger.error(f"Error creating/populating metadata table '{descriptions_table_name}': {e}", exc_info=True)


    def _store_metadata(self, conn: sqlite3.Connection):
        """Store analysis metadata with versioning (excluding column descriptions)"""
        metadata = self.get_metadata_dict()
        # self.row_count might not be final here if called before all batches are written
        # Consider moving total_records update to when is_final is true in write_batch
        metadata['total_records'] = self.row_count 
        metadata['completed_at'] = datetime.now().isoformat()

        conn.executemany(
            '''INSERT INTO analysis_metadata (key, value, version)
               VALUES (?, ?, (SELECT COALESCE(MAX(version), 0) + 1 
                            FROM analysis_metadata WHERE key = ?))''',
            [(k, json.dumps(v), k) for k, v in metadata.items()]
        )

    def write_batch(self, batch: List[Dict[str, Any]], is_final: bool = False) -> Optional[str]:
        """Write a batch of results to SQLite database"""
        if not batch and not is_final:
            return None

        try:
            with self.connection_lock, self.conn as conn: # Use the persistent connection
                batch_start_time = datetime.now()
                success_count = 0
                error_count = 0

                    # Create batch record
                    cursor = conn.execute('''
                        INSERT INTO processing_stats (start_time, records_processed)
                        VALUES (?, ?)
                    ''', (batch_start_time, len(batch)))
                    batch_id = cursor.lastrowid

                    # Process each result
                    for result in batch:
                        try:
                            rel_path = os.path.relpath(
                                os.path.abspath(result['File']),
                                os.path.dirname(self.output_path)
                            )
                        except ValueError:
                            # If we're on different Windows drive letters, relpath raises ValueError.
                            # In that case, just store the full absolute path so we don't skip the row:
                            rel_path = os.path.abspath(result['File'])
                            file_size = os.path.getsize(result['File']) if os.path.exists(result['File']) else 0

                            # Insert main result
                            cursor = conn.execute('''
                                INSERT INTO analysis_results 
                                (file_path, page_number, content_status, text_status,
                                 image_status, file_type, error_message, error_severity,
                                 relative_path, file_size, batch_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                result['File'],
                                result.get('Page', 1),
                                result['Content Status'],
                                result.get('Text Status', ''),
                                result.get('Image Status', ''),
                                result.get('Type', 'Unknown'),
                                result.get('Error'),
                                result.get('Error Severity'),
                                rel_path,
                                file_size,
                                batch_id
                            ))
                            result_id = cursor.lastrowid

                            # Process analysis details
                            if 'Analysis Details' in result:
                                details = []
                                for category, values in result['Analysis Details'].items():
                                    if isinstance(values, dict):
                                        for key, value in values.items():
                                            try:
                                                numeric_value = float(str(value).replace('%', ''))
                                            except (ValueError, TypeError):
                                                numeric_value = None
                                            details.append((
                                                result_id,
                                                category,
                                                key,
                                                str(value),
                                                numeric_value
                                            ))
                                    else:
                                        try:
                                            numeric_value = float(str(values).replace('%', ''))
                                        except (ValueError, TypeError):
                                            numeric_value = None
                                        details.append((
                                            result_id,
                                            category,
                                            'value',
                                            str(values),
                                            numeric_value
                                        ))

                                if details:
                                    conn.executemany('''
                                        INSERT INTO analysis_details 
                                        (result_id, category, detail_type, detail_value, numeric_value)
                                        VALUES (?, ?, ?, ?, ?)
                                    ''', details)

                            success_count += 1
                            self.row_count += 1

                        except Exception as e:
                            error_count += 1
                            print(f"Error processing result: {str(e)}")

                    # Update batch statistics
                    conn.execute('''
                        UPDATE processing_stats 
                        SET end_time = ?, success_count = ?, error_count = ?
                        WHERE batch_id = ?
                    ''', (datetime.now(), success_count, error_count, batch_id))

                    # Final operations
                    if is_final:
                        self._store_metadata(conn)
                        self._create_final_indexes(conn)

            return self.output_path

        except sqlite3.Error as e:
            raise IOError(f"Error writing to SQLite database: {str(e)}")

    def _create_final_indexes(self, conn: sqlite3.Connection):
        """Create additional indexes after all data is loaded"""
        try:
            conn.execute('CREATE INDEX IF NOT EXISTS idx_processed_at ON analysis_results(processed_at)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_batch_id ON analysis_results(batch_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_details_category ON analysis_details(category)')
        except sqlite3.Error as e:
            print(f"Warning: Failed to create final indexes: {str(e)}")

    def cleanup(self):
        """Clean up resources"""
        if self.conn:
            try:
                with self.connection_lock:
                    self.conn.close()
                self.logger.info("SQLite connection closed.")
            except sqlite3.Error as e:
                self.logger.error(f"Error closing SQLite connection: {e}", exc_info=True)
            finally:
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
