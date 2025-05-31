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
import csv
import json
import os
import sqlite3
import logging
import queue
import threading
from dataclasses import dataclass, asdict
from datetime import datetime
from threading import Lock
from output_schema import COLUMN_DESCRIPTIONS, get_column_description, ORDERED_COLUMN_NAMES
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
            self._write_column_descriptions_file()
            # The _descriptions_written flag is now set inside _write_column_descriptions_file
            # or if the file already exists, to prevent re-checks.
            # self._descriptions_written = True # This line will be effectively handled by the logic below

    def _write_column_descriptions_file(self):
        if not self.output_path: # Should not happen if constructor is used properly
            self.logger.warning("Output path not set, cannot write column descriptions file.")
            self._descriptions_written = False # Ensure it can be retried if path becomes available
            return

        base, _ = os.path.splitext(self.output_path)
        desc_file_path = base + "_column_descriptions.txt"

        if os.path.exists(desc_file_path):
            self.logger.debug(f"Column descriptions file {desc_file_path} already exists. Skipping write.")
            self._descriptions_written = True # Mark as "handled" because the file is present
            return

        try:
            with open(desc_file_path, "w", encoding="utf-8") as f:
                f.write("Output Column Descriptions:\n\n")
                for col_name, desc in COLUMN_DESCRIPTIONS.items():
                    f.write(f"Column Name: {col_name}\n")
                    f.write(f"Description: {desc}\n\n")
            self.logger.info(f"Successfully wrote column descriptions to {desc_file_path}")
            self._descriptions_written = True # Mark as written
        except IOError as e:
            self.logger.error(f"Failed to write column descriptions file to {desc_file_path}: {e}", exc_info=True)
            self._descriptions_written = False # Allow retry if it failed

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
        self._fieldnames = ORDERED_COLUMN_NAMES # Use predefined order
        self.csv_file = None
        self.writer = None
        self._csv_header_comments_written_for_current_file = False
        # _ensure_descriptions_written is called by super().__init__

    def _flatten_row_for_csv(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Flattens a single row, especially the 'Analysis Details' part,
        and ensures all columns from self._fieldnames are present."""
        flattened_row = {}
        analysis_details = row_dict.get("Analysis Details", {})
        text_details = analysis_details.get("Text", {})
        image_details = analysis_details.get("Image", {})
        margins_used = analysis_details.get("Margins Used", {})

        for col_name in self._fieldnames:
            if col_name in row_dict:
                flattened_row[col_name] = row_dict[col_name]
            elif col_name == "text_top_content_percentage":
                flattened_row[col_name] = text_details.get("Top Content", "")
            elif col_name == "text_bottom_content_percentage":
                flattened_row[col_name] = text_details.get("Bottom Content", "")
            elif col_name == "image_top_content_percentage":
                flattened_row[col_name] = image_details.get("Top Content", "")
            elif col_name == "image_bottom_content_percentage":
                flattened_row[col_name] = image_details.get("Bottom Content", "")
            elif col_name == "total_margin_content_percentage": # Specific to images
                flattened_row[col_name] = image_details.get("Total Margin Content", "")
            elif col_name == "margins_used_top_margin_percentage":
                flattened_row[col_name] = margins_used.get("Top Margin (%)", "")
            elif col_name == "margins_used_bottom_margin_percentage":
                flattened_row[col_name] = margins_used.get("Bottom Margin (%)", "")
            else:
                flattened_row[col_name] = row_dict.get(col_name, "") # Default to empty string for missing values

        return flattened_row

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
        # This logic is removed as _fieldnames is set in __init__
        # if not self._fieldnames and processed_results:
        #     self._fieldnames = list(processed_results[0].keys())
        #     # If writer was None because fieldnames weren't known, create it now
        #     if self.csv_file and not self.writer:
        #          self.writer = csv.DictWriter(self.csv_file, fieldnames=self._fieldnames, lineterminator='\n')


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
                    sanitized_brief_desc = brief_desc.replace('\t', ' ')
                    self.csv_file.write(f"# {fieldname}: {sanitized_brief_desc}\n")
                self.csv_file.write("\n") # Blank line after comments

                if not self.writer and self._fieldnames: # Ensure writer is created if it wasn't (e.g. first batch was empty)
                    self.writer = csv.DictWriter(self.csv_file, fieldnames=self._fieldnames, lineterminator='\n')

                if self.writer:
                    self.writer.writeheader()
                self._csv_header_comments_written_for_current_file = True

        # Write actual data
        if self.writer and processed_results:
            try:
                # Flatten results before writing
                flattened_results = [self._flatten_row_for_csv(row) for row in processed_results]
                self.writer.writerows(flattened_results)
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
        self.row_count = 0
        self.db_queue = queue.Queue()
        self.db_worker_thread = threading.Thread(target=self._db_worker_loop, daemon=True)
        self.db_worker_thread.start()
        self.db_queue.put({'type': 'init_schema'})

    def _db_worker_loop(self):
        conn = None
        try:
            conn = sqlite3.connect(self.output_path) # Default check_same_thread=True is fine
            self._perform_init_schema(conn) # Initial schema setup

            while True:
                task = self.db_queue.get()
                if task is None or task.get('type') == 'close':
                    self.db_queue.task_done()
                    break

                try:
                    if task['type'] == 'write':
                        self._perform_write_batch(conn, task['data'])
                    elif task['type'] == 'init_schema': # Already called, but can be a no-op or re-entrant
                        self._perform_init_schema(conn)
                except Exception as e:
                    self.logger.error(f"Error processing DB task {task.get('type')}: {e}", exc_info=True)
                finally:
                    self.db_queue.task_done()
        except sqlite3.Error as e:
            self.logger.error(f"SQLite worker thread error: {e}", exc_info=True)
        finally:
            if conn:
                try:
                    conn.commit() # Final commit
                except sqlite3.Error as e:
                    self.logger.error(f"SQLite worker: Error during final commit: {e}", exc_info=True)
                try:
                    conn.close()
                except sqlite3.Error as e:
                    self.logger.error(f"SQLite worker: Error closing connection: {e}", exc_info=True)
            self.logger.info("SQLite worker thread finished and connection closed.")


    def _get_connection_UNUSED(self) -> sqlite3.Connection: # Renamed as it's no longer directly used by handler instance
        """Create or return a database connection with appropriate settings"""
        conn = sqlite3.connect(self.output_path, timeout=60)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = -2000")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _perform_init_schema(self, conn: sqlite3.Connection):
        """Initialize database schema, including column descriptions table."""
        try:
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

            # Store initial metadata - The call to self._store_metadata(conn) was here and is being removed
            # as its logic is now integrated below.

        except sqlite3.Error as e:
            self.logger.error(f"Failed to initialize SQLite database schema: {e}", exc_info=True)
            # Do not raise here, as worker loop handles exceptions

        # Column Descriptions Table (moved into _perform_init_schema)
        descriptions_table_name = "output_column_descriptions"
        try:
            cursor = conn.cursor() # This line was indented one level too far.
            cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {descriptions_table_name} (
                column_name TEXT PRIMARY KEY,
                description TEXT
            )
            ''')
            for col_name, desc_text in COLUMN_DESCRIPTIONS.items():
                cursor.execute(f'''
                INSERT OR IGNORE INTO {descriptions_table_name} (column_name, description)
                VALUES (?, ?)
                ''', (col_name, desc_text))
            conn.commit() # Commit after populating descriptions
            self.logger.info(f"Ensured '{descriptions_table_name}' table exists and is populated.")
        except sqlite3.Error as e:
            self.logger.error(f"Error creating/populating column descriptions table '{descriptions_table_name}': {e}", exc_info=True)

        # Store initial analysis metadata (distinct from column descriptions)
        try:
            metadata_to_store = self.get_metadata_dict()
            # total_records will be 0 initially, can be updated later if needed via a separate task or at the end
            metadata_to_store['total_records_processed_in_db'] = 0
            metadata_to_store['schema_version'] = "1.0" # Example of schema versioning

            # Using INSERT OR REPLACE for simplicity for this initial metadata.
            # For versioning, a more complex strategy might be needed if multiple versions of metadata are stored.
            conn.executemany(
                '''INSERT OR REPLACE INTO analysis_metadata (key, value, updated_at)
                   VALUES (?, ?, CURRENT_TIMESTAMP)''',
                [(k, json.dumps(v)) for k, v in metadata_to_store.items()]
            )
            conn.commit() # Commit after storing analysis metadata
            self.logger.info("Initial analysis metadata stored in SQLite.")
        except sqlite3.Error as e:
            self.logger.error(f"Error storing initial analysis metadata in SQLite: {e}", exc_info=True)


    def _perform_write_batch(self, conn: sqlite3.Connection, batch_data: List[Dict[str, Any]]):
        """Performs the actual database write operations for a batch."""
        if not batch_data:
            return

        try:
            batch_start_time = datetime.now()
            success_count = 0
            error_count = 0

                    # Create batch record
            cursor = conn.cursor()

            # Create batch record in processing_stats
            # self.logger.debug(f"Inserting into processing_stats: start_time={batch_start_time}, records_processed={len(batch_data)}")
            cursor.execute('''
                INSERT INTO processing_stats (start_time, records_processed)
                VALUES (?, ?)
            ''', (batch_start_time.isoformat(), len(batch_data)))
            batch_id = cursor.lastrowid

            # Process each result
            for row_dict in batch_data:
                try:
                    rel_path = ""
                    try:
                        rel_path = os.path.relpath(
                            os.path.abspath(row_dict.get("File", "")),
                            os.path.dirname(self.output_path)
                        )
                    except ValueError: # Handles cases like different drives on Windows
                        rel_path = os.path.abspath(row_dict.get("File", ""))

                    file_size = 0
                    file_path_for_size = row_dict.get("File")
                    if file_path_for_size and os.path.exists(file_path_for_size):
                        try:
                            file_size = os.path.getsize(file_path_for_size)
                        except OSError as e:
                            self.logger.warning(f"Could not get size for file {file_path_for_size}: {e}")

                    analysis_results_data = (
                        row_dict.get("File"),
                        row_dict.get("Page"),
                        row_dict.get("Content Status"),
                        row_dict.get("Text Status"),
                        row_dict.get("Image Status"),
                        row_dict.get("Type"),
                        row_dict.get("Error"),
                        row_dict.get("Error Severity"),
                        rel_path,
                        file_size,
                        batch_id
                    )
                    # self.logger.debug(f"Inserting into analysis_results: {analysis_results_data}")
                    cursor.execute('''
                        INSERT INTO analysis_results
                        (file_path, page_number, content_status, text_status,
                         image_status, file_type, error_message, error_severity,
                         relative_path, file_size, batch_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', analysis_results_data)
                    result_id = cursor.lastrowid

                    # Process analysis details
                    analysis_details_dict = row_dict.get("Analysis Details")
                    if isinstance(analysis_details_dict, dict):
                        details_to_insert = []
                        for category, cat_details in analysis_details_dict.items():
                            if isinstance(cat_details, dict):
                                for detail_key, detail_value in cat_details.items():
                                    numeric_val = None
                                    if isinstance(detail_value, str):
                                        try:
                                            numeric_val = float(detail_value.rstrip('%'))
                                        except ValueError:
                                            pass # Keep None if not convertible
                                    elif isinstance(detail_value, (int, float)):
                                        numeric_val = float(detail_value)

                                    details_to_insert.append((
                                        result_id, category, detail_key, str(detail_value), numeric_val
                                    ))
                        if details_to_insert:
                            # self.logger.debug(f"Inserting into analysis_details for result_id {result_id}: {details_to_insert}")
                            cursor.executemany('''
                                INSERT INTO analysis_details
                                (result_id, category, detail_type, detail_value, numeric_value)
                                VALUES (?, ?, ?, ?, ?)
                            ''', details_to_insert)

                    success_count += 1
                    self.row_count += 1

                except Exception as e: # Catch errors per row
                    error_count += 1
                    self.logger.error(f"Error processing row for DB: {row_dict.get('File', 'Unknown File')}: {e}", exc_info=True)

            # Update batch statistics
            conn.execute('''
                UPDATE processing_stats
                SET end_time = ?, success_count = ?, error_count = ?
                WHERE batch_id = ?
            ''', (datetime.now(), success_count, error_count, batch_id))

            conn.commit() # Commit after each batch
            self.logger.debug(f"SQLite batch written. Success: {success_count}, Errors: {error_count}")

        except sqlite3.Error as e:
            self.logger.error(f"Error writing batch to SQLite: {e}", exc_info=True)
            # Optionally rollback, though auto-commit might handle some cases or commit might fail
            try:
                conn.rollback()
            except sqlite3.Error as re:
                 self.logger.error(f"Rollback failed: {re}", exc_info=True)
        # Removed specific _create_final_indexes and _store_metadata calls from here,
        # as they are part of schema init or should be handled as separate queued tasks if dynamic.

    def write_batch(self, batch: List[Dict[str, Any]], is_final: bool = False) -> Optional[str]:
        """Queue a batch of results for writing to SQLite database."""
        if not batch and not is_final: # Allow empty final batch to trigger cleanup/final commit in worker
            return None

        self.db_queue.put({'type': 'write', 'data': batch})

        if is_final:
            # Optionally, queue a task to update final metadata like total_records if needed
            # self.db_queue.put({'type': 'finalize_metadata', 'total_records': self.row_count})
            self.logger.info("All batches queued for SQLite. Finalizing.")

        return self.output_path # Return path immediately, actual write is async

    def cleanup(self):
        """Signal the DB worker thread to close and wait for it."""
        self.logger.info("SQLiteOutputHandler cleanup: Signaling DB worker to close.")
        if hasattr(self, 'db_queue') and self.db_queue is not None:
            self.db_queue.put(None) # Sentinel to stop the worker
        if hasattr(self, 'db_worker_thread') and self.db_worker_thread.is_alive():
            self.db_worker_thread.join(timeout=10) # Wait for worker to finish
            if self.db_worker_thread.is_alive():
                self.logger.warning("SQLite worker thread did not terminate in time.")
        else:
            self.logger.info("SQLite worker thread was not alive or not initialized.")


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
