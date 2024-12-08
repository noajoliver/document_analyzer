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

# Standard library imports
import os
import sys
import platform
import time
import queue
import logging
from threading import Thread, Event, Lock
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Set, Union
from dataclasses import dataclass, field, replace
from functools import partial

# GUI imports
import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox

# Data processing imports
import pandas as pd
import numpy as np
import fitz
from PIL import Image
from pdf2image import convert_from_path

# Local module imports
from content_analyzer import ContentAnalyzer, PageAnalyzer
from error_handling import ErrorHandler, ErrorSeverity, ProcessingError, ErrorAwareResult
from output_handlers import create_output_handler
from sampling import FileProcessor, SamplingCalculator, SamplingParameters
from pdf_utils import setup_poppler


@dataclass
class ProcessingStats:
    """Tracks processing statistics and timing"""
    total_processed: int = 0
    successful: int = 0
    failed: int = 0
    start_time: Optional[float] = None
    last_update_time: Optional[float] = None
    recent_rates: List[float] = field(default_factory=lambda: [])
    pause_time: Optional[float] = None
    total_pause_duration: float = 0.0

    def start(self):
        """Start or restart processing timer"""
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.recent_rates.clear()
        self.total_pause_duration = 0.0
        self.pause_time = None

    def pause(self):
        """Record pause start time"""
        if not self.pause_time:
            self.pause_time = time.time()

    def resume(self):
        """Calculate and add pause duration"""
        if self.pause_time:
            self.total_pause_duration += time.time() - self.pause_time
            self.pause_time = None
            self.last_update_time = time.time()

    def update(self, processed_count: int):
        """
        Update processing stats and calculate rate

        Args:
            processed_count: Current number of processed items
        """
        if self.pause_time:  # Don't update while paused
            return

        try:
            current_time = time.time()
            if self.last_update_time:
                time_diff = current_time - self.last_update_time
                if time_diff > 0:
                    # Calculate items processed since last update
                    items_diff = processed_count - self.total_processed
                    current_rate = items_diff / time_diff

                    # Keep track of recent processing rates (last 5 updates)
                    self.recent_rates.append(current_rate)
                    if len(self.recent_rates) > 5:
                        self.recent_rates.pop(0)

            self.total_processed = processed_count
            self.last_update_time = current_time
        except Exception as e:
            print(f"Error updating processing stats: {str(e)}")

    def get_elapsed_time(self) -> str:
        """
        Get elapsed time as formatted string, excluding pause time

        Returns:
            str: Formatted elapsed time (HH:MM:SS)
        """
        try:
            if not self.start_time:
                return "00:00:00"

            # Calculate total elapsed time minus pauses
            elapsed = time.time() - self.start_time - self.total_pause_duration
            if self.pause_time:  # Subtract current pause if paused
                elapsed -= (time.time() - self.pause_time)

            elapsed = int(max(0, elapsed))  # Ensure non-negative
            hours = elapsed // 3600
            minutes = (elapsed % 3600) // 60
            seconds = elapsed % 60
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        except Exception as e:
            print(f"Error calculating elapsed time: {str(e)}")
            return "00:00:00"

    def get_estimated_time_remaining(self, total_files: int) -> str:
        """
        Calculate and format estimated time remaining

        Args:
            total_files: Total number of files to process

        Returns:
            str: Formatted time remaining estimate
        """
        try:
            if (not self.start_time or not self.recent_rates or
                    self.total_processed == 0 or self.pause_time):
                return "Calculating..."

            # Use average of recent processing rates
            avg_rate = sum(self.recent_rates) / len(self.recent_rates)
            if avg_rate <= 0:
                return "Calculating..."

            remaining_files = total_files - self.total_processed
            estimated_seconds = remaining_files / avg_rate

            # Format time remaining
            hours = int(estimated_seconds // 3600)
            minutes = int((estimated_seconds % 3600) // 60)
            seconds = int(estimated_seconds % 60)

            # Choose appropriate format based on duration
            if hours > 0:
                return f"{hours}h {minutes}m remaining"
            elif minutes > 0:
                return f"{minutes}m {seconds}s remaining"
            else:
                return f"{seconds}s remaining"
        except Exception as e:
            print(f"Error calculating time remaining: {str(e)}")
            return "Calculating..."

    def get_processing_rate(self) -> str:
        """
        Get current processing rate

        Returns:
            str: Formatted processing rate (files/second)
        """
        try:
            if not self.recent_rates:
                return "0 files/sec"

            avg_rate = sum(self.recent_rates) / len(self.recent_rates)
            if avg_rate >= 10:
                return f"{avg_rate:.0f} files/sec"
            return f"{avg_rate:.1f} files/sec"
        except Exception as e:
            print(f"Error calculating processing rate: {str(e)}")
            return "0 files/sec"


@dataclass
class AnalysisSettings:
    """Configuration settings for document analysis"""
    threshold: float
    output_format: str
    max_rows_per_file: int
    excluded_folders: Set[str]
    use_sampling: bool = False
    use_random_n: bool = False
    random_n_size: Optional[int] = None
    confidence_level: float = 0.95
    margin_of_error: float = 0.05
    sample_size: Optional[int] = None
    total_files: Optional[int] = None
    include_pdfs: bool = True
    include_images: bool = True
    process_subdirectories: bool = True
    minimal_output: bool = False

    def __post_init__(self):
        """Validate settings after initialization"""
        if not 0.1 <= self.threshold <= 10.0:
            raise ValueError("Threshold must be between 0.1 and 10.0")

        if self.output_format not in ['csv', 'parquet', 'sqlite']:
            raise ValueError("Invalid output format")

        if self.max_rows_per_file < 1:
            raise ValueError("Max rows per file must be positive")

        # Validate sampling settings
        if self.use_sampling and self.use_random_n:
            raise ValueError("Cannot use both statistical sampling and random N sampling")

        if self.use_sampling:
            if not 0 < self.confidence_level < 1:
                raise ValueError("Confidence level must be between 0 and 1")
            if not 0 < self.margin_of_error < 1:
                raise ValueError("Margin of error must be between 0 and 1")

        if self.use_random_n:
            if self.random_n_size is None:
                raise ValueError("Random N size must be specified when using random N sampling")
            if self.random_n_size < 1:
                raise ValueError("Random N size must be positive")
            if self.total_files is not None and self.random_n_size > self.total_files:
                raise ValueError("Random N size cannot be larger than total files")


class LicenseViewer:
    def __init__(self, parent):
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("License Information")
        self.dialog.geometry("600x400")
        self.dialog.minsize(500, 300)

        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.dialog)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Main license tab
        main_frame = ttk.Frame(self.notebook)
        self.notebook.add(main_frame, text="Main License")

        main_text = scrolledtext.ScrolledText(
            main_frame,
            wrap=tk.WORD,
            width=70,
            height=20
        )
        main_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        main_text.insert(tk.END, """Document Margin Analyzer
Copyright (C) 2024 Noa J Oliver

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see https://www.gnu.org/licenses/.""")
        main_text.config(state=tk.DISABLED)

        # Third-party licenses tab
        third_party_frame = ttk.Frame(self.notebook)
        self.notebook.add(third_party_frame, text="Third-Party Licenses")

        third_party_text = scrolledtext.ScrolledText(
            third_party_frame,
            wrap=tk.WORD,
            width=70,
            height=20
        )
        third_party_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        third_party_text.insert(tk.END, """This software incorporates components from the following software packages:

PyMuPDF (GPL v3)
- A Python binding for MuPDF, a lightweight PDF and XPS viewer
- https://github.com/pymupdf/PyMuPDF

Poppler (GPL v2)
- A PDF rendering library
- https://poppler.freedesktop.org/

pdf2image (MIT)
- A python module that wraps the pdftoppm utility
- https://github.com/Belval/pdf2image

Pillow (HPND)
- Python Imaging Library Fork
- https://python-pillow.org/

pandas (BSD 3-Clause)
- Data analysis and manipulation tool
- https://pandas.pydata.org/

numpy (BSD 3-Clause)
- Fundamental package for scientific computing
- https://numpy.org/

pyinstaller (Modified GPL with Exception)
- Freezes Python applications into stand-alone executables
- https://www.pyinstaller.org/

requests (Apache 2.0)
- HTTP library for Python
- https://requests.readthedocs.io/

pyarrow (Apache 2.0)
- Python library for Apache Arrow
- https://arrow.apache.org/

Full license texts for these components can be found in their respective repositories.""")
        third_party_text.config(state=tk.DISABLED)

        # Close button
        close_button = ttk.Button(
            self.dialog,
            text="Close",
            command=self.dialog.destroy
        )
        close_button.pack(pady=10)

        # Make dialog modal
        self.dialog.transient(parent)
        self.dialog.grab_set()
        parent.wait_window(self.dialog)


class DocumentAnalyzerGUI:
    """Main GUI application for document margin analysis"""

    # Class constants
    DEFAULT_THRESHOLD = 1.0
    DEFAULT_OUTPUT_FORMAT = 'csv'
    DEFAULT_MAX_ROWS = 80000
    DEFAULT_BATCH_SIZE = 1000
    SUPPORTED_FORMATS = {'.jpg', '.jpeg', '.png', '.tiff', '.tif', '.bmp'}

    def __init__(self, root):
        self.root = root
        self.root.title("Document Margin Analyzer")

        # Initialize configuration variables
        self._init_variables()

        # Setup Poppler for PDF processing
        self.poppler_path = setup_poppler()  # Store the path
        if not self.poppler_path and platform.system() == "Windows":
            messagebox.showwarning(
                "Poppler Setup",
                "Poppler not found. PDF processing may be limited.\n\n"
                "Please ensure Poppler is installed in the poppler-windows directory."
            )

        # Initialize components
        self._init_components()

        # Setup UI
        self.setup_ui()

    def _init_variables(self):
        """Initialize configuration and state variables"""
        # Analysis settings
        self.threshold = tk.DoubleVar(value=self.DEFAULT_THRESHOLD)
        self.output_format = tk.StringVar(value=self.DEFAULT_OUTPUT_FORMAT)
        self.max_rows = tk.IntVar(value=self.DEFAULT_MAX_ROWS)
        self.use_sampling = tk.BooleanVar(value=False)
        self.use_random_n = tk.BooleanVar(value=False)
        self.random_n_size = tk.StringVar(value='100')  # Default to 100 files
        self.confidence_level = tk.StringVar(value='95')
        self.margin_of_error = tk.StringVar(value='5')

        # File type selection
        self.include_pdfs = tk.BooleanVar(value=True)
        self.include_images = tk.BooleanVar(value=True)
        self.file_count_var = tk.StringVar(value="No files selected")

        # Processing state
        self.queue = queue.Queue()
        self.pause_event = Event()
        self.stop_event = Event()
        self.processing_lock = Lock()
        self.results_lock = Lock()
        self.processing = False

        # Path variables
        self.folder_path = tk.StringVar()
        self.save_path = tk.StringVar()

        # CPU core selection
        self.available_cores = os.cpu_count() or 1  # Fallback to 1 if None
        self.default_cores = max(1, self.available_cores // 2)
        self.selected_cores = tk.IntVar(value=self.default_cores)

        # Batch processing settings
        self.batch_size = self.DEFAULT_BATCH_SIZE
        self.current_file_number = 1
        self.results_batch = []
        self.total_rows_written = 0

        # Processing options initialization
        self.processing_options = FileProcessor.ProcessingOptions(
            excluded_folders={'$RECYCLE.BIN', 'System Volume Information'},
            parallel_processing=True,
            batch_size=self.DEFAULT_BATCH_SIZE,
            show_progress=True
        )

        # Last sampling change tracker for mutual exclusivity
        self._last_sampling_change = None

    def _init_components(self):
        """Initialize analysis components and settings tracker"""
        try:
            # Create settings object with current values
            self.settings = self._create_settings()

            # Initialize analyzers and handlers
            self.page_analyzer = PageAnalyzer(self.settings)
            self.error_handler = ErrorHandler()

            # Initialize result tracking
            self.current_output_handler = None
            self.processing_stats = {
                'total_processed': 0,
                'successful': 0,
                'failed': 0,
                'start_time': None
            }
        except Exception as e:
            messagebox.showerror(
                "Initialization Error",
                f"Failed to initialize components: {str(e)}\n\n"
                "The application may not function correctly."
            )
            raise

    def _create_settings(self) -> AnalysisSettings:
        """
        Create settings object from current GUI values

        Returns:
            AnalysisSettings: Configuration settings object

        Raises:
            ValueError: If any settings values are invalid
        """
        try:
            # Convert confidence level and margin of error from percentage to decimal
            confidence = float(self.confidence_level.get()) / 100
            margin = float(self.margin_of_error.get()) / 100

            # Define default excluded folders
            excluded_folders = {'$RECYCLE.BIN', 'System Volume Information'}

            # Create and return settings object
            return AnalysisSettings(
                threshold=self.threshold.get(),
                output_format=self.output_format.get(),
                max_rows_per_file=self.max_rows.get(),
                excluded_folders=excluded_folders,  # Now explicitly passed
                use_sampling=self.use_sampling.get(),
                use_random_n=self.use_random_n.get(),
                random_n_size=int(self.random_n_size.get()) if self.use_random_n.get() else None,
                confidence_level=confidence,
                margin_of_error=margin,
                include_pdfs=self.include_pdfs.get(),
                include_images=self.include_images.get()
            )
        except ValueError as e:
            raise ValueError(f"Invalid settings values: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error creating settings: {str(e)}")

    def update_settings(self) -> bool:
        """
        Update analysis settings based on current GUI values.

        Returns:
            bool: True if settings were updated successfully, False otherwise
        """
        try:
            # Create new settings from current GUI values
            new_settings = AnalysisSettings(
                threshold=self.threshold.get(),
                output_format=self.output_format.get(),
                max_rows_per_file=self.max_rows.get(),
                excluded_folders={'$RECYCLE.BIN', 'System Volume Information'},  # Explicitly set
                use_sampling=self.use_sampling.get(),
                use_random_n=self.use_random_n.get(),
                random_n_size=int(self.random_n_size.get()) if self.use_random_n.get() else None,
                confidence_level=float(self.confidence_level.get()) / 100,
                margin_of_error=float(self.margin_of_error.get()) / 100,
                include_pdfs=self.include_pdfs.get(),
                include_images=self.include_images.get(),
                minimal_output=self.minimal_output.get() if hasattr(self, 'minimal_output') else False
            )

            # Update instance settings
            self.settings = new_settings

            # Recreate page analyzer with new settings
            self.page_analyzer = PageAnalyzer(self.settings)

            # Log the update
            self.log_message("Settings updated:")
            self.log_message(f"  Detection threshold: {self.settings.threshold}%")
            self.log_message(f"  Output format: {self.settings.output_format}")
            if self.settings.use_sampling:
                self.log_message(f"  Statistical sampling enabled (CL: {self.settings.confidence_level * 100}%, "
                                 f"ME: {self.settings.margin_of_error * 100}%)")
            elif self.settings.use_random_n:
                self.log_message(f"  Random N sampling enabled (N: {self.settings.random_n_size})")

            return True

        except Exception as e:
            # Log error and show message to user
            error_msg = f"Failed to update settings: {str(e)}"
            self.log_message(f"Error: {error_msg}")
            messagebox.showerror(
                "Settings Error",
                f"{error_msg}\n\nPlease check your input values."
            )
            return False

    def update_analyzers(self) -> bool:
        """
        Update analyzers with current settings. This includes updating settings
        and recreating all analysis components.

        Returns:
            bool: True if analyzers were updated successfully, False otherwise
        """
        try:
            # First update settings
            if not self.update_settings():
                return False

            # Recreate page analyzer with new settings
            self.page_analyzer = PageAnalyzer(self.settings)

            # Reset processing state
            self.processing_stats = {
                'total_processed': 0,
                'successful': 0,
                'failed': 0,
                'start_time': None
            }

            # Clean up existing output handler if present
            if hasattr(self, 'current_output_handler') and self.current_output_handler:
                try:
                    self.current_output_handler.cleanup()
                except Exception as e:
                    self.log_message(f"Warning: Failed to cleanup output handler: {str(e)}")
                finally:
                    self.current_output_handler = None

            # Clear any existing error handler data
            if hasattr(self, 'error_handler'):
                self.error_handler.clear_errors()

            self.log_message("Analysis components updated with new settings")
            return True

        except Exception as e:
            error_msg = f"Failed to update analyzers: {str(e)}"
            self.log_message(f"Error: {error_msg}")
            messagebox.showerror(
                "Component Error",
                f"{error_msg}\n\nAnalysis components may not function correctly."
            )
            return False

    def cleanup(self):
        """Clean up resources before closing"""
        try:
            # Stop any ongoing processing
            self.stop_event.set()

            # Clean up output handler
            if hasattr(self, 'current_output_handler') and self.current_output_handler:
                try:
                    self.current_output_handler.cleanup()
                except Exception as e:
                    print(f"Error during output handler cleanup: {e}")

            # Clean up error handler
            if hasattr(self, 'error_handler'):
                try:
                    self.error_handler.clear_errors()
                except Exception as e:
                    print(f"Error during error handler cleanup: {e}")

        except Exception as e:
            print(f"Error during cleanup: {e}")
        finally:
            # Ensure we don't prevent window from closing
            self.root.destroy()

    def create_status_bar(self, parent: ttk.Frame) -> ttk.Frame:
        """Create status bar with author and license information"""
        status_bar = ttk.Frame(parent)

        # Author information (left side)
        author_label = ttk.Label(
            status_bar,
            text="© 2024 Noa J Oliver",
            font=('Arial', 8)
        )
        author_label.grid(row=0, column=0, padx=5, pady=2, sticky=tk.W)

        # License link (right side)
        license_link = ttk.Label(
            status_bar,
            text="License Information",
            font=('Arial', 8, 'underline'),
            foreground='blue',
            cursor='hand2'
        )
        license_link.grid(row=0, column=1, padx=5, pady=2, sticky=tk.E)
        license_link.bind('<Button-1>', lambda e: LicenseViewer(self.root))

        # Configure grid weights
        status_bar.columnconfigure(0, weight=1)
        status_bar.columnconfigure(1, weight=0)

        return status_bar

    def create_collapsible_section(self, parent: ttk.Frame, title: str, row: int,
                                   start_expanded: bool = True) -> tuple[ttk.Frame, ttk.Frame, ttk.Button]:
        """
        Create a collapsible section with header and content frame.

        Args:
            parent: Parent widget to contain the section
            title: Section title
            row: Grid row for placement
            start_expanded: Whether section starts expanded (default: True)

        Returns:
            tuple: (container_frame, content_frame, expand_button)
        """
        # Container for the whole section
        container = ttk.Frame(parent)
        container.grid(row=row, column=0, sticky=(tk.W, tk.E), pady=(0, 3))
        container.columnconfigure(0, weight=1)

        # Header frame with button and title
        header_frame = ttk.Frame(container)
        header_frame.grid(row=0, column=0, sticky=(tk.W, tk.E))
        header_frame.columnconfigure(2, weight=1)  # Column after title expands

        # Expand/collapse state
        is_expanded = tk.BooleanVar(value=start_expanded)

        def toggle_section():
            """Handle section expansion/collapse"""
            if is_expanded.get():
                content.grid()
                expand_btn.configure(text="▼")

                # Special handling for Log section
                if title == "Log":
                    current_height = self.root.winfo_height()
                    current_width = self.root.winfo_width()
                    current_x = self.root.winfo_x()
                    current_y = self.root.winfo_y()

                    # Calculate new height with log section, accounting for status bar
                    log_height = 120  # Additional height for log section
                    status_bar_height = 25  # Status bar height
                    new_height = current_height + log_height

                    # Ensure window stays within screen bounds
                    screen_height = self.root.winfo_screenheight()
                    if (current_y + new_height) > (screen_height - 40):
                        # Move window up if it would go off screen
                        new_y = max(20, screen_height - new_height - 40)
                        self.root.geometry(f"{current_width}x{new_height}+{current_x}+{new_y}")
                    else:
                        self.root.geometry(f"{current_width}x{new_height}+{current_x}+{current_y}")

                    # Maintain consistent button spacing
                    if hasattr(self, 'analyze_btn'):
                        button_frame = self.analyze_btn.master
                        button_frame.grid_configure(pady=(0, 20))

                    # Configure log text widget
                    if hasattr(self, 'log_text'):
                        self.log_text.configure(height=7)
            else:
                content.grid_remove()
                expand_btn.configure(text="▶")

                # Special handling for Log section collapse
                if title == "Log":
                    current_width = self.root.winfo_width()
                    current_x = self.root.winfo_x()
                    current_y = self.root.winfo_y()

                    # Restore original window height, accounting for status bar
                    base_height = 720
                    total_height = base_height + 25  # Add status bar height
                    self.root.geometry(f"{current_width}x{total_height}+{current_x}+{current_y}")

                    # Maintain button spacing
                    if hasattr(self, 'analyze_btn'):
                        button_frame = self.analyze_btn.master
                        button_frame.grid_configure(pady=(0, 20))

                    # Reset log text widget
                    if hasattr(self, 'log_text'):
                        self.log_text.configure(height=7)

        # Create expand/collapse button
        expand_btn = ttk.Button(
            header_frame,
            text="▼" if start_expanded else "▶",
            width=2,
            command=lambda: [is_expanded.set(not is_expanded.get()), toggle_section()]
        )
        expand_btn.grid(row=0, column=0, padx=(5, 2))

        # Create title label
        title_label = ttk.Label(header_frame, text=title, font=('Arial', 9, 'bold'))
        title_label.grid(row=0, column=1, sticky=tk.W)

        # Create content frame
        content = ttk.Frame(container, padding="5")
        content.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        content.columnconfigure(0, weight=1)

        # Initialize collapsed state if needed
        if not start_expanded:
            content.grid_remove()

        return container, content, expand_btn

    def log_message(self, message: str) -> None:
        """
        Add a timestamped message to the log window.

        Args:
            message: Message to add to the log

        Note:
            This method will automatically expand the log section
            if the message contains error-related keywords.
        """
        try:
            # Get current timestamp
            timestamp = time.strftime('%H:%M:%S')

            # Create formatted message
            formatted_message = f"{timestamp}: {message}\n"

            # Add message to log
            if hasattr(self, 'log_text'):
                self.log_text.insert(tk.END, formatted_message)
                self.log_text.see(tk.END)  # Auto-scroll to latest message

                # Check for error-related keywords to auto-expand log
                error_keywords = ['error', 'failed', 'warning']
                if any(keyword in message.lower() for keyword in error_keywords):
                    if hasattr(self, 'log_expanded') and hasattr(self, 'log_expand_btn'):
                        if not self.log_expanded.get():
                            self.log_expanded.set(True)
                            self.log_expand_btn.invoke()

            # Also print to console for debugging
            print(formatted_message.strip())

        except Exception as e:
            # Fallback to print if logging fails
            print(f"Failed to log message: {str(e)}")
            print(f"Original message: {message}")

    def setup_ui(self) -> None:
        """Set up the main user interface"""
        # Create main frame with padding
        self.main_frame = ttk.Frame(self.root, padding="10")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.main_frame.columnconfigure(0, weight=1)

        # Create sections in order
        current_row = 0

        # Analysis Settings section
        settings_container, settings_frame, _ = self.create_collapsible_section(
            self.main_frame, "Analysis Settings", row=current_row
        )
        self.setup_file_selection(settings_frame)
        current_row += 1

        # File count display
        self.file_count_var = tk.StringVar(value="")
        self.file_count_label = ttk.Label(
            self.main_frame,
            textvariable=self.file_count_var,
            font=('Arial', 10, 'bold'),
            foreground='navy',
            padding=(5, 5)
        )
        self.file_count_label.grid(
            row=current_row,
            column=0,
            sticky=(tk.W, tk.E),
            pady=5
        )
        current_row += 1

        # Analysis Configuration section
        config_container, config_frame, _ = self.create_collapsible_section(
            self.main_frame, "Analysis Configuration", row=current_row
        )
        self.setup_analysis_config(config_frame)
        current_row += 1

        # Output Configuration section
        output_container, output_frame, _ = self.create_collapsible_section(
            self.main_frame, "Output Configuration", row=current_row
        )
        self.setup_output_config(output_frame)
        current_row += 1

        # Progress section
        progress_container, progress_frame, _ = self.create_collapsible_section(
            self.main_frame, "Progress", row=current_row
        )
        self.setup_progress_section(progress_frame)
        current_row += 1

        # Log section (starts collapsed)
        log_container, log_frame, self.log_expand_btn = self.create_collapsible_section(
            self.main_frame, "Log", row=current_row, start_expanded=False
        )
        self.setup_log_section(log_frame)
        self.log_expanded = tk.BooleanVar(value=False)
        current_row += 1

        # Control buttons
        self.setup_control_buttons(self.main_frame, current_row)
        current_row += 1

        # Status bar at the bottom
        self.status_bar = self.create_status_bar(self.main_frame)
        self.status_bar.grid(row=current_row, column=0, sticky=(tk.W, tk.E))
        current_row += 1

        # Configure row weights for proper expansion
        for i in range(current_row):
            self.main_frame.rowconfigure(i, weight=0)  # Don't expand by default

        # Give weight to the row containing the log section to allow it to expand
        log_row = current_row - 3  # The row where log section was added
        self.main_frame.rowconfigure(log_row, weight=1)

        # Set up initial window geometry
        self.setup_window_geometry()

    def setup_window_geometry(self) -> None:
        """Configure initial window size and position"""
        # Calculate dimensions
        initial_width = 700
        initial_height = 720
        status_bar_height = 25  # Height allocated for status bar

        # Adjust total height
        total_height = initial_height + status_bar_height

        # Get screen dimensions
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()

        # Calculate position (centered horizontally, near top of screen)
        center_x = int((screen_width - initial_width) / 2)
        top_y = 20

        # Set window geometry and constraints
        self.root.geometry(f"{initial_width}x{total_height}+{center_x}+{top_y}")
        self.root.minsize(600, 680 + status_bar_height)  # Adjust minimum size too

    def setup_control_buttons(self, parent: ttk.Frame, row: int) -> None:
        """
        Setup control buttons (Start Analysis, Pause, Stop)

        Args:
            parent: Parent frame to contain the buttons
            row: Grid row for button placement
        """
        button_frame = ttk.Frame(parent)
        button_frame.grid(
            row=row,
            column=0,
            sticky=(tk.W, tk.E),
            pady=(0, 10)  # Add padding at bottom
        )

        # Start Analysis button
        self.analyze_btn = ttk.Button(
            button_frame,
            text="Start Analysis",
            command=self.start_analysis
        )
        self.analyze_btn.grid(row=0, column=0, padx=5)

        # Pause button
        self.pause_btn = ttk.Button(
            button_frame,
            text="Pause",
            command=self.toggle_pause,
            state=tk.DISABLED
        )
        self.pause_btn.grid(row=0, column=1, padx=5)

        # Stop button
        self.stop_btn = ttk.Button(
            button_frame,
            text="Stop",
            command=self.stop_analysis,
            state=tk.DISABLED
        )
        self.stop_btn.grid(row=0, column=2, padx=5)

        # Help button with extra padding on left to separate it
        help_btn = self.create_help_button(
            button_frame,
            "Controls:\n\n"
            "• Start Analysis: Begin processing files\n"
            "• Pause: Temporarily suspend processing\n"
            "• Stop: Cancel the current analysis\n\n"
            "Note: Analysis can be restarted after stopping."
        )
        help_btn.grid(row=0, column=3, padx=(15, 5))

    def create_main_frame(self) -> ttk.Frame:
        """Create and configure the main application frame"""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure root window grid
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        # Configure main frame grid
        main_frame.columnconfigure(0, weight=1)

        return main_frame

    def create_analysis_settings_section(self):
        """Create the Analysis Settings section of the UI"""
        # Create collapsible section
        settings_container, settings_frame, _ = self.create_collapsible_section(
            self.main_frame, "Analysis Settings", row=0
        )

        # File Selection
        file_selection_frame = ttk.Frame(settings_frame)
        file_selection_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=5)

        # Input folder
        input_frame = ttk.Frame(file_selection_frame)
        input_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=2)

        ttk.Label(input_frame, text="Input Folder:").grid(
            row=0, column=0, sticky=tk.W, padx=5
        )
        self.folder_entry = ttk.Entry(input_frame)
        self.folder_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(
            input_frame,
            text="Browse",
            command=self.browse_folder
        ).grid(row=0, column=2, padx=5)

        # Save Location
        save_frame = ttk.Frame(file_selection_frame)
        save_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=2)

        ttk.Label(save_frame, text="Save Location:").grid(
            row=0, column=0, sticky=tk.W, padx=5
        )
        self.save_entry = ttk.Entry(save_frame)
        self.save_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(
            save_frame,
            text="Browse",
            command=self.browse_save_location
        ).grid(row=0, column=2, padx=5)

        # File Types section
        self.create_file_types_section(file_selection_frame)

        # Configure grid weights
        file_selection_frame.columnconfigure(0, weight=1)
        input_frame.columnconfigure(1, weight=1)
        save_frame.columnconfigure(1, weight=1)

    def create_file_types_section(self, parent: ttk.Frame):
        """Create the File Types selection section"""
        type_frame = ttk.Frame(parent)
        type_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=5)

        # Header
        ttk.Label(
            type_frame,
            text="File Types to Process:",
            font=('Arial', 9, 'bold')
        ).grid(row=0, column=0, sticky=tk.W, padx=5)

        # Checkboxes container
        checkbox_frame = ttk.Frame(type_frame)
        checkbox_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=2)

        # PDF Files
        pdf_frame = ttk.Frame(checkbox_frame)
        pdf_frame.grid(row=0, column=0, padx=15)

        ttk.Checkbutton(
            pdf_frame,
            text="PDF Files",
            variable=self.include_pdfs,
            command=lambda: self.update_file_count(trigger="checkbox")
        ).grid(row=0, column=0)

        ttk.Label(
            pdf_frame,
            text=".pdf",
            font=('Arial', 8),
            foreground='gray'
        ).grid(row=0, column=1, padx=(2, 0))

        # Image Files
        image_frame = ttk.Frame(checkbox_frame)
        image_frame.grid(row=0, column=1, padx=15)

        ttk.Checkbutton(
            image_frame,
            text="Image Files",
            variable=self.include_images,
            command=lambda: self.update_file_count(trigger="checkbox")
        ).grid(row=0, column=0)

        ttk.Label(
            image_frame,
            text=f"({', '.join(sorted(ext.replace('.', '') for ext in self.SUPPORTED_FORMATS))})",
            font=('Arial', 8),
            foreground='gray'
        ).grid(row=0, column=1, padx=(2, 0))

        # Configure grid weights
        type_frame.columnconfigure(0, weight=1)

    def setup_settings_section(self, parent: ttk.Frame) -> None:
        """Set up the settings section of the UI"""
        # Create frame with padding
        settings_frame = ttk.LabelFrame(parent, text="Analysis Settings", padding="5")
        settings_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        # File Selection
        self.setup_file_selection(settings_frame)

        # Analysis Configuration
        self.setup_analysis_config(settings_frame)

        # Output Configuration
        self.setup_output_config(settings_frame)

        settings_frame.columnconfigure(1, weight=1)

    def setup_file_selection(self, parent: ttk.Frame) -> None:
        """Setup file selection controls"""
        file_frame = ttk.Frame(parent)
        file_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        # Input folder selection
        input_frame = ttk.Frame(file_frame)
        input_frame.grid(row=0, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=2)

        ttk.Label(input_frame, text="Input Folder:").grid(row=0, column=0, sticky=tk.W, padx=5)
        self.folder_entry = ttk.Entry(input_frame)
        self.folder_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(input_frame, text="Browse", command=self.browse_folder).grid(row=0, column=2, padx=5)
        input_frame.columnconfigure(1, weight=1)

        # Save location selection
        save_frame = ttk.Frame(file_frame)
        save_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=2)

        ttk.Label(save_frame, text="Save Location:").grid(row=0, column=0, sticky=tk.W, padx=5)
        self.save_entry = ttk.Entry(save_frame)
        self.save_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(save_frame, text="Browse", command=self.browse_save_location).grid(row=0, column=2, padx=5)
        save_frame.columnconfigure(1, weight=1)

        # File type selection with recount trigger
        type_frame = ttk.Frame(file_frame)
        type_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)

        # File type selection label
        ttk.Label(type_frame, text="File Types to Process:",
                  font=('Arial', 9, 'bold')).grid(row=1, column=0, sticky=tk.W, padx=5)

        # Checkbox frame
        checkbox_frame = ttk.Frame(type_frame)
        checkbox_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=2)

        # PDF checkbox with icon/symbol
        pdf_frame = ttk.Frame(checkbox_frame)
        pdf_frame.grid(row=0, column=0, padx=15)
        ttk.Checkbutton(
            pdf_frame,
            text="PDF Files",
            variable=self.include_pdfs,
            command=lambda: self.update_file_count(trigger="checkbox")
        ).grid(row=0, column=0)
        ttk.Label(
            pdf_frame,
            text=".pdf",
            font=('Arial', 8),
            foreground='gray'
        ).grid(row=0, column=1, padx=(2, 0))

        # Image checkbox with supported formats
        image_frame = ttk.Frame(checkbox_frame)
        image_frame.grid(row=0, column=1, padx=15)
        ttk.Checkbutton(
            image_frame,
            text="Image Files",
            variable=self.include_images,
            command=lambda: self.update_file_count(trigger="checkbox")
        ).grid(row=0, column=0)
        ttk.Label(
            image_frame,
            text=f"({', '.join(sorted(ext.replace('.', '') for ext in self.SUPPORTED_FORMATS))})",
            font=('Arial', 8),
            foreground='gray'
        ).grid(row=0, column=1, padx=(2, 0))

        # Configure grid weights
        file_frame.columnconfigure(1, weight=1)
        type_frame.columnconfigure(1, weight=1)
        checkbox_frame.columnconfigure(1, weight=1)

    def setup_file_count_section(self, parent: ttk.Frame) -> None:
        """Set up the file count display section"""
        count_frame = ttk.Frame(parent)
        count_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        # Add a separator above the count
        ttk.Separator(count_frame, orient='horizontal').grid(
            row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5
        )

        # Create styled label for file count
        count_label = ttk.Label(
            count_frame,
            textvariable=self.file_count_var,
            font=('Arial', 10, 'bold'),
            foreground='navy'
        )
        count_label.grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)

        count_frame.columnconfigure(0, weight=1)

    def setup_analysis_config(self, parent: ttk.Frame) -> None:
        """Setup analysis configuration controls"""
        config_frame = ttk.Frame(parent)
        config_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        # Threshold control
        self.setup_threshold_control(config_frame)

        # CPU core selection
        self.setup_core_selection(config_frame)

        # Sampling controls
        self.setup_sampling_controls(config_frame)

        config_frame.columnconfigure(1, weight=1)

    def setup_threshold_control(self, parent: ttk.Frame) -> None:
        """Setup threshold control section"""
        threshold_frame = ttk.Frame(parent)
        threshold_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        ttk.Label(threshold_frame, text="Detection Threshold (%):").grid(
            row=0, column=0, sticky=tk.W, padx=5
        )

        threshold_spinbox = ttk.Spinbox(
            threshold_frame,
            from_=0.1,
            to=10.0,
            increment=0.1,
            textvariable=self.threshold,
            width=5,
            format="%.1f"
        )
        threshold_spinbox.grid(row=0, column=1, padx=5)

        threshold_help = self.create_help_button(
            threshold_frame,
            "Adjust the sensitivity of margin content detection:\n\n"
            "• 0.1-0.5%: Extremely sensitive, flags minimal content\n"
            "• 1.0%: Default - standard detection level\n"
            "• 1.1-2.0%: Moderate tolerance\n"
            "• 2.1-5.0%: More tolerant, ignores minor marks\n"
            "• 5.1-10.0%: Very tolerant, only flags substantial content\n\n"
            "Recommended: Use 1.0% for standard document analysis."
        )
        threshold_help.grid(row=0, column=2, padx=5)

    def setup_core_selection(self, parent: ttk.Frame) -> None:
        """Setup CPU core selection controls"""
        core_frame = ttk.Frame(parent)
        core_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        ttk.Label(core_frame, text="CPU Cores:").grid(
            row=0, column=0, sticky=tk.W, padx=5
        )

        core_spinbox = ttk.Spinbox(
            core_frame,
            from_=1,
            to=self.available_cores,
            textvariable=self.selected_cores,
            width=5,
            state='readonly'
        )
        core_spinbox.grid(row=0, column=1, padx=5)

        core_info = ttk.Label(
            core_frame,
            text=f"(Available: {self.available_cores}, Recommended: {self.default_cores})"
        )
        core_info.grid(row=0, column=2, sticky=tk.W, padx=5)

        core_help = self.create_help_button(
            core_frame,
            "Select the number of CPU cores to use for processing:\n\n"
            "• Higher numbers may process faster but use more system resources\n"
            "• Lower numbers will process slower but leave more resources for other tasks\n"
            "• The recommended value is 50% of available cores\n"
            "• If you experience system slowdown, try reducing the number of cores"
        )
        core_help.grid(row=0, column=3, padx=5)

    def setup_sampling_controls(self, parent: ttk.Frame) -> None:
        """Setup sampling controls with both statistical and random N options"""
        sampling_frame = ttk.Frame(parent)
        sampling_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        # Create container for checkboxes
        checkbox_frame = ttk.Frame(sampling_frame)
        checkbox_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        # Statistical sampling checkbox
        ttk.Checkbutton(
            checkbox_frame,
            text="Use Statistical Sampling",
            variable=self.use_sampling,
            command=self.toggle_sampling_options
        ).grid(row=0, column=0, padx=5, sticky=tk.W)

        # Random N sampling checkbox
        ttk.Checkbutton(
            checkbox_frame,
            text="Random Sample of N Files",
            variable=self.use_random_n,
            command=self.toggle_sampling_options
        ).grid(row=0, column=1, padx=5, sticky=tk.W)

        # Statistical sampling options subframe
        self.statistical_options = ttk.Frame(sampling_frame)
        self.statistical_options.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        # Confidence Level
        ttk.Label(self.statistical_options, text="Confidence Level:").grid(
            row=0, column=0, sticky=tk.W, padx=5
        )
        ttk.Combobox(
            self.statistical_options,
            textvariable=self.confidence_level,
            values=['90', '95', '99'],
            state='readonly',
            width=5
        ).grid(row=0, column=1, padx=5)

        # Margin of Error
        ttk.Label(self.statistical_options, text="Margin of Error (%):").grid(
            row=0, column=2, sticky=tk.W, padx=5
        )
        ttk.Combobox(
            self.statistical_options,
            textvariable=self.margin_of_error,
            values=['1', '3', '5', '10'],
            state='readonly',
            width=5
        ).grid(row=0, column=3, padx=5)

        # Random N options subframe
        self.random_n_options = ttk.Frame(sampling_frame)
        self.random_n_options.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        # Number of files selection
        ttk.Label(self.random_n_options, text="Number of Files:").grid(
            row=0, column=0, sticky=tk.W, padx=5
        )
        ttk.Combobox(
            self.random_n_options,
            textvariable=self.random_n_size,
            values=['10', '100', '500', '1000', '5000', '10000'],
            state='readonly',
            width=8
        ).grid(row=0, column=1, padx=5)

        # Initially hide both option frames
        self.statistical_options.grid_remove()
        self.random_n_options.grid_remove()

    def setup_output_config(self, parent: ttk.Frame) -> None:
        """Setup output configuration controls"""
        output_frame = ttk.Frame(parent)
        output_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        # Output format selection
        format_frame = ttk.Frame(output_frame)
        format_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        ttk.Label(format_frame, text="Output Format:").grid(
            row=0, column=0, sticky=tk.W, padx=5
        )
        format_combo = ttk.Combobox(
            format_frame,
            textvariable=self.output_format,
            values=['csv', 'parquet', 'sqlite'],
            state='readonly'
        )
        format_combo.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)

        # Add Minimal Output checkbox
        self.minimal_output = tk.BooleanVar(value=False)
        minimal_frame = ttk.Frame(output_frame)
        minimal_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=2)

        ttk.Checkbutton(
            minimal_frame,
            text="Minimal Output (File, Page, Content Status only)",
            variable=self.minimal_output
        ).grid(row=0, column=0, sticky=tk.W, padx=5)

        # CSV-specific options frame
        self.csv_options = ttk.Frame(output_frame)
        self.csv_options.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=5)

        ttk.Label(self.csv_options, text="Max Rows per File:").grid(
            row=0, column=0, sticky=tk.W, padx=5
        )
        ttk.Combobox(
            self.csv_options,
            textvariable=self.max_rows,
            values=['10000', '50000', '80000', '100000'],
            state='readonly'
        ).grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)

        # Bind format change handler
        self.output_format.trace('w', self.on_format_change)

        # Configure grid weights
        output_frame.columnconfigure(1, weight=1)

    def setup_progress_section(self, parent: ttk.Frame) -> None:
        """Setup progress tracking section"""
        progress_frame = ttk.Frame(parent)
        progress_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=5)

        # Progress bar with original styling
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            progress_frame,
            variable=self.progress_var,
            maximum=100,
            length=300  # Fixed width for progress bar
        )
        self.progress_bar.grid(
            row=0, column=0,
            sticky=(tk.W, tk.E),
            padx=5, pady=5
        )

        # Time information frame
        time_frame = ttk.Frame(progress_frame)
        time_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), padx=5, pady=(0, 2))

        # Elapsed time (left side)
        self.elapsed_var = tk.StringVar(value="Elapsed: 00:00:00")
        ttk.Label(
            time_frame,
            textvariable=self.elapsed_var,
            font=('Arial', 9)
        ).grid(row=0, column=0, sticky=tk.W)

        # Remaining time (right side)
        self.remaining_var = tk.StringVar(value="Remaining: Calculating...")
        ttk.Label(
            time_frame,
            textvariable=self.remaining_var,
            font=('Arial', 9)
        ).grid(row=0, column=1, sticky=tk.E)

        # Processing rate
        self.rate_var = tk.StringVar(value="0 files/sec")
        ttk.Label(
            time_frame,
            textvariable=self.rate_var,
            font=('Arial', 9)
        ).grid(row=1, column=0, columnspan=2, sticky=tk.W)

        # Status label with original styling
        self.status_label = ttk.Label(progress_frame, text="")
        self.status_label.grid(
            row=2, column=0,
            sticky=(tk.W, tk.E),
            padx=5, pady=(0, 5)
        )

        # Configure grid weights to match original
        progress_frame.columnconfigure(0, weight=1)
        time_frame.columnconfigure((0, 1), weight=1)  # Equal weight for elapsed and remaining time

    def setup_log_section(self, parent: ttk.Frame) -> None:
        """Setup log display section"""
        log_frame = ttk.Frame(parent)
        log_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=5)

        # Create scrolled text widget for log
        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            height=7,  # Fixed height in text lines
            width=70,  # Fixed width in characters
            wrap=tk.WORD  # Word wrapping
        )
        self.log_text.grid(
            row=0, column=0,
            sticky=(tk.W, tk.E),
            padx=5, pady=5
        )

        # Configure tag for error messages
        self.log_text.tag_configure(
            'error',
            foreground='red',
            font=('Arial', 9, 'bold')
        )

        # Configure grid weights
        log_frame.columnconfigure(0, weight=1)

    def process_files(self) -> None:
        """Main method for processing all selected files"""
        original_working_dir = os.getcwd()

        try:
            # Initialize processing state
            self.initialize_processing()

            def progress_update(msg: str):
                self.queue.put(("log", msg))

            # Create processing options
            options = FileProcessor.ProcessingOptions(
                excluded_folders={'$RECYCLE.BIN', 'System Volume Information'},
                parallel_processing=True,
                batch_size=self.batch_size,
                show_progress=True
            )

            # Get file list with options and convert all paths to absolute
            files_to_process = [os.path.abspath(f) for f in FileProcessor.get_file_list(
                self.folder_entry.get(),
                self.include_pdfs.get(),
                self.include_images.get(),
                self.SUPPORTED_FORMATS,
                options=options,
                progress_callback=progress_update
            )]

            if not files_to_process:
                self.handle_no_files()
                return

            # Apply sampling if enabled
            if self.settings.use_sampling:
                self.log_message("\nCalculating sample size...")
                params = SamplingParameters(
                    confidence_level=self.settings.confidence_level,
                    margin_of_error=self.settings.margin_of_error,
                    population_size=len(files_to_process)
                )

                sample_size = SamplingCalculator.calculate_sample_size(params)
                self.settings.sample_size = sample_size
                self.settings.total_files = len(files_to_process)

                files_to_process = SamplingCalculator.select_random_files(files_to_process, sample_size)

                self.log_message(
                    f"Using statistical sampling: {sample_size:,} files will be analyzed "
                    f"({(sample_size / len(files_to_process) * 100):.1f}% of total)"
                )

            # Apply random N sampling if enabled
            elif self.settings.use_random_n:
                self.log_message("\nSelecting random files...")
                n_files = min(int(self.random_n_size.get()), len(files_to_process))
                self.settings.random_n_size = n_files
                self.settings.total_files = len(files_to_process)

                files_to_process = SamplingCalculator.select_random_files(files_to_process, n_files)

                self.log_message(
                    f"Using random sampling: {n_files:,} files will be analyzed "
                    f"({(n_files / self.settings.total_files * 100):.1f}% of total)"
                )

            # Initialize output handler
            self.initialize_output_handler(files_to_process)

            total_files = len(files_to_process)
            processed_count = 0

            # Process each file
            results = []
            for file_path in files_to_process:
                if self.stop_event.is_set():
                    break

                while self.pause_event.is_set():
                    if self.stop_event.is_set():
                        break
                    time.sleep(0.1)

                file_results = self.process_single_file(file_path)
                if file_results:
                    # Handle both single results and lists of results
                    if isinstance(file_results, list):
                        results.extend(file_results)
                    else:
                        results.append(file_results)

                processed_count += 1
                progress = (processed_count / total_files) * 100
                self.queue.put(("progress", progress))
                self.queue.put(("status", f"Processed {processed_count:,} of {total_files:,} files"))

            # Write results
            if results:
                self.current_output_handler.write_batch(results, True)

            # Finalize processing
            self.finalize_processing()

        except Exception as e:
            self.handle_processing_error(e, "batch processing")
        finally:
            os.chdir(original_working_dir)
            self.cleanup_processing()

    def process_single_file(self, file_path: str) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Process a single file with appropriate analyzer.

        Args:
            file_path: Path to file to process

        Returns:
            Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]: Analysis results or None
            Returns a list for PDFs (multiple pages) and a single dict for images
        """
        self.log_message(f"Debug: Processing file: {file_path}")

        if self.stop_event.is_set():
            return None

        while self.pause_event.is_set():
            if self.stop_event.is_set():
                return None
            time.sleep(0.1)

        try:
            def minimize_result(result: Dict[str, Any]) -> Dict[str, Any]:
                """Extract only minimal fields if minimal output is selected"""
                if self.minimal_output.get():
                    return {
                        'File': result['File'],
                        'Page': result.get('Page', 1),
                        'Content Status': result['Content Status']
                    }
                return result

            if file_path.lower().endswith('.pdf'):
                results = self.process_pdf(file_path)  # Will return list of results
                if results:
                    return [minimize_result(result) for result in results]
                return None
            else:
                # For image files, use the page_analyzer directly
                result = self.page_analyzer.analyze_image_file(os.path.abspath(file_path))
                if result:
                    return minimize_result(result)
                return None

        except Exception as e:
            self.handle_processing_error(e, file_path)
            return None

    def initialize_processing(self) -> None:
        """Initialize processing state and UI elements"""
        # Reset processing state
        self.current_file_number = 1
        self.total_rows_written = 0
        self.results_batch = []

        # Clear control flags
        self.stop_event.clear()
        self.pause_event.clear()

        # Reset UI elements
        self.progress_var.set(0)
        self.status_label.config(text="Initializing...")
        self.log_text.delete(1.0, tk.END)

        # Create processing options with current settings
        self.processing_options = FileProcessor.ProcessingOptions(
            excluded_folders=self.settings.excluded_folders,
            parallel_processing=True,
            batch_size=self.batch_size,
            show_progress=True,
            max_depth=None  # No depth limit for subdirectories
        )

        # Log start of analysis
        self.log_message("Starting analysis...")
        self.log_message(f"Detection threshold: {self.threshold.get()}%")
        self.log_message(f"Using {self.selected_cores.get()} CPU cores")
        self.log_message("Processing subdirectories: Yes")

        # Log sampling configuration
        if self.use_sampling.get():
            self.log_message("Statistical sampling enabled")
            self.log_message(f"Confidence Level: {self.confidence_level.get()}%")
            self.log_message(f"Margin of Error: {self.margin_of_error.get()}%")
        elif self.use_random_n.get():
            self.log_message("Random N sampling enabled")
            self.log_message(f"Number of files to sample: {self.random_n_size.get()}")

        # Log file type selection
        file_types = []
        if self.include_pdfs.get():
            file_types.append("PDF files")
        if self.include_images.get():
            file_types.append("Image files")
        self.log_message(f"File types selected: {', '.join(file_types)}")

        # Log output configuration
        self.log_message(f"Output format: {self.output_format.get().upper()}")
        if self.output_format.get() == 'csv':
            self.log_message(f"Max rows per file: {self.max_rows.get():,}")
        if hasattr(self, 'minimal_output') and self.minimal_output.get():
            self.log_message("Minimal output mode enabled")

    def prepare_file_list(self) -> List[str]:
        """
        Prepare list of files to process with optional sampling

        Returns:
            List[str]: List of file paths to process
        """
        folder_path = self.folder_entry.get()

        # Get complete file list
        files = FileProcessor.get_file_list(
            folder_path,
            self.include_pdfs.get(),
            self.include_images.get(),
            self.SUPPORTED_FORMATS,
            options=self.processing_options,
            progress_callback=lambda msg: self.log_message(msg)
        )

        total_files = len(files)
        if total_files == 0:
            return []

        self.log_message(f"Found {total_files:,} files to process")

        # Apply sampling if enabled
        if self.use_sampling.get():
            params = SamplingParameters(
                confidence_level=float(self.confidence_level.get()) / 100,
                margin_of_error=float(self.margin_of_error.get()) / 100,
                population_size=total_files
            )

            sample_size = SamplingCalculator.calculate_sample_size(params)
            self.settings.sample_size = sample_size
            self.settings.total_files = total_files

            files = SamplingCalculator.select_random_files(files, sample_size)
            self.log_message(
                f"Using statistical sampling: {sample_size:,} files will be analyzed "
                f"({(sample_size / total_files * 100):.1f}% of total)"
            )

        elif self.use_random_n.get():
            n_files = min(int(self.random_n_size.get()), total_files)
            self.settings.random_n_size = n_files
            self.settings.total_files = total_files

            files = SamplingCalculator.select_random_files(files, n_files)
            self.log_message(
                f"Using random sampling: {n_files:,} files will be analyzed "
                f"({(n_files / total_files * 100):.1f}% of total)"
            )

        return files

    def initialize_output_handler(self, files: List[str]) -> None:
        """
        Initialize appropriate output handler.

        Args:
            files: List of files to be processed

        Raises:
            RuntimeError: If output handler initialization fails
        """
        try:
            save_path = self.save_entry.get()
            self.current_output_handler = create_output_handler(
                self.settings.output_format,
                save_path,
                self.settings
            )
            self.log_message(
                f"Initialized {self.settings.output_format.upper()} output handler"
            )

        except Exception as e:
            raise RuntimeError(f"Failed to initialize output handler: {str(e)}")

    def process_file_batches(self, files: List[str]) -> None:
        """
        Process files in appropriate batches by type

        Args:
            files: List of files to process
        """
        total_files = len(files)
        self.log_message(f"Processing {total_files:,} files...")

        # Separate files by type
        pdf_files = [f for f in files if f.lower().endswith('.pdf')]
        image_files = [f for f in files if os.path.splitext(f.lower())[1] in self.SUPPORTED_FORMATS]

        # Process PDFs
        if pdf_files and not self.stop_event.is_set():
            self.process_pdf_batch(pdf_files)

        # Process images
        if image_files and not self.stop_event.is_set():
            self.process_image_batch(image_files)

    def process_pdf_batch(self, pdf_files: List[str]) -> None:
        """Process batch of PDF files with progress tracking and error handling"""
        self.log_message(f"Processing {len(pdf_files):,} PDF files...")

        for i, file_path in enumerate(pdf_files, 1):
            if self.stop_event.is_set():
                break

            self.log_message(f"Processing PDF {i}/{len(pdf_files)}: {os.path.basename(file_path)}")
            self.queue.put(("progress", (i / len(pdf_files)) * 100))

            try:
                self.process_pdf(file_path)
            except MemoryError as e:
                self.handle_processing_error(e, file_path)
                break  # Stop processing on memory errors
            except Exception as e:
                self.handle_processing_error(e, file_path)
                continue  # Continue with next file on other errors

    def process_pdf(self, pdf_path: str) -> Optional[List[Dict[str, Any]]]:
        """
        Process a single PDF file with multi-threaded page analysis.

        Args:
            pdf_path: Path to PDF file to process

        Returns:
            Optional[List[Dict[str, Any]]]: List of analysis results for all pages or None if processing failed
        """
        self.log_message(f"Debug: Opening PDF: {pdf_path}")

        abs_path = os.path.abspath(pdf_path)
        try:
            with fitz.open(pdf_path) as pdf:
                # Check if PDF is encrypted
                if pdf.is_encrypted:
                    return [{
                        "File": abs_path,
                        "Page": 1,
                        "Content Status": "Page 1 Processing Failed",
                        "Type": "PDF",
                        "Error": "Encryption Error: document closed or encrypted",
                        "Error Severity": "WARNING"
                    }]

                total_pages = len(pdf)
                results = []

                # Process each page using the page_analyzer
                for page_num in range(total_pages):
                    if self.stop_event.is_set():
                        break

                    while self.pause_event.is_set():
                        if self.stop_event.is_set():
                            break
                        time.sleep(0.1)

                    try:
                        result = self.page_analyzer.analyze_pdf_page(
                            pdf[page_num],
                            abs_path,
                            page_num
                        )
                        if result:
                            results.append(result)
                    except Exception as e:
                        error_result = {
                            "File": abs_path,
                            "Page": page_num + 1,
                            "Content Status": f"Page {page_num + 1} Processing Failed",
                            "Type": "PDF",
                            "Error": str(e),
                            "Error Severity": "ERROR"
                        }
                        results.append(error_result)

                return results if results else None

        except fitz.FileDataError as e:
            return [{
                "File": abs_path,
                "Page": 1,
                "Content Status": "Page 1 Processing Failed",
                "Type": "PDF",
                "Error": "Encryption Error: document closed or encrypted",
                "Error Severity": "WARNING"
            }]
        except Exception as e:
            self.handle_processing_error(e, pdf_path)
            return [{
                "File": abs_path,
                "Page": 1,
                "Content Status": "Processing Failed",
                "Type": "PDF",
                "Error": str(e),
                "Error Severity": "ERROR"
            }]

    def process_image_batch(self, image_files: List[str]) -> None:
        """
        Process batch of image files using thread pool

        Args:
            image_files: List of image file paths to process
        """
        self.log_message(f"Processing {len(image_files):,} image files...")
        batch_size = self.selected_cores.get() * 2  # Optimize batch size based on cores

        for i in range(0, len(image_files), batch_size):
            if self.stop_event.is_set():
                break

            batch = image_files[i:i + batch_size]
            current_batch = i // batch_size + 1
            total_batches = (len(image_files) + batch_size - 1) // batch_size

            self.log_message(f"Processing image batch {current_batch}/{total_batches}")
            self.queue.put(("progress", ((i + len(batch)) / len(image_files)) * 100))

            # Process batch using thread pool
            with ThreadPoolExecutor(max_workers=self.selected_cores.get()) as executor:
                # Use list to ensure all futures complete
                try:
                    list(executor.map(self.process_image, batch))
                except Exception as e:
                    self.handle_processing_error(e, "image batch processing")
                    # Continue with next batch

    def process_image(self, image_path: str) -> None:
        """
        Process a single image file

        Args:
            image_path: Path to image file to process
        """
        if self.stop_event.is_set():
            return

        while self.pause_event.is_set():
            if self.stop_event.is_set():
                return
            time.sleep(0.1)

        try:
            result = self.page_analyzer.analyze_image_file(image_path)
            if result:
                self.add_result(result)

        except Exception as e:
            self.handle_processing_error(e, image_path)

    def add_result(self, result: Dict[str, Any]):
        """Add a result and write batch if needed"""
        if result:
            with self.results_lock:
                self.results_batch.append(result)

                if len(self.results_batch) >= self.batch_size:
                    self.write_batch()

    def write_batch(self, is_final: bool = False):
        """Write current batch of results"""
        if not self.results_batch and not is_final:
            return

        try:
            output_file = self.output_handler.write_batch(self.results_batch, is_final)
            if output_file:
                self.log_message(f"Wrote batch of {len(self.results_batch)} results to {output_file}")
            self.results_batch = []

        except Exception as e:
            self.handle_processing_error(e, "batch writing")

    def finalize_processing(self):
        """Finalize processing and write remaining results"""
        if not self.stop_event.is_set():
            if self.results_batch:
                self.write_batch(is_final=True)
            self.complete_analysis()

    def cleanup_processing(self):
        """Clean up resources after processing"""
        if hasattr(self, 'output_handler'):
            try:
                self.output_handler.cleanup()
            except Exception as e:
                self.log_message(f"Error during cleanup: {str(e)}")

        self.queue.put(("complete", None))

    def handle_page_error(self, error: Exception, file_name: str, page_num: int) -> Dict[str, Any]:
        """
        Handle page-specific processing errors

        Args:
            error: Exception that occurred
            file_name: Name of file being processed
            page_num: Page number where error occurred

        Returns:
            Dict containing error information
        """
        error_record = self.error_handler.handle_error(error, file_name, page_num + 1)
        return ErrorAwareResult(
            file_name=os.path.basename(file_name),
            content_status=f"Page {page_num + 1} Processing Failed",
            file_type="PDF",
            error=error_record
        ).to_dict()

    def handle_processing_error(self, error: Exception, context: str):
        """Handle processing errors and update UI"""
        error_record = self.error_handler.handle_error(error, context)
        self.log_message(f"Error in {context}: {str(error)}")

        # Add error result to output if appropriate
        if isinstance(context, str) and os.path.exists(context):
            self.add_result(
                ErrorAwareResult(
                    file_name=os.path.basename(context),
                    content_status="Processing Failed",
                    file_type=self.determine_file_type(context),
                    error=error_record
                ).to_dict()
            )

        # Stop processing on critical errors
        if error_record.severity == ErrorSeverity.CRITICAL:
            self.handle_critical_error(error_record)

    def handle_critical_error(self, error: ProcessingError):
        """Handle critical errors that require stopping the process"""
        self.stop_event.set()
        self.log_message("Critical error encountered - stopping processing")

        error_message = (
            f"A critical error has occurred:\n\n"
            f"{error.category.value}: {error.message}\n\n"
            f"Processing has been stopped. Please check the log for details."
        )

        # Show error dialog in main thread
        self.root.after(0, lambda: messagebox.showerror("Critical Error", error_message))

    def determine_file_type(self, file_path: str) -> str:
        """Determine file type from file extension"""
        ext = os.path.splitext(file_path.lower())[1]
        if ext == '.pdf':
            return "PDF"
        elif ext in self.SUPPORTED_FORMATS:
            return "Image"
        return "Unknown"

    def update_file_count(self, folder_path: Optional[str] = None, trigger: str = "manual") -> int:
        """Update file count and display"""
        try:
            # Use provided path or current folder entry
            path_to_check = folder_path or self.folder_entry.get()
            if not path_to_check:
                self.file_count_var.set("No folder selected")
                self.file_count_label.configure(foreground='gray')
                if hasattr(self, 'settings'):
                    self.settings = replace(self.settings, total_files=0)
                return 0

            if not os.path.exists(path_to_check):
                self.file_count_var.set("Selected folder not found")
                self.file_count_label.configure(foreground='red')
                if hasattr(self, 'settings'):
                    self.settings = replace(self.settings, total_files=0)
                return 0

            # Count files
            files = FileProcessor.get_file_list(
                path_to_check,
                self.include_pdfs.get(),
                self.include_images.get(),
                self.SUPPORTED_FORMATS,
                options=self.processing_options,
                progress_callback=lambda msg: self.log_message(msg) if trigger != "checkbox" else None
            )

            total_files = len(files)

            # No files case
            if total_files == 0:
                message = ("Please select at least one file type"
                           if not (self.include_pdfs.get() or self.include_images.get())
                           else "No compatible files found")
                self._update_count_display(message, 'red', 0)
                return 0

            # Calculate displayed file count based on sampling settings
            if self.use_sampling.get():
                params = SamplingParameters(
                    confidence_level=float(self.confidence_level.get()) / 100,
                    margin_of_error=float(self.margin_of_error.get()) / 100,
                    population_size=total_files
                )
                sample_size = SamplingCalculator.calculate_sample_size(params)
                count_text = f"Files to analyze: {sample_size:,} (statistical sample from {total_files:,} total)"
            elif self.use_random_n.get():
                n_files = min(int(self.random_n_size.get()), total_files)
                count_text = f"Files to analyze: {n_files:,} (random sample from {total_files:,} total)"
            else:
                count_text = f"Files to analyze: {total_files:,}"

            # Add file type breakdown
            pdf_count = len([f for f in files if f.lower().endswith('.pdf')])
            image_count = total_files - pdf_count

            details = []
            if pdf_count > 0:
                details.append(f"{pdf_count:,} PDF{'' if pdf_count == 1 else 's'}")
            if image_count > 0:
                details.append(f"{image_count:,} image{'' if image_count == 1 else 's'}")
            if details:
                count_text += f" ({', '.join(details)})"

            self._update_count_display(count_text, 'navy', total_files)

            # Log details if not triggered by checkbox
            if trigger != "checkbox" and total_files > 0:
                self._log_file_counts(total_files, pdf_count, image_count)

            return total_files

        except Exception as e:
            self._update_count_display("Error counting files", 'red', 0)
            if trigger == "browse":
                messagebox.showerror("Error", f"Error counting files: {str(e)}")
            return 0

    def handle_no_files(self):
        """Handle case when no files are found"""
        self.log_message("No compatible files found in the selected folder!")
        messagebox.showwarning("No Files", "No compatible files found in the selected folder!")
        self.queue.put(("complete", None))

    def analyze_pdf_page(self, page: fitz.Page, file_path: str,
                         page_num: int) -> Dict[str, Any]:
        """
        Analyze a PDF page for margin content.

        Args:
            page: PDF page object to analyze
            file_path: Path to PDF file
            page_num: Page number being analyzed (0-based)

        Returns:
            Dict[str, Any]: Analysis results
        """
        self.log_message(f"Debug: Analyzing PDF page: {file_path}, page {page_num + 1}")

        try:
            # Analyze text content
            text_analysis = self.content_analyzer.analyze_text_blocks(page)

            # Convert to image and analyze
            pix = page.get_pixmap(matrix=fitz.Matrix(
                self.content_analyzer.dpi / 72, self.content_analyzer.dpi / 72))
            image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            image_analysis = self.content_analyzer.analyze_image_content(image)

            # Determine overall content status
            locations = []
            if text_analysis.has_top_content or image_analysis.has_top_content:
                locations.append("header")
            if text_analysis.has_bottom_content or image_analysis.has_bottom_content:
                locations.append("footer")

            content_status = (
                "Content found in " + " and ".join(locations) if locations
                else "All content within margins"
            )

            # Create detailed result with absolute path
            abs_path = os.path.abspath(file_path)  # Convert to absolute path
            result = {
                "File": abs_path,
                "Page": page_num + 1,
                "Content Status": content_status,
                "Text Status": self._format_text_status(text_analysis),
                "Image Status": self._format_image_status(image_analysis),
                "Type": "PDF",
                "Analysis Details": {
                    "Text": {
                        "Top Content": f"{text_analysis.top_content_percentage:.1f}%",
                        "Bottom Content": f"{text_analysis.bottom_content_percentage:.1f}%",
                    },
                    "Image": {
                        "Top Content": f"{image_analysis.top_content_percentage:.1f}%",
                        "Bottom Content": f"{image_analysis.bottom_content_percentage:.1f}%",
                    }
                }
            }

            return result

        except Exception as e:
            # Handle any errors with absolute path
            abs_path = os.path.abspath(file_path)
            return {
                "File": abs_path,
                "Page": page_num + 1,
                "Content Status": f"Page {page_num + 1} Processing Failed",
                "Type": "PDF",
                "Error": str(e),
                "Error Severity": "ERROR"
            }


    def complete_analysis(self) -> None:
        """Handle analysis completion and error reporting"""
        try:
            # Get error information
            error_summary = self.error_handler.get_error_summary()
            critical_errors = self.error_handler.get_critical_errors()

            # Log final statistics
            self.log_final_statistics()

            # Log error summary if there are any errors
            if error_summary:
                self.log_message("\nError Summary:")
                for category, count in error_summary.items():
                    self.log_message(f"{category}: {count:,} occurrences")

            # Show critical errors in GUI if any occurred
            if critical_errors:
                self.show_critical_errors(critical_errors)

            # Update final status
            self.update_final_status(len(self.error_handler.errors))

        except Exception as e:
            self.log_message(f"Error completing analysis: {str(e)}")
            messagebox.showerror("Error", "Failed to complete analysis summary.")

    def log_final_statistics(self):
        """Log final processing statistics"""
        stats = self.calculate_processing_stats()

        self.log_message("\nProcessing Statistics:")
        self.log_message(f"Total Files Processed: {stats['total_files']}")
        self.log_message(f"Successful: {stats['successful']}")
        self.log_message(f"Failed: {stats['failed']}")

        if stats['sampling_used']:
            self.log_message(f"\nSampling Information:")
            self.log_message(f"Original Population: {stats['total_population']}")
            self.log_message(f"Sample Size: {stats['sample_size']}")
            self.log_message(f"Confidence Level: {stats['confidence_level']}%")
            self.log_message(f"Margin of Error: {stats['margin_of_error']}%")

    def calculate_processing_stats(self) -> Dict[str, Any]:
        """Calculate processing statistics"""
        stats = {
            'total_files': len(self.error_handler.errors),
            'successful': 0,
            'failed': 0,
            'sampling_used': self.settings.use_sampling,
            'total_population': self.settings.total_files,
            'sample_size': self.settings.sample_size,
            'confidence_level': float(self.confidence_level.get()),
            'margin_of_error': float(self.margin_of_error.get())
        }

        for error in self.error_handler.errors.values():
            if error.severity == ErrorSeverity.CRITICAL:
                stats['failed'] += 1
            else:
                stats['successful'] += 1

        return stats

    def show_critical_errors(self, critical_errors: List[ProcessingError]):
        """Display critical errors in a message box"""
        message = "Critical errors occurred during processing:\n\n"
        for error in critical_errors[:5]:  # Show first 5 critical errors
            message += f"• {error.file_name}: {error.message}\n"
        if len(critical_errors) > 5:
            message += f"\n(and {len(critical_errors) - 5} more...)"

        messagebox.showwarning("Processing Warnings", message)

    def update_final_status(self, total_errors: int):
        """Update final status message"""
        if total_errors > 0:
            self.queue.put(("status", f"Analysis complete with {total_errors} errors"))
        else:
            self.queue.put(("status", "Analysis complete successfully"))

    def update_progress(self, current: int, total: int) -> None:
        """
        Update progress indicators

        Args:
            current: Current item number
            total: Total number of items
        """
        try:
            # Calculate progress percentage
            progress = (current / total * 100) if total > 0 else 0

            # Update progress bar
            self.queue.put(("progress", progress))

            # Update status message
            status_msg = f"Processing page {current + 1:,}/{total:,}"
            self.queue.put(("status", status_msg))

        except Exception as e:
            self.log_message(f"Error updating progress: {str(e)}")

    def validate_analysis_params(self) -> bool:
        """Validate analysis parameters before starting"""
        if not self.folder_entry.get():
            messagebox.showerror("Error", "Please select an input folder!")
            return False

        if not self.save_entry.get():
            messagebox.showerror("Error", "Please select a save location!")
            return False

        if not self.include_pdfs.get() and not self.include_images.get():
            messagebox.showerror("Error", "Please select at least one file type to process!")
            return False

        if not os.path.exists(self.folder_entry.get()):
            messagebox.showerror("Error", "Selected folder does not exist!")
            return False

        # Add threshold validation
        threshold = self.threshold.get()
        if not 0.1 <= threshold <= 10.0:
            messagebox.showerror(
                "Error",
                "Threshold must be between 0.1% and 10.0%\n"
                "Recommended: Use 1.0% for standard analysis."
            )
            return False

        # Validate sampling options
        if self.use_sampling.get() and self.use_random_n.get():
            messagebox.showerror("Error", "Please select only one sampling method!")
            return False

        if self.use_sampling.get():
            try:
                confidence = float(self.confidence_level.get()) / 100
                margin = float(self.margin_of_error.get()) / 100
                if not (0 < confidence < 1 and 0 < margin < 1):
                    raise ValueError
            except ValueError:
                messagebox.showerror("Error", "Invalid sampling parameters!")
                return False

        if self.use_random_n.get():
            try:
                n_files = int(self.random_n_size.get())
                if n_files < 1:
                    raise ValueError
            except ValueError:
                messagebox.showerror("Error", "Invalid number of files selected!")
                return False

        selected_cores = self.selected_cores.get()
        if selected_cores < 1 or selected_cores > self.available_cores:
            messagebox.showerror(
                "Error",
                f"Please select between 1 and {self.available_cores} cores!"
            )
            return False

        return True

    def start_analysis(self):
        """Start the analysis process"""
        if not self.validate_analysis_params():
            return

        try:
            # Update settings first
            if not self.update_settings():
                return

            # Get current file count
            folder_path = self.folder_entry.get()
            files = FileProcessor.get_file_list(
                folder_path,
                self.include_pdfs.get(),
                self.include_images.get(),
                self.SUPPORTED_FORMATS
            )
            total_files = len(files)

            if total_files == 0:
                messagebox.showwarning(
                    "No Files",
                    "No compatible files found in the selected folder with current settings."
                )
                return

            # Update settings with file counts
            self.settings = AnalysisSettings(
                threshold=self.threshold.get(),
                output_format=self.output_format.get(),
                max_rows_per_file=self.max_rows.get(),
                excluded_folders={'$RECYCLE.BIN', 'System Volume Information'},
                use_sampling=self.use_sampling.get(),
                use_random_n=self.use_random_n.get(),
                random_n_size=int(self.random_n_size.get()) if self.use_random_n.get() else None,
                confidence_level=float(self.confidence_level.get()) / 100,
                margin_of_error=float(self.margin_of_error.get()) / 100,
                include_pdfs=self.include_pdfs.get(),
                include_images=self.include_images.get(),
                total_files=total_files
            )

            # Calculate sample size if sampling is enabled
            if self.settings.use_sampling:
                params = SamplingParameters(
                    confidence_level=self.settings.confidence_level,
                    margin_of_error=self.settings.margin_of_error,
                    population_size=total_files
                )
                sample_size = SamplingCalculator.calculate_sample_size(params)
                self.settings.sample_size = sample_size
                self.log_message(
                    f"Using statistical sampling: {sample_size:,} files will be analyzed "
                    f"({(sample_size / total_files * 100):.1f}% of total)"
                )
            elif self.settings.use_random_n:
                n_files = min(int(self.random_n_size.get()), total_files)
                self.settings.random_n_size = n_files
                self.log_message(
                    f"Using random sampling: {n_files:,} files will be analyzed "
                    f"({(n_files / total_files * 100):.1f}% of total)"
                )

            # Initialize output handler
            try:
                save_path = self.save_entry.get()
                self.output_handler = create_output_handler(
                    self.settings.output_format,
                    save_path,
                    self.settings
                )
                self.log_message(f"Initialized {self.settings.output_format.upper()} output handler")
            except Exception as e:
                raise RuntimeError(f"Failed to initialize output handler: {str(e)}")

            # Initialize processing stats
            self.processing_stats = ProcessingStats()
            self.processing_stats.start()

            # Initialize processing state
            self.processing = True
            self.current_file_number = 1
            self.total_rows_written = 0
            self.results_batch = []

            # Reset control flags
            self.stop_event.clear()
            self.pause_event.clear()

            # Update UI state
            self.analyze_btn.config(state=tk.DISABLED)
            self.pause_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.NORMAL)

            # Reset progress indicators
            self.progress_var.set(0)
            self.elapsed_var.set("Elapsed: 00:00:00")
            self.remaining_var.set("Remaining: Calculating...")
            self.rate_var.set("0 files/sec")
            self.status_label.config(text="Starting analysis...")
            self.log_text.delete(1.0, tk.END)

            # Log initial status and settings
            self.log_message("Starting analysis...")
            self.log_message(f"Detection threshold: {self.threshold.get()}%")
            self.log_message(f"Using {self.selected_cores.get()} CPU cores")

            if self.use_sampling.get():
                self.log_message("Statistical sampling enabled")
                self.log_message(f"Confidence Level: {self.confidence_level.get()}%")
                self.log_message(f"Margin of Error: {self.margin_of_error.get()}%")
            elif self.use_random_n.get():
                self.log_message("Random N sampling enabled")
                self.log_message(f"Number of files: {self.random_n_size.get()}")

            # Create and start thread with immediate non-blocking join
            self.processing_thread = Thread(target=self.process_files, daemon=True)
            self.processing_thread.start()
            self.processing_thread.join(timeout=0.0)  # Non-blocking join to satisfy static analysis

            # Store thread start time for monitoring
            self.processing_start_time = time.time()

            # Start updating UI
            self.update_ui()

        except Exception as e:
            error_msg = str(e)
            self.log_message(f"Error starting analysis: {error_msg}")
            messagebox.showerror(
                "Error",
                f"Failed to start analysis:\n\n{error_msg}\n\nCheck the log for details."
            )
            # Reset processing state
            self.processing = False
            self.analyze_btn.config(state=tk.NORMAL)
            self.pause_btn.config(state=tk.DISABLED)
            self.stop_btn.config(state=tk.DISABLED)

            # Clean up thread if it exists
            if hasattr(self, 'processing_thread') and self.processing_thread.is_alive():
                try:
                    self.stop_event.set()
                    self.processing_thread.join(timeout=2.0)
                except Exception as thread_error:
                    self.log_message(f"Error cleaning up processing thread: {str(thread_error)}")

            # Clean up output handler
            if hasattr(self, 'output_handler'):
                try:
                    self.output_handler.cleanup()
                except Exception as cleanup_error:
                    self.log_message(f"Error during cleanup: {str(cleanup_error)}")

    def _update_count_display(self, text: str, color: str, count: int) -> None:
        """Update file count display and settings"""
        self.file_count_var.set(text)
        self.file_count_label.configure(foreground=color)
        if hasattr(self, 'settings'):
            self.settings = replace(self.settings, total_files=count)

    def _log_file_counts(self, total: int, pdf_count: int, image_count: int) -> None:
        """Log file count details"""
        self.log_message(f"Found {total:,} files to analyze:")
        if pdf_count > 0:
            self.log_message(f"  - {pdf_count:,} PDF file{'' if pdf_count == 1 else 's'}")
        if image_count > 0:
            self.log_message(f"  - {image_count:,} image file{'' if image_count == 1 else 's'}")

    def analyze_image_file(self, image_path: str) -> Dict[str, Any]:
        """
        Analyze an image file for margin content.

        Args:
            image_path: Path to image file to analyze

        Returns:
            Dict[str, Any]: Analysis results
        """
        abs_path = os.path.abspath(image_path)  # Convert to absolute path at the start
        try:
            with Image.open(image_path) as image:
                image = image.convert('RGB')
                analysis = self.content_analyzer.analyze_image_content(image)

                locations = []
                if analysis.has_top_content:
                    locations.append("header")
                if analysis.has_bottom_content:
                    locations.append("footer")

                content_status = (
                    "Content found in " + " and ".join(locations) if locations
                    else "All content within margins"
                )

                return {
                    "File": abs_path,  # Use absolute path
                    "Page": 1,
                    "Content Status": content_status,
                    "Type": "Image",
                    "Analysis Details": {
                        "Top Content": f"{analysis.top_content_percentage:.1f}%",
                        "Bottom Content": f"{analysis.bottom_content_percentage:.1f}%",
                        "Total Margin Content": f"{analysis.total_content_percentage:.1f}%"
                    }
                }

        except Exception as e:
            return {
                "File": abs_path,  # Use absolute path in error case too
                "Page": 1,
                "Content Status": "Processing Failed",
                "Type": "Image",
                "Analysis Details": {},
                "Error": str(e),
                "Error Severity": "ERROR"
            }

    def log_message(self, message: str) -> None:
        """Add timestamped message to log"""
        try:
            timestamp = time.strftime('%H:%M:%S')
            self.log_text.insert(tk.END, f"{timestamp}: {message}\n")
            self.log_text.see(tk.END)

            if any(keyword in message.lower() for keyword in ['error', 'failed', 'warning']):
                # Highlight error messages
                last_line = self.log_text.get("end-2c linestart", "end-1c")
                self.log_text.tag_add('error', f"end-{len(last_line) + 1}c linestart", "end-1c")

                # Auto-expand log for errors
                if hasattr(self, 'log_expanded') and hasattr(self, 'log_expand_btn'):
                    if not self.log_expanded.get():
                        self.log_expanded.set(True)
                        self.log_expand_btn.invoke()

        except Exception as e:
            print(f"Failed to log message: {e}\nOriginal message: {message}")

    def reset_progress(self):
        """Reset progress indicators and logs"""
        self.progress_var.set(0)
        self.status_label.config(text="Starting analysis...")
        self.log_text.delete(1.0, tk.END)
        self.log_message("Starting analysis...")

        # Reset processing state
        self.current_file_number = 1
        self.total_rows_written = 0
        self.results_batch = []
        self.stop_event.clear()
        self.pause_event.clear()

    def update_ui(self) -> None:
        """Update UI with progress and status information"""
        try:
            while True:
                try:
                    msg_type, msg_data = self.queue.get_nowait()
                    try:
                        match msg_type:
                            case "log":
                                self.update_log(msg_data)
                            case "progress":
                                # Update progress bar
                                self.update_progress_bar(msg_data)

                                # Update processing stats and timing info
                                if hasattr(self, 'processing_stats'):
                                    processed_count = int(msg_data * self.settings.total_files / 100)
                                    self.processing_stats.update(processed_count)

                                    # Update timing displays
                                    self.elapsed_var.set(
                                        f"Elapsed: {self.processing_stats.get_elapsed_time()}"
                                    )
                                    self.remaining_var.set(
                                        f"Remaining: {self.processing_stats.get_estimated_time_remaining(self.settings.total_files)}"
                                    )
                                    self.rate_var.set(
                                        self.processing_stats.get_processing_rate()
                                    )
                            case "status":
                                self.update_status(msg_data)
                            case "complete":
                                self.handle_completion()
                                # Don't return here - let task_done() execute first
                                should_return = True
                            case _:
                                self.log_message(f"Unknown message type: {msg_type}")
                    finally:
                        # Ensure task_done() is called for every get()
                        self.queue.task_done()

                    # Check if we should return after task_done()
                    if msg_type == "complete":
                        return

                except queue.Empty:
                    break
        except Exception as e:
            self.handle_ui_error(e)
        finally:
            if self.processing:
                # Schedule next update
                self.root.after(100, self.update_ui)

    def update_ui_state(self, analyzing: bool):
        """Update UI controls based on analysis state"""
        if analyzing:
            self.analyze_btn.config(state=tk.DISABLED)
            self.pause_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.NORMAL)
        else:
            self.analyze_btn.config(state=tk.NORMAL)
            self.pause_btn.config(state=tk.DISABLED)
            self.stop_btn.config(state=tk.DISABLED)

    def update_log(self, message: str):
        """Update log text widget"""
        try:
            self.log_text.insert(tk.END, message + "\n")
            self.log_text.see(tk.END)
        except Exception as e:
            print(f"Error updating log: {str(e)}")

    def update_progress_bar(self, progress: float):
        """Update progress bar value"""
        try:
            self.progress_var.set(progress)
        except Exception as e:
            print(f"Error updating progress bar: {str(e)}")

    def update_status(self, status: str):
        """Update status label text"""
        try:
            self.status_label.config(text=status)
        except Exception as e:
            print(f"Error updating status: {str(e)}")

    def handle_completion(self) -> None:
        """Handle analysis completion"""
        try:
            self.processing = False
            self.update_ui_state(analyzing=False)
            self.log_message("Analysis complete!")

            # Play system notification sound if available
            if platform.system() == "Windows":
                import winsound
                winsound.MessageBeep()

        except Exception as e:
            self.handle_ui_error(e)

    def handle_ui_error(self, error: Exception):
        """Handle errors in UI update"""
        print(f"Error in UI update: {str(error)}")
        self.log_message(f"UI Error: {str(error)}")

    def create_help_button(self, parent, help_text: str, width: int = 2) -> ttk.Button:
        """Create a standardized help button with tooltip"""
        return ttk.Button(
            parent,
            text="?",
            width=width,
            command=lambda: messagebox.showinfo("Help", help_text)
        )

    def create_threshold_tooltip(self):
        return """Threshold determines how much content is allowed in margins before flagging.

    - Default: 1.0% (recommended)
    - Range: 0.1% to 10.0%
    - Increments: 0.1%
    - Lower values = more sensitive
    - Higher values = less sensitive

    The threshold applies to image content detection.
    Any text found in margins is always flagged."""

    def toggle_sampling_options(self) -> None:
        """Show or hide sampling options based on checkbox states"""
        try:
            # Ensure mutual exclusivity
            if self.use_sampling.get() and self.use_random_n.get():
                # If one was just checked, uncheck the other
                if self._last_sampling_change == "statistical":
                    self.use_random_n.set(False)
                else:
                    self.use_sampling.set(False)

            # Show/hide appropriate options
            if self.use_sampling.get():
                self._last_sampling_change = "statistical"
                self.statistical_options.grid()
                self.random_n_options.grid_remove()
            elif self.use_random_n.get():
                self._last_sampling_change = "random_n"
                self.random_n_options.grid()
                self.statistical_options.grid_remove()
            else:
                self.statistical_options.grid_remove()
                self.random_n_options.grid_remove()

            # Update file count display
            self.update_file_count(trigger="checkbox")

        except Exception as e:
            self.handle_ui_error(e)

    def on_format_change(self, *args) -> None:
        """Handle output format changes"""
        try:
            if self.output_format.get() == 'csv':
                self.csv_options.grid()
            else:
                self.csv_options.grid_remove()

            # Update file extension in save location
            if self.save_entry.get():
                current_path = self.save_entry.get()
                base_path = os.path.splitext(current_path)[0]
                new_ext = {
                    'csv': '.csv',
                    'parquet': '.parquet',
                    'sqlite': '.db'
                }.get(self.output_format.get(), '.csv')
                self.save_entry.delete(0, tk.END)
                self.save_entry.insert(0, base_path + new_ext)
        except Exception as e:
            self.handle_ui_error(e)

    def browse_folder(self):
        """Open folder selection dialog and count files"""
        try:
            folder_selected = filedialog.askdirectory(title="Select Folder Containing Documents")
            if folder_selected:
                self.folder_entry.delete(0, tk.END)
                self.folder_entry.insert(0, folder_selected)
                self.log_message(f"Selected folder: {folder_selected}")

                # Update file count
                self.update_file_count(folder_selected, trigger="browse")

        except Exception as e:
            self.file_count_var.set("Error selecting folder")
            self.file_count_label.configure(foreground='red')
            messagebox.showerror("Error", f"Error selecting folder: {str(e)}")

    def browse_save_location(self):
        """Open save location selection dialog"""
        try:
            file_path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[
                    ("CSV files", "*.csv"),
                    ("Parquet files", "*.parquet"),
                    ("SQLite databases", "*.db"),
                    ("All files", "*.*")
                ],
                title="Choose Save Location"
            )
            if file_path:
                self.save_entry.delete(0, tk.END)
                self.save_entry.insert(0, file_path)
        except Exception as e:
            messagebox.showerror("Error", f"Error selecting save location: {str(e)}")

    def toggle_pause(self) -> None:
        """Toggle pause state of analysis and update timing"""
        try:
            if self.pause_event.is_set():
                # Resuming
                self.pause_event.clear()
                self.pause_btn.configure(text="Pause")
                self.log_message("Analysis resumed")
                self.processing_stats.resume()
            else:
                # Pausing
                self.pause_event.set()
                self.pause_btn.configure(text="Resume")
                self.log_message("Analysis paused")
                self.processing_stats.pause()

        except Exception as e:
            self.handle_ui_error(e)
            self.pause_event.clear()

    def stop_analysis(self) -> None:
        """Stop the analysis process with user confirmation"""
        try:
            if messagebox.askyesno("Confirm Stop", "Are you sure you want to stop the analysis?"):
                self.stop_event.set()
                self.log_message("Stopping analysis...")
                self.update_status("Analysis stopped by user")
        except Exception as e:
            self.handle_ui_error(e)

    def add_to_batch(self, result: Dict[str, Any]):
        """Add result to batch with thread safety"""
        with self.results_lock:
            self.results_batch.append(result)

            if len(self.results_batch) >= self.batch_size:
                self.write_current_batch()

    def write_current_batch(self, is_final: bool = False) -> None:
        """Write current batch of results to output"""
        try:
            if not self.results_batch and not is_final:
                return

            output_file = self.current_output_handler.write_batch(
                self.results_batch,
                is_final=is_final
            )

            if output_file:
                batch_size = len(self.results_batch)
                self.log_message(f"Wrote batch of {batch_size:,} results to {output_file}")

            self.results_batch = []
        except Exception as e:
            self.handle_processing_error(e, "writing results")

    def get_next_filename(self) -> str:
        """Generate the next filename when splitting files"""
        if self.current_file_number == 1:
            return self.save_entry.get()

        base, ext = os.path.splitext(self.save_entry.get())
        return f"{base}__{self.current_file_number}{ext}"

    def export_error_report(self):
        """Export detailed error report"""
        try:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            report_path = os.path.join(
                os.path.dirname(self.save_entry.get()),
                f'error_report_{timestamp}.csv'
            )

            error_data = []
            for error in self.error_handler.errors.values():
                error_data.append({
                    'Timestamp': error.timestamp,
                    'File': error.file_name,
                    'Page': error.page_number,
                    'Category': error.category.value,
                    'Severity': error.severity.value,
                    'Message': error.message,
                    'Details': error.details
                })

            if error_data:
                pd.DataFrame(error_data).to_csv(report_path, index=False)
                self.log_message(f"Error report exported to: {report_path}")

        except Exception as e:
            self.log_message(f"Failed to export error report: {str(e)}")

    def create_success_report(self):
        """Create processing success report"""
        try:
            stats = self.calculate_processing_stats()
            report_path = os.path.join(
                os.path.dirname(self.save_entry.get()),
                'processing_report.txt'
            )

            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("Document Margin Analysis Report\n")
                f.write("=" * 30 + "\n\n")

                f.write("Processing Statistics:\n")
                f.write(f"Total Files Processed: {stats['total_files']}\n")
                f.write(f"Successfully Processed: {stats['successful']}\n")
                f.write(f"Failed: {stats['failed']}\n\n")

                if stats['sampling_used']:
                    f.write("Sampling Information:\n")
                    f.write(f"Original Population: {stats['total_population']}\n")
                    f.write(f"Sample Size: {stats['sample_size']}\n")
                    f.write(f"Confidence Level: {stats['confidence_level']}%\n")
                    f.write(f"Margin of Error: {stats['margin_of_error']}%\n\n")

                f.write("Processing Configuration:\n")
                f.write(f"Threshold: {self.threshold.get()}%\n")
                f.write(f"Output Format: {self.output_format.get()}\n")
                f.write(f"CPU Cores Used: {self.selected_cores.get()}\n")

            self.log_message(f"Processing report created: {report_path}")

        except Exception as e:
            self.log_message(f"Failed to create processing report: {str(e)}")


def main() -> None:
    """Main entry point for the application"""
    root = tk.Tk()
    app = None

    try:
        # Calculate window size and position
        window_width = 700
        window_height = 680

        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        center_x = int((screen_width - window_width) / 2)
        top_y = 20

        # Configure window
        root.geometry(f"{window_width}x{window_height}+{center_x}+{top_y}")
        root.minsize(600, 650)

        # Set icon if available
        try:
            icon_path = 'icon.ico'
            if getattr(sys, 'frozen', False):
                icon_path = os.path.join(sys._MEIPASS, 'icon.ico')
            if os.path.exists(icon_path):
                root.iconbitmap(icon_path)
        except Exception:
            pass

        # Create and configure application
        app = DocumentAnalyzerGUI(root)
        root.title("Document Margin Analyzer v2.0")
        root.protocol("WM_DELETE_WINDOW", app.cleanup)

        # Start application
        root.mainloop()

    except Exception as e:
        error_message = f"Fatal error: {str(e)}\n\nPlease report this error if it persists."
        messagebox.showerror("Error", error_message)

        if app:
            try:
                app.cleanup()
            except:
                pass

        sys.exit(1)


if __name__ == "__main__":
    main()
