import math
import random
import os
from typing import List, TypeVar, Sequence, Set, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

T = TypeVar('T')


@dataclass
class SamplingParameters:
    """Parameters for statistical sampling"""
    confidence_level: float
    margin_of_error: float
    population_size: int

    def __post_init__(self):
        """Validate sampling parameters"""
        if not 0 < self.confidence_level < 1:
            raise ValueError("Confidence level must be between 0 and 1")
        if not 0 < self.margin_of_error < 1:
            raise ValueError("Margin of error must be between 0 and 1")
        if self.population_size < 1:
            raise ValueError("Population size must be positive")


class SamplingCalculator:
    """Calculator for statistical sampling"""

    # Z-scores for common confidence levels
    Z_SCORES = {
        0.90: 1.645,
        0.95: 1.96,
        0.99: 2.576
    }

    @classmethod
    def get_z_score(cls, confidence_level: float) -> float:
        """
        Get Z-score for given confidence level

        Args:
            confidence_level: Confidence level (0.90, 0.95, or 0.99)

        Returns:
            Corresponding Z-score

        Raises:
            ValueError: If confidence level is not supported
        """
        z_score = cls.Z_SCORES.get(confidence_level)
        if z_score is None:
            raise ValueError(
                f"Unsupported confidence level: {confidence_level}. "
                f"Supported values are: {list(cls.Z_SCORES.keys())}"
            )
        return z_score

    @classmethod
    def calculate_sample_size(cls, params: SamplingParameters) -> int:
        """
        Calculate required sample size using the following formula:
        n = (Z²pq)N / (NE² + Z²pq)
        where:
        Z = Z-score for confidence level
        p = 0.5 (worst case scenario)
        q = 1-p = 0.5
        N = population size
        E = margin of error

        Args:
            params: SamplingParameters containing confidence level, margin of error,
                   and population size

        Returns:
            Required sample size
        """
        z_score = cls.get_z_score(params.confidence_level)

        # Use p=q=0.5 for maximum sample size
        pq = 0.25

        numerator = (z_score ** 2) * pq * params.population_size
        denominator = (params.population_size * (params.margin_of_error ** 2)) + (
                (z_score ** 2) * pq)

        sample_size = math.ceil(numerator / denominator)

        # Ensure minimum sample size
        return max(sample_size, min(30, params.population_size))

    @staticmethod
    def select_random_files(files: Sequence[T], sample_size: int) -> List[T]:
        """
        Select random files from the population

        Args:
            files: Sequence of files to sample from
            sample_size: Number of files to select

        Returns:
            List of randomly selected files
        """
        if sample_size >= len(files):
            return list(files)

        return random.sample(files, sample_size)

    @classmethod
    def estimate_error_margin(cls, sample_size: int, population_size: int,
                              confidence_level: float) -> float:
        """
        Estimate margin of error for a given sample size

        Args:
            sample_size: Size of the sample
            population_size: Size of the population
            confidence_level: Desired confidence level

        Returns:
            Estimated margin of error
        """
        z_score = cls.get_z_score(confidence_level)
        p = 0.5  # Use 0.5 for maximum margin of error

        # Calculate standard error
        standard_error = math.sqrt((p * (1 - p)) / sample_size)

        # Apply finite population correction
        if population_size > 0:
            correction = math.sqrt((population_size - sample_size) /
                                   (population_size - 1))
            standard_error *= correction

        # Calculate margin of error
        margin_of_error = z_score * standard_error

        return margin_of_error


class FileProcessor:
    """Handles file processing with optional sampling"""

    @dataclass
    class ProcessingOptions:
        """Configuration options for file processing"""
        max_depth: Optional[int] = None  # None means no limit
        excluded_folders: Set[str] = field(default_factory=set)
        parallel_processing: bool = True
        batch_size: int = 1000
        show_progress: bool = True

    @staticmethod
    def get_file_list(folder_path: str, include_pdfs: bool, include_images: bool,
                      supported_formats: Set[str],
                      options: Optional['FileProcessor.ProcessingOptions'] = None,
                      progress_callback: Optional[Callable[[str], None]] = None) -> List[str]:
        """Get list of files to process based on inclusion criteria"""
        files = set()  # Use set for uniqueness
        options = options or FileProcessor.ProcessingOptions()
        current_depth = 0

        # Convert input folder path to absolute path
        abs_folder_path = os.path.abspath(folder_path)

        def should_process_directory(dir_path: str, depth: int) -> bool:
            """Check if directory should be processed based on options"""
            if options.max_depth is not None and depth > options.max_depth:
                return False

            dir_name = os.path.basename(dir_path)
            if dir_name in options.excluded_folders:
                return False

            return True

        def scan_directory(path: str, depth: int):
            """Scan directory for matching files"""
            if not should_process_directory(path, depth):
                return

            try:
                with os.scandir(path) as entries:
                    for entry in entries:
                        if progress_callback:
                            progress_callback(f"Scanning: {entry.path}")

                        if entry.is_file():
                            ext = os.path.splitext(entry.name.lower())[1]
                            if (include_pdfs and ext == '.pdf') or (
                                    include_images and ext in supported_formats):
                                # Use the full path from scandir
                                files.add(entry.path)
                        elif entry.is_dir():
                            scan_directory(entry.path, depth + 1)
            except PermissionError:
                if progress_callback:
                    progress_callback(f"Permission denied: {path}")
            except Exception as e:
                if progress_callback:
                    progress_callback(f"Error scanning {path}: {str(e)}")

        # Start scan from absolute folder path
        scan_directory(abs_folder_path, current_depth)
        return sorted(files)  # Return sorted list of file paths

    @staticmethod
    def process_files_parallel(file_list: List[str],
                               processor: Callable[[str], Dict[str, Any]],
                               max_workers: int,
                               batch_size: int = 1000,
                               progress_callback: Optional[Callable[[int, int], None]] = None) -> List[Dict[str, Any]]:
        """Process files in parallel batches"""
        results = []
        total_files = len(file_list)
        processed_files = 0
        results_lock = Lock()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in range(0, total_files, batch_size):
                batch = file_list[i:i + batch_size]
                futures = []

                for file_path in batch:
                    futures.append(executor.submit(processor, file_path))

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            with results_lock:
                                results.append(result)
                        processed_files += 1
                        if progress_callback:
                            progress_callback(processed_files, total_files)
                    except Exception as e:
                        if progress_callback:
                            progress_callback(f"Error processing file: {str(e)}")

        return results

    @classmethod
    def calculate_sample_size(cls, params: SamplingParameters) -> int:
        """Calculate required sample size"""
        return SamplingCalculator.calculate_sample_size(params)

    @classmethod
    def select_random_files(cls, files: Sequence[T], sample_size: int) -> List[T]:
        """Select random files from the population"""
        return SamplingCalculator.select_random_files(files, sample_size)

    @classmethod
    def prepare_file_list(cls, folder_path: str, settings: 'AnalysisSettings',
                          supported_formats: Set[str]) -> List[str]:
        """
        Prepare list of files to process, applying sampling if enabled

        Args:
            folder_path: Path to folder containing files
            settings: Analysis settings including sampling parameters
            supported_formats: Set of supported image file extensions

        Returns:
            List of file paths to process
        """
        # Get complete file list
        files = cls.get_file_list(
            folder_path,
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

            sample_size = cls.calculate_sample_size(params)
            settings.sample_size = sample_size
            settings.total_files = total_files

            return cls.select_random_files(files, sample_size)

        return files
