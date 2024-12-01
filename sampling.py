import math
import random
import os
from typing import List, TypeVar, Sequence, Set
from dataclasses import dataclass

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

    @staticmethod
    def get_file_list(folder_path: str, include_pdfs: bool, include_images: bool,
                      supported_formats: Set[str]) -> List[str]:
        """
        Get list of files to process based on inclusion criteria

        Args:
            folder_path: Path to folder containing files
            include_pdfs: Whether to include PDF files
            include_images: Whether to include image files
            supported_formats: Set of supported image file extensions

        Returns:
            List of file paths matching criteria
        """
        files = []
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                ext = os.path.splitext(filename.lower())[1]
                if (include_pdfs and ext == '.pdf') or (
                        include_images and ext in supported_formats):
                    files.append(file_path)
        return files

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

            sample_size = SamplingCalculator.calculate_sample_size(params)
            settings.sample_size = sample_size
            settings.total_files = total_files

            return SamplingCalculator.select_random_files(files, sample_size)

        return files