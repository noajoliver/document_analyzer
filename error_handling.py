from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from enum import Enum
import traceback
import logging
import os
from datetime import datetime


class ErrorSeverity(Enum):
    """Enumeration of error severity levels"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ErrorCategory(Enum):
    """Categorization of different error types"""
    FILE_ACCESS = "File Access Error"
    PDF_PROCESSING = "PDF Processing Error"
    IMAGE_PROCESSING = "Image Processing Error"
    MEMORY = "Memory Error"
    PERMISSION = "Permission Error"
    ENCRYPTION = "Encryption Error"
    FORMAT = "Format Error"
    OUTPUT = "Output Error"
    SYSTEM = "System Error"
    UNKNOWN = "Unknown Error"


@dataclass
class ProcessingError:
    """Detailed information about a processing error"""
    category: ErrorCategory
    severity: ErrorSeverity
    message: str
    file_name: str
    page_number: Optional[int] = None
    details: Optional[str] = None
    timestamp: str = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary format"""
        return {
            "category": self.category.value,
            "severity": self.severity.value,
            "message": self.message,
            "file_name": self.file_name,
            "page_number": self.page_number,
            "details": self.details,
            "timestamp": self.timestamp
        }


@dataclass
class ErrorAwareResult:
    """Wrapper for processing results that includes error information"""
    file_name: str
    content_status: str
    file_type: str
    error: Optional[ProcessingError] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary format"""
        result = {
            'File': self.file_name,
            'Content Status': self.content_status,
            'Type': self.file_type
        }

        if self.error:
            result.update({
                'Error': f"{self.error.category.value}: {self.error.message}",
                'Error Severity': self.error.severity.value
            })

        return result


class ErrorHandler:
    """Handles error logging and management"""

    def __init__(self, log_dir: str = "logs"):
        self.log_dir = log_dir
        self.setup_logging()
        self.errors: Dict[str, ProcessingError] = {}

    def setup_logging(self):
        """Setup logging configuration"""
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

        log_file = os.path.join(
            self.log_dir,
            f"document_analyzer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def handle_error(self, error: Exception, file_name: str,
                     page_number: Optional[int] = None) -> ProcessingError:
        """Process and categorize an error"""

        # Categorize the error
        category = self.categorize_error(error)
        severity = self.determine_severity(error)

        # Create error record
        error_record = ProcessingError(
            category=category,
            severity=severity,
            message=str(error),
            file_name=file_name,
            page_number=page_number,
            details=traceback.format_exc()
        )

        # Store error
        key = f"{file_name}_{page_number if page_number else 'full'}"
        self.errors[key] = error_record

        # Log error
        self.log_error(error_record)

        return error_record

    def categorize_error(self, error: Exception) -> ErrorCategory:
        """Categorize an error based on its type"""
        error_type = type(error).__name__

        categories = {
            'FileNotFoundError': ErrorCategory.FILE_ACCESS,
            'PermissionError': ErrorCategory.PERMISSION,
            'MemoryError': ErrorCategory.MEMORY,
            'PIL.UnidentifiedImageError': ErrorCategory.IMAGE_PROCESSING,
            'PIL.Image.DecompressionBombError': ErrorCategory.IMAGE_PROCESSING,
            'fitz.FileDataError': ErrorCategory.PDF_PROCESSING,
            'sqlite3.Error': ErrorCategory.OUTPUT,
            'pd.errors.EmptyDataError': ErrorCategory.FORMAT
        }

        # Special handling for PDF encryption
        if 'encrypted' in str(error).lower():
            return ErrorCategory.ENCRYPTION

        # Special handling for memory-related errors
        if 'memory' in str(error).lower():
            return ErrorCategory.MEMORY

        return categories.get(error_type, ErrorCategory.UNKNOWN)

    def determine_severity(self, error: Exception) -> ErrorSeverity:
        """Determine the severity of an error"""
        # Critical errors that should stop processing
        if isinstance(error, (MemoryError, SystemError)):
            return ErrorSeverity.CRITICAL

        # Serious errors that might affect results
        if isinstance(error, (PermissionError, FileNotFoundError)):
            return ErrorSeverity.ERROR

        # Errors that might be recoverable
        if isinstance(error, (ValueError, TypeError)):
            return ErrorSeverity.WARNING

        # Default to ERROR for unknown cases
        return ErrorSeverity.ERROR

    def log_error(self, error: ProcessingError):
        """Log error details"""
        log_message = (
            f"File: {error.file_name}\n"
            f"Page: {error.page_number if error.page_number else 'N/A'}\n"
            f"Category: {error.category.value}\n"
            f"Severity: {error.severity.value}\n"
            f"Message: {error.message}\n"
            f"Details: {error.details}\n"
            f"Timestamp: {error.timestamp}\n"
            f"{'-' * 80}"
        )

        if error.severity == ErrorSeverity.CRITICAL:
            logging.critical(log_message)
        elif error.severity == ErrorSeverity.ERROR:
            logging.error(log_message)
        else:
            logging.warning(log_message)

    def get_error_summary(self) -> Dict[str, int]:
        """Get summary of errors by category"""
        summary = {}
        for error in self.errors.values():
            category = error.category.value
            summary[category] = summary.get(category, 0) + 1
        return summary

    def get_critical_errors(self) -> List[ProcessingError]:
        """Get list of critical errors"""
        return [
            error for error in self.errors.values()
            if error.severity == ErrorSeverity.CRITICAL
        ]

    def get_errors_by_severity(self, severity: ErrorSeverity) -> List[ProcessingError]:
        """Get list of errors by severity level"""
        return [
            error for error in self.errors.values()
            if error.severity == severity
        ]

    def get_errors_by_category(self, category: ErrorCategory) -> List[ProcessingError]:
        """Get list of errors by category"""
        return [
            error for error in self.errors.values()
            if error.category == category
        ]

    def has_critical_errors(self) -> bool:
        """Check if there are any critical errors"""
        return any(
            error.severity == ErrorSeverity.CRITICAL
            for error in self.errors.values()
        )

    def clear_errors(self):
        """Clear all stored errors"""
        self.errors.clear()

    def get_error_statistics(self) -> Dict[str, Dict[str, int]]:
        """Get detailed error statistics"""
        stats = {
            'by_severity': {},
            'by_category': {}
        }

        # Count by severity
        for severity in ErrorSeverity:
            count = len(self.get_errors_by_severity(severity))
            if count > 0:
                stats['by_severity'][severity.value] = count

        # Count by category
        for category in ErrorCategory:
            count = len(self.get_errors_by_category(category))
            if count > 0:
                stats['by_category'][category.value] = count

        return stats
