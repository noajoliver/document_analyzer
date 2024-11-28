"""Utilities for PDF processing"""
import os
import sys
import platform


def setup_poppler() -> str:
    """
    Configure Poppler for PDF processing

    Returns:
        str: Path to Poppler binaries if setup successful, empty string otherwise
    """
    if platform.system() == "Windows":
        if getattr(sys, 'frozen', False):
            # Running as compiled executable
            base_path = sys._MEIPASS
            poppler_path = os.path.join(base_path, 'poppler')
        else:
            # Running as script
            poppler_base = os.path.abspath('poppler-windows')
            poppler_path = os.path.join(poppler_base, 'poppler-23.08.0', 'Library', 'bin')

        if os.path.exists(poppler_path):
            os.environ['PATH'] = poppler_path + os.pathsep + os.environ['PATH']
            os.environ['POPPLER_PATH'] = poppler_path

            # Configure pdf2image to use this path
            from pdf2image import pdf2image
            pdf2image.POPPLER_PATH = poppler_path

            return poppler_path
        else:
            print(f"Warning: Poppler path not found at {poppler_path}")
            return ""

    return ""  # Return empty string for non-Windows systems
