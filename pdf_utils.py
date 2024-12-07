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
