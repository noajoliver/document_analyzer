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

import os
import shutil
import stat
import platform
import time
from pathlib import Path
from typing import List


def handle_readonly_files(func, path, exc_info):
    """
    Error handler for shutil.rmtree to handle read-only files
    """
    if not os.access(path, os.W_OK):
        os.chmod(path, stat.S_IWUSR)
        func(path)
    else:
        raise


def find_partial_builds() -> List[Path]:
    """Find any partial or incomplete build directories"""
    partial_builds = []
    patterns = ['temp_build_*', 'backup_build_*', 'build.*', 'dist.*']

    for pattern in patterns:
        partial_builds.extend(Path('.').glob(pattern))

    return partial_builds


def clean_build():
    """Clean all build artifacts and temporary files"""
    paths_to_remove = [
        'build',
        'dist',
        '__pycache__',
        'document_analyzer.spec',  # Specific spec file
        '*.pyc',
        '*.pyo',
        '*.pyd',
        '*.log',
        'logs/*.log',
        'logs',
        'poppler.zip',
        '.pytest_cache',
        '.coverage',
        '**/**.pyc',
        '**/**.pyo',
        '**/**.pyd',
        '**/__pycache__',
        'temp_build_*',
        'backup_build_*',
        'build.*',
        'dist.*',
        '.venv',
        # Add database and output file patterns
        '*.parquet',
        '*.db',
        '*.db-journal',
        '*.db-wal',
        '*.db-shm',
        '*_metadata.json'
    ]

    # Remove PyInstaller cache directories
    cache_dirs = [
        os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'pyinstaller'),
        os.path.join(os.path.expanduser('~'), 'AppData', 'Roaming', 'pyinstaller'),
    ]

    def handle_readonly_files(func, path, exc_info):
        """Error handler for handling read-only files and locked databases"""
        try:
            if platform.system() == "Windows":
                if not os.access(path, os.W_OK):
                    # Clear readonly flag
                    os.chmod(path, stat.S_IWUSR)
                    func(path)
                # Handle locked database files
                elif path.endswith(('.db', '.db-journal', '.db-wal', '.db-shm')):
                    try:
                        import sqlite3
                        conn = sqlite3.connect(path)
                        conn.close()
                        time.sleep(0.1)  # Give system time to release file
                        func(path)
                    except:
                        pass
            else:
                raise
        except Exception as e:
            print(f"Warning: Could not remove {path}: {str(e)}")

    # Find and remove partial builds first
    partial_builds = find_partial_builds()
    for path in partial_builds:
        try:
            if path.is_file():
                path.unlink()
            else:
                shutil.rmtree(path, onerror=handle_readonly_files)
            print(f"Removed partial build: {path}")
        except Exception as e:
            print(f"Error removing partial build {path}: {e}")

    # Remove PyInstaller cache
    for cache_dir in cache_dirs:
        if os.path.exists(cache_dir):
            try:
                shutil.rmtree(cache_dir, onerror=handle_readonly_files)
                print(f"Removed PyInstaller cache: {cache_dir}")
            except Exception as e:
                print(f"Error removing PyInstaller cache at {cache_dir}: {e}")

    # Clean all paths
    for path in paths_to_remove:
        try:
            if '*' in path:
                # Handle wildcards with recursive glob
                for p in Path('.').rglob(path.replace('**/', '')):
                    try:
                        if p.is_file():
                            p.unlink()
                        elif p.is_dir():
                            shutil.rmtree(p, onerror=handle_readonly_files)
                        print(f"Removed: {p}")
                    except Exception as e:
                        print(f"Error removing {p}: {e}")
            else:
                if os.path.isfile(path):
                    os.remove(path)
                    print(f"Removed file: {path}")
                elif os.path.isdir(path):
                    shutil.rmtree(path, onerror=handle_readonly_files)
                    print(f"Removed directory: {path}")
        except Exception as e:
            print(f"Error removing {path}: {e}")

    # Verify clean
    verify_clean()


def verify_clean():
    """Verify that all build artifacts have been removed"""
    critical_paths = [
        'build',
        'dist',
        'document_analyzer.spec',
        'poppler-windows',
        '.venv',
        'temp_build_*',
        'backup_build_*'
    ]
    remaining = []

    for path in critical_paths:
        if '*' in path:
            # Check for wildcard paths
            if list(Path('.').glob(path)):
                remaining.append(path)
        elif os.path.exists(path):
            remaining.append(path)

    if remaining:
        print("\nWarning: Some paths could not be removed:")
        for path in remaining:
            print(f"- {path}")

        if platform.system() == "Windows":
            print("\nTo remove these files in Windows:")
            print("1. Open Command Prompt as administrator")
            print("2. Run: rd /s /q <path>")
            print("   or: del /f /q <path> (for files)")
    else:
        print("\nAll critical build artifacts successfully removed.")


if __name__ == "__main__":
    print("Starting cleanup process...")
    clean_build()
    print("\nCleaning complete. To rebuild:")
    print("1. python create_executable.py")
    print("\nNote: Run as administrator if you encounter permission issues.")