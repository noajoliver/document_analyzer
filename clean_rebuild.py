import os
import shutil
import stat
import platform
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
        'pdf_analyzer.spec',  # Specific spec file
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
    ]

    # Remove PyInstaller cache directories
    cache_dirs = [
        os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'pyinstaller'),
        os.path.join(os.path.expanduser('~'), 'AppData', 'Roaming', 'pyinstaller'),
    ]

    for cache_dir in cache_dirs:
        if os.path.exists(cache_dir):
            try:
                shutil.rmtree(cache_dir, onerror=handle_readonly_files)
                print(f"Removed PyInstaller cache: {cache_dir}")
            except Exception as e:
                print(f"Error removing PyInstaller cache at {cache_dir}: {e}")

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
        'pdf_analyzer.spec',
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