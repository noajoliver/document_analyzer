import os
import shutil
from pathlib import Path


def clean_build():
    """Clean all build artifacts and temporary files"""
    paths_to_remove = [
        'build',
        'dist',
        '__pycache__',
        'pdf_analyzer.spec',
        '*.pyc',
        '*.pyo',
        '*.pyd',
        '*.log',
        'logs/*.log',
        'logs',  # Remove logs directory
        'poppler-windows',  # Remove Poppler directory
        'poppler.zip',  # Remove Poppler zip if exists
        '.pytest_cache',  # Remove pytest cache if present
        '.coverage',  # Remove coverage data if present
        '*.spec',  # Remove all spec files
        '**/**.pyc',  # Remove all pyc files in subdirectories
        '**/**.pyo',  # Remove all pyo files in subdirectories
        '**/**.pyd',  # Remove all pyd files in subdirectories
        '**/__pycache__',  # Remove all pycache directories in subdirectories
    ]

    # Remove PyInstaller cache directories
    cache_dirs = [
        os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'pyinstaller'),
        os.path.join(os.path.expanduser('~'), 'AppData', 'Roaming', 'pyinstaller'),
    ]

    for cache_dir in cache_dirs:
        if os.path.exists(cache_dir):
            try:
                shutil.rmtree(cache_dir)
                print(f"Removed PyInstaller cache: {cache_dir}")
            except Exception as e:
                print(f"Error removing PyInstaller cache at {cache_dir}: {e}")

    # Clean all paths
    for path in paths_to_remove:
        try:
            if '*' in path:
                # Handle wildcards with recursive glob for better coverage
                for p in Path('.').rglob(path.replace('**/', '')):
                    if p.is_file():
                        p.unlink()
                    elif p.is_dir():
                        shutil.rmtree(p)
                    print(f"Removed: {p}")
            else:
                if os.path.isfile(path):
                    os.remove(path)
                    print(f"Removed file: {path}")
                elif os.path.isdir(path):
                    shutil.rmtree(path)
                    print(f"Removed directory: {path}")
        except Exception as e:
            print(f"Error removing {path}: {e}")

    # Verify clean
    verify_clean()


def verify_clean():
    """Verify that all build artifacts have been removed"""
    critical_paths = ['build', 'dist', 'pdf_analyzer.spec', 'poppler-windows']
    remaining = []

    for path in critical_paths:
        if os.path.exists(path):
            remaining.append(path)

    if remaining:
        print("\nWarning: Some paths could not be removed:")
        for path in remaining:
            print(f"- {path}")
        print("\nPlease try removing these manually or run as administrator.")
    else:
        print("\nAll critical build artifacts successfully removed.")


if __name__ == "__main__":
    print("Starting cleanup process...")
    clean_build()
    print("\nCleaning complete. To rebuild:")
    print("1. python build_config.py")
    print("2. pyinstaller pdf_analyzer.spec")
    print("\nNote: Run these commands as administrator if you encounter permission issues.")
