import os
import platform
import shutil
import sys
import zipfile
from pathlib import Path

import requests


def verify_dependencies():
    """Verify critical dependencies are available"""
    missing_deps = []

    print("Verifying dependencies...")

    # Dictionary of dependencies and their import names
    dependencies = {
        'PyMuPDF': 'fitz',
        'pdf2image': 'pdf2image',
        'Pillow': 'PIL',
        'pandas': 'pandas',
        'numpy': 'numpy',
        'pyinstaller': 'PyInstaller',
        'requests': 'requests',
        'pyarrow': 'pyarrow',
    }

    for package, import_name in dependencies.items():
        try:
            __import__(import_name)
            print(f"✓ {package} verified")
        except ImportError:
            missing_deps.append(package)
            print(f"✗ {package} not found")

    # Additional checks for SQLite (built into Python)
    try:
        import sqlite3
        print("✓ SQLite3 support verified")
    except ImportError:
        missing_deps.append('sqlite3')
        print("✗ SQLite3 support not found")

    if missing_deps:
        print("\nMissing dependencies:")
        for dep in missing_deps:
            print(f"  - {dep}")
        return False

    print("\nAll dependencies verified successfully!")
    return True

def download_poppler():
    """Download and set up Poppler based on the operating system."""

    if platform.system() == "Windows":
        # Download Poppler for Windows
        poppler_url = \
            "https://github.com/oschwartz10612/poppler-windows/releases/download/v23.08.0-0/Release-23.08.0-0.zip"
        poppler_dir = "poppler-windows"

        if not os.path.exists(poppler_dir):
            print("Downloading Poppler for Windows...")
            try:
                response = requests.get(poppler_url, timeout=30)
                response.raise_for_status()
                zip_path = "poppler.zip"

                with open(zip_path, 'wb') as f:
                    f.write(response.content)

                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(poppler_dir)

                os.remove(zip_path)

                # Verify the binary path exists
                bin_path = os.path.join(poppler_dir, 'poppler-23.08.0', 'Library', 'bin')
                if os.path.exists(bin_path):
                    print(f"Poppler downloaded and extracted successfully to {bin_path}!")
                    return os.path.abspath(bin_path)
                else:
                    print(f"Error: Expected binary path {bin_path} not found after extraction")
                    return None

            except requests.RequestException as e:
                print(f"Error downloading Poppler: {str(e)}")
                print("Please download Poppler manually from:")
                print(poppler_url)
                print("And extract it to a 'poppler-windows' directory.")
                return None
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                return None
        else:
            # Check if binaries exist in the expected location
            bin_path = os.path.join(poppler_dir, 'poppler-23.08.0', 'Library', 'bin')
            if os.path.exists(bin_path):
                print(f"Using existing Poppler installation at {bin_path}")
                return os.path.abspath(bin_path)
            else:
                print(f"Error: Poppler binaries not found at {bin_path}")
                print("Please delete the poppler-windows directory and run this script again")
                return None

    elif platform.system() == "Linux":
        print("\nOn Linux, please ensure poppler-utils is installed via your package manager:")
        print("For Ubuntu/Debian:")
        print("    sudo apt-get update")
        print("    sudo apt-get install poppler-utils")
        print("\nFor CentOS/RHEL:")
        print("    sudo yum install poppler-utils")
        return None

    elif platform.system() == "Darwin":
        print("\nOn macOS, please ensure poppler is installed via homebrew:")
        print("    brew update")
        print("    brew install poppler")
        return None

    else:
        print(f"Unsupported operating system: {platform.system()}")
        return None


def create_spec_file(poppler_path):
    """Create a PyInstaller spec file with the correct configuration."""

    spec_content = f"""# -*- mode: python ; coding: utf-8 -*-

import os
import json
from pathlib import Path
from datetime import datetime

block_cipher = None

# Poppler binary path configuration
POPPLER_PATH = r'{poppler_path}'  # Path to Poppler binaries
POPPLER_DATA = []  # Will store Poppler binary files

# Collect Poppler binaries if on Windows
if {platform.system() == "Windows"} and os.path.exists(POPPLER_PATH):
    for pattern in ['*.dll', '*.exe']:
        for file_path in Path(POPPLER_PATH).glob(pattern):
            POPPLER_DATA.append((str(file_path), 'poppler'))

# Data files configuration
data_files = [
    ('README.md', '.'),
    ('icon.ico', '.'),
    ('requirements.txt', '.'),
]

# Add additional module files
module_files = [
    'content_analyzer.py',
    'error_handling.py',
    'output_handlers.py',
    'sampling.py',
    'pdf_utils.py'
]

for module in module_files:
    if os.path.exists(module):
        data_files.append((module, '.'))

a = Analysis(
    ['pdf_analyzer_gui.py'],
    pathex=[],
    binaries=POPPLER_DATA,
    datas=data_files,
    hiddenimports=[
        'PIL._tkinter_finder',
        'pandas',
        'numpy',
        'fitz',
        'pdf2image',
        'pdf2image.pdf2image',
        'concurrent.futures',
        'threading',
        'queue',
        'tkinter',
        'tkinter.ttk',
        'tkinter.messagebox',
        'tkinter.scrolledtext',
        'tkinter.filedialog',
        'pyarrow',
        'pyarrow.parquet',
        'pyarrow.lib',
        'winsound',
        'PIL.ImageDraw',
        'PIL.ImageFilter',
    ],
    hookspath=[],
    hooksconfig={{}},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='DocumentMarginAnalyzer',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='icon.ico' if os.path.exists('icon.ico') else None,
)

# Ensure dist directory exists
if not os.path.exists('dist'):
    os.makedirs('dist')

# Create version info file
version_info = {{
    'version': '2.0.0',
    'build_date': datetime.now().strftime('%Y-%m-%d'),
    'description': 'Document Margin Analyzer',
    'copyright': '© 2024'
}}

# Write version info
with open('dist/version.json', 'w', encoding='utf-8') as f:
    json.dump(version_info, f, indent=2)

# Create README in dist folder
readme_content = '''Document Margin Analyzer

This application analyzes PDF documents and images for content in header and footer areas.

Features:
- Configurable margin detection threshold (0.1-10.0%)
- Multiple output formats (CSV, Parquet, SQLite)
- Statistical sampling for large document sets
- Comprehensive error handling and reporting
- Multi-threaded processing
- Progress tracking and detailed logging

Requirements:
- Windows 10 or later (for Windows version)
- No additional software installation needed (Poppler is included)

Usage:
1. Launch DocumentMarginAnalyzer.exe
2. Configure analysis settings:
   - Detection threshold (0.1-10.0%)
   - Output format
   - Optional statistical sampling
3. Select input folder containing PDFs and/or images
4. Choose save location for results
5. Click "Start Analysis"

Output:
- Analysis results in chosen format (CSV/Parquet/SQLite)
- Detailed processing report
- Error report (if any errors occurred)
- Processing logs in the logs directory

For support, please report issues on the project repository.
'''

with open('dist/README.txt', 'w', encoding='utf-8') as f:
    f.write(readme_content)

# Create logs directory in dist
logs_dir = os.path.join('dist', 'logs')
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
"""

    with open('pdf_analyzer.spec', 'w', encoding='utf-8') as f:
        f.write(spec_content)
    print("Created PyInstaller spec file with Poppler configuration")


def create_requirements():
    """Create requirements.txt file with specific versions."""
    requirements = """
PyMuPDF==1.23.8
pdf2image==1.16.3
Pillow==10.1.0
pandas==2.1.4
numpy==1.26.2
pyinstaller==6.3.0
requests==2.32.0
pyarrow==14.0.1
"""
    with open('requirements.txt', 'w', encoding='utf-8') as f:
        f.write(requirements.strip())
    print("Created requirements.txt file")


def cleanup_old_files():
    """Clean up old build files and directories."""
    print("Cleaning up old build files...")

    cleanup_paths = [
        'pdf_analyzer.spec',
        'build',
        'dist',
        '__pycache__',
        '*.pyc',
        '*.pyo',
        '*.pyd',
        'logs/*.log',
    ]

    for path in cleanup_paths:
        try:
            if '*' in path:
                # Handle wildcards
                for file in Path('.').glob(path):
                    if file.is_file():
                        file.unlink()
                        print(f"Removed file: {file}")
            elif os.path.isfile(path):
                os.remove(path)
                print(f"Removed file: {path}")
            elif os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
                print(f"Removed directory: {path}")
        except Exception as e:
            print(f"Error cleaning up {path}: {str(e)}")


def check_python_version():
    """Check if the Python version is compatible."""
    if sys.version_info < (3, 8):
        print("Error: Python 3.8 or higher is required")
        return False
    return True


def verify_module_files():
    """Verify that all required module files are present."""
    required_files = [
        'pdf_analyzer_gui.py',
        'content_analyzer.py',
        'error_handling.py',
        'output_handlers.py',
        'sampling.py',
        'pdf_utils.py'
    ]

    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)

    if missing_files:
        print("\nError: Missing required module files:")
        for file in missing_files:
            print(f"  - {file}")
        return False

    return True


def create_directory_structure():
    """Create necessary directory structure."""
    directories = ['logs']

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")


def main():
    """Main entry point for the build configuration script."""
    print("Document Analyzer Build Configuration")
    print("=" * 35)

    # Check Python version
    if not check_python_version():
        return

    # Add dependency verification
    if not verify_dependencies():
        print("\nPlease install missing dependencies using:")
        print("pip install -r requirements.txt")
        return

    # Verify all module files are present
    if not verify_module_files():
        return

    # Clean up old files
    cleanup_old_files()

    # Create directory structure
    create_directory_structure()

    # Create requirements.txt
    create_requirements()

    # Download and setup Poppler
    poppler_path = download_poppler()

    # Create spec file
    create_spec_file(poppler_path)

    # Print next steps
    print("\nSetup complete! To build the executable, follow these steps:")
    print("\n1. First install requirements:")
    print("   pip install -r requirements.txt")
    print("\n2. Then run PyInstaller:")
    print("   pyinstaller pdf_analyzer.spec")

    if platform.system() == "Windows":
        print("\nNote: The executable will be created in the 'dist' folder as 'DocumentMarginAnalyzer.exe'")
    else:
        print(f"\nNote: On {platform.system()}, make sure poppler is installed via your package manager.")

    print("\nAdditional Notes:")
    print("- The application will run in windowed mode")
    print("- All dependencies are packaged with the executable")
    print("- A README.txt file will be created in the dist folder")
    print("- Version information will be included in version.json")
    print("- Check the logs directory for detailed processing logs")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBuild configuration cancelled by user")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {str(e)}")
        print("Please report this error if it persists")
    finally:
        print("\nBuild configuration process complete")