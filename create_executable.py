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
import sys
import shutil
import subprocess
import platform
import time
import ctypes
import traceback
import venv
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime


def cleanup_old_files():
    """Clean up old build files and directories"""
    print("Cleaning up old build files...")

    cleanup_paths = [
        'document_analyzer.spec',
        'build',
        'dist',
        '__pycache__',
        '*.pyc',
        '*.pyo',
        '*.pyd',
        'logs/*.log',
        '*.parquet',  # Add cleanup for parquet files
        '*.db',       # Add cleanup for SQLite databases
        '*.db-journal',  # Add cleanup for SQLite journal files
        '*.db-wal',     # Add cleanup for SQLite WAL files
        '*.db-shm',     # Add cleanup for SQLite shared memory files
        '*_metadata.json'  # Add cleanup for metadata files
    ]

    for path in cleanup_paths:
        try:
            if '*' in path:
                # Handle wildcards
                for file in Path('.').glob(path):
                    try:
                        if file.is_file():
                            file.unlink()
                            print(f"Removed file: {file}")
                        elif file.is_dir():
                            shutil.rmtree(file)
                            print(f"Removed directory: {file}")
                    except Exception as e:
                        print(f"Error cleaning up {file}: {str(e)}")
            else:
                if os.path.isfile(path):
                    os.remove(path)
                    print(f"Removed file: {path}")
                elif os.path.isdir(path):
                    shutil.rmtree(path)
                    print(f"Removed directory: {path}")
        except Exception as e:
            print(f"Error cleaning up {path}: {str(e)}")

    print("Cleanup completed")

def is_admin() -> bool:
    """Check if script is running with administrator privileges"""
    try:
        if platform.system() == "Windows":
            return ctypes.windll.shell32.IsUserAnAdmin()
        else:
            return os.geteuid() == 0
    except Exception:
        return False


def get_python_path() -> str:
    """Get the correct Python interpreter path"""
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        # We're in a virtual environment
        if platform.system() == "Windows":
            return os.path.join(sys.prefix, 'Scripts', 'python.exe')
        return os.path.join(sys.prefix, 'bin', 'python')
    return sys.executable


def run_command(command: str, cwd: Optional[str] = None) -> Tuple[bool, str]:
    """
    Run a command and return success status and output.

    Args:
        command: Command to run
        cwd: Working directory for command execution

    Returns:
        Tuple[bool, str]: Success status and command output/error message
    """
    try:
        # Replace 'python' with actual Python path
        if command.startswith('python '):
            command = f'"{get_python_path()}" {command[7:]}'

        result = subprocess.run(
            command,
            shell=True,
            check=True,
            cwd=cwd,
            capture_output=True,
            text=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr


def create_temp_build_dir() -> str:
    """Create a temporary build directory with timestamp"""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    build_dir = os.path.abspath(f"temp_build_{timestamp}")
    os.makedirs(build_dir, exist_ok=True)
    print(f"Created temporary build directory: {build_dir}")
    return build_dir

def backup_successful_build(dist_dir: str) -> Optional[str]:
    """
    Create a backup of a successful build

    Args:
        dist_dir: Path to distribution directory

    Returns:
        Optional[str]: Path to backup directory if successful
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = os.path.abspath(f"backup_build_{timestamp}")
        shutil.copytree(dist_dir, backup_dir)
        print(f"Created backup in: {backup_dir}")
        return backup_dir
    except Exception as e:
        print(f"Warning: Failed to create backup: {e}")
        return None


def copy_required_files(source_dir: str, temp_dir: str):
    """
    Copy all required files to temporary build directory

    Args:
        source_dir: Source directory containing original files
        temp_dir: Temporary build directory to copy files to
    """
    required_files = [
        'document_analyzer_gui.py',
        'content_analyzer.py',
        'error_handling.py',
        'output_handlers.py',
        'sampling.py',
        'pdf_utils.py',
        'build_config.py',
        'README.md',
        'requirements.txt',
        'clean_rebuild.py'
    ]

    print("Copying required files...")

    # Ensure target directory exists
    os.makedirs(temp_dir, exist_ok=True)

    # Copy each required file
    for file in required_files:
        try:
            source_path = os.path.abspath(os.path.join(source_dir, file))
            dest_path = os.path.abspath(os.path.join(temp_dir, file))

            if os.path.exists(source_path):
                shutil.copy2(source_path, dest_path)
                print(f"Copied: {file}")

                # Verify the copy was successful
                if not os.path.exists(dest_path):
                    print(f"Warning: Failed to verify copy of {file}")
                elif os.path.getsize(source_path) != os.path.getsize(dest_path):
                    print(f"Warning: Size mismatch for {file}")
            else:
                print(f"Warning: Required file not found at {source_path}")
        except Exception as e:
            print(f"Error copying {file}: {str(e)}")

    # Copy icon if exists
    icon_path = os.path.join(source_dir, 'icon.ico')
    if os.path.exists(icon_path):
        try:
            icon_dest = os.path.join(temp_dir, 'icon.ico')
            shutil.copy2(icon_path, icon_dest)
            print("Copied: icon.ico")
        except Exception as e:
            print(f"Error copying icon.ico: {str(e)}")

    # Copy poppler directory with verification
    poppler_source = os.path.join(source_dir, 'poppler-windows')
    if os.path.exists(poppler_source):
        try:
            poppler_dest = os.path.join(temp_dir, 'poppler-windows')
            print(f"Copying Poppler from {poppler_source} to {poppler_dest}")

            # Remove destination if it exists
            if os.path.exists(poppler_dest):
                shutil.rmtree(poppler_dest)

            shutil.copytree(poppler_source, poppler_dest)
            print("Copied: poppler-windows directory")

            # Verify critical Poppler files
            bin_path = os.path.join(poppler_dest, 'poppler-23.08.0', 'Library', 'bin')
            if os.path.exists(bin_path):
                print("Verified Poppler binaries location")

                # Check critical files
                critical_files = ['pdfinfo.exe', 'pdftoppm.exe', 'pdftocairo.exe']
                missing_files = [f for f in critical_files
                                 if not os.path.exists(os.path.join(bin_path, f))]
                if missing_files:
                    print("Warning: Missing critical Poppler files:")
                    for f in missing_files:
                        print(f"  - {f}")
            else:
                print(f"Warning: Poppler binaries not found at {bin_path}")
        except Exception as e:
            print(f"Error copying Poppler directory: {str(e)}")
    else:
        print("WARNING: Poppler directory not found in source directory!")


def setup_virtual_env(temp_dir: str) -> bool:
    """
    Set up a virtual environment for the build

    Args:
        temp_dir: Temporary build directory path

    Returns:
        bool: True if setup was successful
    """
    try:
        venv_dir = os.path.join(temp_dir, '.venv')
        print("Creating virtual environment...")
        venv.create(venv_dir, with_pip=True)

        # Get python path in new venv
        if platform.system() == "Windows":
            python_path = os.path.join(venv_dir, 'Scripts', 'python.exe')
            pip_path = os.path.join(venv_dir, 'Scripts', 'pip.exe')
        else:
            python_path = os.path.join(venv_dir, 'bin', 'python')
            pip_path = os.path.join(venv_dir, 'bin', 'pip')

        if not os.path.exists(python_path):
            print(f"Error: Python executable not found at {python_path}")
            return False

        print(f"Using Python from: {python_path}")

        # Upgrade pip first using python -m pip to avoid path issues
        print("Upgrading pip...")
        upgrade_cmd = [python_path, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"]
        result = subprocess.run(
            upgrade_cmd,
            check=True,
            cwd=temp_dir,
            capture_output=True,
            text=True
        )

        # Install requirements using python -m pip
        print("Installing requirements...")
        requirements_path = os.path.join(temp_dir, 'requirements.txt')
        print(f"Requirements file path: {requirements_path}")

        if not os.path.exists(requirements_path):
            print(f"Error: Requirements file not found at {requirements_path}")
            return False

        # First install core dependencies
        core_deps = ["wheel", "setuptools", "numpy"]
        for dep in core_deps:
            print(f"Installing {dep}...")
            install_cmd = [python_path, "-m", "pip", "install", dep]
            subprocess.run(install_cmd, check=True, cwd=temp_dir, capture_output=True)

        # Then install from requirements file
        install_cmd = [python_path, "-m", "pip", "install", "-r", requirements_path]
        try:
            result = subprocess.run(
                install_cmd,
                check=True,
                cwd=temp_dir,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Error installing requirements: {e.stderr}")
            return False

        # Verify critical packages
        verify_packages = [
            'pandas',
            'pyarrow',
            'fitz',  # PyMuPDF
            'PIL',  # Pillow
            'numpy'
        ]

        for package in verify_packages:
            try:
                cmd = [python_path, "-c", f"import {package}"]
                subprocess.run(cmd, check=True, cwd=temp_dir, capture_output=True)
                print(f"✓ {package} verified")
            except subprocess.CalledProcessError:
                print(f"✗ Failed to verify {package}")
                return False

        print("Virtual environment setup completed successfully")
        return True

    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e.cmd}")
        print(f"Error output: {e.stderr}")
        return False
    except Exception as e:
        print(f"Error setting up virtual environment: {e}")
        return False


def setup_poppler(temp_dir: str) -> Optional[str]:
    """
    Ensure Poppler is properly set up in the build directory

    Args:
        temp_dir: Temporary build directory path

    Returns:
        Optional[str]: Path to Poppler binaries if successful, None otherwise
    """
    try:
        print("\nVerifying Poppler setup...")

        # First check if Poppler already exists in temp directory
        bin_path = os.path.join(temp_dir, 'poppler-windows', 'poppler-23.08.0', 'Library', 'bin')
        if os.path.exists(bin_path):
            print(f"Found existing Poppler installation at: {bin_path}")
            return os.path.abspath(bin_path)

        print("Downloading and setting up Poppler...")
        from build_config import download_poppler
        poppler_path = download_poppler()

        if not poppler_path:
            print("Error: Failed to obtain Poppler path")
            print("\nTo manually install Poppler:")
            print(
                "1. Download from: https://github.com/oschwartz10612/poppler-windows/releases/download/v23.08.0-0/Release-23.08.0-0.zip")
            print("2. Create 'poppler-windows' directory in the project root")
            print("3. Extract the downloaded zip into that directory")
            print(f"4. Verify that this path exists: {bin_path}")
            return None

        # Verify the installation
        if not os.path.exists(poppler_path):
            print(f"Error: Poppler path does not exist: {poppler_path}")
            return None

        # Check for critical files
        critical_files = ['pdfinfo.exe', 'pdftoppm.exe', 'pdftocairo.exe']
        missing_files = [f for f in critical_files if not os.path.exists(os.path.join(poppler_path, f))]

        if missing_files:
            print("Error: Missing critical Poppler files:")
            for file in missing_files:
                print(f"  - {file}")
            return None

        print(f"Poppler setup verified successfully at: {poppler_path}")
        return poppler_path

    except ImportError as e:
        print(f"Error importing build_config: {str(e)}")
        print("Stack trace:", traceback.format_exc())
        return None
    except Exception as e:
        print(f"Error setting up Poppler: {str(e)}")
        print("Stack trace:", traceback.format_exc())
        return None


def verify_build(source_dir: str, dist_dir: str) -> bool:
    """
    Verify the build was successful

    Args:
        source_dir: Original source directory
        dist_dir: Distribution directory path

    Returns:
        bool: True if build verification passed
    """
    exe_name = "DocumentMarginAnalyzer.exe" if platform.system() == "Windows" else "DocumentMarginAnalyzer"
    exe_path = os.path.join(dist_dir, exe_name)

    if not os.path.exists(exe_path):
        print(f"Error: Built executable not found at {exe_path}")
        return False

    required_files = ['version.json', 'README.txt']
    missing_files = []

    for file in required_files:
        if not os.path.exists(os.path.join(dist_dir, file)):
            missing_files.append(file)

    if missing_files:
        print("Error: Missing required files in distribution:")
        for file in missing_files:
            print(f"  - {file}")
        return False

    # Verify logs directory exists
    logs_dir = os.path.join(dist_dir, 'logs')
    if not os.path.exists(logs_dir):
        print("Error: Logs directory not found in distribution")
        return False

    # Check for poppler directory on Windows
    if platform.system() == "Windows":
        poppler_dir = os.path.join(dist_dir, 'poppler')
        print(f"Checking for Poppler in distribution at: {poppler_dir}")

        if not os.path.exists(poppler_dir):
            print("Error: Poppler directory not found in distribution")
            print("Checking for binaries in source location...")

            # Check original Poppler location
            src_poppler = os.path.join(source_dir, 'poppler-windows',
                                       'poppler-23.08.0', 'Library', 'bin')
            if os.path.exists(src_poppler):
                print("Poppler found in source location, copying to distribution...")
                os.makedirs(poppler_dir, exist_ok=True)
                shutil.copytree(src_poppler, os.path.join(poppler_dir, 'bin'))
            else:
                print("Error: Poppler not found in source location either")
                return False

        # Verify critical Poppler files
        bin_dir = os.path.join(poppler_dir, 'bin')
        critical_files = ['pdfinfo.exe', 'pdftoppm.exe', 'pdftocairo.exe']
        for file in critical_files:
            file_path = os.path.join(bin_dir, file)
            if not os.path.exists(file_path):
                print(f"Error: Critical Poppler file missing: {file}")
                return False

        print("Poppler verification completed successfully")

    return True


def main():
    """Main build process"""
    if platform.system() == "Windows" and not is_admin():
        print("Warning: Running without administrator privileges. This may cause issues.")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return 1

    print("Starting complete build process...")

    # Store original working directory
    original_dir = os.path.abspath(os.getcwd())
    temp_dir = None
    backup_dir = None

    try:
        # Run clean_rebuild first
        print("\nStep 1: Cleaning previous build artifacts...")
        success, output = run_command("python clean_rebuild.py")
        if not success:
            print(f"Error during cleanup: {output}")
            return 1

        # Add the new cleanup call here
        cleanup_old_files()

        # Create and move to temporary build directory
        print("\nStep 2: Setting up temporary build environment...")
        temp_dir = create_temp_build_dir()
        copy_required_files(original_dir, temp_dir)
        os.chdir(temp_dir)

        # Set up virtual environment
        print("\nStep 3: Setting up virtual environment...")
        if not setup_virtual_env(temp_dir):
            print("Failed to set up virtual environment")
            return 1

        # Setup Poppler first
        print("\nStep 4: Setting up Poppler...")
        poppler_path = None
        if platform.system() == "Windows":
            poppler_path = setup_poppler(temp_dir)
            if not poppler_path:
                print("Failed to set up Poppler")
                return 1

        print("\nStep 5: Running build configuration...")
        try:
            from build_config import create_spec_file

            # Create spec file with Poppler path
            print("Creating PyInstaller spec file...")
            create_spec_file(poppler_path)

            # Verify spec file creation
            if not os.path.exists('document_analyzer.spec'):
                print("Error: Spec file creation failed")
                return 1

            # Verify spec file content
            with open('document_analyzer.spec', 'r') as f:
                spec_content = f.read()
                if 'document_analyzer_gui.py' not in spec_content:
                    print("Error: Spec file appears to be invalid")
                    return 1

            print("Successfully created and verified spec file")

        except ImportError as e:
            print(f"Error importing build_config: {str(e)}")
            return 1
        except Exception as e:
            print(f"Error in build configuration: {str(e)}")
            print("Stack trace:", traceback.format_exc())
            return 1

        print("\nStep 6: Running PyInstaller...")
        success, output = run_command("pyinstaller document_analyzer.spec")
        if not success:
            print(f"Error during PyInstaller execution: {output}")
            print("PyInstaller Output:")
            print(output)
            return 1

        # Verify build
        print("\nStep 7: Verifying build...")
        if not verify_build(original_dir, os.path.join(temp_dir, "dist")):
            print("Build verification failed.")
            return 1

        # Move distribution to original directory
        print("\nStep 8: Moving build artifacts...")
        final_dist = os.path.join(original_dir, "dist")
        if os.path.exists(final_dist):
            shutil.rmtree(final_dist)
        shutil.move(os.path.join(temp_dir, "dist"), final_dist)
        print(f"Successfully moved distribution to: {final_dist}")

        # Create backup of successful build
        backup_dir = backup_successful_build(final_dist)

        print("\nBuild completed successfully!")
        print(f"Executable can be found in: {final_dist}")
        if backup_dir:
            print(f"Backup created in: {backup_dir}")

        return 0

    except Exception as e:
        print(f"Build failed with error: {str(e)}")
        print("Stack trace:", traceback.format_exc())
        return 1

    finally:
        # Return to original directory
        os.chdir(original_dir)

        # Clean up temporary directory
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                print(f"\nCleaned up temporary build directory: {temp_dir}")
            except Exception as e:
                print(f"Warning: Failed to clean up temporary directory {temp_dir}: {str(e)}")

if __name__ == "__main__":
    sys.exit(main())