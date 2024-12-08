# Document Margin Analyzer

An application for analyzing PDF documents and images for content in header and footer areas.

## Features
- Configurable margin detection threshold (0.1-10.0%)
  * Default 1.0% threshold for standard detection
  * Fine control with 0.1% increments
  * Recommended settings:
    - 1.0%: Standard analysis (default)
    - 0.1-0.5%: High sensitivity
    - 1.1-2.0%: Moderate tolerance
    - 2.1-5.0%: Lower sensitivity
    - 5.1-10.0%: Minimal sensitivity

## Requirements
- Windows 10 or later
- Python 3.8 or higher
- Required dependencies (installed via requirements.txt)

## Installation
1. Clone or download the repository
2. Create a virtual environment:
   ```bash
   python -m venv .venv
   ```
3. Activate the virtual environment:
   ```bash
   .venv\Scripts\activate
   ```
4. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
1. Launch the application:
   ```bash
   python document_analyzer_gui.py
   ```
2. Configure analysis settings:
   - Set detection threshold (1-20%)
   - Choose output format
   - Enable optional statistical sampling
3. Select input folder containing PDFs and/or images
4. Choose save location for results
5. Click "Start Analysis"

## Output
- Analysis results in chosen format (CSV/Parquet/SQLite)
- Detailed processing report
- Error report (if any errors occurred)
- Processing logs in the logs directory

## Building
To build the executable:
1. Run build configuration:
   ```bash
   python build_config.py
   ```
2. Install requirements:
   ```bash
   pip install -r requirements.txt
   ```
3. Build executable:
   ```bash
   pyinstaller pdf_analyzer.spec
   ```

## License
© 2024 All rights reserved.