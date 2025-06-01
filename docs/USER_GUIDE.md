# Document Margin Analyzer
## User Guide

## Table of Contents
1. [Getting Started](#getting-started)
2. [Interface Overview](#interface-overview)
3. [Configuration Options](#configuration-options)
4. [Analysis Process](#analysis-process)
5. [Output Formats](#output-formats)
6. [Troubleshooting](#troubleshooting)
7. [Advanced Features](#advanced-features)

## Getting Started

### System Requirements
- Windows 10 or later
- Minimum 4GB RAM (8GB recommended)
- Screen resolution: 1024x768 or higher
- Sufficient disk space for output files (varies by dataset size)

### What This Tool Does
Document Margin Analyzer examines PDF documents and images to detect content in header and footer margin areas. This is useful for:
- Document compliance checking
- Quality control workflows
- Automated document processing
- Identifying improperly formatted documents

## Interface Overview

### Main Application Window
![Main Application Interface](images/main-window.png)

### Analysis Settings Section
![Analysis Settings](images/analysis-settings.png)

#### Input Selection
![Input Folder Selection](images/input-selection.png)

#### Save Location
![Save Location Selection](images/save-location.png)

#### File Types
![File Type Selection](images/file-types.png)

- **PDF Files**: Analyzes both text and image content within PDFs
- **Image Files**: Supports JPG, JPEG, PNG, BMP, and TIFF formats

## Configuration Options

### Analysis Configuration
![Analysis Configuration](images/analysis-config.png)

#### Margin Configuration
- **Top Margin (%)**: Percentage of page height to analyze from the top (0-50%)
- **Bottom Margin (%)**: Percentage of page height to analyze from the bottom (0-50%)
- Default: 5% for both margins
- Example: 5% means the top 5% and bottom 5% of each page will be analyzed

#### Detection Threshold
![Threshold Configuration](images/threshold-config.png)

- **Range**: 0.1% to 10.0%
- **Default**: 0.5% (recommended for standard analysis)
- **Sensitivity Levels**:
  - 0.1-0.4%: Extremely sensitive, flags minimal content (even tiny dots or marks)
  - 0.5%: Standard detection level (recommended)
  - 0.6-2.0%: Moderate tolerance, ignores very small marks
  - 2.1-5.0%: Ignores minor marks and artifacts
  - 5.1-10.0%: Only flags substantial content
- **Note**: The threshold represents the percentage of the margin area that must contain content to be flagged

#### CPU Configuration
![CPU Core Selection](images/cpu-config.png)

- **Available Cores**: Shows your system's total CPU cores
- **Default**: Uses all available cores minus one
- **Range**: 1 to maximum available cores
- **Performance Tips**:
  - More cores = faster processing
  - Leave 1-2 cores free for system responsiveness
  - Recommended: 75% of available cores for optimal performance

### Sampling Configuration
![Sampling Options](images/sampling-options.png)

#### Statistical Sampling
- **Purpose**: Analyze a representative sample instead of all files
- **Confidence Level**: 90%, 95%, or 99% (default: 95%)
- **Margin of Error**: 1%, 3%, 5%, or 10% (default: 5%)
- **Use Case**: Large document sets where complete analysis is impractical

#### Random N Sampling
- **Purpose**: Analyze a fixed number of randomly selected files
- **Options**: 10, 100, 500, 1000, 5000, or 10000 files
- **Use Case**: Quick quality checks or testing

### Output Configuration
![Output Settings](images/output-config.png)

#### Output Format
- **CSV**: Comma-separated values, compatible with Excel
  - Max rows per file: 10,000 to 100,000 (configurable)
  - Files are split automatically when limit is reached
- **Parquet**: Efficient columnar format for large datasets
  - Best for data analysis workflows
  - Smaller file sizes than CSV
- **SQLite**: Database format for querying results
  - Ideal for complex analysis
  - Supports SQL queries

#### Minimal Output Mode
- **Option**: "Minimal Output (File, Page, Content Status only)"
- **Purpose**: Reduces output to essential information only
- **Includes**: File path, page number, and content detection status
- **Excludes**: Detailed percentages, analysis details, and error information

### Progress Monitoring
![Progress Section](images/progress-section.png)

### Log View
![Log Section](images/log-section.png)

## Analysis Process

1. Configure Input/Output
   - Select input folder containing documents
   - Choose save location
   - Select file types to process (PDF/Images)

2. Configure Analysis Settings
   - Set detection threshold (0.1-10.0%)
   - Configure CPU cores
   - Enable sampling if needed
   - Select output format

3. Start Analysis
   - Click "Start Analysis"
   - Monitor progress
   - View log for details

4. Review Results
   - Check output files
   - Review processing report
   - Address any errors

## Output Formats

### Output File Naming
- Base format: `document_analysis_YYYYMMDD_HHMMSS`
- CSV with splitting: `document_analysis_YYYYMMDD_HHMMSS_part1.csv`, etc.
- Extensions: `.csv`, `.parquet`, `.db` (SQLite)

### Output Columns

#### Standard Output Columns
1. **File**: Absolute path to the analyzed document
2. **Page**: Page number (1-based, always 1 for images)
3. **Type**: File type (PDF or Image)
4. **Content Status**: Overall detection result
   - "All content within margins" - No content detected in margins
   - "Content found in header" - Content detected in top margin
   - "Content found in footer" - Content detected in bottom margin
   - "Content found in header and footer" - Content in both margins
   - "Processing Failed" - Error during analysis

#### Detailed Analysis Columns (Full Output Mode)
5. **Text Status**: Text detection status (PDF only)
6. **Image Status**: Image/graphic content detection status
7. **text_top_content_percentage**: Percentage of top margin with text (PDF only)
8. **text_bottom_content_percentage**: Percentage of bottom margin with text (PDF only)
9. **image_top_content_percentage**: Percentage of top margin with image content
10. **image_bottom_content_percentage**: Percentage of bottom margin with image content
11. **total_margin_content_percentage**: Combined margin content percentage (images only)
12. **margins_used_top_margin_percentage**: Top margin setting used for analysis
13. **margins_used_bottom_margin_percentage**: Bottom margin setting used for analysis
14. **Error**: Error details if processing failed
15. **Error Severity**: Severity level (WARNING, ERROR)

### Working with Output Files

#### CSV Files
- Open directly in Excel or any spreadsheet application
- Use text import wizard for large files
- Files are automatically split when row limit is reached

#### Parquet Files
- Use Python with pandas: `pd.read_parquet('file.parquet')`
- Compatible with Apache Spark, R, and other data tools
- Compressed format saves disk space

#### SQLite Database
- Use any SQLite browser or client
- Query with SQL: `SELECT * FROM results WHERE content_status != 'All content within margins'`
- Table name: `results`

## Troubleshooting

### Common Issues and Solutions

#### 1. Input/Output Errors
- **"Please select an input folder!"** - Click Browse to select a folder containing documents
- **"Please select a save location!"** - Choose where to save the analysis results
- **"Selected folder does not exist!"** - Verify the folder path is correct and accessible
- **"Please select at least one file type to process!"** - Enable either PDF or Image processing

#### 2. Configuration Errors
- **"Threshold must be between 0.1% and 10.0%"** - Adjust the detection threshold within valid range
- **"Please select only one sampling method!"** - Choose either Statistical or Random N sampling, not both
- **"Invalid sampling parameters!"** - Check confidence level and margin of error values

#### 3. Processing Errors
- **PDF Processing Failed**: 
  - Ensure PDF is not corrupted
  - Check if PDF is password-protected
  - Verify sufficient memory available
- **Image Processing Failed**:
  - Confirm image format is supported (JPG, PNG, BMP, TIFF)
  - Check if image file is corrupted
  - Ensure image is not too large for memory

#### 4. Performance Issues
- **Slow Processing**:
  - Reduce number of CPU cores if system becomes unresponsive
  - Enable sampling for very large datasets
  - Close other memory-intensive applications
  - Check available disk space (need 2-3x dataset size)
- **Memory Errors**:
  - Process fewer files at once using sampling
  - Reduce DPI setting in configuration (if available)
  - Use 64-bit version of the application

#### 5. Output File Issues
- **Cannot Write Output**:
  - Ensure save location has write permissions
  - Check available disk space
  - Close output file if open in another program
- **CSV File Too Large for Excel**:
  - Reduce max rows per file setting
  - Use Parquet format for large datasets
  - Open in specialized data tools

### Error Messages in Output

The application includes detailed error tracking:
- **Error Column**: Contains specific error details
- **Error Severity**: WARNING (minor issues) or ERROR (processing failures)
- **Content Status**: Will show "Processing Failed" for files with errors

### Getting Help

1. **Check the Log**: The log section shows detailed processing information
2. **Review Error Summary**: After analysis, check the error summary for patterns
3. **User Guide Link**: Click the User Guide link in the status bar
4. **GitHub Issues**: Report bugs at the project repository

## Advanced Features

### Multi-threaded Processing
- The application uses parallel processing for improved performance
- Each CPU core processes files independently
- Progress is updated in real-time

### Batch Processing
- Files are processed in batches for memory efficiency
- Default batch size: 100 files
- Automatic memory management prevents overload

### Error Recovery
- Processing continues even if individual files fail
- Failed files are logged and included in output
- Comprehensive error reporting for troubleshooting

### Status Bar Features
- **User Guide Link**: Click to open this guide in your browser
- **Version Information**: Shows current application version
- **License Link**: View GPL v3.0 license details

### Keyboard Shortcuts
- Currently, the application uses standard Windows shortcuts:
  - Ctrl+C: Copy selected text from log
  - Ctrl+A: Select all text in input fields
  - Tab: Navigate between controls
  - Enter: Activate focused button

### Tips for Best Results

1. **Optimal Settings**:
   - Use 0.5% threshold for standard documents
   - Set margins to match your document layout (typically 5-10%)
   - Use all available CPU cores minus one

2. **Large Datasets**:
   - Enable statistical sampling with 95% confidence
   - Use Parquet output format
   - Process in multiple batches if needed

3. **Quality Assurance**:
   - Start with a small sample to verify settings
   - Review the log for any warnings
   - Check a few results manually to confirm accuracy

## About
© 2024 Noa J Oliver
This program is free software under the GNU General Public License v3.0.
For updates and support, visit the project repository.