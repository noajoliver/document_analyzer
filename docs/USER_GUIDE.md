# Document Margin Analyzer
## Comprehensive User Guide

## Table of Contents
1. [Getting Started](#getting-started)
2. [Interface Overview](#interface-overview)
3. [Configuration Options](#configuration-options)
4. [Analysis Process](#analysis-process)
5. [Output Understanding](#output-understanding)
6. [Advanced Features](#advanced-features)

## Getting Started

### System Requirements
- Windows 10 or later
- Minimum 4GB RAM (8GB recommended)
- Screen resolution: 1024x768 or higher


### Installation
1. Extract the DocumentMarginAnalyzer package to your desired location
2. No additional installation required - the software is portable
3. Double-click DocumentMarginAnalyzer.exe to launch


## Interface Overview

### Main Application Window
![Main Application Interface](images/main-window.png)
*The Document Margin Analyzer main interface with all sections collapsed*

### Analysis Settings Section
![Analysis Settings Expanded](images/analysis-settings.png)
*Analysis Settings section showing input and output configuration*

#### Input Selection
![Input Folder Selection](images/input-selection.png)
*Input folder browser and selection dialog*

- **Input Folder**: Select the directory containing your documents
  - Use Browse button to navigate
  - Supports drag and drop
  - Shows selected folder path

#### Save Location
![Save Location Selection](images/save-location.png)
*Save location configuration with format options*

- **Save Location**: Choose where analysis results will be saved
  - Automatic extension based on format
  - Create new directory option
  - Overwrite protection

#### File Types
![File Type Selection](images/file-types.png)
*File type selection with format details*

- **PDF Files**: 
  - Supports single and multi-page documents
  - Handles encrypted PDFs (with warning)
  - Shows .pdf extension

- **Image Files**:
  - Supported formats shown in interface
  - Format icons for visual reference
  - File extension list: .jpg, .jpeg, .png, .bmp, .tiff, .tif

### Analysis Configuration Section
![Analysis Configuration Expanded](images/analysis-config.png)
*Complete Analysis Configuration panel*

#### Detection Threshold
![Threshold Configuration](images/threshold-config.png)
*Threshold adjustment with sensitivity guide*

Threshold settings guide:
- 0.1-0.5%: Extremely sensitive
  ![High Sensitivity Example](images/threshold-high.png)
  *Example of high sensitivity detection*

- 1.0%: Standard detection (recommended)
  ![Standard Sensitivity Example](images/threshold-standard.png)
  *Example of standard sensitivity detection*

- 1.1-2.0%: Moderate tolerance
  ![Moderate Sensitivity Example](images/threshold-moderate.png)
  *Example of moderate sensitivity detection*

- 2.1-5.0%: Lower sensitivity
  ![Low Sensitivity Example](images/threshold-low.png)
  *Example of low sensitivity detection*

- 5.1-10.0%: Minimal sensitivity
  ![Minimal Sensitivity Example](images/threshold-minimal.png)
  *Example of minimal sensitivity detection*

#### CPU Configuration
![CPU Core Selection](images/cpu-config.png)
*CPU core selection with system information*

- Shows available cores
- Recommended settings
- Performance impact guide

#### Sampling Configuration
![Sampling Options](images/sampling-options.png)
*Statistical and Random sampling configuration*

##### Statistical Sampling
![Statistical Sampling Setup](images/statistical-sampling.png)
*Statistical sampling configuration panel*

- Confidence Level selection
- Margin of Error adjustment
- Sample size calculation

##### Random N Sampling
![Random Sampling Setup](images/random-sampling.png)
*Random N sampling configuration panel*

- Sample size input
- Population display
- Random seed option

### Output Configuration Section
![Output Configuration Expanded](images/output-config.png)
*Output Configuration with all options*

#### Format Selection
![Output Format Options](images/output-formats.png)
*Available output format options*

- CSV Configuration
  ![CSV Options](images/csv-options.png)
  *CSV-specific settings*

- Parquet Configuration
  ![Parquet Options](images/parquet-options.png)
  *Parquet-specific settings*

- SQLite Configuration
  ![SQLite Options](images/sqlite-options.png)
  *SQLite-specific settings*

### Progress Section
![Progress Section Expanded](images/progress-section.png)
*Progress tracking with all metrics*

#### Progress Bar
![Progress Indicators](images/progress-indicators.png)
*Detailed progress indicators and metrics*

#### Processing Statistics
![Processing Stats](images/processing-stats.png)
*Real-time processing statistics display*

### Log Section
![Log Section Expanded](images/log-section.png)
*Log section showing processing details*

#### Log Display
![Log Details](images/log-details.png)
*Detailed log entries with timestamps*

## Analysis Process

### Starting Analysis
![Start Analysis](images/start-analysis.png)
*Analysis startup configuration confirmation*

### During Processing
![Processing State](images/processing-state.png)
*Active processing with controls*

#### Control Options
![Control Buttons](images/control-buttons.png)
*Available control buttons during processing*

### Completion
![Analysis Complete](images/analysis-complete.png)
*Analysis completion summary*

## Output Understanding

### Results Overview
![Results Summary](images/results-summary.png)
*Summary of analysis results*

### CSV Output
![CSV Results](images/csv-results.png)
*Example CSV output format*

### Parquet Output
![Parquet Results](images/parquet-results.png)
*Parquet output structure*

### SQLite Output
![SQLite Results](images/sqlite-results.png)
*SQLite database structure and contents*

### Processing Report
![Processing Report](images/processing-report.png)
*Detailed processing report example*

### Error Report
![Error Report](images/error-report.png)
*Error report with categories and details*


---

# Image Credits
All screenshots are from Document Margin Analyzer version 2.0
© 2024 Noa J Oliver

This program is free software under the GNU General Public License v3.0.
