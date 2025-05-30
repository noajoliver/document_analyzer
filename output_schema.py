# output_schema.py

COLUMN_DESCRIPTIONS = {
    "File": "Absolute path to the analyzed document or image file.",
    "Page": "Page number of the analyzed section within the document. For image files, this value is always 1.",
    "Content Status": "Overall status indicating if content was detected in the defined margin areas. Common values: 'All content within margins', 'Content found in header', 'Content found in footer', 'Content found in header and footer', 'Processing Failed'. May also contain specific error messages related to page processing.",
    "Type": "The type of file analyzed, typically 'PDF' or 'Image'.",

    # PDF-specific text analysis columns (often present if Type is PDF)
    "Text Status": "Status of text-based content detection within the page's margins. Example: 'Text found in header'. Relevant primarily for PDF files.",

    # Image content analysis columns (present for Images and PDFs)
    "Image Status": "Status of image-based content (non-textual marks, lines, etc.) detection within the page's margins. Example: 'Image content found in footer'.",

    # Flattened 'Analysis Details' - these names might need adjustment based on actual flattening in output_handlers.py
    # Assuming dot notation for now as a placeholder for keys in the descriptions.
    # The actual column names in CSVs might be 'analysis_details_text_top_content_percentage', etc.
    # For now, the keys in this dict should be what the user would see as column headers.
    # The worker should try to use the most likely flattened column name.
    # Let's assume the flattening uses underscores for now based on common practice.

    "text_top_content_percentage": "PDFs only: Percentage of the defined top margin area that contains detected text elements.",
    "text_bottom_content_percentage": "PDFs only: Percentage of the defined bottom margin area that contains detected text elements.",
    "image_top_content_percentage": "Percentage of the defined top margin area that contains detected image features (non-textual content).",
    "image_bottom_content_percentage": "Percentage of the defined bottom margin area that contains detected image features (non-textual content).",
    "total_margin_content_percentage": "For images: Total percentage of the combined top and bottom margin areas that contains detected image features.", # As seen in PageAnalyzer.analyze_image_file
    "margins_used_top_margin_percentage": "The percentage of the page height used to define the top margin for analysis (e.g., 5.0 for 5%).",
    "margins_used_bottom_margin_percentage": "The percentage of the page height used to define the bottom margin for analysis (e.g., 5.0 for 5%).",

    # Error columns (if an error occurred for a specific file/page)
    "Error": "Provides details of any error encountered during the processing of this specific file or page. Blank if no error occurred.",
    "Error Severity": "Indicates the severity of the encountered error (e.g., WARNING, ERROR). Blank if no error occurred."
}

ORDERED_COLUMN_NAMES = [
    "File",
    "Page",
    "Type",
    "Content Status",
    "Text Status",
    "Image Status",
    "text_top_content_percentage",
    "text_bottom_content_percentage",
    "image_top_content_percentage",
    "image_bottom_content_percentage",
    "total_margin_content_percentage",
    "margins_used_top_margin_percentage",
    "margins_used_bottom_margin_percentage",
    "Error",
    "Error Severity"
]

# Future extension: Could also include data types, example values, etc.
# For now, just descriptions.

def get_column_description(column_name: str) -> str:
    """Returns the description for a given column name, or a default message if not found."""
    return COLUMN_DESCRIPTIONS.get(column_name, f"No description available for column: {column_name}")
