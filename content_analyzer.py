import fitz
from PIL import Image
import numpy as np
import math
import os
from typing import Dict, Any, Tuple, Optional, List
from dataclasses import dataclass


@dataclass
class MarginMeasurements:
    """Stores measurements for margin analysis"""
    width: float
    height: float
    dpi: int
    margin_pixels: int
    threshold: float

    @property
    def top_margin(self) -> int:
        """Get top margin in pixels"""
        return self.margin_pixels

    @property
    def bottom_margin(self) -> int:
        """Get bottom margin in pixels"""
        return self.height - self.margin_pixels

    @property
    def normalized_threshold(self) -> float:
        """Get threshold as decimal"""
        return self.threshold / 100.0


@dataclass
class MarginAnalysisResult:
    """Stores results of margin content analysis"""
    has_top_content: bool
    has_bottom_content: bool
    top_content_percentage: float
    bottom_content_percentage: float
    total_content_percentage: float


class ContentAnalyzer:
    """Analyzes document content for margin violations"""

    DEFAULT_THRESHOLD = 1.0  # Updated default threshold

    def __init__(self, threshold: float = DEFAULT_THRESHOLD, dpi: int = 200):
        """
        Initialize analyzer with settings

        Args:
            threshold: Percentage threshold for content detection (0.1-10.0)
            dpi: DPI for image conversion
        """
        # Validate threshold range
        if not 0.1 <= threshold <= 10.0:
            raise ValueError("Threshold must be between 0.1 and 10.0")

        self.threshold = threshold
        self.dpi = dpi
        self.inch_to_pt = 72
        self.margin = 0.5 * self.inch_to_pt  # 0.5 inch margins

    def get_measurements(self, width: int, height: int) -> MarginMeasurements:
        """Calculate margin measurements for given dimensions"""
        margin_pixels = math.ceil(self.margin * (self.dpi / self.inch_to_pt))
        return MarginMeasurements(
            width=width,
            height=height,
            dpi=self.dpi,
            margin_pixels=margin_pixels,
            threshold=self.threshold
        )

    def analyze_image_content(self, image: Image.Image) -> MarginAnalysisResult:
        """
        Analyze image content in margins

        Args:
            image: PIL Image object

        Returns:
            MarginAnalysisResult with analysis details
        """
        # Convert image to grayscale for more accurate content detection
        gray_image = image.convert('L')

        # Get measurements
        measurements = self.get_measurements(image.width, image.height)

        # Convert to numpy array for efficient processing
        img_array = np.array(gray_image)

        # Analyze top margin
        top_margin = img_array[:measurements.margin_pixels, :]
        top_pixels = np.sum(top_margin < 250)  # Less than 250 indicates non-white
        top_percentage = top_pixels / (measurements.width * measurements.margin_pixels)

        # Analyze bottom margin
        bottom_margin = img_array[-measurements.margin_pixels:, :]
        bottom_pixels = np.sum(bottom_margin < 250)
        bottom_percentage = bottom_pixels / (measurements.width * measurements.margin_pixels)

        # Calculate total affected area
        total_margin_pixels = 2 * measurements.width * measurements.margin_pixels
        total_percentage = (top_pixels + bottom_pixels) / total_margin_pixels

        return MarginAnalysisResult(
            has_top_content=top_percentage > measurements.normalized_threshold,
            has_bottom_content=bottom_percentage > measurements.normalized_threshold,
            top_content_percentage=top_percentage * 100,
            bottom_content_percentage=bottom_percentage * 100,
            total_content_percentage=total_percentage * 100
        )

    def analyze_text_blocks(self, page: fitz.Page) -> MarginAnalysisResult:
        """
        Analyze text content in margins - any text in margins is considered a violation

        Args:
            page: fitz.Page object

        Returns:
            MarginAnalysisResult with analysis details
        """
        measurements = self.get_measurements(
            width=int(page.rect.width),
            height=int(page.rect.height)
        )

        # Track content in margins
        has_top_content = False
        has_bottom_content = False
        top_content_area = 0
        bottom_content_area = 0
        margin_violations = {"top": [], "bottom": []}

        # Analyze text blocks
        blocks = page.get_text("dict")["blocks"]
        for block in blocks:
            if block['type'] == 0:  # Text block
                bbox = block["bbox"]
                text = " ".join([span["text"] for line in block["lines"]
                                 for span in line["spans"]]).strip()

                if not text:  # Skip empty blocks
                    continue

                # Calculate block area in margins
                if bbox[1] <= measurements.margin_pixels:  # Top margin
                    has_top_content = True
                    area = self._calculate_overlap_area(
                        bbox, 0, measurements.margin_pixels)
                    top_content_area += area
                    margin_violations["top"].append((text, area))

                if bbox[3] >= measurements.bottom_margin:  # Bottom margin
                    has_bottom_content = True
                    area = self._calculate_overlap_area(
                        bbox, measurements.bottom_margin, measurements.height)
                    bottom_content_area += area
                    margin_violations["bottom"].append((text, area))

        # Calculate percentages (still calculate these for reporting purposes)
        margin_area = measurements.width * measurements.margin_pixels
        top_percentage = (top_content_area / margin_area) * 100
        bottom_percentage = (bottom_content_area / margin_area) * 100
        total_percentage = ((top_content_area + bottom_content_area) /
                            (2 * margin_area)) * 100

        return MarginAnalysisResult(
            has_top_content=has_top_content,  # Any text in margin is a violation
            has_bottom_content=has_bottom_content,  # Any text in margin is a violation
            top_content_percentage=top_percentage,
            bottom_content_percentage=bottom_percentage,
            total_content_percentage=total_percentage
        )

    def _calculate_overlap_area(self, bbox: Tuple[float, float, float, float],
                              margin_start: float, margin_end: float) -> float:
        """Calculate area of overlap between text block and margin"""
        overlap_height = min(bbox[3], margin_end) - max(bbox[1], margin_start)
        if overlap_height <= 0:
            return 0
        return overlap_height * (bbox[2] - bbox[0])


class PageAnalyzer:
    """Analyzes complete pages combining text and image analysis"""

    def __init__(self, settings: 'AnalysisSettings'):
        """
        Initialize page analyzer

        Args:
            settings: Analysis settings including threshold
        """
        self.content_analyzer = ContentAnalyzer(threshold=settings.threshold)
        self.settings = settings

    def analyze_pdf_page(self, page: fitz.Page, file_name: str,
                         page_num: int) -> Dict[str, Any]:
        """
        Analyze a PDF page for margin content

        Args:
            page: fitz.Page object to analyze
            file_name: Name of PDF file
            page_num: Page number

        Returns:
            Dictionary containing analysis results
        """
        # Analyze text content
        text_analysis = self.content_analyzer.analyze_text_blocks(page)

        # Convert to image and analyze
        pix = page.get_pixmap(matrix=fitz.Matrix(
            self.content_analyzer.dpi / 72, self.content_analyzer.dpi / 72))
        image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        image_analysis = self.content_analyzer.analyze_image_content(image)

        # Determine overall status - any text in margins or image content above threshold
        has_margin_content = (text_analysis.has_top_content or  # Any text in margins
                              text_analysis.has_bottom_content or
                              image_analysis.has_top_content or  # Image content above threshold
                              image_analysis.has_bottom_content)

        # Create detailed result
        result = {
            "File": os.path.basename(file_name),
            "Page": page_num + 1,
            "Content Status": "Content found in header or footer" if has_margin_content
            else "All content within margins",
            "Text Status": self._format_text_status(text_analysis),
            "Image Status": self._format_image_status(image_analysis),
            "Type": "PDF",
            "Analysis Details": {
                "Text": {
                    "Top Content": f"{text_analysis.top_content_percentage:.1f}%",
                    "Bottom Content": f"{text_analysis.bottom_content_percentage:.1f}%",
                },
                "Image": {
                    "Top Content": f"{image_analysis.top_content_percentage:.1f}%",
                    "Bottom Content": f"{image_analysis.bottom_content_percentage:.1f}%",
                }
            }
        }

        return result

    def analyze_image_file(self, image_path: str) -> Dict[str, Any]:
        """
        Analyze an image file for margin content

        Args:
            image_path: Path to image file

        Returns:
            Dictionary containing analysis results
        """
        with Image.open(image_path) as image:
            image = image.convert('RGB')
            analysis = self.content_analyzer.analyze_image_content(image)

            return {
                "File": os.path.basename(image_path),
                "Page": 1,
                "Content Status": "Content found in header or footer" if
                (analysis.has_top_content or analysis.has_bottom_content)
                else "All content within margins",
                "Type": "Image",
                "Analysis Details": {
                    "Top Content": f"{analysis.top_content_percentage:.1f}%",
                    "Bottom Content": f"{analysis.bottom_content_percentage:.1f}%",
                    "Total Margin Content": f"{analysis.total_content_percentage:.1f}%"
                }
            }

    def _format_status_message(self, analysis: MarginAnalysisResult) -> str:
        """Format analysis result as status message"""
        if not analysis.has_top_content and not analysis.has_bottom_content:
            return "All content within margins"

        locations = []
        if analysis.has_top_content:
            locations.append("header")
        if analysis.has_bottom_content:
            locations.append("footer")

        return f"Content found in {' and '.join(locations)}"

    def _format_text_status(self, analysis: MarginAnalysisResult) -> str:
        """Format text analysis result as status message"""
        if not analysis.has_top_content and not analysis.has_bottom_content:
            return "All content within margins"

        locations = []
        if analysis.has_top_content:
            locations.append("header")
        if analysis.has_bottom_content:
            locations.append("footer")

        return f"Text found in {' and '.join(locations)}"

    def _format_image_status(self, analysis: MarginAnalysisResult) -> str:
        """Format image analysis result as status message"""
        locations = []
        if analysis.has_top_content:
            locations.append("header")
        if analysis.has_bottom_content:
            locations.append("footer")

        if not locations:
            return "All content within margins"
        return f"Image content found in {' and '.join(locations)}"