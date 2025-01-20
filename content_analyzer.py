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
        return self.threshold  # Remove the /100.0 since threshold is already a percentage


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

    def analyze_image_content(
            self,
            image: Image.Image,
            top_margin_percent: float = 5.0,
            bottom_margin_percent: float = 5.0
    ) -> MarginAnalysisResult:
        """
        Analyze image content in margins based on user-specified
        top_margin_percent and bottom_margin_percent.

        Args:
            image (PIL.Image.Image): PIL Image object.
            top_margin_percent (float): Percent of image height treated as top margin.
            bottom_margin_percent (float): Percent of image height treated as bottom margin.

        Returns:
            MarginAnalysisResult: Contains details about how much content is
                                  found in the top/bottom margin areas.
        """
        # 1) Print current detection threshold for debugging
        print(f"\nAnalyzing with threshold: {self.threshold}%")

        # 2) Convert image to grayscale for content detection
        gray_image = image.convert('L')

        # 3) Convert to numpy array for efficient pixel-level processing
        img_array = np.array(gray_image)
        img_height, img_width = img_array.shape

        # 4) Calculate how many rows belong to top/bottom margins
        top_margin_pixels = int(img_height * (top_margin_percent / 100.0))
        bottom_margin_pixels = int(img_height * (bottom_margin_percent / 100.0))

        # 5) Isolate top margin region in the array
        top_margin_region = img_array[:top_margin_pixels, :]
        # Count “non-white” pixels in top margin
        top_pixels = np.sum(top_margin_region < 250)
        # Compute what fraction of top margin area is non-white
        # top_margin_area = (top_margin_pixels * img_width)
        top_margin_area = top_margin_pixels * img_width
        if top_margin_area == 0:
            top_margin_area = 1  # Prevent division by zero

        top_percentage = (top_pixels / top_margin_area) * 100.0

        # 6) Isolate bottom margin region
        bottom_margin_region = img_array[-bottom_margin_pixels:, :]
        bottom_pixels = np.sum(bottom_margin_region < 250)
        bottom_margin_area = bottom_margin_pixels * img_width
        if bottom_margin_area == 0:
            bottom_margin_area = 1

        bottom_percentage = (bottom_pixels / bottom_margin_area) * 100.0

        # 7) Debug prints: Show computed percentages vs. threshold
        print(f"Top margin content: {top_percentage:.2f}% (threshold {self.threshold}%)")
        print(f"Bottom margin content: {bottom_percentage:.2f}% (threshold {self.threshold}%)")
        print(f"Top > threshold? {top_percentage > self.threshold}")
        print(f"Bottom > threshold? {bottom_percentage > self.threshold}")

        # 8) Calculate total margin area and total margin content
        total_margin_area = top_margin_area + bottom_margin_area
        total_pixels = top_pixels + bottom_pixels
        total_percentage = (total_pixels / total_margin_area) * 100.0

        # 9) Determine whether top/bottom margin content exceeds threshold
        result = MarginAnalysisResult(
            has_top_content=(top_percentage > self.threshold),
            has_bottom_content=(bottom_percentage > self.threshold),
            top_content_percentage=top_percentage,
            bottom_content_percentage=bottom_percentage,
            total_content_percentage=total_percentage
        )

        # 10) Print final booleans for clarity
        print(f"Final has_top_content={result.has_top_content}")
        print(f"Final has_bottom_content={result.has_bottom_content}")

        return result

    def analyze_text_blocks(
            self,
            page: fitz.Page,
            top_margin_percent: float = 5.0,
            bottom_margin_percent: float = 5.0
    ) -> MarginAnalysisResult:
        """
        Analyze text content in margins based on user-specified
        top_margin_percent and bottom_margin_percent. Any text in
        the margin areas is considered a violation.

        Args:
            page (fitz.Page): The PyMuPDF page object
            top_margin_percent (float): Percent of page height considered top margin
            bottom_margin_percent (float): Percent of page height considered bottom margin

        Returns:
            MarginAnalysisResult: Analysis details for top/bottom margin text
        """
        # 1) Determine page dimensions in pixels
        page_width = int(page.rect.width)
        page_height = int(page.rect.height)

        # 2) Convert margin percentages to pixel coordinates
        top_margin_pixels = int(page_height * (top_margin_percent / 100.0))
        bottom_margin_start = page_height - int(page_height * (bottom_margin_percent / 100.0))

        # 3) Track content in margins
        has_top_content = False
        has_bottom_content = False
        top_content_area = 0.0
        bottom_content_area = 0.0
        margin_violations = {"top": [], "bottom": []}

        # 4) Acquire text blocks from the PDF page
        blocks = page.get_text("dict")["blocks"]
        for block in blocks:
            if block['type'] == 0:  # Text block
                bbox = block["bbox"]  # [x0, y0, x1, y1]
                text_lines = []
                for line in block["lines"]:
                    for span in line["spans"]:
                        text_lines.append(span["text"])
                text = " ".join(text_lines).strip()

                if not text:  # Skip empty blocks
                    continue

                # 5) Check if block overlaps the top margin
                #    If the top edge of block (bbox[1]) is within top_margin_pixels
                if bbox[1] <= top_margin_pixels:
                    has_top_content = True

                    # Optionally calculate exact overlap if desired
                    area = self._calculate_overlap_area(
                        bbox,
                        0,  # top boundary of the page
                        top_margin_pixels  # top margin boundary
                    )
                    top_content_area += area
                    margin_violations["top"].append((text, area))

                # 6) Check if block overlaps the bottom margin
                #    If the bottom edge of block (bbox[3]) is >= bottom_margin_start
                if bbox[3] >= bottom_margin_start:
                    has_bottom_content = True

                    # Optionally calculate exact overlap if desired
                    area = self._calculate_overlap_area(
                        bbox,
                        bottom_margin_start,  # bottom margin start
                        page_height  # page bottom boundary
                    )
                    bottom_content_area += area
                    margin_violations["bottom"].append((text, area))

        # 7) Calculate margin areas in pixels
        #    For consistency, treat each margin as page_width * margin_height
        top_margin_area = page_width * top_margin_pixels
        bottom_margin_area = page_width * (page_height - bottom_margin_start)

        # Safeguard against zero-height margins
        if top_margin_area <= 0:
            top_margin_area = 1
        if bottom_margin_area <= 0:
            bottom_margin_area = 1

        # 8) Derive percentages
        top_percentage = (top_content_area / top_margin_area) * 100.0
        bottom_percentage = (bottom_content_area / bottom_margin_area) * 100.0

        # For total percentage, compare sum of content to sum of margin areas
        total_margin_area = top_margin_area + bottom_margin_area
        total_content_area = top_content_area + bottom_content_area
        total_percentage = (total_content_area / total_margin_area) * 100.0

        # 9) Return a MarginAnalysisResult
        return MarginAnalysisResult(
            has_top_content=has_top_content,
            has_bottom_content=has_bottom_content,
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
        # Add debug print to verify the threshold
        print(f"Initializing PageAnalyzer with threshold: {settings.threshold}%")
        self.content_analyzer = ContentAnalyzer(threshold=settings.threshold)
        self.settings = settings
        self.top_margin_percent = settings.top_margin_percent
        self.bottom_margin_percent = settings.bottom_margin_percent

        # Verify the threshold was set correctly
        print(f"ContentAnalyzer threshold set to: {self.content_analyzer.threshold}%")

    def analyze_pdf_page(self, page: fitz.Page, file_name: str, page_num: int) -> Dict[str, Any]:
        """
        Analyze a single PDF page for margin content, using user-configurable margin settings.

        Args:
            page (fitz.Page): The PDF page object to analyze.
            file_name (str): Name or path of the PDF file.
            page_num (int): Zero-based page index.

        Returns:
            Dict[str, Any]: Dictionary containing analysis results and margin details.
        """
        try:
            # ----------------------------------------------------------------------
            # 1) Retrieve user-configurable margin percentages from settings
            # ----------------------------------------------------------------------
            top_margin_percent = getattr(self.settings, "top_margin_percent", 5.0)
            bottom_margin_percent = getattr(self.settings, "bottom_margin_percent", 5.0)

            # ----------------------------------------------------------------------
            # 2) Analyze text content, passing margin info (if your analyzer supports it)
            # ----------------------------------------------------------------------
            text_analysis = self.content_analyzer.analyze_text_blocks(
                page,
                top_margin_percent=top_margin_percent,
                bottom_margin_percent=bottom_margin_percent
            )

            # ----------------------------------------------------------------------
            # 3) Convert the PDF page to an image (using PyMuPDF -> PIL),
            #    and analyze the image content with the same margin settings
            # ----------------------------------------------------------------------
            pix = page.get_pixmap(
                matrix=fitz.Matrix(self.content_analyzer.dpi / 72, self.content_analyzer.dpi / 72)
            )
            image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

            image_analysis = self.content_analyzer.analyze_image_content(
                image,
                top_margin_percent=top_margin_percent,
                bottom_margin_percent=bottom_margin_percent
            )

            # ----------------------------------------------------------------------
            # 4) Determine overall content status (header/footer) based on analysis
            # ----------------------------------------------------------------------
            locations = []
            if text_analysis.has_top_content or image_analysis.has_top_content:
                locations.append("header")
            if text_analysis.has_bottom_content or image_analysis.has_bottom_content:
                locations.append("footer")

            content_status = (
                "Content found in " + " and ".join(locations) if locations
                else "All content within margins"
            )

            # ----------------------------------------------------------------------
            # 5) Build and return a detailed result dictionary
            # ----------------------------------------------------------------------
            result = {
                "File": file_name,
                "Page": page_num + 1,
                "Content Status": content_status,
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
                    },
                    "Margins Used": {
                        "Top Margin (%)": top_margin_percent,
                        "Bottom Margin (%)": bottom_margin_percent,
                    }
                }
            }

            return result

        except Exception as exc:
            # You could either raise the exception or return an error dict
            return {
                "File": file_name,
                "Page": page_num + 1,
                "Content Status": "Error",
                "Type": "PDF",
                "Error": str(exc),
                "Analysis Details": {}
            }


    def analyze_image_file(self, image_path: str) -> Dict[str, Any]:
        """
        Analyze an image file for margin content

        Args:
            image_path: Path to image file

        Returns:
            Dict containing analysis results
        """
        try:
            with Image.open(image_path) as image:
                image = image.convert('RGB')
                analysis = self.content_analyzer.analyze_image_content(image)

                # Determine locations of content
                locations = []
                if analysis.has_top_content:
                    locations.append("header")
                if analysis.has_bottom_content:
                    locations.append("footer")

                content_status = (
                    "Content found in " + " and ".join(locations) if locations
                    else "All content within margins"
                )

                return {
                    "File": image_path,
                    "Page": 1,
                    "Content Status": content_status,
                    "Type": "Image",
                    "Analysis Details": {
                        "Top Content": f"{analysis.top_content_percentage:.1f}%",
                        "Bottom Content": f"{analysis.bottom_content_percentage:.1f}%",
                        "Total Margin Content": f"{analysis.total_content_percentage:.1f}%"
                    }
                }

        except Exception as e:
            return {
                "File": os.path.basename(image_path),
                "Page": 1,
                "Content Status": "Processing Failed",
                "Type": "Image",
                "Analysis Details": {},
                "Error": str(e),
                "Error Severity": "ERROR"
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
