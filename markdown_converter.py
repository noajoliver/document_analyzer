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
import re
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import markdown
from dataclasses import dataclass
import logging
import fitz  # PyMuPDF

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DocumentStyle:
    """Style configuration for PDF document"""
    font_name: str = "helvetica"
    font_size: float = 11
    header_font_size: float = 16
    subheader_font_size: float = 14
    code_font_name: str = "courier"
    text_color: Tuple[float, float, float] = (0, 0, 0)
    link_color: Tuple[float, float, float] = (0, 0, 0.8)
    margin_top: float = 72.0    # 1 inch in points
    margin_right: float = 72.0
    margin_bottom: float = 72.0
    margin_left: float = 72.0
    line_spacing: float = 1.2

class MarkdownConverter:
    """Converts Markdown files to PDF using PyMuPDF"""

    def __init__(self, style: Optional[DocumentStyle] = None):
        """
        Initialize converter with style settings

        Args:
            style: Document style configuration
        """
        self.style = style or DocumentStyle()

    def convert_file(self, input_path: str, output_path: str) -> None:
        """
        Convert a Markdown file to PDF

        Args:
            input_path: Path to input Markdown file
            output_path: Path for output PDF file

        Raises:
            FileNotFoundError: If input file doesn't exist
            RuntimeError: If conversion fails
        """
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")

        try:
            # Read markdown content
            with open(input_path, 'r', encoding='utf-8') as f:
                md_content = f.read()

            # Convert markdown to HTML
            html_content = markdown.markdown(
                md_content,
                extensions=['tables', 'fenced_code', 'codehilite', 'toc']
            )

            # Create PDF
            self._create_pdf(html_content, output_path)

        except Exception as e:
            raise RuntimeError(f"Failed to convert {input_path} to PDF: {str(e)}")

    def _create_pdf(self, html_content: str, output_path: str) -> None:
        """Create PDF document from HTML content"""
        # Create new PDF document
        doc = fitz.Document()

        # Parse HTML into sections
        sections = self._parse_html_sections(html_content)

        # Process each section
        current_y = self.style.margin_top
        page = doc.new_page()

        for section in sections:
            # Check if we need a new page
            if current_y > page.rect.height - self.style.margin_bottom:
                page = doc.new_page()
                current_y = self.style.margin_top

            # Write section content
            current_y = self._write_section(page, section, current_y)

        # Save the document
        doc.save(output_path)

    def _parse_html_sections(self, html_content: str) -> List[Dict[str, Any]]:
        """Parse HTML content into sections for processing"""
        sections = []
        current_section = {"type": "text", "content": ""}

        # Simple HTML parser
        lines = html_content.split('\n')
        for line in lines:
            if line.startswith('<h1'):
                if current_section["content"]:
                    sections.append(current_section)
                current_section = {"type": "heading1", "content": self._strip_tags(line)}
                sections.append(current_section)
                current_section = {"type": "text", "content": ""}
            elif line.startswith('<h2'):
                if current_section["content"]:
                    sections.append(current_section)
                current_section = {"type": "heading2", "content": self._strip_tags(line)}
                sections.append(current_section)
                current_section = {"type": "text", "content": ""}
            elif line.startswith('<pre'):
                if current_section["content"]:
                    sections.append(current_section)
                current_section = {"type": "code", "content": self._strip_tags(line)}
                sections.append(current_section)
                current_section = {"type": "text", "content": ""}
            else:
                cleaned_line = self._strip_tags(line)
                if cleaned_line:
                    current_section["content"] += cleaned_line + "\n"

        if current_section["content"]:
            sections.append(current_section)

        return sections

    def _write_section(self, page: fitz.Page, section: Dict[str, Any], start_y: float) -> float:
        """
        Write a section to the page and return the new y position

        Args:
            page: PDF page to write to
            section: Section content and type
            start_y: Starting y position

        Returns:
            New y position after writing section
        """
        content = section["content"].strip()
        if not content:
            return start_y

        current_y = start_y
        text_width = page.rect.width - self.style.margin_left - self.style.margin_right

        if section["type"] == "heading1":
            # Main heading
            font_size = self.style.header_font_size
            page.insert_text(
                point=(self.style.margin_left, current_y + font_size),
                text=content,
                fontname=self.style.font_name,
                fontsize=font_size,
                color=self.style.text_color
            )
            current_y += font_size * 1.5

        elif section["type"] == "heading2":
            # Subheading
            font_size = self.style.subheader_font_size
            page.insert_text(
                point=(self.style.margin_left, current_y + font_size),
                text=content,
                fontname=self.style.font_name,
                fontsize=font_size,
                color=self.style.text_color
            )
            current_y += font_size * 1.5

        elif section["type"] == "code":
            # Code block
            font_size = self.style.font_size
            # Add background rectangle
            code_rect = fitz.Rect(
                self.style.margin_left - 5,
                current_y,
                page.rect.width - self.style.margin_right + 5,
                current_y + (font_size * len(content.split('\n')) * self.style.line_spacing) + 10
            )
            page.draw_rect(code_rect, color=(0.95, 0.95, 0.95), fill=(0.95, 0.95, 0.95))

            # Write code content
            for line in content.split('\n'):
                if line.strip():
                    page.insert_text(
                        point=(self.style.margin_left, current_y + font_size),
                        text=line,
                        fontname=self.style.code_font_name,
                        fontsize=font_size,
                        color=self.style.text_color
                    )
                current_y += font_size * self.style.line_spacing
            current_y += 5  # Add padding after code block

        else:
            # Regular text
            font_size = self.style.font_size
            words = content.split()
            line = []
            line_width = 0

            for word in words:
                word_width = fitz.get_text_length(
                    word + " ",
                    fontname=self.style.font_name,
                    fontsize=font_size
                )

                if line_width + word_width > text_width:
                    # Write current line
                    if line:
                        text = " ".join(line)
                        page.insert_text(
                            point=(self.style.margin_left, current_y + font_size),
                            text=text,
                            fontname=self.style.font_name,
                            fontsize=font_size,
                            color=self.style.text_color
                        )
                        current_y += font_size * self.style.line_spacing
                        line = []
                        line_width = 0

                line.append(word)
                line_width += word_width

            # Write remaining line if any
            if line:
                text = " ".join(line)
                page.insert_text(
                    point=(self.style.margin_left, current_y + font_size),
                    text=text,
                    fontname=self.style.font_name,
                    fontsize=font_size,
                    color=self.style.text_color
                )
                current_y += font_size * self.style.line_spacing

        return current_y + (self.style.font_size * 0.5)  # Add padding between sections

    @staticmethod
    def _strip_tags(text: str) -> str:
        """Remove HTML tags from text"""
        return re.sub(r'<[^>]+>', '', text).strip()

def convert_project_docs(project_root: str, output_dir: str) -> List[str]:
    """
    Convert all project documentation to PDF

    Args:
        project_root: Root directory of project
        output_dir: Directory for PDF output

    Returns:
        List of generated PDF paths
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    converter = MarkdownConverter()
    generated_pdfs = []

    # Find all markdown files
    for root, _, files in os.walk(project_root):
        for file in files:
            if file.lower().endswith('.md'):
                input_path = os.path.join(root, file)
                output_name = os.path.splitext(file)[0] + '.pdf'
                output_path = os.path.join(output_dir, output_name)

                try:
                    logger.info(f"Converting {file}...")
                    converter.convert_file(input_path, output_path)
                    generated_pdfs.append(output_path)
                except Exception as e:
                    logger.error(f"Error converting {file}: {str(e)}")

    return generated_pdfs