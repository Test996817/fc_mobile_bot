"""
Screenshot Analyzer for Universe of Heroes
"""

import logging
import re
from typing import Tuple

logger = logging.getLogger(__name__)


class ScreenshotAnalyzer:
    def __init__(self):
        self.ocr_available = False
        self.pytesseract = None
        self.Image = None
        
        try:
            import pytesseract
            from PIL import Image
            self.pytesseract = pytesseract
            self.Image = Image
            self.pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
            self.ocr_available = True
            logger.info("OCR module loaded successfully")
        except ImportError as e:
            logger.warning(f"OCR not available: {e}")
    
    def extract_text(self, photo_path: str) -> str:
        if not self.ocr_available:
            return ""
        try:
            image = self.Image.open(photo_path)
            text = self.pytesseract.image_to_string(image, lang='eng+rus')
            return text
        except Exception as e:
            logger.error(f"OCR error: {e}")
            return ""
    
    def extract_scores(self, text: str) -> Tuple[int, int]:
        lines = text.strip().split('\n')
        
        for line in lines[:5]:
            line = line.strip()
            if not line:
                continue
            
            match = re.search(r'(\d{1,2})\s*[^\w\d\s]+\s*(\d{1,2})', line)
            if match:
                try:
                    s1, s2 = int(match.group(1)), int(match.group(2))
                    if 0 <= s1 <= 99 and 0 <= s2 <= 99:
                        return s1, s2
                except ValueError:
                    pass
        
        all_text = text.replace('\n', ' ')
        pairs = re.findall(r'(\d{1,2})\s*[^\w\d\s]+\s*(\d{1,2})', all_text)
        for s1, s2 in pairs:
            try:
                n1, n2 = int(s1), int(s2)
                if 0 <= n1 <= 99 and 0 <= n2 <= 99:
                    return n1, n2
            except ValueError:
                continue
        
        return None, None
