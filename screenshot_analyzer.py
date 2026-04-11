"""
Screenshot Analyzer for Universe of Heroes
"""

import logging
import re
import numpy as np
from typing import Tuple

logger = logging.getLogger(__name__)


class ScreenshotAnalyzer:
    def __init__(self):
        self.ocr_available = False
        try:
            import easyocr
            self.reader = easyocr.Reader(['en', 'ru'], gpu=False, verbose=False)
            self.ocr_available = True
            logger.info("OCR module loaded successfully")
        except ImportError as e:
            logger.warning(f"OCR not available: {e}")
    
    def extract_text(self, photo_path: str) -> str:
        if not self.ocr_available:
            return ""
        try:
            from PIL import Image
            import cv2
            image = Image.open(photo_path)
            image_np = np.array(image)
            image_cv = cv2.cvtColor(image_np, cv2.COLOR_RGB2BGR)
            results = self.reader.readtext(image_cv)
            text = ' '.join([item[1] for item in results])
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
