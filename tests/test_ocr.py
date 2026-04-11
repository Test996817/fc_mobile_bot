"""
OCR Tests for EasyOCR migration
"""

import pytest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from screenshot_analyzer import ScreenshotAnalyzer


class TestOCR:
    @pytest.fixture
    def analyzer(self):
        return ScreenshotAnalyzer()
    
    def test_ocr_available(self, analyzer):
        """Test that OCR module initializes correctly"""
        assert analyzer.ocr_available == True
        assert analyzer.reader is not None
    
    def test_extract_text_from_image(self, analyzer):
        """Test text extraction from a real screenshot"""
        fixtures_dir = os.path.join(os.path.dirname(__file__), 'fixtures')
        test_images = [f for f in os.listdir(fixtures_dir) if f.endswith(('.png', '.jpg', '.jpeg'))]
        
        if not test_images:
            pytest.skip("No test images in fixtures folder")
        
        for image_name in test_images:
            image_path = os.path.join(fixtures_dir, image_name)
            text = analyzer.extract_text(image_path)
            print(f"\n--- Extracted from {image_name} ---")
            print(text)
            assert text != "", f"Failed to extract text from {image_name}"
    
    def test_extract_scores(self, analyzer):
        """Test score extraction from screenshots"""
        fixtures_dir = os.path.join(os.path.dirname(__file__), 'fixtures')
        test_images = [f for f in os.listdir(fixtures_dir) if f.endswith(('.png', '.jpg', '.jpeg'))]
        
        if not test_images:
            pytest.skip("No test images in fixtures folder")
        
        for image_name in test_images:
            image_path = os.path.join(fixtures_dir, image_name)
            text = analyzer.extract_text(image_path)
            score1, score2 = analyzer.extract_scores(text)
            print(f"\n--- Scores from {image_name} ---")
            print(f"Score: {score1}:{score2}")
