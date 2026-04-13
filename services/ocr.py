import logging
import os
import re
import shutil
import unicodedata
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ScreenshotAnalyzer:
    DEFAULT_MAX_PLAUSIBLE_SCORE = 20

    def __init__(self):
        self.ocr_available = False
        self.max_plausible_score = self._load_max_plausible_score()
        self._ocr_lang = "eng+rus"
        self._tesseract_cmd = None
        try:
            import pytesseract
            from PIL import Image
            self.pytesseract = pytesseract
            self.Image = Image

            env_cmd = os.getenv("TESSERACT_CMD", "").strip()
            if env_cmd:
                if os.path.isabs(env_cmd):
                    if os.path.exists(env_cmd):
                        self._tesseract_cmd = env_cmd
                    else:
                        logger.warning("TESSERACT_CMD path does not exist: %s", env_cmd)
                else:
                    resolved_env_cmd = shutil.which(env_cmd)
                    if resolved_env_cmd:
                        self._tesseract_cmd = resolved_env_cmd
                    else:
                        logger.warning("TESSERACT_CMD binary is not found in PATH: %s", env_cmd)

            if not self._tesseract_cmd:
                detected_cmd = shutil.which("tesseract")
                if detected_cmd:
                    self._tesseract_cmd = detected_cmd

            if self._tesseract_cmd:
                self.pytesseract.pytesseract.tesseract_cmd = self._tesseract_cmd

            try:
                self.pytesseract.get_tesseract_version()
                self.ocr_available = True
                logger.info("OCR module loaded successfully")
            except Exception as version_error:
                self.ocr_available = False
                logger.warning("OCR binary is unavailable: %s", version_error)
        except ImportError as e:
            logger.warning(f"OCR not available: {e}")

    def _load_max_plausible_score(self) -> int:
        raw_value = os.getenv("OCR_MAX_SCORE", str(self.DEFAULT_MAX_PLAUSIBLE_SCORE)).strip()
        try:
            value = int(raw_value)
            if 0 <= value <= 99:
                return value
        except (TypeError, ValueError):
            pass

        logger.warning(
            "Invalid OCR_MAX_SCORE '%s', using default %d",
            raw_value,
            self.DEFAULT_MAX_PLAUSIBLE_SCORE,
        )
        return self.DEFAULT_MAX_PLAUSIBLE_SCORE
    
    def extract_text(self, photo_path: str) -> str:
        if not self.ocr_available:
            return ""
        try:
            if self._tesseract_cmd:
                self.pytesseract.pytesseract.tesseract_cmd = self._tesseract_cmd
            image = self.Image.open(photo_path)
            text = self.pytesseract.image_to_string(image, lang=self._ocr_lang)
            return text
        except Exception as e:
            logger.error(f"OCR error: {e}")
            if "not installed" in str(e) or "No such file or directory" in str(e):
                self.ocr_available = False
            if self._ocr_lang != "eng":
                try:
                    if self._tesseract_cmd:
                        self.pytesseract.pytesseract.tesseract_cmd = self._tesseract_cmd
                    image = self.Image.open(photo_path)
                    text = self.pytesseract.image_to_string(image, lang="eng")
                    self._ocr_lang = "eng"
                    return text
                except Exception:
                    pass
            return ""
    
    def extract_scores(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        if not text:
            return None, None

        def normalize_ocr_chars(value: str) -> str:
            mapped = {
                'O': '0', 'o': '0', 'Q': '0', 'D': '0',
                'I': '1', 'l': '1', '|': '1',
                'S': '5', 's': '5',
                'B': '8',
                'Z': '2',
            }
            normalized = []
            for ch in value:
                normalized.append(mapped.get(ch, ch))
            return ''.join(normalized)

        def valid_pair(a: int, b: int) -> bool:
            return 0 <= a <= self.max_plausible_score and 0 <= b <= self.max_plausible_score

        lines = [normalize_ocr_chars(line.strip()) for line in text.split('\n') if line.strip()]
        candidates: List[Tuple[int, int, int]] = []  # score1, score2, confidence
        score_pattern = re.compile(r'(?<![\dA-Za-zА-Яа-я])(\d{1,2})\s*[:\-–—]\s*(\d{1,2})(?![\dA-Za-zА-Яа-я])')

        for idx, line in enumerate(lines[:15]):
            for m in score_pattern.finditer(line):
                s1, s2 = int(m.group(1)), int(m.group(2))
                if valid_pair(s1, s2):
                    confidence = 120 - idx
                    candidates.append((s1, s2, confidence))

        if not candidates:
            return None, None

        aggregated: Dict[Tuple[int, int], Dict[str, int]] = {}
        for s1, s2, confidence in candidates:
            key = (s1, s2)
            current = aggregated.get(key)
            if not current:
                aggregated[key] = {
                    "sum_conf": confidence,
                    "count": 1,
                    "max_conf": confidence,
                }
                continue
            current["sum_conf"] += confidence
            current["count"] += 1
            current["max_conf"] = max(current["max_conf"], confidence)

        ranking = sorted(
            aggregated.items(),
            key=lambda item: (item[1]["sum_conf"], item[1]["count"], item[1]["max_conf"]),
            reverse=True,
        )

        best_pair, best_stats = ranking[0]
        if len(ranking) > 1:
            second_stats = ranking[1][1]
            score_gap = best_stats["sum_conf"] - second_stats["sum_conf"]

            # Если OCR дал несколько близких вариантов, считаем скрин неоднозначным.
            if score_gap < 20 and best_stats["count"] == 1:
                return None, None

        return best_pair[0], best_pair[1]

    def normalize_nick(self, value: str) -> str:
        if not value:
            return ""
        norm = unicodedata.normalize("NFKC", value).lower().replace('@', '').strip()
        # оставляем только буквы/цифры для устойчивого fuzzy-сопоставления
        return ''.join(ch for ch in norm if ch.isalnum())

    def extract_nick_tokens(self, text: str) -> List[str]:
        if not text:
            return []

        tokens = []
        seen = set()
        patterns = [
            r'@([A-Za-zА-Яа-я0-9_.-]{2,32})',
            r'\b([A-Za-zА-Яа-я0-9_.-]{3,32})\b',
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, text):
                token = match.group(1).strip().lower().lstrip('@')
                if token.isdigit():
                    continue
                if token not in seen:
                    seen.add(token)
                    tokens.append(token)

                norm_token = self.normalize_nick(token)
                if norm_token and norm_token not in seen:
                    seen.add(norm_token)
                    tokens.append(norm_token)

        return tokens
