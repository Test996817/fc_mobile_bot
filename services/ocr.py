import logging
import os
import re
import unicodedata
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

ZONES_BY_RESOLUTION: Dict[Tuple[int, int], Dict[str, Tuple[int, int, int, int]]] = {
    (2560, 1600): {
        "team1": (494, 304, 609, 352),
        "score": (1102, 268, 1472, 388),
        "team2": (1900, 300, 2100, 350),
    },
    (1280, 591): {
        "team1": (274, 60, 413, 103),
        "score": (555, 49, 731, 109),
        "team2": (879, 63, 1015, 103),
    },
    (1280, 576): {
        "team1": (274, 60, 413, 103),
        "score": (555, 49, 731, 109),
        "team2": (879, 63, 1015, 103),
    },
    (1170, 540): {
        "team1": (255, 61, 287, 77),
        "score": (506, 44, 666, 96),
        "team2": (851, 61, 927, 77),
    },
    (2414, 1080): {
        "team1": (545, 119, 699, 187),
        "score": (1055, 94, 1336, 190),
        "team2": (1648, 116, 1890, 188),
    },
    (1280, 800): {
        "team1": (267, 257, 349, 277),
        "score": (553, 241, 733, 301),
        "team2": (935, 256, 1030, 280),
    },
}


def get_zones_for_resolution(img_w: int, img_h: int) -> Optional[Dict[str, Tuple[int, int, int, int]]]:
    """Возвращает зоны для заданного разрешения или ближайшего."""
    zones = ZONES_BY_RESOLUTION.get((img_w, img_h))
    if zones:
        return zones

    for ref_res, ref_zones in ZONES_BY_RESOLUTION.items():
        if abs(img_w - ref_res[0]) < 100:
            return ref_zones

    return None


def scale_coords(
    x1: int, y1: int, x2: int, y2: int, img_w: int, img_h: int, ref_w: int = 2560, ref_h: int = 1600
) -> Tuple[int, int, int, int]:
    """Масштабирует координаты под разрешение изображения."""
    scale_x = img_w / ref_w
    scale_y = img_h / ref_h
    return (int(x1 * scale_x), int(y1 * scale_y), int(x2 * scale_x), int(y2 * scale_y))


class ScreenshotAnalyzer:
    DEFAULT_MAX_PLAUSIBLE_SCORE = 20

    def __init__(self):
        self.ocr_available = False
        self.reader = None
        self._ocr_lang = self._load_ocr_languages()
        self._ocr_contrast_enhance = os.getenv("OCR_CONTRAST_ENHANCE", "false").lower() == "true"
        self.max_plausible_score = self._load_max_plausible_score()
        try:
            import easyocr
            self.easyocr = easyocr
            self.reader = easyocr.Reader(
                self._ocr_lang,
                gpu=False,
                verbose=False,
                download_enabled=True,
            )
            self.ocr_available = True
            logger.info("EasyOCR module loaded successfully")
        except ImportError as e:
            logger.warning(f"EasyOCR not available: {e}")
        except Exception as e:
            logger.warning(f"EasyOCR init error: {e}")

    def _load_ocr_languages(self) -> List[str]:
        raw = os.getenv("OCR_LANG", "en,ru").strip()
        langs = [lang.strip() for lang in raw.split(",") if lang.strip()]
        if not langs:
            return ["en", "ru"]
        return langs

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
        if not self.ocr_available or not self.reader:
            return ""
        try:
            results = self.reader.readtext(photo_path, detail=0)
            return "\n".join(results)
        except Exception as e:
            logger.error(f"EasyOCR error: {e}")
            return ""

    def extract_teams_and_score(self, photo_path: str) -> Optional[Dict]:
        """Распознаёт названия команд и счёт из скриншота FC Mobile.

        Использует координаты зон, адаптированные под разрешение изображения.

        Returns:
            {"team1": str, "team2": str, "score": str} или None при ошибке.
        """
        if not self.ocr_available or not self.reader:
            return None
        try:
            import cv2
        except ImportError:
            logger.warning("OpenCV not available for coordinate-based extraction")
            return None

        try:
            img = cv2.imread(photo_path)
            if img is None:
                return None
            h, w = img.shape[:2]
            zones = get_zones_for_resolution(w, h)
            if not zones:
                logger.warning(f"Unknown resolution {w}x{h}")
                return None

            result = {}
            for label, (x1, y1, x2, y2) in zones.items():
                roi = img[y1:y2, x1:x2]
                ocr_result = self.reader.readtext(roi, detail=1)
                if ocr_result:
                    text = ocr_result[0][1].strip()
                    result[label] = text

            if len(result) >= 3:
                return {
                    "team1": result.get("team1", ""),
                    "score": result.get("score", ""),
                    "team2": result.get("team2", ""),
                }
            return None
        except Exception as e:
            logger.error(f"Error in extract_teams_and_score: {e}")
            return None

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

        lines_list = [normalize_ocr_chars(line.strip()) for line in text.split('\n') if line.strip()]
        candidates: List[Tuple[int, int, int]] = []
        cyrillic = "\u0410-\u044f"
        score_pattern = re.compile(
            r'(?<![\dA-Za-z' + cyrillic + r'])(\d{1,2})\s*[:\-\u2013\u2014]\s*(\d{1,2})(?![\dA-Za-z' + cyrillic + r'])'
        )
        for idx, line in enumerate(lines_list[:15]):
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
                aggregated[key] = {"sum_conf": confidence, "count": 1, "max_conf": confidence}
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

            if score_gap < 20 and best_stats["count"] == 1:
                return None, None

        return best_pair[0], best_pair[1]

    def normalize_nick(self, value: str) -> str:
        if not value:
            return ""
        norm = unicodedata.normalize("NFKC", value).lower().replace('@', '').strip()
        return ''.join(ch for ch in norm if ch.isalnum())

    def extract_nick_tokens(self, text: str) -> List[str]:
        if not text:
            return []

        tokens = []
        seen = set()
        cyrillic = "\u0410-\u044f"
        patterns = [
            r'@([A-Za-z' + cyrillic + r'0-9_.\-]{2,32})',
            r'\b([A-Za-z' + cyrillic + r'0-9_.\-]{3,32})\b',
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
