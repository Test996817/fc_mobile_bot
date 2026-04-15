import logging
import os
import re
import unicodedata
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


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
                paragraph_text=False,
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

    def extract_fc_match_info(self, photo_path: str) -> Optional[Dict]:
        """
        Extracts player nicks and score from FC Mobile screenshot using coordinates.
        Returns: {"player1_nick": str, "player2_nick": str, "score1": int, "score2": int}
        or None if recognition failed.
        """
        if not self.ocr_available or not self.reader:
            return None
        try:
            results = self.reader.readtext(photo_path, detail=1)
        except Exception as e:
            logger.error(f"EasyOCR error in extract_fc_match_info: {e}")
            return None
        if not results:
            return None
        score_pattern = re.compile(r'^(\d{1,2})\s*[-\u2013\u2014:]\s*(\d{1,2})$')
        sorted_results = sorted(results, key=lambda r: (r[0][0][1], -r[2]))
        score_info = None
        for bbox, text, conf in sorted_results:
            text_clean = text.strip()
            m = score_pattern.match(text_clean)
            if m:
                s1, s2 = int(m.group(1)), int(m.group(2))
                if 0 <= s1 <= self.max_plausible_score and 0 <= s2 <= self.max_plausible_score:
                    center_x = (bbox[0][0] + bbox[2][0]) / 2
                    center_y = (bbox[0][1] + bbox[2][1]) / 2
                    score_info = {"score1": s1, "score2": s2, "center_x": center_x, "center_y": center_y}
                    break
        if not score_info:
            return None
        nick_candidates_left = []
        nick_candidates_right = []
        for bbox, text, conf in results:
            text_clean = text.strip()
            if not text_clean or len(text_clean) < 2:
                continue
            if score_pattern.match(text_clean):
                continue
            if text_clean.isdigit() and len(text_clean) <= 2:
                continue
            skip_words = {"GOAL", "CHANCES", "MY", "THEIR", "GREAT", "GOOD", "BASIC", "CONTINUE", "DARK", "BEASTS", "Aura", "Power"}
            if text_clean.upper() in skip_words:
                continue
            center_x = (bbox[0][0] + bbox[2][0]) / 2
            center_y = (bbox[0][1] + bbox[2][1]) / 2
            y_diff = abs(center_y - score_info["center_y"])
            if y_diff > 100:
                continue
            if center_x < score_info["center_x"]:
                nick_candidates_left.append((text_clean, conf, center_x))
            elif center_x > score_info["center_x"]:
                nick_candidates_right.append((text_clean, conf, center_x))

        def pick_best_nick(candidates: List[Tuple[str, float, float]]) -> Optional[str]:
            if not candidates:
                return None
            candidates.sort(key=lambda c: (-c[1], abs(c[2] - score_info["center_x"])))
            nick = candidates[0][0]
            nick = re.sub(r'[^A-Za-z0-9_.\-]', '', nick)
            return nick if nick else None

        player1_nick = pick_best_nick(nick_candidates_left)
        player2_nick = pick_best_nick(nick_candidates_right)
        if not player1_nick or not player2_nick:
            return None
        return {
            "player1_nick": player1_nick,
            "player2_nick": player2_nick,
            "score1": score_info["score1"],
            "score2": score_info["score2"],
        }

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
            r'@([A-Za-z0' + cyrillic + r'0-9_.\-]{2,32})',
            r'\b([A-Za-z0' + cyrillic + r'0-9_.\-]{3,32})\b',
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
