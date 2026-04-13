import os
import unicodedata
from datetime import datetime
from typing import Dict, List, Tuple

from PIL import Image, ImageDraw, ImageFont


class GraphicsRenderer:
    def __init__(self, output_dir: str = "tmp/graphics"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self._font_cache = {}

    def _palette(self, theme: str) -> Dict[str, Tuple[int, int, int]]:
        if theme == "bright":
            return {
                "bg": (245, 249, 255),
                "panel": (255, 255, 255),
                "title": (8, 47, 122),
                "text": (30, 41, 59),
                "muted": (71, 85, 105),
                "line": (147, 197, 253),
                "accent": (2, 132, 199),
            }
        return {
            "bg": (249, 250, 251),
            "panel": (255, 255, 255),
            "title": (17, 24, 39),
            "text": (31, 41, 55),
            "muted": (75, 85, 99),
            "line": (209, 213, 219),
            "accent": (55, 65, 81),
        }

    def _font(self, size: int) -> ImageFont.FreeTypeFont:
        if size in self._font_cache:
            return self._font_cache[size]

        candidates = [
            "DejaVuSans.ttf",
            "NotoSans-Regular.ttf",
            "LiberationSans-Regular.ttf",
            "arial.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
            "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
            "C:/Windows/Fonts/arial.ttf",
            "C:/Windows/Fonts/segoeui.ttf",
        ]
        for name in candidates:
            try:
                font = ImageFont.truetype(name, size)
                self._font_cache[size] = font
                return font
            except Exception:
                continue

        fallback = ImageFont.load_default()
        self._font_cache[size] = fallback
        return fallback

    def _clean_text(self, value: str) -> str:
        if value is None:
            return ""
        text = unicodedata.normalize("NFKC", str(value))
        text = text.replace("\ufe0f", "")
        return "".join(ch for ch in text if unicodedata.category(ch) != "Cc")

    def _fit_text(self, draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, max_width: int) -> str:
        text = self._clean_text(text)
        if draw.textlength(text, font=font) <= max_width:
            return text

        ellipsis = "..."
        if draw.textlength(ellipsis, font=font) > max_width:
            return ""

        lo, hi = 0, len(text)
        while lo < hi:
            mid = (lo + hi + 1) // 2
            candidate = text[:mid] + ellipsis
            if draw.textlength(candidate, font=font) <= max_width:
                lo = mid
            else:
                hi = mid - 1
        return text[:lo] + ellipsis

    def _draw_right(self, draw: ImageDraw.ImageDraw, right_x: int, y: int, text: str, font: ImageFont.FreeTypeFont, fill):
        value = self._clean_text(text)
        w = draw.textlength(value, font=font)
        draw.text((right_x - w, y), value, fill=fill, font=font)

    def _output_path(self, prefix: str) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        return os.path.join(self.output_dir, f"{prefix}_{ts}.png")

    def render_groups_table_image(
        self,
        tournament_name: str,
        groups_data: Dict[str, List[Dict]],
        theme: str = "minimal",
        orientation: str = "vertical",
    ) -> str:
        palette = self._palette(theme)
        title_font = self._font(38)
        header_font = self._font(24)
        body_font = self._font(20)
        small_font = self._font(16)

        width = 1200 if orientation == "vertical" else 1800
        height = 2200 if orientation == "vertical" else 1300

        image = Image.new("RGB", (width, height), palette["bg"])
        draw = ImageDraw.Draw(image)

        draw.text((40, 30), "ГРУППОВОЙ ЭТАП", fill=palette["title"], font=title_font)
        draw.text((40, 82), self._clean_text(tournament_name), fill=palette["muted"], font=small_font)
        draw.line((40, 115, width - 40, 115), fill=palette["line"], width=3)

        groups_order = ["A", "B", "C", "D"]
        if orientation == "vertical":
            x = 40
            y = 140
            block_h = 480
            block_w = width - 80

            for group in groups_order:
                self._draw_group_block(
                    draw,
                    x,
                    y,
                    block_w,
                    block_h,
                    group,
                    groups_data.get(group, []),
                    palette,
                    header_font,
                    body_font,
                    small_font,
                )
                y += block_h + 20
        else:
            pad = 40
            gap = 20
            col_w = (width - pad * 2 - gap) // 2
            block_h = 520
            positions = [(pad, 140), (pad + col_w + gap, 140), (pad, 140 + block_h + 20), (pad + col_w + gap, 140 + block_h + 20)]

            for idx, group in enumerate(groups_order):
                x, y = positions[idx]
                self._draw_group_block(
                    draw,
                    x,
                    y,
                    col_w,
                    block_h,
                    group,
                    groups_data.get(group, []),
                    palette,
                    header_font,
                    body_font,
                    small_font,
                )

        path = self._output_path("groups")
        image.save(path, format="PNG")
        return path

    def _draw_group_block(
        self,
        draw: ImageDraw.ImageDraw,
        x: int,
        y: int,
        w: int,
        h: int,
        group: str,
        players: List[Dict],
        palette: Dict[str, Tuple[int, int, int]],
        header_font: ImageFont.FreeTypeFont,
        body_font: ImageFont.FreeTypeFont,
        small_font: ImageFont.FreeTypeFont,
    ):
        draw.rounded_rectangle((x, y, x + w, y + h), radius=18, fill=palette["panel"], outline=palette["line"], width=2)
        draw.text((x + 18, y + 14), f"ГРУППА {group}", fill=palette["title"], font=header_font)
        draw.line((x + 16, y + 56, x + w - 16, y + 56), fill=palette["line"], width=2)

        header_y = y + 70
        nick_x = x + 18
        mp_r = x + w - 350
        wins_r = x + w - 300
        losses_r = x + w - 250
        draws_r = x + w - 200
        goals_r = x + w - 125
        points_r = x + w - 50

        draw.text((nick_x, header_y), "Игрок", fill=palette["muted"], font=small_font)
        self._draw_right(draw, mp_r, header_y, "И", small_font, palette["muted"])
        self._draw_right(draw, wins_r, header_y, "В", small_font, palette["muted"])
        self._draw_right(draw, losses_r, header_y, "П", small_font, palette["muted"])
        self._draw_right(draw, draws_r, header_y, "Н", small_font, palette["muted"])
        self._draw_right(draw, goals_r, header_y, "Мячи", small_font, palette["muted"])
        self._draw_right(draw, points_r, header_y, "О", small_font, palette["muted"])
        draw.line((x + 16, header_y + 26, x + w - 16, header_y + 26), fill=palette["line"], width=1)

        row_y = header_y + 34
        row_h = 38

        for idx, p in enumerate(players[:10], 1):
            raw_nick = p.get("ingame_nick") or "?"
            mp = p.get("matches_played", 0)
            wins = p.get("wins", 0)
            losses = p.get("losses", 0)
            draws = p.get("draws", 0)
            gs = p.get("goals_scored", 0)
            gc = p.get("goals_conceded", 0)
            points = p.get("points", wins * 3 + draws)

            nick = self._fit_text(draw, f"{idx}. {raw_nick}", body_font, max_width=(mp_r - nick_x - 16))

            if idx % 2 == 0:
                draw.rectangle((x + 12, row_y - 2, x + w - 12, row_y + row_h - 2), fill=(250, 251, 253))

            draw.text((nick_x, row_y), nick, fill=palette["text"], font=body_font)
            self._draw_right(draw, mp_r, row_y, str(mp), body_font, palette["text"])
            self._draw_right(draw, wins_r, row_y, str(wins), body_font, palette["text"])
            self._draw_right(draw, losses_r, row_y, str(losses), body_font, palette["text"])
            self._draw_right(draw, draws_r, row_y, str(draws), body_font, palette["text"])
            self._draw_right(draw, goals_r, row_y, f"{gs}:{gc}", body_font, palette["text"])
            self._draw_right(draw, points_r, row_y, str(points), body_font, palette["accent"])

            row_y += row_h

    def render_playoff_bracket_image(
        self,
        tournament_name: str,
        stages_data: List[Tuple[str, int, List[Dict]]],
        theme: str = "minimal",
        orientation: str = "vertical",
    ) -> str:
        palette = self._palette(theme)
        title_font = self._font(38)
        header_font = self._font(24)
        body_font = self._font(20)
        small_font = self._font(16)

        width = 1200 if orientation == "vertical" else 1900
        height = 1900 if orientation == "vertical" else 1200

        image = Image.new("RGB", (width, height), palette["bg"])
        draw = ImageDraw.Draw(image)

        draw.text((40, 30), "ПЛЕЙ-ОФФ", fill=palette["title"], font=title_font)
        draw.text((40, 82), self._clean_text(tournament_name), fill=palette["muted"], font=small_font)
        draw.line((40, 115, width - 40, 115), fill=palette["line"], width=3)

        if orientation == "vertical":
            x, y = 40, 140
            block_w = width - 80
            for stage, wins_needed, matches in stages_data:
                block_h = max(140, 72 + max(1, len(matches)) * 38)
                self._draw_stage_block(draw, x, y, block_w, block_h, stage, wins_needed, matches, palette, header_font, body_font)
                y += block_h + 18
        else:
            pad = 40
            gap = 20
            col_w = (width - pad * 2 - gap * 4) // 5
            y = 150
            for idx, (stage, wins_needed, matches) in enumerate(stages_data):
                x = pad + idx * (col_w + gap)
                block_h = height - 220
                self._draw_stage_block(draw, x, y, col_w, block_h, stage, wins_needed, matches, palette, header_font, body_font)

        path = self._output_path("playoff")
        image.save(path, format="PNG")
        return path

    def _draw_stage_block(
        self,
        draw: ImageDraw.ImageDraw,
        x: int,
        y: int,
        w: int,
        h: int,
        stage: str,
        wins_needed: int,
        matches: List[Dict],
        palette: Dict[str, Tuple[int, int, int]],
        header_font: ImageFont.FreeTypeFont,
        body_font: ImageFont.FreeTypeFont,
    ):
        stage_title = stage.upper() if stage not in ("final", "bronze") else ("ФИНАЛ" if stage == "final" else "БРОНЗА")

        draw.rounded_rectangle((x, y, x + w, y + h), radius=18, fill=palette["panel"], outline=palette["line"], width=2)
        draw.text((x + 14, y + 10), f"{stage_title}", fill=palette["title"], font=header_font)
        draw.text((x + 14, y + 42), f"До {wins_needed} побед", fill=palette["muted"], font=body_font)
        draw.line((x + 12, y + 72, x + w - 12, y + 72), fill=palette["line"], width=1)

        row_y = y + 82
        for m in matches:
            p1 = self._fit_text(draw, m.get("player1_nick") or "?", body_font, max_width=130)
            p2 = self._fit_text(draw, m.get("player2_nick") or "?", body_font, max_width=130)
            w1 = m.get("player1_wins", 0)
            w2 = m.get("player2_wins", 0)
            status = m.get("status", "pending")
            mark = "✅" if status == "completed" else ("⚽" if (w1 > 0 or w2 > 0) else "•")
            draw.text((x + 14, row_y), f"{mark} {p1} {w1}-{w2} {p2}", fill=palette["text"], font=body_font)
            row_y += 34
