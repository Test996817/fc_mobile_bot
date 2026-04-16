import logging
import os
import re
import random
import asyncio
import html
import shutil
import unicodedata

from difflib import SequenceMatcher
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)

from services import EloCalculator, ScreenshotAnalyzer, GraphicsRenderer

USE_POSTGRES = bool(os.getenv('DATABASE_URL'))
if USE_POSTGRES:
    from db.postgres import Database
    from db.postgres import AVAILABLE_FORMATS as AVAILABLE_FORMATS
else:
    from db.sqlite import Database
    from db.sqlite import AVAILABLE_FORMATS

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class TournamentBot:
    def __init__(self, token: str):
        self.token = token
        self.db = Database()
        self.elo = EloCalculator()
        self.graphics = GraphicsRenderer()
        self.screenshot_analyzer = ScreenshotAnalyzer()
        self.application = Application.builder().token(token).build()
        self.admin_notifications = {}
        self.cooldowns = {}
        self.media_groups_buffer = {}
        self.media_groups_tasks = {}
        self.setup_handlers()
    
    def setup_handlers(self):
        self.application.add_handler(CommandHandler("admin", self.cmd_admin))
        self.application.add_handler(CommandHandler("tournament_create", self.cmd_create_tournament))
        self.application.add_handler(CommandHandler("tournament_start", self.cmd_start_tournament))
        self.application.add_handler(CommandHandler("tournament_end", self.cmd_end_tournament))
        self.application.add_handler(CommandHandler("allmatches", self.cmd_matches))
        self.application.add_handler(CommandHandler("playoff", self.cmd_playoff))
        self.application.add_handler(CommandHandler("pw", self.cmd_playoff_win))
        self.application.add_handler(CommandHandler("elo", self.cmd_elo))
        self.application.add_handler(CommandHandler("tp", self.cmd_tech_loss))
        self.application.add_handler(CommandHandler("replace", self.cmd_replace))
        self.application.add_handler(CommandHandler("removeplayer", self.cmd_remove_player))
        self.application.add_handler(CommandHandler("cancelmatch", self.cmd_cancel_match))
        self.application.add_handler(CommandHandler("notifyall", self.cmd_notify_all))
        self.application.add_handler(CommandHandler("gresult", self.cmd_gresult))
        self.application.add_handler(CommandHandler("refreshreg", self.cmd_refresh_reg))
        self.application.add_handler(CommandHandler("regen_matches", self.cmd_regen_matches))
        self.application.add_handler(CommandHandler("resetelo", self.cmd_resetelo))
        self.application.add_handler(CommandHandler("rewrite", self.cmd_rewrite_result))
        self.application.add_handler(CommandHandler("tinfo", self.cmd_tinfo))
        self.application.add_handler(CommandHandler("dbstats", self.cmd_dbstats))
        self.application.add_handler(CommandHandler("finalpost", self.cmd_finalpost))
        self.application.add_handler(CommandHandler("gtable", self.cmd_groups_graphic))
        self.application.add_handler(CommandHandler("pbracket", self.cmd_playoff_graphic))
        self.application.add_handler(CommandHandler("resend_groups", self.cmd_resend_groups))
        self.application.add_handler(CommandHandler("undo_playoff", self.cmd_undo_playoff))

        self.application.add_handler(MessageHandler(
            filters.Regex(r'^!nick\s+(\S.+)'), 
            self.cmd_set_nick
        ))
        self.application.add_handler(MessageHandler(
            filters.Regex(r'^!profile'), 
            self.cmd_profile
        ))
        self.application.add_handler(MessageHandler(
            filters.Regex(r'^!matches'), 
            self.cmd_my_matches
        ))
        self.application.add_handler(MessageHandler(
            filters.Regex(r'^!commands'), 
            self.cmd_commands
        ))
        self.application.add_handler(MessageHandler(
            filters.Regex(r'^RESULTS_TOPIC$'), 
            self.handle_results_topic_message
        ))
        self.application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.Regex(r'(?i)^\s*\+\s*рез\b'),
            self.handle_gresult_text
        ))
        self.application.add_handler(MessageHandler(filters.PHOTO, self.handle_photo))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
        self.application.add_error_handler(self.handle_error)

    async def handle_error(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        logger.exception("Unhandled exception while processing update", exc_info=context.error)

    async def notify_admin(self, chat_id: int, message: str, message_thread_id: Optional[int] = None):
        try:
            kwargs = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
            if message_thread_id:
                kwargs["message_thread_id"] = message_thread_id
            await self.application.bot.send_message(**kwargs)
        except Exception as e:
            logger.error(f"Failed to send admin notification: {e}")

    def _copyable_nick(self, value: str) -> str:
        return f"<code>{html.escape(value or '?')}</code>"

    def _display_nick(self, value: str) -> str:
        return (value or '?').strip() or '?'

    def _format_match_result_block(
        self,
        match_id: int,
        p1_nick: str,
        score1: int,
        score2: int,
        p2_nick: str,
        winner_name: str,
        p1_old_rating: int,
        p1_new_rating: int,
        p2_old_rating: int,
        p2_new_rating: int,
    ) -> str:
        p1_delta = p1_new_rating - p1_old_rating
        p2_delta = p2_new_rating - p2_old_rating
        plain = (
            f"✅ Матч #{match_id}: {p1_nick} {score1}:{score2} {p2_nick}\n"
            f"🏆 {winner_name}\n"
            f"📈 ELO: {p1_nick} {p1_old_rating}→{p1_new_rating} ({p1_delta:+d}) | "
            f"{p2_nick} {p2_old_rating}→{p2_new_rating} ({p2_delta:+d})"
        )
        return self.as_monospace_block(plain)
    
    async def cmd_commands(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (
            "📋 КОМАНДЫ:\n\n"
            "!nick [ник] - установить игровой ник\n"
            "!profile - твой профиль и статистика\n"
            "!matches - твои матчи\n"
            "/elo - таблица рейтинга\n\n"
            "📸 Отправь 4 скриншота с результатом в топик результатов:\n"
            "@Player1 - @Player2"
        )
        await update.message.reply_text(text)
    
    async def handle_results_topic_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        topic_id = update.message.message_thread_id
        if not topic_id:
            await update.message.reply_text("Эта команда должна быть отправлена в топике.")
            return
        
        self.db.update_tournament_results_topic(tournament['id'], topic_id)
        await update.message.reply_text(f"✅ Топик результатов сохранён (ID: {topic_id})")


    def generate_groups_table(self, tournament_id: int) -> str:
        text = "━━━━━━━━━━━━━━━━━━━━\n🏆 ГРУППОВОЙ ЭТАП\n━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for group_key in ['A', 'B', 'C', 'D']:
            standings = self.db.get_group_standings(tournament_id, f"Группа {group_key}")
            
            text += f"📊 ГРУППА {group_key}\n"
            text += "━━━━━━━━━━━━━━━━━━━━\n"
            text += "Игрок          | И | В | П | Н | О | Мячи\n"
            text += "━━━━━━━━━━━━━━━━━━━━\n"
            
            if standings:
                for p in standings:
                    nick = (p.get('ingame_nick') or '?')[:14]
                    nick = nick.ljust(14)
                    matches = p.get('matches_played', 0)
                    wins = p.get('wins', 0)
                    losses = p.get('losses', 0)
                    draws = p.get('draws', 0)
                    points = p.get('points', 0)
                    gs = p.get('goals_scored', 0)
                    gc = p.get('goals_conceded', 0)
                    text += f"{nick} | {matches} | {wins} | {losses} | {draws} | {points} | {gs}:{gc}\n"
            else:
                text += "Пусто\n"
            
            text += "\n"
        
        return text.rstrip()

    def parse_visual_options(self, args: List[str]) -> Tuple[str, str, Optional[str]]:
        theme = "minimal"
        orientation = "vertical"

        valid_themes = {"minimal", "bright"}
        valid_orientations = {"vertical", "horizontal"}

        if len(args) >= 1:
            val = args[0].lower()
            if val in valid_themes:
                theme = val
            elif val in valid_orientations:
                orientation = val
            else:
                return theme, orientation, "Неверная тема/ориентация"

        if len(args) >= 2:
            val = args[1].lower()
            if val in valid_orientations:
                orientation = val
            elif val in valid_themes and len(args) == 2:
                theme = val
            else:
                return theme, orientation, "Неверная ориентация"

        return theme, orientation, None

    def get_groups_data(self, tournament_id: int) -> Dict[str, List[Dict]]:
        data = {}
        for group_key in ["A", "B", "C", "D"]:
            data[group_key] = self.db.get_group_standings(tournament_id, f"Группа {group_key}")
        return data

    def get_playoff_stages_data(self, tournament_id: int) -> List[Tuple[str, int, List[Dict]]]:
        stages = [("1/8", 3), ("1/4", 3), ("1/2", 4), ("bronze", 4), ("final", 4)]
        output = []
        for stage, wins_needed in stages:
            matches = self.db.get_playoff_matches(tournament_id, stage)
            output.append((stage, wins_needed, matches))
        return output

    def as_monospace_block(self, text: str) -> str:
        return f"<pre>{html.escape(text)}</pre>"

    async def send_groups_graphic(
        self,
        chat_id: int,
        tournament: Dict,
        theme: str = "minimal",
        orientation: str = "vertical",
        message_thread_id: int = None,
        create_if_missing: bool = True,
    ) -> bool:
        path = None
        try:
            groups_data = self.get_groups_data(tournament["id"])
            path = self.graphics.render_groups_table_image(
                tournament_name=tournament["name"],
                groups_data=groups_data,
                theme=theme,
                orientation=orientation,
            )
            target_message_id = tournament.get("groups_graphic_message_id")

            if target_message_id:
                try:
                    with open(path, "rb") as img:
                        media = InputMediaPhoto(
                            media=img,
                            caption=f"📊 Таблица групп ({theme}/{orientation})",
                        )
                        await self.application.bot.edit_message_media(
                            chat_id=chat_id,
                            message_id=target_message_id,
                            media=media,
                        )
                except Exception as edit_err:
                    logger.warning(f"Could not edit groups graphic message {target_message_id}: {edit_err}")
                    if not create_if_missing:
                        return False
                    with open(path, "rb") as img:
                        sent = await self.application.bot.send_photo(
                            chat_id=chat_id,
                            photo=img,
                            caption=f"📊 Таблица групп ({theme}/{orientation})",
                            message_thread_id=message_thread_id,
                        )
                    self.db.update_tournament_groups_graphic_message(tournament["id"], sent.message_id)
            else:
                if not create_if_missing:
                    return False
                with open(path, "rb") as img:
                    sent = await self.application.bot.send_photo(
                        chat_id=chat_id,
                        photo=img,
                        caption=f"📊 Таблица групп ({theme}/{orientation})",
                        message_thread_id=message_thread_id,
                    )
                self.db.update_tournament_groups_graphic_message(tournament["id"], sent.message_id)
            return True
        except Exception as e:
            logger.error(f"Error sending groups graphic: {e}")
            return False
        finally:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    async def send_playoff_graphic(
        self,
        chat_id: int,
        tournament: Dict,
        theme: str = "minimal",
        orientation: str = "vertical",
        message_thread_id: int = None,
        create_if_missing: bool = True,
    ) -> bool:
        path = None
        try:
            stages_data = self.get_playoff_stages_data(tournament["id"])
            path = self.graphics.render_playoff_bracket_image(
                tournament_name=tournament["name"],
                stages_data=stages_data,
                theme=theme,
                orientation=orientation,
            )
            target_message_id = tournament.get("playoff_graphic_message_id")

            if target_message_id:
                try:
                    with open(path, "rb") as img:
                        media = InputMediaPhoto(
                            media=img,
                            caption=f"🏆 Сетка плей-офф ({theme}/{orientation})",
                        )
                        await self.application.bot.edit_message_media(
                            chat_id=chat_id,
                            message_id=target_message_id,
                            media=media,
                        )
                except Exception as edit_err:
                    logger.warning(f"Could not edit playoff graphic message {target_message_id}: {edit_err}")
                    if not create_if_missing:
                        return False
                    with open(path, "rb") as img:
                        sent = await self.application.bot.send_photo(
                            chat_id=chat_id,
                            photo=img,
                            caption=f"🏆 Сетка плей-офф ({theme}/{orientation})",
                            message_thread_id=message_thread_id,
                        )
                    self.db.update_tournament_playoff_graphic_message(tournament["id"], sent.message_id)
            else:
                if not create_if_missing:
                    return False
                with open(path, "rb") as img:
                    sent = await self.application.bot.send_photo(
                        chat_id=chat_id,
                        photo=img,
                        caption=f"🏆 Сетка плей-офф ({theme}/{orientation})",
                        message_thread_id=message_thread_id,
                    )
                self.db.update_tournament_playoff_graphic_message(tournament["id"], sent.message_id)
            return True
        except Exception as e:
            logger.error(f"Error sending playoff graphic: {e}")
            return False
        finally:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass
    
    async def send_groups_table(self, chat_id: int, tournament_id: int, message_thread_id: int = None) -> Tuple[int, int]:
        text = self.as_monospace_block(self.generate_groups_table(tournament_id))
        
        try:
            send_kwargs = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
            }
            if message_thread_id:
                send_kwargs["message_thread_id"] = message_thread_id

            msg = await self.application.bot.send_message(**send_kwargs)

            reply_kwargs = {"text": "📋 Отправьте скриншоты сюда"}
            if message_thread_id:
                reply_kwargs["message_thread_id"] = message_thread_id
            topic_msg = await msg.reply_text(**reply_kwargs)
            topic_id = topic_msg.message_thread_id
            
            self.db.update_tournament_groups_info(tournament_id, topic_id or 0, msg.message_id)
            
            return topic_id, msg.message_id
        except Exception as e:
            logger.error(f"Error sending groups table: {e}")
            return 0, 0
    
    async def update_groups_table(self, chat_id: int, message_id: int, tournament_id: int):
        text = self.as_monospace_block(self.generate_groups_table(tournament_id))
        
        try:
            await self.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode="HTML",
            )
            return True
        except Exception as e:
            logger.error(f"Error updating groups table: {e}")
            return False

    async def send_groups_table_message(self, chat_id: int, tournament_id: int, message_thread_id: int = None) -> int:
        text = self.as_monospace_block(self.generate_groups_table(tournament_id))
        try:
            kwargs = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
            }
            if message_thread_id:
                kwargs["message_thread_id"] = message_thread_id

            msg = await self.application.bot.send_message(**kwargs)
            topic_id = message_thread_id or msg.message_thread_id or 0
            self.db.update_tournament_groups_info(tournament_id, topic_id, msg.message_id)
            return msg.message_id
        except Exception as e:
            logger.error(f"Error sending tracked groups table: {e}")
            return 0

    async def cmd_resend_groups(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)

        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return

        target_thread_id = tournament.get('results_topic_id') or update.message.message_thread_id
        if not target_thread_id:
            await update.message.reply_text(
                "Не задан топик результатов. Отправьте 'RESULTS_TOPIC' в нужный топик."
            )
            return

        topic_id, message_id = await self.send_groups_table(
            chat_id,
            tournament['id'],
            message_thread_id=target_thread_id,
        )

        if not message_id:
            await update.message.reply_text("❌ Не удалось переотправить таблицу групп.")
            return

        await update.message.reply_text(
            f"✅ Таблица групп переотправлена в топик {topic_id or target_thread_id}."
        )

    def submit_playoff_elo_games(self, p1_nick: str, p2_nick: str, p1_wins: int, p2_wins: int,
                                 match_id: int, p1_before: int, p2_before: int,
                                 status: str = 'completed') -> Tuple[int, int]:
        p1 = self.db.get_player_by_nick(p1_nick)
        p2 = self.db.get_player_by_nick(p2_nick)
        if not p1 or not p2:
            return 0, 0

        p1_total_delta = 0
        p2_total_delta = 0

        for _ in range(p1_wins):
            new_r1, new_r2, _ = self.elo.calculate(p1['rating'], p2['rating'], 1.0)
            delta = new_r1 - p1['rating']
            p1_total_delta += delta
            p2_total_delta -= delta
            self.db.update_player_stats(p1['user_id'], 'win', 0, 0, delta)
            self.db.update_player_stats(p2['user_id'], 'loss', 0, 0, new_r2 - p2['rating'])
            p1['rating'] = new_r1
            p2['rating'] = new_r2

        for _ in range(p2_wins):
            new_r1, new_r2, _ = self.elo.calculate(p1['rating'], p2['rating'], 0.0)
            delta = new_r2 - p2['rating']
            p2_total_delta += delta
            p1_total_delta -= delta
            self.db.update_player_stats(p2['user_id'], 'win', 0, 0, delta)
            self.db.update_player_stats(p1['user_id'], 'loss', 0, 0, new_r1 - p1['rating'])
            p1['rating'] = new_r1
            p2['rating'] = new_r2

        self.db.update_playoff_match(
            match_id,
            p1_wins,
            p2_wins,
            status,
            player1_elo_before=p1_before,
            player2_elo_before=p2_before,
            elo_applied=True
        )

        return p1_total_delta, p2_total_delta

    async def _submit_playoff_result(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                      tournament: Dict, nick1: str, score1: int, score2: int, nick2: str):
        if score1 == score2:
            await update.message.reply_text("❌ Ничья невозможна в плей-офф.")
            return

        def nick_key(n: str) -> str:
            return (n or "").strip().casefold()

        n1_key = nick_key(nick1)
        n2_key = nick_key(nick2)

        all_stages = ['1/8', '1/4', '1/2', 'bronze', 'final']
        found = []

        for stage in all_stages:
            matches = self.db.get_playoff_matches(tournament['id'], stage)
            for m in matches:
                if m.get('status') not in ('pending', 'in_progress'):
                    continue
                p1_nick = m.get('player1_nick') or ''
                p2_nick = m.get('player2_nick') or ''
                m1_key = nick_key(p1_nick)
                m2_key = nick_key(p2_nick)
                if (m1_key == n1_key and m2_key == n2_key) or (m1_key == n2_key and m2_key == n1_key):
                    found.append((stage, m))

        if not found:
            await update.message.reply_text("Нет ожидающего playoff-матча между этими игроками.")
            return

        if len(found) > 1:
            lines = ["⚠️ Найдено несколько матчей. Используй /pw:"]
            for stage, m in found:
                lines.append(f"/pw {stage} {m['match_num']} ...")
            await update.message.reply_text("\n".join(lines))
            return

        stage, playoff_match = found[0]
        wins_needed = 3 if stage in ('1/8', '1/4') else 4

        if nick_key(playoff_match.get('player1_nick') or '') == n1_key:
            p1_wins = score1
            p2_wins = score2
        else:
            p1_wins = score2
            p2_wins = score1

        p1 = self.db.get_player_by_nick(playoff_match['player1_nick'])
        p2 = self.db.get_player_by_nick(playoff_match['player2_nick'])
        p1_before = p1['rating'] if p1 else 0
        p2_before = p2['rating'] if p2 else 0

        status = 'completed' if (p1_wins >= wins_needed or p2_wins >= wins_needed) else 'in_progress'

        winner_nick = playoff_match['player1_nick'] if p1_wins > p2_wins else playoff_match['player2_nick']
        loser_nick = playoff_match['player2_nick'] if p1_wins > p2_wins else playoff_match['player1_nick']

        elo_delta_p1 = 0
        elo_delta_p2 = 0

        if status == 'completed':
            self.advance_playoff(tournament['id'], stage, playoff_match['match_num'], winner_nick, loser_nick)
            elo_delta_p1, elo_delta_p2 = self.submit_playoff_elo_games(
                playoff_match['player1_nick'],
                playoff_match['player2_nick'],
                p1_wins,
                p2_wins,
                playoff_match['id'],
                p1_before,
                p2_before,
            )

        bracket_text = self.format_playoff_bracket(tournament['id'])

        if tournament.get('playoff_message_id'):
            try:
                await context.bot.edit_message_text(
                    chat_id=tournament['chat_id'],
                    message_id=tournament['playoff_message_id'],
                    text=bracket_text,
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
            except Exception as e:
                logger.error(f"Error editing playoff bracket: {e}")

        result_text = f"✅ Записан результат {stage} #{playoff_match['match_num']}:\n"
        result_text += f"{playoff_match['player1_nick']} {p1_wins}-{p2_wins} {playoff_match['player2_nick']}\n"
        if status == 'completed':
            p1_sign = '+' if elo_delta_p1 >= 0 else ''
            p2_sign = '+' if elo_delta_p2 >= 0 else ''
            result_text += f"📈 ELO: {playoff_match['player1_nick']} {p1_sign}{elo_delta_p1} | {playoff_match['player2_nick']} {p2_sign}{elo_delta_p2}\n"
            if stage == 'final':
                result_text += f"🏆 {winner_nick} — чемпион турнира!"
            elif stage == 'bronze':
                result_text += f"🥉 {winner_nick} занимает 3 место!"
            else:
                result_text += f"🏆 {winner_nick} проходит в следующий раунд!"
        else:
            result_text += f"⏳ {wins_needed} побед для прохода. Текущий счёт: {p1_wins}-{p2_wins}"

        await update.message.reply_text(result_text)

    async def _submit_gresult(self, update: Update, context: ContextTypes.DEFAULT_TYPE, nick1: str, score_arg: str, nick2: str):
        score_match = re.match(r'(\d+)[-–:](\d+)', score_arg)
        if not score_match:
            await update.message.reply_text("Неверный формат счёта. Используйте: /gresult Player1 13-10 Player2")
            return

        score1 = int(score_match.group(1))
        score2 = int(score_match.group(2))

        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return

        p1 = self.db.get_player_by_nick(nick1)
        p2 = self.db.get_player_by_nick(nick2)

        if not p1:
            await update.message.reply_text(f"Игрок '{nick1}' не найден.")
            return
        if not p2:
            await update.message.reply_text(f"Игрок '{nick2}' не найден.")
            return

        match = self.db.find_match_between_players(tournament['id'], p1['user_id'], p2['user_id'])
        if not match:
            await self._submit_playoff_result(update, context, tournament, nick1, score1, score2, nick2)
            return

        entered_scores = {
            p1['user_id']: score1,
            p2['user_id']: score2,
        }
        match_score1 = entered_scores.get(match['player1_id'])
        match_score2 = entered_scores.get(match['player2_id'])
        if match_score1 is None or match_score2 is None:
            await update.message.reply_text(
                "❌ Введенная пара не соответствует участникам матча."
            )
            return

        if match_score1 > match_score2:
            winner_id = match['player1_id']
        elif match_score2 > match_score1:
            winner_id = match['player2_id']
        else:
            winner_id = None

        await self.process_match_result(
            match,
            match_score1,
            match_score2,
            winner_id,
            update.effective_user.id,
            send_notification=False,
        )

        p1_new = self.db.get_player(p1['user_id'])
        p2_new = self.db.get_player(p2['user_id'])

        p1_nick = self._display_nick(p1.get('ingame_nick'))
        p2_nick = self._display_nick(p2.get('ingame_nick'))
        if winner_id == p1['user_id']:
            winner_name = p1_nick
        elif winner_id == p2['user_id']:
            winner_name = p2_nick
        else:
            winner_name = "Ничья"

        text = self._format_match_result_block(
            match_id=match['id'],
            p1_nick=p1_nick,
            score1=score1,
            score2=score2,
            p2_nick=p2_nick,
            winner_name=winner_name,
            p1_old_rating=p1['rating'],
            p1_new_rating=p1_new['rating'],
            p2_old_rating=p2['rating'],
            p2_new_rating=p2_new['rating'],
        )

        output_thread_id = tournament.get('results_topic_id') or update.message.message_thread_id
        await self._send_results_reply(context, update.effective_chat.id, output_thread_id, text)

    async def cmd_gresult(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args or len(context.args) < 3:
            await update.message.reply_text(
                "Использование: /gresult Player1 13-10 Player2"
            )
            return

        nick1 = context.args[0]
        score_arg = context.args[1]
        nick2 = context.args[2]
        await self._submit_gresult(update, context, nick1, score_arg, nick2)

    async def handle_gresult_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or not update.message.text:
            return

        text = update.message.text.strip()
        match = re.match(r'(?i)^\+\s*рез\s+(\S+)\s+(\d+\s*[-–:]\s*\d+)\s+(\S+)\s*$', text)
        if not match:
            await update.message.reply_text("Использование: + рез Player1 13-10 Player2")
            return

        nick1 = match.group(1)
        score_arg = re.sub(r'\s+', '', match.group(2))
        nick2 = match.group(3)
        await self._submit_gresult(update, context, nick1, score_arg, nick2)
    
    async def cmd_refresh_reg(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        if tournament['status'] != 'registration':
            await update.message.reply_text("Регистрация закрыта.")
            return
        
        await self.send_join_message(update.effective_chat.id, tournament['id'])
        await update.message.reply_text("✅ Сообщение с регистрацией обновлено!")

    async def cmd_resetelo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        affected = self.db.reset_all_ratings(1000)
        await update.message.reply_text(
            f"✅ ELO сброшен до 1000 для {affected} игроков."
        )

    def recalculate_player_stats_and_elo(self):
        self.db.reset_all_player_stats_and_ratings(1000)
        completed_matches = self.db.get_completed_matches_ordered()

        for m in completed_matches:
            score1 = m.get('player1_score')
            score2 = m.get('player2_score')
            if score1 is None or score2 is None:
                continue

            p1 = self.db.get_player(m['player1_id'])
            p2 = self.db.get_player(m['player2_id'])
            if not p1 or not p2:
                continue

            if m.get('winner_id') is None or score1 == score2:
                result1 = result2 = 'draw'
                change1 = 0
                change2 = 0
            elif m.get('winner_id') == m['player1_id']:
                result1, result2 = 'win', 'loss'
                new_r1, new_r2, _ = self.elo.calculate(p1['rating'], p2['rating'], 1.0)
                change1 = new_r1 - p1['rating']
                change2 = new_r2 - p2['rating']
            else:
                result1, result2 = 'loss', 'win'
                new_r1, new_r2, _ = self.elo.calculate(p1['rating'], p2['rating'], 0.0)
                change1 = new_r1 - p1['rating']
                change2 = new_r2 - p2['rating']

            self.db.update_player_stats(m['player1_id'], result1, score1, score2, change1)
            self.db.update_player_stats(m['player2_id'], result2, score2, score1, change2)

    async def cmd_rewrite_result(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        if len(context.args) < 3:
            await update.message.reply_text("Использование: /rewrite Player1 13-10 Player2")
            return

        nick1 = context.args[0]
        score_arg = context.args[1]
        nick2 = context.args[2]

        score_match = re.match(r'(\d+)[-–:](\d+)', score_arg)
        if not score_match:
            await update.message.reply_text("Неверный формат счёта. Используйте: /rewrite Player1 13-10 Player2")
            return

        score1 = int(score_match.group(1))
        score2 = int(score_match.group(2))

        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return

        p1 = self.db.get_player_by_nick(nick1)
        p2 = self.db.get_player_by_nick(nick2)
        if not p1 or not p2:
            await update.message.reply_text("Один из игроков не найден.")
            return

        matches = self.db.get_tournament_matches(tournament['id'])
        pair_matches = [
            m for m in matches
            if (
                (m['player1_id'] == p1['user_id'] and m['player2_id'] == p2['user_id']) or
                (m['player1_id'] == p2['user_id'] and m['player2_id'] == p1['user_id'])
            )
        ]

        if not pair_matches:
            await update.message.reply_text("Матч между этими игроками не найден.")
            return

        target_match = sorted(pair_matches, key=lambda m: m['id'], reverse=True)[0]
        entered_scores = {
            p1['user_id']: score1,
            p2['user_id']: score2,
        }
        match_score1 = entered_scores.get(target_match['player1_id'])
        match_score2 = entered_scores.get(target_match['player2_id'])

        if match_score1 is None or match_score2 is None:
            await update.message.reply_text("❌ Не удалось сопоставить счёт с участниками матча.")
            return

        if match_score1 > match_score2:
            winner_id = target_match['player1_id']
        elif match_score2 > match_score1:
            winner_id = target_match['player2_id']
        else:
            winner_id = None

        self.db.update_match_result(
            target_match['id'],
            match_score1,
            match_score2,
            winner_id,
            update.effective_user.id,
            target_match.get('screenshot_id'),
        )
        self.recalculate_player_stats_and_elo()

        groups_message_id = tournament.get('groups_message_id')
        if groups_message_id:
            await self.update_groups_table(update.effective_chat.id, groups_message_id, tournament['id'])

        await update.message.reply_text(
            f"✅ Матч #{target_match['id']} перезаписан: {nick1} {score1}:{score2} {nick2}"
        )
    
    async def cmd_profile(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.message.chat.type == 'private':
            await update.message.reply_text("Эта команда работает только в группах.")
            return
        
        user_id = update.effective_user.id
        player = self.db.get_player(user_id)
        
        if not player or not player['ingame_nick']:
            await update.message.reply_text("Сначала установи ник: !nick [твой_ник]")
            return
        
        text = (
            f"👤 Профиль\n\n"
            f"Ник: {player['ingame_nick']}\n"
            f"Рейтинг ELO: {player['rating']}\n\n"
            f"📊 Статистика:\n"
            f"Побед: {player['wins']}\n"
            f"Поражений: {player['losses']}\n"
            f"Ничьих: {player['draws']}\n\n"
            f"⚽ Голы:\n"
            f"Забито: {player['goals_scored']}\n"
            f"Пропущено: {player['goals_conceded']}"
        )
        await update.message.reply_text(text)
    
    async def cmd_my_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        matches = self.db.get_player_matches(user_id, tournament['id'], 'pending')
        
        if not matches:
            await update.message.reply_text("У тебя нет ожидающих матчей.")
            return
        
        text = "⚽ Твои матчи:\n\n"
        
        for m in matches:
            opponent_id = m['player2_id'] if m['player1_id'] == user_id else m['player1_id']
            opponent = self.db.get_player(opponent_id)
            opponent_name = opponent['ingame_nick'] if opponent else "?"
            
            text += f"#{m['id']} vs {opponent_name}\n"
            if m['group_name']:
                text += f"   {m['group_name']}\n"
            text += "\n"
        
        await update.message.reply_text(text)
    
    async def cmd_set_nick(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        match = re.match(r'^!nick\s+(\S.+)', update.message.text)
        if not match:
            return
        
        nick = match.group(1).strip()
        user_id = update.effective_user.id
        
        if len(nick) < 2 or len(nick) > 30:
            await update.message.reply_text("Ник должен быть от 2 до 30 символов.")
            return
        
        old_player = self.db.get_player_by_nick(nick)
        if old_player and old_player['user_id'] != user_id:
            await update.message.reply_text("Этот ник уже используется другим игроком.")
            return
        
        self.db.add_player(user_id, update.effective_user.username or str(user_id), nick)
        
        await update.message.reply_text(f"✅ Ник установлен: {nick}")
    
    async def cmd_join(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.message.chat.type == 'private':
            return
        
        user_id = update.effective_user.id
        username = update.effective_user.username
        
        player = self.db.get_player(user_id)
        
        if not player or not player['ingame_nick']:
            await update.message.reply_text(
                "Сначала установи ник: !nick [твой_игровой_ник]\n"
                "Пример: !nick Ronaldo"
            )
            return
        
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира в этой группе.")
            return
        
        if tournament['status'] != 'registration':
            await update.message.reply_text("Регистрация на турнир закрыта.")
            return
        
        players = tournament['players']
        already_registered = any(p['user_id'] == user_id for p in players)
        
        if already_registered:
            await update.message.reply_text("Ты уже подал заявку на этот турнир!")
            return
        
        if tournament['max_players'] and len(players) >= tournament['max_players']:
            await update.message.reply_text("Турнир уже заполнен.")
            return
        
        self.db.add_player_to_tournament(tournament['id'], user_id, 'joined')
        
        await update.message.reply_text(
            f"✅ Ты присоединился к турниру!\n"
            f"Турнир: {tournament['name']}"
        )
        
        await self.update_join_message(chat_id, tournament['id'])
    
    async def cmd_leave(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        if tournament['status'] != 'registration':
            await update.message.reply_text("Нельзя покинуть турнир после его начала.")
            return
        
        self.db.remove_player_from_tournament(tournament['id'], user_id)
        await update.message.reply_text("Ты покинул турнир.")
        await self.update_join_message(chat_id, tournament['id'])
    
    async def cmd_report_result(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        import time
        match = re.match(r'^!(win|loss|draw)\s+@(\S+)', update.message.text)
        if not match:
            return
        
        result_type = match.group(1)
        opponent_nick = match.group(2)
        
        user_id = update.effective_user.id
        player = self.db.get_player(user_id)
        
        current_time = time.time()
        if user_id in self.cooldowns:
            last_submission = self.cooldowns[user_id]
            if current_time - last_submission < 180:
                remaining = int(180 - (current_time - last_submission))
                await update.message.reply_text(
                    f"⏳ Подожди {remaining} сек. перед следующей отправкой результата."
                )
                return
        
        if not player:
            await update.message.reply_text("Ты не зарегистрирован.")
            return
        
        opponent = self.db.get_player_by_nick(opponent_nick)
        if not opponent:
            await update.message.reply_text(f"Игрок @{opponent_nick} не найден.")
            return
        
        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        matches = self.db.get_player_matches(user_id, tournament['id'], 'pending')
        match_found = None
        
        for m in matches:
            if (m['player1_id'] == user_id and m['player2_id'] == opponent['user_id']) or \
               (m['player2_id'] == user_id and m['player1_id'] == opponent['user_id']):
                match_found = m
                break
        
        if not match_found:
            await update.message.reply_text("Нет ожидающего матча с этим игроком.")
            return
        
        if result_type == 'win':
            score1, score2 = 1, 0
            winner_id = user_id
        elif result_type == 'loss':
            score1, score2 = 0, 1
            winner_id = opponent['user_id']
        else:
            score1, score2 = 0, 0
            winner_id = None
        
        await self.process_match_result(match_found, score1, score2, winner_id, user_id)
        self.cooldowns[user_id] = current_time
        
        await update.message.reply_text(
            f"✅ Результат сообщен!\n"
            f"Счёт: {score1}:{score2}\n"
            f"Ожидай подтверждения."
        )
    
    async def handle_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.info(f"handle_photo called: chat={update.effective_chat.id}, user={update.effective_user.id}")
        msg = update.message
        if not msg or not msg.photo:
            logger.warning(f"handle_photo: no message or photo. msg={msg is not None}, photo={msg.photo if msg else None}")
            return

        photo = msg.photo[-1]
        media_group_id = msg.media_group_id

        if media_group_id:
            key = (update.effective_chat.id, msg.message_thread_id or 0, update.effective_user.id, media_group_id)
            payload = self.media_groups_buffer.get(key)
            if payload is None:
                payload = {
                    "chat_id": update.effective_chat.id,
                    "thread_id": msg.message_thread_id,
                    "user_id": update.effective_user.id,
                    "photos": [],
                    "caption": "",
                }
                self.media_groups_buffer[key] = payload

            payload["photos"].append(photo)
            if msg.caption and not payload["caption"]:
                payload["caption"] = msg.caption

            existing_task = self.media_groups_tasks.get(key)
            if existing_task and not existing_task.done():
                existing_task.cancel()

            self.media_groups_tasks[key] = asyncio.create_task(self._flush_media_group(key, context))
            return

        await self._process_photos_batch(
            context=context,
            chat_id=update.effective_chat.id,
            thread_id=msg.message_thread_id,
            user_id=update.effective_user.id,
            photos=[photo],
            caption=msg.caption or "",
        )

    async def _flush_media_group(self, key, context: ContextTypes.DEFAULT_TYPE):
        try:
            await asyncio.sleep(1.2)
            payload = self.media_groups_buffer.pop(key, None)
            self.media_groups_tasks.pop(key, None)
            if not payload:
                return

            await self._process_photos_batch(
                context=context,
                chat_id=payload["chat_id"],
                thread_id=payload["thread_id"],
                user_id=payload["user_id"],
                photos=payload["photos"],
                caption=payload["caption"],
            )
        except asyncio.CancelledError:
            return

    async def _send_results_reply(self, context: ContextTypes.DEFAULT_TYPE, chat_id: int, thread_id: int, text: str):
        kwargs = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if thread_id:
            kwargs["message_thread_id"] = thread_id
        await context.bot.send_message(**kwargs)

    async def _process_photos_batch(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        thread_id: int,
        user_id: int,
        photos,
        caption: str,
    ):
        import time

        logger.info(f"_process_photos_batch called: chat={chat_id}, user={user_id}, photos_count={len(photos)}")
        
        screenshots_dir = "screenshots"
        os.makedirs(screenshots_dir, exist_ok=True)

        is_admin = self.db.is_admin(user_id)
        player = self.db.get_player(user_id)
        if not player and not is_admin:
            logger.info(f"_process_photos_batch: user {user_id} not a player and not admin")
            return

        tournament = self.db.get_tournament_by_chat(chat_id)
        if not tournament:
            logger.info(f"_process_photos_batch: no tournament for chat {chat_id}")
            return

        results_topic_id = tournament.get('results_topic_id')
        output_thread_id = results_topic_id or thread_id
        if results_topic_id and thread_id != results_topic_id:
            logger.info(f"_process_photos_batch: wrong thread. thread_id={thread_id}, results_topic_id={results_topic_id}")
            return

        logger.info(f"_process_photos_batch: tournament found, id={tournament['id']}, results_topic={results_topic_id}")

        current_time = time.time()
        if (not is_admin) and user_id in self.cooldowns:
            last_submission = self.cooldowns[user_id]
            if current_time - last_submission < 180:
                remaining = int(180 - (current_time - last_submission))
                await self._send_results_reply(
                    context,
                    chat_id,
                    output_thread_id,
                    f"⏳ Подожди {remaining} сек. перед следующей отправкой результата.",
                )
                return

        pending_matches = [
            m for m in self.db.get_tournament_matches(tournament['id'])
            if m.get('status') in ('pending', 'in_progress')
        ]

        # Проверяем также playoff-матчи
        playoff_stages = ['1/8', '1/4', '1/2', 'bronze', 'final']
        pending_playoff = []
        for stage in playoff_stages:
            playoff_matches = self.db.get_playoff_matches(tournament['id'], stage)
            pending_playoff.extend([
                m for m in playoff_matches
                if m.get('status') in ('pending', 'in_progress')
            ])

        logger.info(f"_process_photos_batch: pending_matches={len(pending_matches)}, pending_playoff={len(pending_playoff)}, caption='{caption[:30] if caption else ''}'")

        # Если есть pending playoff - пробуем OCR все фото
        if pending_playoff:
            ocr_results = await self._extract_all_playoff_results(photos, screenshots_dir)
            if ocr_results:
                playoff_by_ocr = self._find_playoff_match_by_ocr_nicks(
                    ocr_results['player1_nick'], ocr_results['player2_nick'], tournament,
                )
                if playoff_by_ocr:
                    stage, playoff_match = playoff_by_ocr
                    await self._submit_playoff_result_from_photo(
                        context, chat_id, output_thread_id, output_thread_id,
                        tournament, stage, playoff_match, ocr_results['p1_wins'], ocr_results['p2_wins'],
                    )
                    return

        # Если нет pending-матчей — ошибка
        if not pending_matches:
            await self._send_results_reply(
                context,
                chat_id,
                output_thread_id,
                "❌ Нет ожидающих матчей для отправки результата.",
            )
            return

        tournament_players = [
            p for p in (tournament.get('players') or [])
            if p.get('tournament_status') == 'joined'
        ]
        players_by_id = {}
        for tp in tournament_players:
            try:
                players_by_id[int(tp.get('user_id'))] = tp
            except Exception:
                continue

        pending_users = {}
        pending_match_rows = []
        for m in pending_matches:
            try:
                p1_id = int(m.get('player1_id'))
                p2_id = int(m.get('player2_id'))
            except Exception:
                continue

            p1 = players_by_id.get(p1_id) or self.db.get_player(p1_id)
            p2 = players_by_id.get(p2_id) or self.db.get_player(p2_id)
            if not p1 or not p2:
                continue
            pending_match_rows.append((m, p1, p2))
            pending_users[int(p1['user_id'])] = p1
            pending_users[int(p2['user_id'])] = p2

        def resolve_match_by_text(source_text: str):
            if not source_text or not pending_users:
                return None

            cleaned_text = unicodedata.normalize("NFKC", source_text)
            cleaned_text = cleaned_text.replace("\u200f", "").replace("\u200e", "")
            first_line = cleaned_text.strip().split('\n')[0]
            m = re.search(r'(.+?)\s*(?:-|–|—|vs|VS|:){1}\s*(.+)', first_line)
            if not m:
                return None

            left_raw = m.group(1).replace('@', '').strip()
            right_raw = m.group(2).replace('@', '').strip()
            left_norm = self.screenshot_analyzer.normalize_nick(left_raw)
            right_norm = self.screenshot_analyzer.normalize_nick(right_raw)

            def resolve_tournament_user(target_norm: str):
                if not target_norm:
                    return None

                # exact normalized match first
                exact = [tp for tp in tournament_players if self.screenshot_analyzer.normalize_nick(tp.get('ingame_nick') or '') == target_norm]
                if len(exact) == 1:
                    return exact[0]

                # substring strong match
                contains = [
                    tp for tp in tournament_players
                    if target_norm in self.screenshot_analyzer.normalize_nick(tp.get('ingame_nick') or '')
                    or self.screenshot_analyzer.normalize_nick(tp.get('ingame_nick') or '') in target_norm
                ]
                if len(contains) == 1:
                    return contains[0]

                best = None
                best_score = 0.0
                for tp in tournament_players:
                    tp_norm = self.screenshot_analyzer.normalize_nick(tp.get('ingame_nick') or '')
                    if not tp_norm:
                        continue
                    score = SequenceMatcher(None, target_norm, tp_norm).ratio()
                    if target_norm in tp_norm or tp_norm in target_norm:
                        score = max(score, 0.96)
                    if score > best_score:
                        best_score = score
                        best = tp
                if best and best_score >= 0.50:
                    return best
                return None

            if left_norm and right_norm:
                tp1 = resolve_tournament_user(left_norm)
                tp2 = resolve_tournament_user(right_norm)
                if tp1 and tp2 and tp1.get('user_id') != tp2.get('user_id'):
                    uid1 = int(tp1['user_id'])
                    uid2 = int(tp2['user_id'])
                    for pmatch, pp1, pp2 in pending_match_rows:
                        a = int(pp1.get('user_id'))
                        b = int(pp2.get('user_id'))
                        if a == uid1 and b == uid2:
                            return pmatch, pp1, pp2
                        if a == uid2 and b == uid1:
                            return pmatch, pp2, pp1
                    # Пара распознана однозначно, но подходящий матч не найден.
                    # Не подбираем другой матч по fuzzy, чтобы не записывать неверного соперника.
                    return None

            if left_norm and right_norm:
                best_candidate = None
                best_total = 0.0
                for pmatch, pp1, pp2 in pending_match_rows:
                    p1_norm = self.screenshot_analyzer.normalize_nick(pp1.get('ingame_nick') or '')
                    p2_norm = self.screenshot_analyzer.normalize_nick(pp2.get('ingame_nick') or '')
                    if not p1_norm or not p2_norm:
                        continue

                    direct_total = SequenceMatcher(None, left_norm, p1_norm).ratio() + SequenceMatcher(None, right_norm, p2_norm).ratio()
                    reverse_total = SequenceMatcher(None, left_norm, p2_norm).ratio() + SequenceMatcher(None, right_norm, p1_norm).ratio()

                    if direct_total >= reverse_total:
                        total = direct_total
                        candidate = (pmatch, pp1, pp2)
                    else:
                        total = reverse_total
                        candidate = (pmatch, pp2, pp1)

                    if total > best_total:
                        best_total = total
                        best_candidate = candidate

                if best_candidate and best_total >= 1.10:
                    return best_candidate

            def resolve_caption_user(raw_nick: str):
                target = self.screenshot_analyzer.normalize_nick(raw_nick)
                if not target:
                    return None
                best_user_id = None
                best_score = 0.0
                for uid, p in pending_users.items():
                    p_norm = self.screenshot_analyzer.normalize_nick(p.get('ingame_nick') or '')
                    if not p_norm:
                        continue
                    score = SequenceMatcher(None, target, p_norm).ratio()
                    if target in p_norm or p_norm in target:
                        score = max(score, 0.95)
                    if score > best_score:
                        best_score = score
                        best_user_id = uid
                if best_user_id and best_score >= 0.68:
                    return pending_users[best_user_id]
                return None

            cp1 = resolve_caption_user(left_raw)
            cp2 = resolve_caption_user(right_raw)
            if cp1 and cp2 and cp1['user_id'] != cp2['user_id']:
                cmatch = self.db.find_match_between_players(tournament['id'], cp1['user_id'], cp2['user_id'])
                if cmatch:
                    return cmatch, cp1, cp2
            return None

        # --- Шаг 1: пробуем извлечь ники и счёт прямо со скриншотов (EasyOCR) ---
        ocr_match_info = await self._try_extract_fc_match_info(photos, screenshots_dir)

        if ocr_match_info:
            ocr_p1_nick = ocr_match_info["player1_nick"]
            ocr_p2_nick = ocr_match_info["player2_nick"]
            ocr_score1 = ocr_match_info["score1"]
            ocr_score2 = ocr_match_info["score2"]

            def nick_key(n: str) -> str:
                return (n or "").strip().casefold()

            ocr_k1 = nick_key(ocr_p1_nick)
            ocr_k2 = nick_key(ocr_p2_nick)

            # Точное совпадение ников
            matched_row = None
            for pmatch, pp1, pp2 in pending_match_rows:
                m1 = nick_key(pp1.get("ingame_nick") or "")
                m2 = nick_key(pp2.get("ingame_nick") or "")
                if (m1 == ocr_k1 and m2 == ocr_k2) or (m1 == ocr_k2 and m2 == ocr_k1):
                    matched_row = (pmatch, pp1, pp2)
                    break

            # Fuzzy-поиск, если точного совпадения нет
            if not matched_row:
                matched_row = self._find_match_by_ocr_nicks(
                    ocr_p1_nick, ocr_p2_nick, pending_match_rows, tournament_players,
                )

            if matched_row:
                match, cp1, cp2 = matched_row
                if nick_key(cp1.get("ingame_nick") or "") == ocr_k1:
                    p1_score, p2_score = ocr_score1, ocr_score2
                else:
                    p1_score, p2_score = ocr_score2, ocr_score1

                if p1_score > p2_score:
                    winner_id = match["player1_id"]
                elif p2_score > p1_score:
                    winner_id = match["player2_id"]
                else:
                    winner_id = None

                match_notification = await self.process_match_result(
                    match, p1_score, p2_score, winner_id, user_id, send_notification=False,
                )
                self.cooldowns[user_id] = current_time

                p1 = self.db.get_player(match["player1_id"])
                p2 = self.db.get_player(match["player2_id"])
                p1_nick = self._copyable_nick(p1.get("ingame_nick"))
                p2_nick = self._copyable_nick(p2.get("ingame_nick"))
                ocr_summary = (
                    f"✅ Результат записан (OCR скриншот): "
                    f"{p1_nick} {p1_score}:{p2_score} {p2_nick}"
                )
                full_text = (
                    f"{match_notification}\n\n{ocr_summary}"
                    if match_notification else ocr_summary
                )
                await self._send_results_reply(context, chat_id, output_thread_id, full_text)
                return

            # Матч не найден в группе — пробуем найти playoff-матч
            playoff_found = self._find_playoff_match_by_ocr_nicks(
                ocr_p1_nick, ocr_p2_nick, tournament,
            )
            if playoff_found:
                stage, playoff_match = playoff_found
                wins_needed = 3 if stage in ('1/8', '1/4') else 4

                pm1_nick = playoff_match.get('player1_nick') or ''
                pm2_nick = playoff_match.get('player2_nick') or ''

                if nick_key(pm1_nick) == ocr_k1:
                    p1_wins, p2_wins = ocr_score1, ocr_score2
                else:
                    p1_wins, p2_wins = ocr_score2, ocr_score1

                status = 'completed' if (p1_wins >= wins_needed or p2_wins >= wins_needed) else 'in_progress'

                p1 = self.db.get_player_by_nick(pm1_nick)
                p2 = self.db.get_player_by_nick(pm2_nick)
                p1_before = p1['rating'] if p1 else 0
                p2_before = p2['rating'] if p2 else 0

                if status == 'completed':
                    winner_nick = pm1_nick if p1_wins > p2_wins else pm2_nick
                    loser_nick = pm2_nick if p1_wins > p2_wins else pm1_nick
                    self.advance_playoff(tournament['id'], stage, playoff_match['match_num'], winner_nick, loser_nick)
                    elo_delta_p1, elo_delta_p2 = self.submit_playoff_elo_games(
                        pm1_nick, pm2_nick, p1_wins, p2_wins,
                        playoff_match['id'], p1_before, p2_before,
                    )

                self.db.update_playoff_match(playoff_match['id'], p1_wins, p2_wins, status)
                self.cooldowns[user_id] = current_time

                # Обновляем сетку плей-офф
                bracket_text = self.format_playoff_bracket(tournament['id'])
                if tournament.get('playoff_message_id'):
                    try:
                        await context.bot.edit_message_text(
                            chat_id=tournament['chat_id'],
                            message_id=tournament['playoff_message_id'],
                            text=bracket_text,
                            parse_mode='HTML',
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        logger.error(f"Error editing playoff bracket after OCR: {e}")

                result_text = f"✅ Записан результат {stage} #{playoff_match['match_num']}:\n"
                result_text += f"{pm1_nick} {p1_wins}-{p2_wins} {pm2_nick}\n"
                if status == 'completed':
                    p1_sign = '+' if elo_delta_p1 >= 0 else ''
                    p2_sign = '+' if elo_delta_p2 >= 0 else ''
                    result_text += f"📈 ELO: {pm1_nick} {p1_sign}{elo_delta_p1} | {pm2_nick} {p2_sign}{elo_delta_p2}\n"
                    if stage == 'final':
                        result_text += f"🏆 {winner_nick} — чемпион турнира!"
                    elif stage == 'bronze':
                        result_text += f"🥉 {winner_nick} занимает 3 место!"
                    else:
                        result_text += f"🏆 {winner_nick} проходит в следующий раунд!"
                else:
                    result_text += f"⏳ {wins_needed} побед для прохода. Текущий счёт: {p1_wins}-{p2_wins}"

                await self._send_results_reply(context, chat_id, output_thread_id, result_text)
                return
            # Матч не найден по OCR-никам — фоллбэк на caption

        # --- Шаг 2: фоллбэк — резолвим матч из caption, счёт из скриншотов ---
        caption_match = resolve_match_by_text(caption)
        
        # Если caption не распознан как групповой матч — пробуем playoff
        if not caption_match:
            caption_nicks = self._extract_nicks_from_caption(caption)
            
            # Если caption пустой, но есть pending playoff — пробуем найти по OCR никам
            if not caption_nicks and pending_playoff:
                ocr_info = await self._try_extract_fc_match_info(photos, screenshots_dir)
                if ocr_info:
                    # Ищем playoff матч по OCR никам
                    playoff_by_ocr = self._find_playoff_match_by_ocr_nicks(
                        ocr_info['player1_nick'], ocr_info['player2_nick'], tournament,
                    )
                    if playoff_by_ocr:
                        stage, playoff_match = playoff_by_ocr
                        await self._submit_playoff_result_from_photo(
                            context, chat_id, output_thread_id, output_thread_id,
                            tournament, stage, playoff_match, ocr_info['score1'], ocr_info['score2'],
                        )
                        return
            
            if caption_nicks:
                playoff_found = self._find_playoff_match_by_nicks(
                    caption_nicks[0], caption_nicks[1], tournament,
                )
                if playoff_found:
                    stage, playoff_match = playoff_found
                    
                    # Пробуем extract_fc_match_info (более точный OCR)
                    ocr_info = await self._try_extract_fc_match_info(photos, screenshots_dir)
                    if ocr_info:
                        ocr_score1 = ocr_info['score1']
                        ocr_score2 = ocr_info['score2']
                        await self._submit_playoff_result_from_photo(
                            context, chat_id, output_thread_id, output_thread_id,
                            tournament, stage, playoff_match, ocr_score1, ocr_score2,
                        )
                        return
                    
                    # Fallback: отдельное извлечение счёта
                    recognized_scores = await self._extract_scores_from_photos(photos, screenshots_dir, context)
                    if recognized_scores is None:
                        await self._send_results_reply(
                            context, chat_id, output_thread_id,
                            "❌ Не удалось распознать счёт на скриншотах.",
                        )
                        return
                    total = len(photos)
                    total_s1 = sum(s1 for _, s1, _ in recognized_scores)
                    total_s2 = sum(s2 for _, _, s2 in recognized_scores)
                    p1_wins, p2_wins = total_s1, total_s2
                    await self._submit_playoff_result_from_photo(
                        context, chat_id, output_thread_id, output_thread_id,
                        tournament, stage, playoff_match, p1_wins, p2_wins,
                    )
                    return
            
            await self._send_results_reply(
                context,
                chat_id,
                output_thread_id,
                "Не удалось распознать игроков. Укажи @nick1 - @nick2 в подписи к скриншотам.",
            )
            return
        match, cp1, cp2 = caption_match

        recognized_scores = []
        unrecognized = []

        for i, photo in enumerate(photos, start=1):
            try:
                photo_file = await context.bot.get_file(photo.file_id)
                safe_file_id = re.sub(r"[^A-Za-z0-9_-]+", "_", photo.file_id)
                safe_file_id = safe_file_id[:120] if safe_file_id else f"photo_{i}"
                photo_path = os.path.join(screenshots_dir, f"match_{safe_file_id}.jpg")
                await photo_file.download_to_drive(photo_path)

                screenshot_text = self.screenshot_analyzer.extract_text(photo_path)
                score1, score2 = self.screenshot_analyzer.extract_scores(screenshot_text)

                if score1 is None or score2 is None:
                    unrecognized.append(i)
                    continue
                recognized_scores.append((i, score1, score2, photo.file_id))
            except Exception as e:
                logger.error(f"Error processing photo {photo.file_id}: {e}")
                unrecognized.append(i)

        total = len(photos)
        if not recognized_scores:
            text = (
                "❌ Не удалось распознать счёт на скриншотах.\n"
                "Проверь качество скрина или внеси результат вручную через /gresult."
            )
            await self._send_results_reply(context, chat_id, output_thread_id, text)
            return

        if len(recognized_scores) != total:
            per_screen = ", ".join([f"#{idx} {s1}:{s2}" for idx, s1, s2, _ in recognized_scores])
            text = (
                "❌ Обнаружен неполный/неоднозначный набор счётов, результат не записан.\n"
                f"Распознано скринов: {len(recognized_scores)}/{total}"
            )
            if per_screen:
                text += f"\nСчета по скринам: {per_screen}"
            if unrecognized:
                text += f"\n⚠️ Не распознано скринов: {', '.join(map(str, unrecognized))}"
            text += "\nОтправь скрины повторно или внеси результат вручную через /gresult."
            await self._send_results_reply(context, chat_id, output_thread_id, text)
            return

        total_s1 = sum(s1 for _, s1, _, _ in recognized_scores)
        total_s2 = sum(s2 for _, _, s2, _ in recognized_scores)
        chosen_file_id = recognized_scores[0][3]
        score_by_user = {
            cp1['user_id']: total_s1,
            cp2['user_id']: total_s2,
        }
        p1_score = score_by_user.get(match['player1_id'])
        p2_score = score_by_user.get(match['player2_id'])

        if p1_score is None or p2_score is None:
            await self._send_results_reply(
                context,
                chat_id,
                output_thread_id,
                "❌ Подпись не соответствует участникам матча. Используй /gresult.",
            )
            return

        if p1_score > p2_score:
            winner_id = match['player1_id']
        elif p2_score > p1_score:
            winner_id = match['player2_id']
        else:
            winner_id = None

        match_notification = await self.process_match_result(
            match,
            p1_score,
            p2_score,
            winner_id,
            user_id,
            chosen_file_id,
            send_notification=False,
        )
        self.cooldowns[user_id] = current_time

        p1 = self.db.get_player(match['player1_id'])
        p2 = self.db.get_player(match['player2_id'])
        p1_nick = self._copyable_nick(p1.get('ingame_nick'))
        p2_nick = self._copyable_nick(p2.get('ingame_nick'))
        per_screen = ", ".join([f"#{idx} {s1}:{s2}" for idx, s1, s2, _ in recognized_scores])
        ocr_summary = f"✅ Результат записан (сумма игр): {p1_nick} {p1_score}:{p2_score} {p2_nick}"
        ocr_summary += f"\nРаспознано скринов: {len(recognized_scores)}/{total}"
        if per_screen:
            ocr_summary += f"\nСчета по скринам: {per_screen}"
        if unrecognized:
            ocr_summary += f"\n⚠️ Не распознано скринов: {', '.join(map(str, unrecognized))}"

        full_text = f"{match_notification}\n\n{ocr_summary}" if match_notification else ocr_summary
        await self._send_results_reply(context, chat_id, output_thread_id, full_text)

    async def _try_extract_fc_match_info(
        self, photos, screenshots_dir: str,
    ) -> Optional[Dict]:
        """Try to extract player nicks and score from screenshots using EasyOCR."""
        for i, photo in enumerate(photos, start=1):
            try:
                photo_file = await self.application.bot.get_file(photo.file_id)
                safe_file_id = re.sub(r"[^A-Za-z0-9_-]+", "_", photo.file_id)
                safe_file_id = safe_file_id[:120] if safe_file_id else f"photo_{i}"
                photo_path = os.path.join(screenshots_dir, f"match_{safe_file_id}.jpg")
                await photo_file.download_to_drive(photo_path)

                info = self.screenshot_analyzer.extract_teams_and_score(photo_path)
                if info:
                    score_text = info.get("score", "")
                    score_parts = re.split(r"[\s\-:]+", score_text.replace("-", " "))
                    score1 = int(score_parts[0]) if score_parts and score_parts[0].isdigit() else 0
                    score2 = int(score_parts[-1]) if score_parts and score_parts[-1].isdigit() else 0

                    result = {
                        "player1_nick": info.get("team1", ""),
                        "player2_nick": info.get("team2", ""),
                        "score1": score1,
                        "score2": score2,
                    }
                    logger.info(
                        f"OCR extracted: {result['player1_nick']} "
                        f"{result['score1']}:{result['score2']} {result['player2_nick']}"
                    )
                    return result
            except Exception as e:
                logger.error(f"Error in _try_extract_fc_match_info photo {i}: {e}")
        return None

    async def _extract_all_playoff_results(
        self, photos, screenshots_dir: str,
    ) -> Optional[Dict]:
        """Extract all photos and calculate total wins for playoff."""
        p1_wins = 0
        p2_wins = 0
        player1_nick = None
        player2_nick = None
        processed = 0

        for i, photo in enumerate(photos, start=1):
            try:
                photo_file = await self.application.bot.get_file(photo.file_id)
                safe_file_id = re.sub(r"[^A-Za-z0-9_-]+", "_", photo.file_id)
                safe_file_id = safe_file_id[:120] if safe_file_id else f"photo_{i}"
                photo_path = os.path.join(screenshots_dir, f"match_{safe_file_id}.jpg")
                await photo_file.download_to_drive(photo_path)

                info = self.screenshot_analyzer.extract_teams_and_score(photo_path)
                if not info:
                    continue

                score_text = info.get("score", "")
                score_parts = re.split(r"[\s\-:]+", score_text.replace("-", " "))
                score1 = int(score_parts[0]) if score_parts and score_parts[0].isdigit() else 0
                score2 = int(score_parts[-1]) if score_parts and score_parts[-1].isdigit() else 0

                if player1_nick is None:
                    player1_nick = info.get("team1", "")
                    player2_nick = info.get("team2", "")

                if score1 > score2:
                    p1_wins += 1
                elif score2 > score1:
                    p2_wins += 1

                processed += 1
                logger.info(f"Photo {i}: {player1_nick} {score1}-{score2} {player2_nick} -> p1_wins={p1_wins}, p2_wins={p2_wins}")

            except Exception as e:
                logger.error(f"Error in _extract_all_playoff_results photo {i}: {e}")

        if not player1_nick or not player2_nick:
            return None

        logger.info(f"Total: {player1_nick} {p1_wins}-{p2_wins} {player2_nick} (processed {processed} photos)")

        return {
            "player1_nick": player1_nick,
            "player2_nick": player2_nick,
            "p1_wins": p1_wins,
            "p2_wins": p2_wins,
        }

    def _find_match_by_ocr_nicks(
        self,
        ocr_nick1: str,
        ocr_nick2: str,
        pending_match_rows: List[Tuple],
        tournament_players: List[Dict],
    ) -> Optional[Tuple]:
        """Fuzzy-match OCR-extracted nicks against pending matches."""
        def nick_key(n: str) -> str:
            return (n or "").strip().casefold()

        ocr_k1 = nick_key(ocr_nick1)
        ocr_k2 = nick_key(ocr_nick2)

        best_candidate = None
        best_total = 0.0

        for pmatch, pp1, pp2 in pending_match_rows:
            p1_norm = self.screenshot_analyzer.normalize_nick(pp1.get("ingame_nick") or "")
            p2_norm = self.screenshot_analyzer.normalize_nick(pp2.get("ingame_nick") or "")
            if not p1_norm or not p2_norm:
                continue

            ocr1_norm = self.screenshot_analyzer.normalize_nick(ocr_nick1)
            ocr2_norm = self.screenshot_analyzer.normalize_nick(ocr_nick2)

            direct_total = (
                SequenceMatcher(None, ocr1_norm, p1_norm).ratio()
                + SequenceMatcher(None, ocr2_norm, p2_norm).ratio()
            )
            reverse_total = (
                SequenceMatcher(None, ocr1_norm, p2_norm).ratio()
                + SequenceMatcher(None, ocr2_norm, p1_norm).ratio()
            )

            if direct_total >= reverse_total:
                total = direct_total
                candidate = (pmatch, pp1, pp2)
            else:
                total = reverse_total
                candidate = (pmatch, pp2, pp1)

            if total > best_total:
                best_total = total
                best_candidate = candidate

        if best_candidate and best_total >= 1.10:
            return best_candidate
        return None

    def _find_playoff_match_by_ocr_nicks(
        self,
        ocr_nick1: str,
        ocr_nick2: str,
        tournament: Dict,
    ) -> Optional[Tuple[str, Dict]]:
        """Find a pending playoff match by OCR-extracted nicks with fuzzy matching."""
        def nick_key(n: str) -> str:
            return (n or "").strip().casefold()

        ocr_k1 = nick_key(ocr_nick1)
        ocr_k2 = nick_key(ocr_nick2)
        ocr1_norm = self.screenshot_analyzer.normalize_nick(ocr_nick1)
        ocr2_norm = self.screenshot_analyzer.normalize_nick(ocr_nick2)

        all_stages = ['1/8', '1/4', '1/2', 'bronze', 'final']
        best_match = None
        best_total = 0.0

        for stage in all_stages:
            matches = self.db.get_playoff_matches(tournament['id'], stage)
            for m in matches:
                if m.get('status') not in ('pending', 'in_progress'):
                    continue
                p1_nick = m.get('player1_nick') or ''
                p2_nick = m.get('player2_nick') or ''
                m1_key = nick_key(p1_nick)
                m2_key = nick_key(p2_nick)

                # Exact match first
                if (m1_key == ocr_k1 and m2_key == ocr_k2) or (m1_key == ocr_k2 and m2_key == ocr_k1):
                    return (stage, m)

                # Fuzzy match
                p1_norm = self.screenshot_analyzer.normalize_nick(p1_nick)
                p2_norm = self.screenshot_analyzer.normalize_nick(p2_nick)
                if not p1_norm or not p2_norm:
                    continue

                direct_total = (
                    SequenceMatcher(None, ocr1_norm, p1_norm).ratio()
                    + SequenceMatcher(None, ocr2_norm, p2_norm).ratio()
                )
                reverse_total = (
                    SequenceMatcher(None, ocr1_norm, p2_norm).ratio()
                    + SequenceMatcher(None, ocr2_norm, p1_norm).ratio()
                )

                total = max(direct_total, reverse_total)
                if total > best_total:
                    best_total = total
                    best_match = (stage, m)

        if best_match and best_total >= 1.10:
            return best_match
        return None

    def _extract_nicks_from_caption(self, caption: str) -> Optional[Tuple[str, str]]:
        """Extract player nicks from caption text."""
        if not caption:
            return None
        cleaned = unicodedata.normalize("NFKC", caption)
        cleaned = cleaned.replace("\u200f", "").replace("\u200e", "")
        first_line = cleaned.strip().split('\n')[0]
        m = re.search(r'(.+?)\s*(?:-|–|—|vs|VS|:){1}\s*(.+)', first_line)
        if not m:
            return None
        left = m.group(1).replace('@', '').strip()
        right = m.group(2).replace('@', '').strip()
        if left and right:
            return (left, right)
        return None

    def _find_playoff_match_by_nicks(
        self,
        nick1: str,
        nick2: str,
        tournament: Dict,
    ) -> Optional[Tuple[str, Dict]]:
        """Find a pending playoff match by nicks (exact/fuzzy matching)."""
        def nick_key(n: str) -> str:
            return (n or "").strip().casefold()

        k1 = nick_key(nick1)
        k2 = nick_key(nick2)
        norm1 = self.screenshot_analyzer.normalize_nick(nick1)
        norm2 = self.screenshot_analyzer.normalize_nick(nick2)

        all_stages = ['1/8', '1/4', '1/2', 'bronze', 'final']
        best_match = None
        best_total = 0.0

        for stage in all_stages:
            matches = self.db.get_playoff_matches(tournament['id'], stage)
            for m in matches:
                if m.get('status') not in ('pending', 'in_progress'):
                    continue
                p1_nick = m.get('player1_nick') or ''
                p2_nick = m.get('player2_nick') or ''
                m1_key = nick_key(p1_nick)
                m2_key = nick_key(p2_nick)

                if (m1_key == k1 and m2_key == k2) or (m1_key == k2 and m2_key == k1):
                    return (stage, m)

                p1_norm = self.screenshot_analyzer.normalize_nick(p1_nick)
                p2_norm = self.screenshot_analyzer.normalize_nick(p2_nick)
                if not p1_norm or not p2_norm:
                    continue

                direct = SequenceMatcher(None, norm1, p1_norm).ratio() + SequenceMatcher(None, norm2, p2_norm).ratio()
                reverse = SequenceMatcher(None, norm1, p2_norm).ratio() + SequenceMatcher(None, norm2, p1_norm).ratio()
                total = max(direct, reverse)
                if total > best_total:
                    best_total = total
                    best_match = (stage, m)

        if best_match and best_total >= 1.10:
            return best_match
        return None

    async def _extract_scores_from_photos(
        self, photos, screenshots_dir: str, context,
    ) -> Optional[List[Tuple[int, int, str]]]:
        """Extract scores from all photos. Returns list of (score1, score2, file_id) or None."""
        recognized_scores = []
        unrecognized = []

        for i, photo in enumerate(photos, start=1):
            try:
                photo_file = await context.bot.get_file(photo.file_id)
                safe_file_id = re.sub(r"[^A-Za-z0-9_-]+", "_", photo.file_id)
                safe_file_id = safe_file_id[:120] if safe_file_id else f"photo_{i}"
                photo_path = os.path.join(screenshots_dir, f"match_{safe_file_id}.jpg")
                await photo_file.download_to_drive(photo_path)

                screenshot_text = self.screenshot_analyzer.extract_text(photo_path)
                score1, score2 = self.screenshot_analyzer.extract_scores(screenshot_text)

                if score1 is None or score2 is None:
                    unrecognized.append(i)
                    continue
                recognized_scores.append((score1, score2, photo.file_id))
            except Exception as e:
                logger.error(f"Error processing photo {photo.file_id}: {e}")
                unrecognized.append(i)

        if not recognized_scores:
            return None
        return recognized_scores

    async def _submit_playoff_result_from_photo(
        self, context, chat_id, output_thread_id, thread_id,
        tournament, stage, playoff_match, p1_wins, p2_wins,
    ):
        """Submit playoff result from photo handler."""
        wins_needed = 3 if stage in ('1/8', '1/4') else 4
        status = 'completed' if (p1_wins >= wins_needed or p2_wins >= wins_needed) else 'in_progress'

        pm1_nick = playoff_match.get('player1_nick') or ''
        pm2_nick = playoff_match.get('player2_nick') or ''

        p1 = self.db.get_player_by_nick(pm1_nick)
        p2 = self.db.get_player_by_nick(pm2_nick)
        p1_before = p1['rating'] if p1 else 0
        p2_before = p2['rating'] if p2 else 0

        elo_delta_p1 = 0
        elo_delta_p2 = 0
        winner_nick = None

        if status == 'completed':
            winner_nick = pm1_nick if p1_wins > p2_wins else pm2_nick
            loser_nick = pm2_nick if p1_wins > p2_wins else pm1_nick
            self.advance_playoff(tournament['id'], stage, playoff_match['match_num'], winner_nick, loser_nick)
            elo_delta_p1, elo_delta_p2 = self.submit_playoff_elo_games(
                pm1_nick, pm2_nick, p1_wins, p2_wins,
                playoff_match['id'], p1_before, p2_before,
            )

        self.db.update_playoff_match(playoff_match['id'], p1_wins, p2_wins, status)

        bracket_text = self.format_playoff_bracket(tournament['id'])
        if tournament.get('playoff_message_id'):
            try:
                await context.bot.edit_message_text(
                    chat_id=tournament['chat_id'],
                    message_id=tournament['playoff_message_id'],
                    text=bracket_text,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.error(f"Error editing playoff bracket: {e}")

        result_text = f"✅ Записан результат {stage} #{playoff_match['match_num']}:\n"
        result_text += f"{pm1_nick} {p1_wins}-{p2_wins} {pm2_nick}\n"
        if status == 'completed':
            p1_sign = '+' if elo_delta_p1 >= 0 else ''
            p2_sign = '+' if elo_delta_p2 >= 0 else ''
            result_text += f"📈 ELO: {pm1_nick} {p1_sign}{elo_delta_p1} | {pm2_nick} {p2_sign}{elo_delta_p2}\n"
            if stage == 'final':
                result_text += f"🏆 {winner_nick} — чемпион турнира!"
            elif stage == 'bronze':
                result_text += f"🥉 {winner_nick} занимает 3 место!"
            else:
                result_text += f"🏆 {winner_nick} проходит в следующий раунд!"
        else:
            result_text += f"⏳ {wins_needed} побед для прохода. Текущий счёт: {p1_wins}-{p2_wins}"

        await self._send_results_reply(context, chat_id, output_thread_id, result_text)

    async def process_match_result(self, match: Dict, score1: int, score2: int,
                                  winner_id: int, reported_by: int, screenshot_id: str = None,
                                  send_notification: bool = True) -> str:
        self.db.update_match_result(match['id'], score1, score2, winner_id, 
                                   reported_by, screenshot_id)
        
        p1 = self.db.get_player(match['player1_id'])
        p2 = self.db.get_player(match['player2_id'])
        
        if winner_id is None:
            result1 = result2 = 'draw'
            goals1 = goals2 = score1
            change1 = 0
            change2 = 0
        elif winner_id == match['player1_id']:
            result1, result2 = 'win', 'loss'
            goals1, goals2 = score1, score2
            new_r1, new_r2, _ = self.elo.calculate(p1['rating'], p2['rating'], 1.0)
            change1 = new_r1 - p1['rating']
            change2 = new_r2 - p2['rating']
        else:
            result1, result2 = 'loss', 'win'
            goals1, goals2 = score1, score2
            new_r1, new_r2, _ = self.elo.calculate(p1['rating'], p2['rating'], 0.0)
            change1 = new_r1 - p1['rating']
            change2 = new_r2 - p2['rating']
        
        self.db.update_player_stats(match['player1_id'], result1, goals1, goals2, change1)
        self.db.update_player_stats(match['player2_id'], result2, goals2, goals1, change2)
        
        p1_new = self.db.get_player(match['player1_id'])
        p2_new = self.db.get_player(match['player2_id'])

        p1_nick = self._display_nick(p1.get('ingame_nick'))
        p2_nick = self._display_nick(p2.get('ingame_nick'))
        p1_new_nick = self._display_nick(p1_new.get('ingame_nick'))
        p2_new_nick = self._display_nick(p2_new.get('ingame_nick'))

        winner_name = p1_new_nick if winner_id == match['player1_id'] else p2_new_nick if winner_id else "Ничья"

        notification = self._format_match_result_block(
            match_id=match['id'],
            p1_nick=p1_nick,
            score1=score1,
            score2=score2,
            p2_nick=p2_nick,
            winner_name=winner_name,
            p1_old_rating=p1['rating'],
            p1_new_rating=p1_new['rating'],
            p2_old_rating=p2['rating'],
            p2_new_rating=p2_new['rating'],
        )
        
        tournament = self.db.get_tournament(match['tournament_id'])
        if tournament:
            if send_notification:
                await self.notify_admin(
                    tournament['chat_id'],
                    notification,
                    tournament.get('results_topic_id'),
                )

            if match.get('group_name'):
                groups_message_id = tournament.get('groups_message_id')
                if groups_message_id:
                    updated = await self.update_groups_table(
                        tournament['chat_id'],
                        groups_message_id,
                        tournament['id'],
                    )
                    if not updated:
                        thread_id = tournament.get('groups_topic_id') or tournament.get('results_topic_id')
                        await self.send_groups_table_message(
                            chat_id=tournament['chat_id'],
                            tournament_id=tournament['id'],
                            message_thread_id=thread_id,
                        )

            if match.get('group_name') and tournament.get('groups_graphic_message_id'):
                thread_id = tournament.get('results_topic_id')
                await self.send_groups_graphic(
                    chat_id=tournament['chat_id'],
                    tournament=tournament,
                    theme='minimal',
                    orientation='vertical',
                    message_thread_id=thread_id,
                    create_if_missing=False,
                )

        return notification
    
    async def cmd_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("У тебя нет прав админа.")
            return
        
        text = (
            "👑 ПАНЕЛЬ АДМИНИСТРАТОРА\n\n"
            "⚙️ Управление турниром:\n"
            "/tournament_create Название [формат] - создать турнир\n"
            "/tournament_start - начать турнир\n"
            "/tournament_end - завершить турнир\n"
            "/refreshreg - обновить пост регистрации\n"
            "/regen_matches [ID] - пересоздать матчи\n\n"
            "🎮 Результаты матчей:\n"
            "/gresult Player1 13-10 Player2 - результат вручную\n"
            "+ рез Player1 13-10 Player2 - результат текстом\n"
            "/rewrite Player1 13-10 Player2 - перезаписать матч\n"
            "/cancelmatch <match_id> - отмена матча\n"
            "/tp [ник] - техническое поражение\n\n"
            "👥 Управление игроками:\n"
            "/replace [old] [new] - замена игрока\n"
            "/removeplayer <nick> - удалить участника\n"
            "/notifyall - пинг по регистрации\n\n"
            "🏆 Плей-офф:\n"
            "/playoff [ники...] - генерация сетки\n"
            "/pw [стадия] [№] [ник] [счёт] - результат\n"
            "/undo_playoff [стадия] [№] - откатить результат\n\n"
            "📊 Визуал и таблицы:\n"
            "/gtable - таблица групп (текст)\n"
            "/pbracket - сетка плей-офф (текст)\n"
            "/resend_groups - переотправить таблицу\n\n"
            "📈 Аналитика:\n"
            "/elo - таблица рейтинга\n"
            "/tinfo [ID] - информация по турниру\n"
            "/finalpost [ID] - финальный пост\n"
            "/dbstats - статистика базы\n"
            "/resetelo - сбросить ELO до 1000\n"
            "/allmatches - все оставшиеся матчи\n\n"
            "👤 Пользовательские команды:\n"
            "!nick [ник] - установить ник\n"
            "!profile - профиль и статистика\n"
            "!matches - мои матчи\n"
            "!commands - список команд"
        )
        await update.message.reply_text(text)

    async def cmd_create_tournament(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("Нет прав.")
            return
        
        try:
            args = context.args
            if not args:
                await update.message.reply_text(
                    "Использование: /tournament_create Название [формат]\n\n"
                    "Форматы:\n"
                    "- classical - Классический (4 группы по 8, 32 игрока)\n"
                    "- elimination - Single Elimination"
                )
                return
            
            name = args[0]
            format_name = args[1] if len(args) > 1 else "classical"
            
            if format_name not in AVAILABLE_FORMATS:
                await update.message.reply_text("Неизвестный формат.")
                return
            
            chat_id = update.effective_chat.id
            topic_id = update.message.message_thread_id
            
            existing = self.db.get_tournament_by_chat(chat_id)
            if existing:
                await update.message.reply_text("В этой группе уже есть активный турнир.")
                return
            
            format_obj = AVAILABLE_FORMATS[format_name]
            max_players = 32 if format_name == "classical" else None
            groups_count = 4 if format_name == "classical" else 0
            
            tournament_id = self.db.create_tournament(
                name=name,
                format=format_name,
                chat_id=chat_id,
                created_by=update.effective_user.id,
                max_players=max_players,
                groups_count=groups_count,
                topic_id=topic_id
            )
            
            await update.message.reply_text(
                f"✅ Турнир создан!\n\n"
                f"ID: {tournament_id}\n"
                f"Название: {name}\n"
                f"Формат: {format_obj.name}\n"
                f"Игроков: до {max_players or '∞'}"
            )
            
            await self.send_join_message(chat_id, tournament_id)
            
        except ValueError:
            await update.message.reply_text("Неверные параметры.")

    async def cmd_regen_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        tournament = None
        if context.args:
            try:
                tournament = self.db.get_tournament(int(context.args[0]))
            except Exception:
                await update.message.reply_text("Использование: /regen_matches [tournament_id]")
                return
        else:
            tournament = self.db.get_tournament_by_chat(update.effective_chat.id)

        if not tournament:
            await update.message.reply_text("❌ Турнир не найден.")
            return

        if tournament.get('status') != TournamentStatus.IN_PROGRESS.value:
            await update.message.reply_text("❌ Турнир должен быть в статусе in_progress.")
            return

        if tournament.get('format') != 'classical':
            await update.message.reply_text("❌ Команда поддерживается только для классического формата.")
            return

        joined = [p for p in tournament.get('players', []) if p.get('tournament_status') == 'joined']
        if len(joined) < 2:
            await update.message.reply_text("❌ Недостаточно игроков для генерации матчей.")
            return

        self.db.delete_tournament_matches(tournament['id'])

        # Если группы уже были назначены — сохраняем их, иначе создаем заново.
        has_existing_groups = any((p.get('group_name') or '').strip() for p in joined)
        created_matches = 0

        if not has_existing_groups:
            self.create_group_stage(tournament, joined)
            created_matches = len(self.db.get_tournament_matches(tournament['id']))
        else:
            groups = {}
            for p in joined:
                group_name = (p.get('group_name') or '').strip()
                if not group_name:
                    continue
                groups.setdefault(group_name, []).append(p)

            for group_name, group_players in groups.items():
                for i, p1 in enumerate(group_players):
                    for p2 in group_players[i + 1:]:
                        self.db.create_match(
                            tournament_id=tournament['id'],
                            player1_id=p1['user_id'],
                            player2_id=p2['user_id'],
                            round_num=1,
                            group_name=group_name,
                            match_type='group',
                            deadline_days=tournament.get('deadline_days', 3)
                        )
                        created_matches += 1

        if tournament.get('groups_message_id'):
            await self.update_groups_table(
                chat_id=tournament['chat_id'],
                message_id=tournament['groups_message_id'],
                tournament_id=tournament['id']
            )

        await update.message.reply_text(
            f"✅ Матчи пересозданы.\n"
            f"Турнир: {tournament['name']} (ID {tournament['id']})\n"
            f"Сгенерировано матчей: {created_matches}"
        )
    
    async def send_join_message(self, chat_id: int, tournament_id: int):
        tournament = self.db.get_tournament(tournament_id)
        if not tournament:
            return
        
        players = tournament['players']
        joined = [p for p in players if p['tournament_status'] == 'joined']
        
        text = f"🏆 {tournament['name']}\n"
        text += "━━━━━━━━━━━━━━━━━━━━━━\n"
        text += f"📊 Участники: {len(joined)}/{tournament['max_players'] or '∞'}\n\n"
        
        if joined:
            for i, p in enumerate(joined, 1):
                nick = html.escape(p['ingame_nick'] or "?")
                user_id = p.get('user_id')
                if user_id:
                    linked_nick = f"<a href=\"tg://user?id={user_id}\">{nick}</a>"
                else:
                    linked_nick = nick
                text += f"  {i}. {linked_nick} (ELO: {p['rating']})\n"
        else:
            text += "  Пока никто не присоединился\n"
        
        text += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        text += "Нажмите кнопку ниже чтобы присоединиться!"
        
        keyboard = [
            [InlineKeyboardButton("✅ ПРИСОЕДИНИТЬСЯ", callback_data="join_tournament")],
            [InlineKeyboardButton("❌ ОТМЕНА УЧАСТИЯ", callback_data="leave_tournament")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            msg = await self.application.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                message_thread_id=tournament.get('topic_id'),
                parse_mode='HTML'
            )
            self.db.update_tournament_reg_message(tournament_id, msg.message_id)
        except Exception as e:
            logger.error(f"Error sending join message: {e}")
    
    async def update_join_message(self, tournament_id: int):
        tournament = self.db.get_tournament(tournament_id)
        if not tournament:
            return
        
        players = tournament['players']
        joined = [p for p in players if p['tournament_status'] == 'joined']
        
        text = f"🏆 {tournament['name']}\n"
        text += "━━━━━━━━━━━━━━━━━━━━━━\n"
        text += f"📊 Участники: {len(joined)}/{tournament['max_players'] or '∞'}\n\n"
        
        if joined:
            for i, p in enumerate(joined, 1):
                nick = html.escape(p['ingame_nick'] or "?")
                user_id = p.get('user_id')
                if user_id:
                    linked_nick = f"<a href=\"tg://user?id={user_id}\">{nick}</a>"
                else:
                    linked_nick = nick
                text += f"  {i}. {linked_nick} (ELO: {p['rating']})\n"
        else:
            text += "  Пока никто не присоединился\n"
        
        text += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        text += "Нажмите кнопку ниже чтобы присоединиться!"
        
        keyboard = [
            [InlineKeyboardButton("✅ ПРИСОЕДИНИТЬСЯ", callback_data="join_tournament")],
            [InlineKeyboardButton("❌ ОТМЕНА УЧАСТИЯ", callback_data="leave_tournament")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        reg_message_id = tournament.get('reg_message_id')
        target_chat_id = tournament.get('chat_id')
        topic_id = tournament.get('topic_id')
        
        logger.info(f"Updating join message: reg_message_id={reg_message_id}, chat_id={target_chat_id}, topic_id={topic_id}")
        
        if reg_message_id and target_chat_id:
            try:
                await self.application.bot.edit_message_text(
                    chat_id=target_chat_id,
                    message_id=reg_message_id,
                    text=text,
                    reply_markup=reply_markup,
                    message_thread_id=topic_id,
                    parse_mode='HTML'
                )
                logger.info("Join message updated successfully")
            except Exception as e:
                logger.error(f"Error updating join message: {e}")
                logger.info("Message may be too old. Use /refreshreg to recreate.")
    
    async def cmd_start_tournament(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        try:
            tournament_id = int(context.args[0]) if context.args else None
        except ValueError:
            await update.message.reply_text("Использование: /tournament_start [ID]")
            return
        
        chat_id = update.effective_chat.id
        
        if tournament_id:
            tournament = self.db.get_tournament(tournament_id)
        else:
            tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Турнир не найден.")
            return
        
        joined = [p for p in tournament['players'] if p['tournament_status'] == 'joined']
        
        if len(joined) < tournament['min_players']:
            await update.message.reply_text(
                f"Недостаточно игроков. Нужно минимум {tournament['min_players']}, "
                f"зарегистрировано {len(joined)}"
            )
            return

        self.db.snapshot_tournament_ratings(
            tournament['id'],
            [p['user_id'] for p in joined]
        )
        
        self.db.delete_tournament_matches(tournament['id'])
        self.db.update_tournament_status(tournament['id'], 'in_progress')
        
        format_obj = AVAILABLE_FORMATS.get(tournament['format'], AVAILABLE_FORMATS['single_elimination'])
        
        if format_obj.has_groups:
            self.create_group_stage(tournament, joined)
            
            results_topic_id = tournament.get('results_topic_id')
            if results_topic_id:
                try:
                    await self.application.bot.send_message(
                        chat_id=chat_id,
                        text=f"🏆 Турнир '{tournament['name']}' начат!",
                        message_thread_id=results_topic_id
                    )
                    await self.send_groups_table(
                        chat_id,
                        tournament['id'],
                        message_thread_id=results_topic_id,
                    )
                except Exception as e:
                    logger.error(f"Error sending to results topic: {e}")
                    await update.message.reply_text(
                        f"🏆 Турнир '{tournament['name']}' начат!\n\nГрупповой этап создан!"
                    )
            else:
                await update.message.reply_text(
                    f"🏆 Турнир '{tournament['name']}' начат!\n\nГрупповой этап создан!"
                )
        else:
            self.create_knockout_bracket(tournament, joined)
            await update.message.reply_text(
                f"🏆 Турнир '{tournament['name']}' начат!\n\nПлей-офф создан!"
            )
    
    def create_group_stage(self, tournament: Dict, players: List[Dict]):
        import math
        
        groups_count = tournament['groups_count'] or 4
        players_per_group = math.ceil(len(players) / groups_count)
        
        players_copy = list(players)
        random.shuffle(players_copy)
        
        groups = {}
        for i, player in enumerate(players_copy):
            group_num = (i % groups_count) + 1
            group_name = f"Группа {chr(64 + group_num)}"
            
            self.db.set_player_group(tournament['id'], player['user_id'], group_name)
            
            if group_name not in groups:
                groups[group_name] = []
            groups[group_name].append(player)

        for group_name, group_players in groups.items():
            for i, p1 in enumerate(group_players):
                for p2 in group_players[i+1:]:
                    self.db.create_match(
                        tournament['id'],
                        p1['user_id'],
                        p2['user_id'],
                        round_num=1,
                        group_name=group_name,
                        match_type='group',
                        deadline_days=tournament['deadline_days']
                    )
    
    def create_knockout_bracket(self, tournament: Dict, players: List[Dict]):
        import math
        
        n = len(players)
        next_power = 2 ** math.ceil(math.log2(n)) if n > 1 else 1
        
        players_copy = list(players)
        while len(players_copy) < next_power:
            players_copy.append(None)
        
        random.shuffle(players_copy)
        
        for i in range(0, len(players_copy), 2):
            if players_copy[i] and players_copy[i+1]:
                self.db.create_match(
                    tournament['id'],
                    players_copy[i]['user_id'],
                    players_copy[i+1]['user_id'],
                    round_num=1,
                    match_type='knockout',
                    deadline_days=tournament['deadline_days']
                )
    
    async def cmd_end_tournament(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        try:
            tournament_id = int(context.args[0]) if context.args else None
        except ValueError:
            await update.message.reply_text("Использование: /tournament_end [ID]")
            return
        
        if tournament_id:
            tournament = self.db.get_tournament(tournament_id)
        else:
            tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        
        if not tournament:
            await update.message.reply_text("Турнир не найден.")
            return
        
        self.db.update_tournament_status(tournament['id'], 'completed')

        final_post = self.build_tournament_final_post(tournament)
        await self.application.bot.send_message(
            chat_id=update.effective_chat.id,
            text=final_post,
            message_thread_id=update.message.message_thread_id
        )
        
        await update.message.reply_text(
            f"🏆 Турнир '{tournament['name']}' завершён!"
        )
    
    async def cmd_list_tournaments(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        chat_id = update.effective_chat.id
        tournaments = self.db.get_tournaments_by_chat(chat_id)
        
        if not tournaments:
            await update.message.reply_text("Нет турниров.")
            return
        
        text = "🏆 ТУРНИРЫ:\n\n"
        for t in tournaments[:10]:
            status_emoji = {"registration": "📝", "in_progress": "🏃", "completed": "✅"}.get(t['status'], "❓")
            text += f"{status_emoji} #{t['id']} {t['name']}\n"
            text += f"   Формат: {t['format']} | Статус: {t['status']}\n"
            text += f"   Игроков: {len(t['players'])}/{t['max_players'] or '∞'}\n\n"
        
        await update.message.reply_text(text)

    async def cmd_tinfo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        try:
            tournament_id = int(context.args[0]) if context.args else None
        except ValueError:
            await update.message.reply_text("Использование: /tinfo [ID]")
            return

        if tournament_id:
            tournament = self.db.get_tournament(tournament_id)
        else:
            tournament = self.db.get_tournament_by_chat(update.effective_chat.id)

        if not tournament:
            await update.message.reply_text("Турнир не найден.")
            return

        matches = self.db.get_tournament_matches(tournament['id'])
        pending = len([m for m in matches if m['status'] == 'pending'])
        in_progress = len([m for m in matches if m['status'] == 'in_progress'])
        completed = len([m for m in matches if m['status'] == 'completed'])
        joined = [p for p in tournament['players'] if p['tournament_status'] == 'joined']

        playoff_matches = self.db.get_playoff_matches(tournament['id'])
        playoff_completed = len([m for m in playoff_matches if m.get('status') == 'completed'])

        text = (
            f"📌 ТУРНИР #{tournament['id']}\n\n"
            f"Название: {tournament['name']}\n"
            f"Формат: {tournament['format']}\n"
            f"Статус: {tournament['status']}\n"
            f"Раунд: {tournament.get('current_round', 0)}\n"
            f"Участников: {len(joined)}/{tournament.get('max_players') or '∞'}\n\n"
            f"Матчи:\n"
            f"- pending: {pending}\n"
            f"- in_progress: {in_progress}\n"
            f"- completed: {completed}\n\n"
            f"Плей-офф: {playoff_completed}/{len(playoff_matches)} завершено\n"
            f"topic_id: {tournament.get('topic_id')}\n"
            f"results_topic_id: {tournament.get('results_topic_id')}\n"
            f"reg_message_id: {tournament.get('reg_message_id')}"
        )
        await update.message.reply_text(text)

    async def cmd_finalpost(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        try:
            tournament_id = int(context.args[0]) if context.args else None
        except ValueError:
            await update.message.reply_text("Использование: /finalpost [ID]")
            return

        if tournament_id:
            tournament = self.db.get_tournament(tournament_id)
        else:
            tournament = self.db.get_tournament_by_chat(update.effective_chat.id)

        if not tournament:
            await update.message.reply_text("Турнир не найден.")
            return

        final_post = self.build_tournament_final_post(tournament)
        await self.application.bot.send_message(
            chat_id=update.effective_chat.id,
            text=final_post,
            message_thread_id=update.message.message_thread_id
        )
        await update.message.reply_text("✅ Финальный пост отправлен.")

    async def cmd_dbstats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        tables = [
            'players', 'tournaments', 'tournament_players',
            'matches', 'playoff_matches', 'tournament_rating_snapshots'
        ]

        counts = {}
        for table in tables:
            try:
                self.db.cursor.execute(f'SELECT COUNT(*) FROM {table}')
                row = self.db.cursor.fetchone()
                counts[table] = row[0] if row is not None else 0
            except Exception:
                counts[table] = 'n/a'

        size_text = 'n/a'
        try:
            if USE_POSTGRES:
                self.db.cursor.execute("SELECT pg_database_size(current_database())")
                row = self.db.cursor.fetchone()
                size_bytes = row[0] if row is not None else 0
            else:
                db_path = os.path.join(os.getcwd(), 'tournament_bot.db')
                size_bytes = os.path.getsize(db_path) if os.path.exists(db_path) else 0
            size_text = f"{size_bytes / (1024 * 1024):.2f} MB"
        except Exception:
            pass

        text = (
            "🗄️ DB STATS\n\n"
            f"Размер базы: {size_text}\n\n"
            f"players: {counts['players']}\n"
            f"tournaments: {counts['tournaments']}\n"
            f"tournament_players: {counts['tournament_players']}\n"
            f"matches: {counts['matches']}\n"
            f"playoff_matches: {counts['playoff_matches']}\n"
            f"rating_snapshots: {counts['tournament_rating_snapshots']}"
        )
        await update.message.reply_text(text)

    async def cmd_groups_graphic(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            await update.message.reply_text("Нет активного турнира в этом чате.")
            return

        chat_id = update.effective_chat.id
        thread_id = update.message.message_thread_id or tournament.get('groups_topic_id') or tournament.get('results_topic_id')
        groups_message_id = tournament.get('groups_message_id')

        if groups_message_id:
            updated = await self.update_groups_table(chat_id, groups_message_id, tournament["id"])
            if updated:
                return

        sent_message_id = await self.send_groups_table_message(
            chat_id=chat_id,
            tournament_id=tournament["id"],
            message_thread_id=thread_id,
        )
        if not sent_message_id:
            await update.message.reply_text("❌ Не удалось отправить таблицу групп.")

    async def cmd_undo_playoff(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "Использование: /undo_playoff [стадия] [№]\n"
                "Пример: /undo_playoff 1/8 3"
            )
            return

        stage = context.args[0]
        try:
            match_num = int(context.args[1])
        except ValueError:
            await update.message.reply_text("Номер матча должен быть числом.")
            return

        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return

        matches = self.db.get_playoff_matches(tournament["id"], stage)
        target = None
        for m in matches:
            if m.get("match_num") == match_num:
                target = m
                break

        if not target:
            await update.message.reply_text(f"Матч {stage} #{match_num} не найден.")
            return

        if target.get("status") == "pending":
            await update.message.reply_text("Этот матч ещё не начался.")
            return

        self.db.revert_playoff_match_elo(target["id"])

        if target.get("status") == "completed":
            winner_nick = target.get("player1_nick") if (target.get("player1_wins") or 0) > (target.get("player2_wins") or 0) else target.get("player2_nick")
            self._undo_playoff_advance(tournament["id"], stage, match_num, winner_nick)

        self.db.update_playoff_match(target["id"], player1_wins=0, player2_wins=0, status="pending")

        # Обновляем сетку плей-офф
        bracket_text = self.format_playoff_bracket(tournament["id"])
        if tournament.get("playoff_message_id"):
            try:
                await context.bot.edit_message_text(
                    chat_id=tournament["chat_id"],
                    message_id=tournament["playoff_message_id"],
                    text=bracket_text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.error(f"Error editing playoff bracket after undo: {e}")

        await update.message.reply_text(
            f"✅ Результат {stage} #{match_num} откатчен.\n"
            f"{target.get('player1_nick')} vs {target.get('player2_nick')} - ожидает результат."
        )

    def _undo_playoff_advance(self, tournament_id: int, stage: str, match_num: int, winner_nick: str):
        next_stage_map = {
            "1/8": "1/4",
            "1/4": "1/2",
            "1/2": "final",
        }
        next_stage = next_stage_map.get(stage)
        if not next_stage:
            return

        target_match_num = (match_num + 1) // 2
        next_matches = self.db.get_playoff_matches(tournament_id, next_stage)
        for m in next_matches:
            if m.get("match_num") == target_match_num:
                if m.get("player1_nick") == winner_nick:
                    self.db.update_playoff_match(m["id"], player1_nick=None)
                elif m.get("player2_nick") == winner_nick:
                    self.db.update_playoff_match(m["id"], player2_nick=None)
                break

    async def cmd_playoff_graphic(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            await update.message.reply_text("Нет активного турнира в этом чате.")
            return

        await update.message.reply_text(
            self.format_playoff_bracket(tournament["id"]),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    async def cmd_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        try:
            tournament_id = int(context.args[0]) if context.args else None
        except ValueError:
            tournament_id = None
        
        if tournament_id:
            tournament = self.db.get_tournament(tournament_id)
        else:
            tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        
        if not tournament:
            await update.message.reply_text("Турнир не найден.")
            return
        
        matches = self.db.get_tournament_matches(tournament['id'])
        pending = [m for m in matches if m['status'] == 'pending']

        if not pending:
            await update.message.reply_text(f"⚽ МАТЧИ '{tournament['name']}':\n\nОставшихся матчей нет.")
            return

        def player_tag(player: Optional[Dict]) -> str:
            if not player:
                return "?"
            username = (player.get('username') or '').strip()
            if username:
                return f"@{username}"
            fallback_nick = (player.get('ingame_nick') or '').strip()
            return fallback_nick or "?"

        grouped_matches = {}
        for m in pending:
            group_label = (m.get('group_name') or '').strip() or "Без группы"
            grouped_matches.setdefault(group_label, []).append(m)

        lines = [
            f"⚽ ОСТАВШИЕСЯ МАТЧИ '{tournament['name']}':",
            "",
            f"⏳ Всего: {len(pending)}",
            "",
        ]

        for group_name in sorted(grouped_matches.keys(), key=lambda g: (g == "Без группы", g)):
            group_items = grouped_matches[group_name]
            lines.append(f"📊 {group_name} ({len(group_items)}):")

            for m in group_items:
                p1 = self.db.get_player(m['player1_id'])
                p2 = self.db.get_player(m['player2_id'])
                lines.append(f"{player_tag(p1)} vs {player_tag(p2)}")

            lines.append("")

        text = "\n".join(lines).rstrip()

        max_len = 4000
        if len(text) <= max_len:
            await update.message.reply_text(text)
            return

        chunks = []
        current = ""
        for line in text.split("\n"):
            candidate = f"{current}\n{line}" if current else line
            if len(candidate) > max_len:
                if current:
                    chunks.append(current)
                    current = line
                else:
                    chunks.append(line[:max_len])
                    current = line[max_len:]
            else:
                current = candidate

        if current:
            chunks.append(current)

        for chunk in chunks:
            await update.message.reply_text(chunk)
    
    def create_next_knockout_round(self, tournament: Dict, current_round: int):
        matches = self.db.get_tournament_matches(tournament['id'], 'completed')
        matches = [m for m in matches if m['round_num'] == current_round]
        
        winners = []
        for m in matches:
            if m['winner_id']:
                winners.append(self.db.get_player(m['winner_id']))
        
        for i in range(0, len(winners), 2):
            if i + 1 < len(winners):
                self.db.create_match(
                    tournament['id'],
                    winners[i]['user_id'],
                    winners[i+1]['user_id'],
                    round_num=current_round + 1,
                    match_type='knockout',
                    deadline_days=tournament['deadline_days']
                )
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data == "noop":
            await query.answer()
        
        elif data == "join_tournament":
            user_id = query.from_user.id
            player = self.db.get_player(user_id)
            
            if not player or not player['ingame_nick']:
                await query.answer("Сначала установи ник: !nick [твой_ник]", show_alert=True)
                return
            
            tournament = self.db.get_tournament_by_chat(query.message.chat.id)
            if not tournament:
                await query.answer("Нет активного турнира", show_alert=True)
                return
            
            if tournament['status'] != 'registration':
                await query.answer("Регистрация закрыта!", show_alert=True)
                return
            
            existing = self.db.get_player_tournament_status(tournament['id'], user_id)
            
            if existing:
                if existing['tournament_status'] == 'joined':
                    await query.answer("Вы уже участник!", show_alert=True)
                else:
                    self.db.update_tournament_player_status(tournament['id'], user_id, 'joined', user_id)
                    await self.update_join_message(tournament['id'])
                    await query.answer("Вы присоединились!")
            else:
                self.db.add_player_to_tournament(tournament['id'], user_id, 'joined')
                await self.update_join_message(tournament['id'])
                await query.answer("Вы присоединились!")
        
        elif data == "leave_tournament":
            user_id = query.from_user.id
            tournament = self.db.get_tournament_by_chat(query.message.chat.id)
            
            if not tournament:
                await query.answer("Нет активного турнира", show_alert=True)
                return
            
            if tournament['status'] != 'registration':
                await query.answer("Регистрация закрыта!", show_alert=True)
                return
            
            existing = self.db.get_player_tournament_status(tournament['id'], user_id)
            if existing and existing['tournament_status'] == 'joined':
                self.db.remove_player_from_tournament(tournament['id'], user_id)
                await self.update_join_message(tournament['id'])
                await query.answer("Вы покинули турнир!")
            else:
                await query.answer("Вы не были участником", show_alert=True)
    
    async def cmd_elo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        top = self.db.get_top_players(50)
        
        if not top:
            await update.message.reply_text("Таблица ELO пуста.")
            return
        
        text = "🏆 РЕЙТИНГ ELO:\n\n"
        text += "<pre>\n"
        text += f"{'#':<3} {'Ник':<18} {'ELO':>6}\n"
        text += "─" * 30 + "\n"
        
        for i, p in enumerate(top[:30], 1):
            nick = (p['ingame_nick'] or p.get('nick', '?'))[:16]
            text += f"{i:<3} {nick:<18} {p['rating']:>6}\n"
        
        text += "</pre>"
        
        await update.message.reply_text(text, parse_mode='HTML')
    
    async def cmd_tech_loss(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        if not context.args:
            await update.message.reply_text("Использование: /tp [ник]")
            return
        
        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            await update.message.reply_text("Нет активного турнира в этом чате.")
            return
        
        nick = context.args[0]
        player = self.db.get_player_by_nick(nick)
        
        if not player:
            await update.message.reply_text(f"Игрок '{nick}' не найден.")
            return
        
        participant = self.db.get_player_tournament_status(tournament['id'], player['user_id'])
        if not participant or participant.get('tournament_status') != 'joined':
            await update.message.reply_text(
                f"Игрок '{nick}' не участвует в турнире '{tournament['name']}' этого чата."
            )
            return
        
        await update.message.reply_text(
            f"Техническое поражение для {nick} в турнире '{tournament['name']}'."
        )
    
    async def cmd_replace(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        if len(context.args) < 2:
            await update.message.reply_text("Использование: /replace [old_nick] [new_nick]")
            return

        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            await update.message.reply_text("Нет активного турнира в этом чате.")
            return

        old_nick = context.args[0]
        new_nick = context.args[1]

        old_player = self.db.get_player_by_nick(old_nick)
        new_player = self.db.get_player_by_nick(new_nick)
        if not old_player:
            await update.message.reply_text(f"Игрок '{old_nick}' не найден.")
            return
        if not new_player:
            await update.message.reply_text(f"Игрок '{new_nick}' не найден.")
            return
        if old_player['user_id'] == new_player['user_id']:
            await update.message.reply_text("❌ Нельзя заменить игрока на самого себя.")
            return

        old_participant = self.db.get_player_tournament_status(tournament['id'], old_player['user_id'])
        new_participant = self.db.get_player_tournament_status(tournament['id'], new_player['user_id'])
        if old_participant and old_participant.get('tournament_status') == 'joined':
            if new_participant:
                await update.message.reply_text(
                    f"Игрок '{new_nick}' уже добавлен в турнир '{tournament['name']}'."
                )
                return

            replaced = self.db.replace_tournament_player(
                tournament['id'],
                old_player['user_id'],
                new_player['user_id'],
            )
            if not replaced:
                await update.message.reply_text("❌ Не удалось выполнить замену участника.")
                return
        else:
            if not new_participant or new_participant.get('tournament_status') != 'joined':
                await update.message.reply_text(
                    f"Игрок '{old_nick}' не участвует в турнире '{tournament['name']}'."
                )
                return

        reassigned = self.db.reassign_open_matches_player(
            tournament['id'],
            old_player['user_id'],
            new_player['user_id'],
        )

        groups_message_id = tournament.get('groups_message_id')
        if groups_message_id:
            await self.update_groups_table(update.effective_chat.id, groups_message_id, tournament['id'])

        await update.message.reply_text(
            f"✅ Замена выполнена: {old_nick} → {new_nick}\n"
            f"Обновлено матчей: {reassigned}"
        )

    async def cmd_remove_player(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        if len(context.args) < 1:
            await update.message.reply_text("Использование: /removeplayer <nick>")
            return

        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            await update.message.reply_text("Нет активного турнира в этом чате.")
            return

        nick = context.args[0].strip()
        player = self.db.get_player_by_nick(nick)
        if not player:
            await update.message.reply_text(f"Игрок '{nick}' не найден.")
            return

        participant = self.db.get_player_tournament_status(tournament['id'], player['user_id'])
        if not participant:
            await update.message.reply_text(
                f"Игрок '{nick}' не участвует в турнире '{tournament['name']}'."
            )
            return

        deleted_open_matches = self.db.delete_open_matches_for_player(
            tournament['id'],
            player['user_id'],
        )
        self.db.remove_player_from_tournament(tournament['id'], player['user_id'])

        groups_message_id = tournament.get('groups_message_id')
        if groups_message_id:
            await self.update_groups_table(update.effective_chat.id, groups_message_id, tournament['id'])

        await update.message.reply_text(
            f"✅ Игрок '{nick}' удалён из турнира.\n"
            f"Удалено открытых матчей: {deleted_open_matches}\n"
            f"Completed-матчи сохранены."
        )

    async def cmd_cancel_match(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        if len(context.args) < 1:
            await update.message.reply_text("Использование: /cancelmatch <match_id>")
            return

        try:
            match_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ Неверный формат match_id")
            return

        match = self.db.get_match_by_id(match_id)

        if match:
            tournament = self.db.get_tournament(match['tournament_id'])
            if not tournament or tournament.get('chat_id') != update.effective_chat.id:
                await update.message.reply_text(f"❌ Матч #{match_id} не принадлежит этому чату")
                return

            player1 = self.db.get_player(match['player1_id'])
            player2 = self.db.get_player(match['player2_id'])
            p1_nick = player1.get('ingame_nick', '?') if player1 else '?'
            p2_nick = player2.get('ingame_nick', '?') if player2 else '?'

            success, player1, player2 = self.db.cancel_match(match_id)

            if success:
                p1_elo = player1.get('rating') if player1 else '?'
                p2_elo = player2.get('rating') if player2 else '?'

                await update.message.reply_text(
                    f"✅ Результат матча #{match_id} ({p1_nick} vs {p2_nick}) снят\n"
                    f"🕓 Матч снова в статусе pending\n"
                    f"📈 ELO: {p1_nick} → {p1_elo} | {p2_nick} → {p2_elo}"
                )
            else:
                await update.message.reply_text(f"❌ Ошибка при отмене матча #{match_id}")
            return

        all_stages = ['1/8', '1/4', '1/2', 'bronze', 'final']
        found_playoff = None
        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if tournament:
            for stage in all_stages:
                matches = self.db.get_playoff_matches(tournament['id'], stage)
                for pm in matches:
                    if pm.get('id') == match_id and pm.get('status') == 'completed':
                        found_playoff = (stage, pm)
                        break
                if found_playoff:
                    break

        if not found_playoff:
            await update.message.reply_text(f"❌ Матч #{match_id} не найден")
            return

        stage, playoff_match = found_playoff
        p1_nick = playoff_match.get('player1_nick') or '?'
        p2_nick = playoff_match.get('player2_nick') or '?'

        reverted = self.db.revert_playoff_match_elo(match_id)
        if reverted:
            self._clear_next_round_slot(tournament['id'], stage, playoff_match['match_num'])

            bracket_text = self.format_playoff_bracket(tournament['id'])
            if tournament.get('playoff_message_id'):
                try:
                    await context.bot.edit_message_text(
                        chat_id=tournament['chat_id'],
                        message_id=tournament['playoff_message_id'],
                        text=bracket_text,
                        parse_mode='HTML',
                        disable_web_page_preview=True
                    )
                except Exception as e:
                    logger.error(f"Error editing playoff bracket on cancel: {e}")

            await update.message.reply_text(
                f"✅ Результат {stage} #{playoff_match['match_num']} ({p1_nick} vs {p2_nick}) снят\n"
                f"🕓 Матч снова в статусе pending"
            )
        else:
            await update.message.reply_text(f"❌ Ошибка при отмене playoff-матча #{match_id}")

    def _clear_next_round_slot(self, tournament_id: int, stage: str, match_num: int):
        stage_order = {'1/8': '1/4', '1/4': '1/2', '1/2': 'final'}
        if stage not in stage_order:
            return
        next_stage = stage_order[stage]
        target_match_num = (match_num + 1) // 2
        self.db.clear_playoff_match_slot(tournament_id, next_stage, target_match_num)

    async def cmd_notify_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        all_players = self.db.get_tournament_players(tournament['id'])
        joined = [p for p in all_players if p.get('tournament_status') == 'joined']
        joined_ids = {p['user_id'] for p in joined}
        
        all_players_with_nick = self.db.get_all_players()
        not_joined = [p for p in all_players_with_nick if p['user_id'] not in joined_ids]
        
        text = "📢 ВСЕ НА РЕГИСТРАЦИЮ!\n\n"
        text += f"🏆 {tournament['name']}\n"
        text += f"📊 Зарегистрировано: {len(joined)}/{tournament['max_players']}\n\n"
        
        if not_joined:
            mentions = " ".join(f"@{p.get('ingame_nick', 'unknown')}" for p in not_joined)
            text += f"Не зарегистрированы: {mentions}\n\n"
        
        text += "❗️ Нажмите кнопку ниже чтобы присоединиться!"
        
        keyboard = [
            [InlineKeyboardButton("✅ ПРИСОЕДИНИТЬСЯ", callback_data="join_tournament")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await self.application.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                message_thread_id=update.message.message_thread_id
            )
        except Exception as e:
            logger.error(f"Error sending notify: {e}")
            await update.message.reply_text("Ошибка при отправке уведомления.")
    
    async def cmd_playoff(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        forced_nicks = [arg.strip() for arg in context.args if arg.strip()]

        def nick_key(value: str) -> str:
            return (value or "").strip().casefold()

        forced_lookup = {nick_key(n): n for n in forced_nicks}
        applied_forced = []
        not_found_forced = set(forced_lookup.values())

        group_top4 = {}
        for group_key in ['A', 'B', 'C', 'D']:
            standings = self.db.get_group_standings(tournament['id'], f"Группа {group_key}")
            group_top = standings[:4] if standings else []

            if forced_lookup and standings:
                forced_in_group = []
                for p in standings:
                    key = nick_key(p.get('ingame_nick'))
                    if key in forced_lookup:
                        forced_in_group.append(p)

                for forced_player in forced_in_group:
                    forced_name = forced_player.get('ingame_nick')
                    forced_name_key = nick_key(forced_name)
                    if forced_name_key in forced_lookup:
                        not_found_forced.discard(forced_lookup[forced_name_key])

                    already_inside = any(
                        nick_key(x.get('ingame_nick')) == forced_name_key
                        for x in group_top
                    )
                    if already_inside:
                        continue

                    if len(group_top) >= 4:
                        replace_idx = None
                        for idx in range(len(group_top) - 1, -1, -1):
                            current_key = nick_key(group_top[idx].get('ingame_nick'))
                            if current_key not in forced_lookup:
                                replace_idx = idx
                                break

                        if replace_idx is None:
                            continue

                        replaced = group_top[replace_idx]
                        group_top[replace_idx] = forced_player
                        applied_forced.append(
                            (group_key, replaced.get('ingame_nick', '?'), forced_name)
                        )
                    else:
                        group_top.append(forced_player)
                        applied_forced.append((group_key, None, forced_name))

            group_top4[group_key] = group_top

        invalid_groups = [
            f"Группа {group_key}: {len(group_top4.get(group_key, []))}/4"
            for group_key in ['A', 'B', 'C', 'D']
            if len(group_top4.get(group_key, [])) < 4
        ]
        if invalid_groups:
            await update.message.reply_text(
                "❌ Нельзя собрать 1/8: не хватает участников в группах.\n" + "\n".join(invalid_groups)
            )
            return

        def seed(group_key: str, place: int) -> Dict:
            return group_top4[group_key][place - 1]
        
        self.db.clear_playoff_matches(tournament['id'])
        
        pairings = [
            (seed('A', 1), seed('B', 4)),
            (seed('C', 1), seed('D', 4)),
            (seed('B', 2), seed('A', 3)),
            (seed('D', 2), seed('C', 3)),
            (seed('B', 1), seed('A', 4)),
            (seed('D', 1), seed('C', 4)),
            (seed('A', 2), seed('B', 3)),
            (seed('C', 2), seed('D', 3)),
        ]

        for i, (p1, p2) in enumerate(pairings, start=1):
            self.db.add_playoff_match(tournament['id'], '1/8', i,
                                     p1['ingame_nick'], p2['ingame_nick'])
        
        bracket_text = self.format_playoff_bracket(tournament['id'])
        
        msg = await update.message.reply_text(bracket_text, parse_mode='HTML', disable_web_page_preview=True)

        if applied_forced:
            lines = ["✅ Применены ручные проходы в плей-офф:"]
            for group_key, replaced_nick, forced_nick in applied_forced:
                if replaced_nick:
                    lines.append(f"Группа {group_key}: {forced_nick} вместо {replaced_nick}")
                else:
                    lines.append(f"Группа {group_key}: добавлен {forced_nick}")
            await update.message.reply_text("\n".join(lines))

        if not_found_forced:
            missing = ", ".join(sorted(not_found_forced))
            await update.message.reply_text(
                f"⚠️ Эти ники не найдены в группах текущего турнира: {missing}"
            )
        
        self.db.set_playoff_message_id(tournament['id'], msg.message_id)

    def set_playoff_match_slot(self, tournament_id: int, stage: str, match_num: int, slot: int, nick: str):
        matches = self.db.get_playoff_matches(tournament_id, stage)
        existing = next((m for m in matches if m['match_num'] == match_num), None)
        p1 = existing['player1_nick'] if existing else None
        p2 = existing['player2_nick'] if existing else None
        if slot == 1:
            p1 = nick
        else:
            p2 = nick
        self.db.add_playoff_match(tournament_id, stage, match_num, p1, p2)

    def advance_playoff(self, tournament_id: int, stage: str, match_num: int, winner_nick: str, loser_nick: str):
        if stage == '1/8':
            target_match = (match_num + 1) // 2
            slot = 1 if match_num % 2 == 1 else 2
            self.set_playoff_match_slot(tournament_id, '1/4', target_match, slot, winner_nick)
        elif stage == '1/4':
            target_match = (match_num + 1) // 2
            slot = 1 if match_num % 2 == 1 else 2
            self.set_playoff_match_slot(tournament_id, '1/2', target_match, slot, winner_nick)
        elif stage == '1/2':
            final_slot = 1 if match_num == 1 else 2
            bronze_slot = 1 if match_num == 1 else 2
            self.set_playoff_match_slot(tournament_id, 'final', 1, final_slot, winner_nick)
            self.set_playoff_match_slot(tournament_id, 'bronze', 1, bronze_slot, loser_nick)

    def get_tournament_player_stats(self, tournament_id: int) -> Dict[int, Dict]:
        stats = {}
        matches = self.db.get_tournament_matches(tournament_id, 'completed')

        for m in matches:
            p1_id = m['player1_id']
            p2_id = m['player2_id']
            s1 = m['player1_score'] or 0
            s2 = m['player2_score'] or 0

            if p1_id not in stats:
                stats[p1_id] = {'matches': 0, 'wins': 0, 'draws': 0, 'losses': 0, 'goals_scored': 0, 'goals_conceded': 0}
            if p2_id not in stats:
                stats[p2_id] = {'matches': 0, 'wins': 0, 'draws': 0, 'losses': 0, 'goals_scored': 0, 'goals_conceded': 0}

            stats[p1_id]['matches'] += 1
            stats[p2_id]['matches'] += 1
            stats[p1_id]['goals_scored'] += s1
            stats[p1_id]['goals_conceded'] += s2
            stats[p2_id]['goals_scored'] += s2
            stats[p2_id]['goals_conceded'] += s1

            if s1 > s2:
                stats[p1_id]['wins'] += 1
                stats[p2_id]['losses'] += 1
            elif s2 > s1:
                stats[p2_id]['wins'] += 1
                stats[p1_id]['losses'] += 1
            else:
                stats[p1_id]['draws'] += 1
                stats[p2_id]['draws'] += 1

        return stats

    def build_tournament_final_post(self, tournament: Dict) -> str:
        tournament_id = tournament['id']
        date_str = datetime.now().strftime('%d.%m.%Y')
        joined = [p for p in tournament['players'] if p['tournament_status'] == 'joined']
        stats = self.get_tournament_player_stats(tournament_id)

        final_matches = self.db.get_playoff_matches(tournament_id, 'final')
        bronze_matches = self.db.get_playoff_matches(tournament_id, 'bronze')

        first_place = '—'
        second_place = '—'
        third_place = None

        if final_matches:
            final_match = final_matches[0]
            if final_match['status'] == 'completed':
                if final_match['player1_wins'] > final_match['player2_wins']:
                    first_place = final_match['player1_nick'] or '—'
                    second_place = final_match['player2_nick'] or '—'
                else:
                    first_place = final_match['player2_nick'] or '—'
                    second_place = final_match['player1_nick'] or '—'

        if bronze_matches:
            bronze_match = bronze_matches[0]
            if bronze_match['status'] == 'completed':
                if bronze_match['player1_wins'] > bronze_match['player2_wins']:
                    third_place = bronze_match['player1_nick'] or '—'
                else:
                    third_place = bronze_match['player2_nick'] or '—'

        best_wins_nick = '—'
        best_wins_value = 0
        best_avg_nick = '—'
        best_avg_value = 0.0
        mvp_nick = '—'
        mvp_score = -10**9

        for user_id, s in stats.items():
            player = self.db.get_player(user_id)
            if not player:
                continue
            nick = player.get('ingame_nick') or 'Unknown'

            if s['wins'] > best_wins_value:
                best_wins_value = s['wins']
                best_wins_nick = nick

            if s['matches'] > 0:
                avg_goals = s['goals_scored'] / s['matches']
                if avg_goals > best_avg_value:
                    best_avg_value = avg_goals
                    best_avg_nick = nick

                score = s['wins'] * 3 + (s['goals_scored'] - s['goals_conceded']) + avg_goals
                if score > mvp_score:
                    mvp_score = score
                    mvp_nick = nick

        gains = self.db.get_tournament_rating_gains(tournament_id)
        top_gains = gains[:3]

        lines = [
            "━━━━━━━━━━━━━━━━━━━━",
            "🏆 ТУРНИР ЗАВЕРШЕН",
            "━━━━━━━━━━━━━━━━━━━━",
            "",
            f"📌 Название: {tournament['name']}",
            f"📅 Дата: {date_str}",
            f"👥 Участников: {len(joined)}",
            "",
            f"🥇 1 место: {first_place}",
            f"🥈 2 место: {second_place}",
        ]

        if third_place:
            lines.append(f"🥉 3 место: {third_place}")

        lines.extend([
            "",
            "📊 Лучшие показатели:",
            f"🔥 MVP турнира: {mvp_nick}",
            f"⚔️ Больше всего побед: {best_wins_nick} ({best_wins_value})",
            f"🎯 Лучший средний показатель по голам: {best_avg_nick} - {best_avg_value:.3f}",
            "",
            "📈 Топ прирост ELO:",
        ])

        if top_gains:
            for row in top_gains:
                gain = row['gain']
                sign = '+' if gain >= 0 else ''
                lines.append(f"{sign}{gain}  {row['ingame_nick']}")
        else:
            lines.append("Нет данных")

        lines.extend([
            "",
            "👏 Спасибо всем за участие!",
            "Следующий турнир скоро — следите за анонсом.",
            "━━━━━━━━━━━━━━━━━━━━",
        ])

        return "\n".join(lines)

    def format_playoff_bracket(self, tournament_id: int) -> str:
        stages = [('1/8', 8, 3), ('1/4', 4, 3), ('1/2', 2, 4), ('bronze', 1, 4), ('final', 1, 4)]
        
        text = "🏆 ПЛЕЙ-ОФФ\n\n"
        text += "<pre>\n"
        
        for stage, num_matches, wins_needed in stages:
            matches = self.db.get_playoff_matches(tournament_id, stage)
            if stage == '1/8':
                text += "• 1/8 "
                text += f"(до {wins_needed} побед)\n"
            elif stage == '1/4':
                text += "\n• 1/4 "
                text += f"(до {wins_needed} побед)\n"
            elif stage == '1/2':
                text += "\n• 1/2 "
                text += f"(до {wins_needed} побед)\n"
            elif stage == 'bronze':
                text += "\n🥉 БРОНЗА "
                text += f"(до {wins_needed} побед)\n"
            else:
                text += "\n🏆 ФИНАЛ "
                text += f"(до {wins_needed} побед)\n"
            
            for match in matches:
                p1 = (match['player1_nick'] or '?')[:12]
                p2 = (match['player2_nick'] or '?')[:12]
                w1 = match['player1_wins']
                w2 = match['player2_wins']
                if match['status'] == 'completed':
                    text += f"✅ {p1:<12} {w1}-{w2} {p2:<12}\n"
                elif w1 > 0 or w2 > 0:
                    text += f"⚽ {p1:<12} {w1}-{w2} {p2:<12}\n"
                else:
                    text += f"  {p1:<12} vs {p2:<12}\n"
        
        text += "\n</pre>"
        text += "⚽ - идут игры, ✅ - завершено"
        
        return text
    
    async def cmd_playoff_win(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        if not context.args or len(context.args) < 3:
            await update.message.reply_text(
                "Использование: /pw [стадия] [номер] [ник_победителя] [счёт1] [счёт2]\n"
                "Пример: /pw 1/8 1 Player1 3 1\n"
                "Стадии: 1/8, 1/4, 1/2, bronze, final"
            )
            return
        
        stage = context.args[0]
        if stage not in ['1/8', '1/4', '1/2', 'bronze', 'final']:
            await update.message.reply_text("Неверная стадия. Используйте: 1/8, 1/4, 1/2, bronze, final")
            return
        
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        try:
            match_num = int(context.args[1])
            winner_nick = context.args[2]
            wins_needed = 3 if stage in ['1/8', '1/4'] else 4
            
            w1 = int(context.args[3]) if len(context.args) > 3 else wins_needed
            w2 = int(context.args[4]) if len(context.args) > 4 else 0
        except ValueError:
            await update.message.reply_text("Неверный формат.")
            return
        
        matches = self.db.get_playoff_matches(tournament['id'], stage)
        match = next((m for m in matches if m['match_num'] == match_num), None)
        
        if not match:
            await update.message.reply_text(f"Матч {stage} #{match_num} не найден.")
            return
        
        if match['player1_nick'] == winner_nick:
            player1_wins = w1
            player2_wins = w2
        elif match['player2_nick'] == winner_nick:
            player1_wins = w2
            player2_wins = w1
        else:
            await update.message.reply_text(f"Игрок '{winner_nick}' не найден в этом матче.")
            return
        
        status = 'completed' if (player1_wins >= wins_needed or player2_wins >= wins_needed) else 'in_progress'
        
        self.db.update_playoff_match(match['id'], player1_wins, player2_wins, status)

        if status == 'completed':
            winner_nick_resolved = match['player1_nick'] if player1_wins > player2_wins else match['player2_nick']
            loser_nick_resolved = match['player2_nick'] if player1_wins > player2_wins else match['player1_nick']
            self.advance_playoff(tournament['id'], stage, match_num, winner_nick_resolved, loser_nick_resolved)
        
        bracket_text = self.format_playoff_bracket(tournament['id'])
        
        if tournament.get('playoff_message_id'):
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=tournament['playoff_message_id'],
                    text=bracket_text,
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
            except Exception as e:
                logger.error(f"Error editing playoff text message: {e}")


        
        result_text = f"✅ Записан результат:\n"
        result_text += f"{match['player1_nick']} {player1_wins}-{player2_wins} {match['player2_nick']}\n"
        if status == 'completed':
            if stage == 'final':
                result_text += f"\n🏆 {winner_nick} — чемпион турнира!"
            elif stage == 'bronze':
                result_text += f"\n🥉 {winner_nick} занимает 3 место!"
            else:
                result_text += f"\n🏆 {winner_nick} проходит в следующий раунд!"
        
        await update.message.reply_text(result_text)
    
    def run(self):
        print("Bot FC Mobile Tournament v2 started...")
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    if not TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN not found in .env")
    
    bot = TournamentBot(TOKEN)
    
    ADMIN_IDS = os.getenv("ADMIN_IDS")
    if ADMIN_IDS:
        for admin_id in ADMIN_IDS.split(','):
            try:
                bot.db.add_admin(int(admin_id.strip()))
            except ValueError:
                pass
    
    bot.run()
