"""
UNIVERSE OF HEROES - Tournament Bot
FC Mobile Tournament Management System
"""

import logging
import os
import re
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)

from database import Database
from elo_calculator import EloCalculator
from screenshot_analyzer import ScreenshotAnalyzer

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

GROUPS_CONFIG = {
    'A': {'topic_name': 'МСТИТЕЛИ', 'group_name': 'Группа A'},
    'B': {'topic_name': 'СТРАЖИ ГАЛАКТИКИ', 'group_name': 'Группа B'},
    'C': {'topic_name': 'ЛИГА СПРАВЕДЛИВОСТИ', 'group_name': 'Группа C'},
    'D': {'topic_name': 'ОТРЯД САМОУБИЙЦ', 'group_name': 'Группа D'},
}

PLAYOFF_CONFIG = {'topic_name': 'ПЛЕЙ-ОФФ РЕЗУЛЬТАТЫ'}

PLAYERS = {
    'Группа A': [
        'ОТС', 'Шторм', 'Китаец', 'RealMadrid',
        'Danis RM', 'AR-Unlucky', 'AR-Timeless', 'Beshenyi',
        'Bumblebee', 'Ar-iceberg', 'NotEternal', 'Енот',
        '116Rus', 'Vishera', 'JLбычый', 'Daredevil'
    ],
    'Группа B': [
        'zloy', 'Ростас', 'GLAcapelllkzxc', 'alz',
        'V1RɄS', 'Zenit', 'Т卂Г卂Н卩ОГツ', 'Mihasik',
        'ЧерныйНеегрр', 'дедскв', 'gg', 'likshonn',
        'nsGUW-117', 'Маджента', 'Cristiano2828', 'AnatoliyIva4'
    ],
    'Группа C': [
        'Yarchee', 'SoEz', '(FCSM) Спартач', 'RTOTY-WIZARD',
        'Strongmann', 'RonaldoR9', 'GlGreshnik', 'zenit78',
        'GLDJAMBORZ', 'КОРЖИМАН', 'GLAngarsk', 'SEEAL',
        'Freezy', 'Черти58', '3lodeu', 'Seriu'
    ],
    'Группа D': [
        'Старушка Изольда', 'nsMor1arty', 'artsmile', 'Guess Who',
        'Лизун0', 'GLBek_07', 'БЕЗБАШЕННЫЙ', 'VitalyRus',
        'kOFFe', 'CONSTANTINO', 'DeLPaPa', 'Exclusive',
        'ミКРУПЬЕ彡', 'LazyMaxx', 'Karbon', 'ШинникЯр'
    ]
}


class UniverseHeroesBot:
    def __init__(self, token: str):
        self.token = token
        self.db = Database()
        self.elo = EloCalculator()
        self.screenshot_analyzer = ScreenshotAnalyzer()
        self.application = Application.builder().token(token).build()
        self.pending_results = {}
        self.playoff_message_id = None
        self.setup_handlers()
    
    def setup_handlers(self):
        self.application.add_handler(CommandHandler("admin", self.cmd_admin))
        self.application.add_handler(CommandHandler("nextround", self.cmd_next_round))
        self.application.add_handler(CommandHandler("standings", self.cmd_standings))
        self.application.add_handler(CommandHandler("elo", self.cmd_elo))
        self.application.add_handler(CommandHandler("tp", self.cmd_tech_loss))
        self.application.add_handler(CommandHandler("replace", self.cmd_replace))
        self.application.add_handler(CommandHandler("cancelmatch", self.cmd_cancel_match))
        self.application.add_handler(CommandHandler("group", self.cmd_group))
        self.application.add_handler(CommandHandler("initplayers", self.cmd_init_players))
        self.application.add_handler(CommandHandler("clearplayers", self.cmd_clear_players))
        
        self.application.add_handler(MessageHandler(
            filters.Regex(r'^!nick\s+(\S.+)'), 
            self.cmd_set_nick
        ))
        self.application.add_handler(MessageHandler(
            filters.Regex(r'^!commands'), 
            self.cmd_commands
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
            filters.Regex(r'^!help'), 
            self.cmd_help
        ))
        
        self.application.add_handler(MessageHandler(filters.PHOTO, self.handle_photo))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
    
    async def cmd_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("У тебя нет прав админа.")
            return
        
        text = (
            "Admin commands:\n"
            "/nextround [A/B/C/D/playoff] - next round\n"
            "/standings [A/B/C/D] - group standings\n"
            "/elo - ELO table\n"
            "/tp [nick] - technical loss\n"
            "/replace [old] [new] - replace player\n"
            "/cancelmatch [nick1] [nick2] - cancel match\n"
            "/group [A/B/C/D] - post table to topic"
        )
        await update.message.reply_text(text)
    
    async def cmd_next_round(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /nextround [A/B/C/D/playoff]")
            return
        
        group_key = context.args[0].upper()
        
        if group_key == 'PLAYOFF':
            await self.generate_playoff_bracket(update, context)
            return
        
        if group_key not in GROUPS_CONFIG:
            await update.message.reply_text("Invalid group. Use: A, B, C, D or playoff")
            return
        
        group_name = GROUPS_CONFIG[group_key]['group_name']
        await self.generate_group_round(update, group_name, group_key)
    
    async def generate_group_round(self, update: Update, group_name: str, group_key: str):
        players = self.db.get_group_standings(group_name)
        if not players:
            await update.message.reply_text(f"No players in {group_name}.")
            return
        
        players_nicks = [p['player_nick'] for p in players]
        
        pending = self.db.get_group_matches(group_name=group_name, status='pending')
        if pending:
            await update.message.reply_text(f"Pending matches exist in {group_name}!")
            return
        
        completed = self.db.get_group_matches(group_name=group_name, status='completed')
        current_round = 1
        if completed:
            current_round = max(m['round_num'] for m in completed) + 1
        
        if current_round > 6:
            await update.message.reply_text(f"Group stage {group_name} completed!")
            return
        
        self.db.clear_group_matches(group_name)
        
        pairs = []
        shuffled = list(players_nicks)
        random.shuffle(shuffled)
        
        for i in range(0, len(shuffled), 2):
            if i + 1 < len(shuffled):
                player1_home = random.randint(0, 1)
                pairs.append((shuffled[i], shuffled[i+1], player1_home))
        
        matches_text = f"Round {current_round} - {group_name}\n\n"
        
        for idx, (p1, p2, p1_home) in enumerate(pairs, 1):
            self.db.add_group_match(group_name, p1, p2, current_round, idx, p1_home)
            matches_text += f"{idx}. {p1} vs {p2}\n"
        
        self.db.clear_group_standings(group_name)
        for nick in players_nicks:
            self.db.update_group_standings(nick, group_name)
        
        await update.message.reply_text(matches_text)
        
        topic_name = GROUPS_CONFIG[group_key]['topic_name']
        await self.send_to_topic(update, context, topic_name, matches_text)
    
    async def generate_playoff_bracket(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        self.db.clear_playoff_matches()
        
        top_16 = []
        for group_key in ['A', 'B', 'C', 'D']:
            group_name = GROUPS_CONFIG[group_key]['group_name']
            standings = self.db.get_group_standings(group_name)
            if standings:
                top_4 = standings[:4]
                for p in top_4:
                    top_16.append((p['player_nick'], group_key))
        
        if len(top_16) < 16:
            await update.message.reply_text(f"Not enough players for playoffs. Need 16, have {len(top_16)}")
            return
        
        random.shuffle(top_16)
        
        stages = [('1/8', 8), ('1/4', 4), ('1/2', 2), ('final', 1)]
        
        for stage, count in stages:
            for i in range(count):
                if stage == 'final':
                    self.db.add_playoff_match(stage, 1, top_16[0][0], top_16[1][0] if len(top_16) > 1 else None)
                else:
                    p1_idx = i * 2
                    p2_idx = i * 2 + 1
                    self.db.add_playoff_match(stage, i + 1, top_16[p1_idx][0], top_16[p2_idx][0])
        
        bracket_text = self.format_playoff_bracket()
        
        await update.message.reply_text("Playoff bracket created!")
        
        topic_name = PLAYOFF_CONFIG['topic_name']
        msg = await self.send_to_topic(update, context, topic_name, bracket_text)
        
        if msg:
            self.playoff_message_id = msg.message_id
    
    def format_playoff_bracket(self) -> str:
        text = "UNIVERSE OF HEROES - PLAYOFF\n\n"
        
        stages = ['1/8', '1/4', '1/2', 'final']
        
        for stage in stages:
            matches = self.db.get_playoff_matches(stage)
            if not matches:
                continue
            
            text += f"* {stage}\n"
            
            if stage == 'final':
                m = matches[0]
                p1 = m['player1_nick'] or 'TBD'
                p2 = m['player2_nick'] or 'TBD'
                if m['status'] == 'completed':
                    if m['player1_goals'] is not None:
                        text += f" {p1} {m['player1_goals']} - {m['player2_goals']} {p2}\n"
                    else:
                        text += f" {p1} {m['player1_wins']} - {m['player2_wins']} {p2}\n"
                else:
                    text += f" {p1} - {p2}\n"
            else:
                for m in matches:
                    p1 = m['player1_nick'] or 'TBD'
                    p2 = m['player2_nick'] or 'TBD'
                    if m['status'] == 'completed':
                        text += f" {p1} {m['player1_wins']} - {m['player2_wins']} {p2}\n"
                    else:
                        text += f" {p1} - {p2}\n"
            
            text += "\n"
        
        text += "Best of 5: need 3 wins to advance"
        return text
    
    async def send_to_topic(self, update: Update, context: ContextTypes.DEFAULT_TYPE, topic_name: str, text: str):
        try:
            chat_id = update.effective_chat.id
            forum_topics = await context.bot.get_forum_topics(chat_id)
            
            topic_id = None
            for topic in forum_topics:
                if topic.name.lower() == topic_name.lower():
                    topic_id = topic.id
                    break
            
            if topic_id:
                msg = await context.bot.send_message(
                    chat_id=chat_id,
                    message_thread_id=topic_id,
                    text=text
                )
                return msg
            else:
                await update.message.reply_text(f"Topic '{topic_name}' not found.")
                return None
        except Exception as e:
            logger.error(f"Error sending to topic: {e}")
            await update.message.reply_text(f"Error: {e}")
            return None
    
    async def cmd_standings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /standings [A/B/C/D]")
            return
        
        group_key = context.args[0].upper()
        if group_key not in GROUPS_CONFIG:
            await update.message.reply_text("Invalid group. Use: A, B, C, D")
            return
        
        group_name = GROUPS_CONFIG[group_key]['group_name']
        standings = self.db.get_group_standings(group_name)
        
        if not standings:
            await update.message.reply_text(f"Table for {group_name} is empty.")
            return
        
        text = f"📊 {group_name}\n\n"
        text += "┌────┬──────────────────────┬────┬────┬────┬────┬────┬────────┐\n"
        text += "│ #  │ Ник                  │ И  │ В  │ П  │ Н  │ О  │ Голы   │\n"
        text += "├────┼──────────────────────┼────┼────┼────┼────┼────┼────────┤\n"
        
        for i, p in enumerate(standings, 1):
            nick = p['player_nick'][:20].ljust(20)
            goals = f"{p['goals_scored']}-{p['goals_conceded']}"
            text += f"│{i:>2}. │ {nick} │{p['games']:>2} │{p['wins']:>2} │{p['losses']:>2} │{p['draws']:>2} │{p['points']:>2} │ {goals:<6} │\n"
        
        text += "└────┴──────────────────────┴────┴────┴────┴────┴────┴────────┘"
        
        await update.message.reply_text(text)
    
    async def cmd_elo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        elo_table = self.db.get_elo_table(50)
        
        if not elo_table:
            await update.message.reply_text("ELO table is empty.")
            return
        
        text = "ELO Rating Table:\n\n"
        
        for i, p in enumerate(elo_table, 1):
            text += f"{i}. {p['player_nick']} {p['rating']}\n"
        
        await update.message.reply_text(text)
    
    async def cmd_tech_loss(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /tp [nick]")
            return
        
        loser_nick = context.args[0]
        loser = self.db.get_player(loser_nick)
        
        if not loser:
            await update.message.reply_text(f"Player '{loser_nick}' not found.")
            return
        
        pending = self.db.get_group_matches(nick=loser_nick, status='pending')
        
        if not pending:
            await update.message.reply_text(f"No active matches for {loser_nick}.")
            return
        
        match = pending[0]
        opponent = match['player2_nick'] if match['player1_nick'] == loser_nick else match['player1_nick']
        
        self.db.update_group_match_result(match['id'], 0, 74, "ADMIN_TP")
        self.db.update_group_standings(loser_nick, match['group_name'])
        self.db.update_group_standings(opponent, match['group_name'])
        
        elo_loser = self.db.get_elo(loser_nick)
        elo_winner = self.db.get_elo(opponent)
        
        if elo_loser and elo_winner:
            new_r1, new_r2, change = self.elo.calculate(elo_winner['rating'], elo_loser['rating'], 0.0)
            self.db.update_elo(loser_nick, change, 'loss', 0, 74)
            self.db.update_elo(opponent, -change, 'win', 74, 0)
        
        await update.message.reply_text(f"Tech loss for {loser_nick}. Score: 0:74 (vs {opponent})")
    
    async def cmd_replace(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            return
        
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /replace [old_nick] [new_nick]")
            return
        
        old_nick = context.args[0]
        new_nick = context.args[1]
        
        old_player = self.db.get_player(old_nick)
        if not old_player:
            await update.message.reply_text(f"Player '{old_nick}' not found.")
            return
        
        new_player = self.db.get_player(new_nick)
        if new_player and not new_player['is_active']:
            await update.message.reply_text(f"Nick '{new_nick}' was previously replaced. Use another nick.")
            return
        
        if new_player and new_player['is_active']:
            await update.message.reply_text(f"Nick '{new_nick}' is already in use.")
            return
        
        self.db.replace_player(old_nick, new_nick)
        
        await update.message.reply_text(f"Replacement done!\n{old_nick} -> {new_nick}\nELO of old nick preserved.")
    
    async def cmd_cancel_match(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            return
        
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /cancelmatch [nick1] [nick2]")
            return
        
        nick1 = context.args[0]
        nick2 = context.args[1]
        
        matches = self.db.get_group_matches(nick=nick1, status='pending')
        match = None
        for m in matches:
            if (m['player1_nick'] == nick1 and m['player2_nick'] == nick2) or \
               (m['player1_nick'] == nick2 and m['player2_nick'] == nick1):
                match = m
                break
        
        if not match:
            await update.message.reply_text(f"Match between {nick1} and {nick2} not found.")
            return
        
        self.db.cancel_group_match(match['id'])
        await update.message.reply_text(f"Match {nick1} vs {nick2} cancelled.")
    
    async def cmd_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            return
        
        if not context.args:
            await update.message.reply_text("Usage: /group [A/B/C/D]")
            return
        
        group_key = context.args[0].upper()
        if group_key not in GROUPS_CONFIG:
            await update.message.reply_text("Invalid group. Use: A, B, C, D")
            return
        
        group_name = GROUPS_CONFIG[group_key]['group_name']
        standings = self.db.get_group_standings(group_name)
        
        if not standings:
            await update.message.reply_text(f"Table for {group_name} is empty.")
            return
        
        text = f"{group_name}\n"
        text += "-" * 40 + "\n"
        text += f"{'#':<3} {'Nick':<20} {'G':>2} {'W':>2} {'L':>2} {'D':>2} {'Pts':>3} {'Goals':>8}\n"
        text += "-" * 40 + "\n"
        
        for i, p in enumerate(standings, 1):
            goals = f"{p['goals_scored']}-{p['goals_conceded']}"
            text += f"{i:<3} {p['player_nick']:<20} {p['games']:>2} {p['wins']:>2} {p['losses']:>2} {p['draws']:>2} {p['points']:>3} {goals:>8}\n"
        
        topic_name = GROUPS_CONFIG[group_key]['topic_name']
        await self.send_to_topic(update, context, topic_name, text)
    
    async def cmd_init_players(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            return
        
        total_added = 0
        for group_name, players in PLAYERS.items():
            for nick in players:
                if self.db.add_player(nick):
                    self.db.update_group_standings(nick, group_name)
                    total_added += 1
        
        await update.message.reply_text(f"Added {total_added} players and initialized standings!")
    
    async def cmd_clear_players(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            return
        
        self.db.cursor.execute('DELETE FROM players')
        self.db.cursor.execute('DELETE FROM elo')
        self.db.cursor.execute('DELETE FROM group_standings')
        self.db.cursor.execute('DELETE FROM group_matches')
        self.db.commit()
        
        await update.message.reply_text("Database cleared!")
    
    async def cmd_set_nick(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        match = re.match(r'^!nick\s+(\S.+)', update.message.text)
        if not match:
            return
        
        nick = match.group(1).strip()
        
        if len(nick) < 2 or len(nick) > 30:
            await update.message.reply_text("Nick must be 2-30 characters.")
            return
        
        self.db.add_player(nick)
        await update.message.reply_text(f"Nick set: {nick}")
    
    async def cmd_profile(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Use !matches [nick] to view your matches.")
    
    async def cmd_my_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        msg_text = update.message.text.replace('!matches', '').strip()
        
        if not msg_text:
            await update.message.reply_text("Usage: !matches [nick]")
            return
        
        matches = self.db.get_group_matches(nick=msg_text, status='pending')
        
        if not matches:
            await update.message.reply_text(f"No pending matches for {msg_text}.")
            return
        
        text = f"Matches for {msg_text}:\n\n"
        
        for m in matches:
            opponent = m['player2_nick'] if m['player1_nick'] == msg_text else m['player1_nick']
            text += f"vs {opponent} ({m['group_name']}, round {m['round_num']})\n"
        
        await update.message.reply_text(text)
    
    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (
            "Commands:\n\n"
            "!nick [nick] - set gaming nick\n"
            "!matches [nick] - my matches\n"
            "/elo - ELO table\n"
            "/standings [A/B/C/D] - group table\n\n"
            "Send screenshot with caption:\n"
            "@Player1 - @Player2"
        )
        await update.message.reply_text(text)
    
    async def cmd_commands(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (
            "UNIVERSE OF HEROES - Commands\n\n"
            "Player:\n"
            "!nick [nick] - set your gaming nick\n"
            "!matches [nick] - view your matches\n"
            "!profile - view profile\n\n"
            "Public:\n"
            "/elo - ELO rating table\n"
            "/standings [A/B/C/D] - group standings\n"
            "/help - show this help\n\n"
            "Results:\n"
            "Send screenshot with caption:\n"
            "@Player1 - @Player2"
        )
        await update.message.reply_text(text)
    
    async def handle_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        caption = update.message.caption or ""
        if not caption:
            return
        
        nick_match = re.match(r'@?(\S+)\s*[-]\s*@?(\S+)', caption)
        if not nick_match:
            return
        
        nick1 = nick_match.group(1).replace('@', '')
        nick2 = nick_match.group(2).replace('@', '')
        
        p1 = self.db.get_player(nick1)
        p2 = self.db.get_player(nick2)
        
        if not p1:
            await update.message.reply_text(f"Player '{nick1}' not found.")
            return
        if not p2:
            await update.message.reply_text(f"Player '{nick2}' not found.")
            return
        
        photos = update.message.photo
        photo = photos[-1]
        
        try:
            photo_file = await context.bot.get_file(photo.file_id)
            photo_path = f"screenshots/{photo.file_id}.jpg"
            await photo_file.download_to_drive(photo_path)
            
            screenshot_text = self.screenshot_analyzer.extract_text(photo_path)
            score1, score2 = self.screenshot_analyzer.extract_scores(screenshot_text)
            
            if score1 is None or score2 is None:
                await update.message.reply_text(
                    f"Could not recognize score.\n{nick1} vs {nick2}\n\n"
                    "Enter result manually:\n"
                    "1) ?-?"
                )
                self.pending_results[update.effective_user.id] = {
                    'nick1': nick1,
                    'nick2': nick2,
                    'mode': 'manual'
                }
                return
            
            await self.confirm_result(update, nick1, nick2, score1, score2)
            
        except Exception as e:
            logger.error(f"Error processing photo: {e}")
            await update.message.reply_text(f"Error: {e}")
    
    async def confirm_result(self, update: Update, nick1: str, nick2: str, score1: int, score2: int):
        text = (
            f"Confirm result:\n\n"
            f"{nick1} vs {nick2}\n"
            f"Score: {score1} - {score2}\n\n"
            "Correct? (Yes/No)"
        )
        
        self.pending_results[update.effective_user.id] = {
            'nick1': nick1,
            'nick2': nick2,
            'score1': score1,
            'score2': score2
        }
        
        keyboard = [
            [InlineKeyboardButton("Yes", callback_data="confirm_yes"),
             InlineKeyboardButton("No", callback_data="confirm_no")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(text, reply_markup=reply_markup)
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data == "confirm_yes":
            user_id = query.from_user.id
            if user_id in self.pending_results:
                result = self.pending_results[user_id]
                await self.save_result(query, result)
                del self.pending_results[user_id]
        
        elif data == "confirm_no":
            user_id = query.from_user.id
            if user_id in self.pending_results:
                del self.pending_results[user_id]
            await query.edit_message_text("Cancelled.")
    
    async def save_result(self, update, result: Dict):
        nick1 = result['nick1']
        nick2 = result['nick2']
        score1 = result['score1']
        score2 = result['score2']
        
        match = self.find_match(nick1, nick2)
        if not match:
            await update.message.reply_text("Active match not found.")
            return
        
        self.db.update_group_match_result(match['id'], score1, score2, nick1)
        self.process_elo(nick1, nick2, score1, score2, match['group_name'])
        self.db.update_group_standings(nick1, match['group_name'])
        self.db.update_group_standings(nick2, match['group_name'])
        
        await update.message.reply_text(
            f"Result saved!\n\n{nick1} {score1} - {score2} {nick2}"
        )
    
    def find_match(self, nick1: str, nick2: str) -> Optional[Dict]:
        matches = self.db.get_group_matches(nick=nick1, status='pending')
        for m in matches:
            if (m['player1_nick'] == nick1 and m['player2_nick'] == nick2) or \
               (m['player1_nick'] == nick2 and m['player2_nick'] == nick1):
                return m
        return None
    
    def process_elo(self, nick1: str, nick2: str, score1: int, score2: int, group_name: str):
        elo1 = self.db.get_elo(nick1)
        elo2 = self.db.get_elo(nick2)
        
        if not elo1 or not elo2:
            return
        
        if score1 > score2:
            new_r1, new_r2, change = self.elo.calculate(elo1['rating'], elo2['rating'], 1.0)
            self.db.update_elo(nick1, change, 'win', score1, score2)
            self.db.update_elo(nick2, -change, 'loss', score2, score1)
        elif score2 > score1:
            new_r1, new_r2, change = self.elo.calculate(elo1['rating'], elo2['rating'], 0.0)
            self.db.update_elo(nick1, change, 'loss', score1, score2)
            self.db.update_elo(nick2, -change, 'win', score2, score1)
        else:
            self.db.update_elo(nick1, 0, 'draw', score1, score2)
            self.db.update_elo(nick2, 0, 'draw', score2, score1)
    
    def run(self):
        print("Universe of Heroes Bot started...")
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    if not TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN not found in .env")
    
    bot = UniverseHeroesBot(TOKEN)
    
    ADMIN_IDS = os.getenv("ADMIN_IDS")
    if ADMIN_IDS:
        for admin_id in ADMIN_IDS.split(','):
            try:
                bot.db.add_admin(int(admin_id.strip()))
            except ValueError:
                pass
    
    bot.run()
