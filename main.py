# main.py - FC Mobile Tournament Bot v2
import logging
import sqlite3
import json
import os
import re
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

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

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class TournamentStatus(Enum):
    REGISTRATION = "registration"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@dataclass
class TournamentFormat:
    name: str
    has_groups: bool
    description: str


AVAILABLE_FORMATS: Dict[str, TournamentFormat] = {
    "single_elimination": TournamentFormat(
        name="Single Elimination",
        has_groups=False,
        description="Выбывание после первого поражения"
    ),
    "classical": TournamentFormat(
        name="Классический",
        has_groups=True,
        description="4 группы по 8, плей-офф"
    ),
}


class Database:
    def __init__(self, db_name: str = "tournament_bot.db"):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.create_tables()
    
    def create_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                ingame_nick TEXT,
                rating INTEGER DEFAULT 1000,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                draws INTEGER DEFAULT 0,
                goals_scored INTEGER DEFAULT 0,
                goals_conceded INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournaments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                format TEXT NOT NULL,
                max_players INTEGER,
                min_players INTEGER DEFAULT 4,
                chat_id INTEGER NOT NULL,
                status TEXT DEFAULT 'registration',
                deadline_days INTEGER DEFAULT 3,
                current_round INTEGER DEFAULT 0,
                groups_count INTEGER DEFAULT 0,
                created_by INTEGER NOT NULL,
                playoff_message_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        try:
            self.cursor.execute('ALTER TABLE tournaments ADD COLUMN playoff_message_id INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                group_name TEXT,
                approved_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tournament_id, user_id)
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id INTEGER NOT NULL,
                player1_id INTEGER NOT NULL,
                player2_id INTEGER NOT NULL,
                player1_score INTEGER,
                player2_score INTEGER,
                winner_id INTEGER,
                round_num INTEGER DEFAULT 1,
                group_name TEXT,
                match_type TEXT DEFAULT 'round',
                status TEXT DEFAULT 'pending',
                screenshot_id TEXT,
                reported_by INTEGER,
                reported_at TIMESTAMP,
                deadline_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS playoff_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id INTEGER NOT NULL,
                stage TEXT NOT NULL,
                match_num INTEGER NOT NULL,
                player1_nick TEXT,
                player2_nick TEXT,
                player1_wins INTEGER DEFAULT 0,
                player2_wins INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                message_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tournament_id, stage, match_num)
            )
        ''')
        
        self.conn.commit()
    
    def add_player(self, user_id: int, username: str, ingame_nick: str = None) -> bool:
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO players (user_id, username, ingame_nick)
                VALUES (?, ?, ?)
            ''', (user_id, username, ingame_nick))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding player: {e}")
            return False
    
    def get_player(self, user_id: int) -> Optional[Dict]:
        self.cursor.execute('SELECT * FROM players WHERE user_id = ?', (user_id,))
        row = self.cursor.fetchone()
        if row:
            return self._row_to_player(row)
        return None
    
    def get_player_by_nick(self, ingame_nick: str) -> Optional[Dict]:
        self.cursor.execute('SELECT * FROM players WHERE ingame_nick = ?', (ingame_nick,))
        row = self.cursor.fetchone()
        if row:
            return self._row_to_player(row)
        return None
    
    def _row_to_player(self, row) -> Dict:
        return {
            'user_id': row[0],
            'username': row[1],
            'ingame_nick': row[2],
            'rating': row[3],
            'wins': row[4],
            'losses': row[5],
            'draws': row[6],
            'goals_scored': row[7],
            'goals_conceded': row[8],
            'created_at': row[9]
        }
    
    def update_player_nick(self, user_id: int, ingame_nick: str):
        self.cursor.execute('UPDATE players SET ingame_nick = ? WHERE user_id = ?', 
                          (ingame_nick, user_id))
        self.conn.commit()
    
    def add_admin(self, user_id: int):
        self.cursor.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (user_id,))
        self.conn.commit()
    
    def is_admin(self, user_id: int) -> bool:
        self.cursor.execute('SELECT 1 FROM admins WHERE user_id = ?', (user_id,))
        return self.cursor.fetchone() is not None
    
    def create_tournament(self, name: str, format: str, chat_id: int, 
                         created_by: int, max_players: int = None, 
                         min_players: int = 4, deadline_days: int = 3,
                         groups_count: int = 0) -> int:
        self.cursor.execute('''
            INSERT INTO tournaments (name, format, chat_id, created_by, 
                                   max_players, min_players, deadline_days, groups_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (name, format, chat_id, created_by, max_players, min_players, deadline_days, groups_count))
        self.conn.commit()
        return self.cursor.lastrowid
    
    def get_tournament(self, tournament_id: int) -> Optional[Dict]:
        self.cursor.execute('SELECT * FROM tournaments WHERE id = ?', (tournament_id,))
        row = self.cursor.fetchone()
        if row:
            return self._row_to_tournament(row)
        return None
    
    def get_tournament_by_chat(self, chat_id: int) -> Optional[Dict]:
        self.cursor.execute('''
            SELECT * FROM tournaments 
            WHERE status = 'registration'
            ORDER BY created_at DESC LIMIT 1
        ''')
        row = self.cursor.fetchone()
        if row:
            self.cursor.execute('UPDATE tournaments SET chat_id = ? WHERE id = ?', (chat_id, row[0]))
            self.conn.commit()
            return self._row_to_tournament(row)
        
        self.cursor.execute('''
            SELECT * FROM tournaments 
            WHERE status = 'in_progress'
            ORDER BY created_at DESC LIMIT 1
        ''')
        row = self.cursor.fetchone()
        if row:
            return self._row_to_tournament(row)
        
        return None
    
    def get_tournaments_by_chat(self, chat_id: int) -> List[Dict]:
        self.cursor.execute('''
            SELECT * FROM tournaments 
            WHERE chat_id = ?
            ORDER BY created_at DESC
        ''', (chat_id,))
        return [self._row_to_tournament(row) for row in self.cursor.fetchall()]
    
    def _row_to_tournament(self, row) -> Dict:
        players = self.get_tournament_players(row[0])
        return {
            'id': row[0],
            'name': row[1],
            'format': row[2],
            'max_players': row[3],
            'min_players': row[4],
            'chat_id': row[5],
            'status': row[6],
            'deadline_days': row[7],
            'current_round': row[8],
            'groups_count': row[9],
            'created_by': row[10],
            'players': players
        }
    
    def update_tournament_status(self, tournament_id: int, status: str):
        self.cursor.execute('UPDATE tournaments SET status = ? WHERE id = ?', 
                          (status, tournament_id))
        self.conn.commit()
    
    def update_tournament_round(self, tournament_id: int, round_num: int):
        self.cursor.execute('UPDATE tournaments SET current_round = ? WHERE id = ?', 
                          (round_num, tournament_id))
        self.conn.commit()
    
    def get_tournament_players(self, tournament_id: int, status: str = None) -> List[Dict]:
        query = '''
            SELECT p.*, tp.status, tp.group_name 
            FROM tournament_players tp
            JOIN players p ON tp.user_id = p.user_id
            WHERE tp.tournament_id = ?
        '''
        params = [tournament_id]
        if status:
            query += ' AND tp.status = ?'
            params.append(status)
        
        self.cursor.execute(query, params)
        players = []
        for row in self.cursor.fetchall():
            p = self._row_to_player(row[:10])
            p['tournament_status'] = row[10]
            p['group_name'] = row[11]
            players.append(p)
        return players
    
    def add_player_to_tournament(self, tournament_id: int, user_id: int, status: str = 'pending') -> bool:
        try:
            self.cursor.execute('''
                INSERT INTO tournament_players (tournament_id, user_id, status)
                VALUES (?, ?, ?)
            ''', (tournament_id, user_id, status))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            self.cursor.execute('''
                UPDATE tournament_players SET status = ? WHERE tournament_id = ? AND user_id = ?
            ''', (status, tournament_id, user_id))
            self.conn.commit()
            return True
    
    def get_player_tournament_status(self, tournament_id: int, user_id: int) -> Optional[Dict]:
        self.cursor.execute('''
            SELECT p.*, tp.status, tp.group_name 
            FROM tournament_players tp
            JOIN players p ON tp.user_id = p.user_id
            WHERE tp.tournament_id = ? AND tp.user_id = ?
        ''', (tournament_id, user_id))
        row = self.cursor.fetchone()
        if row:
            p = self._row_to_player(row[:10])
            p['tournament_status'] = row[10]
            p['group_name'] = row[11]
            return p
        return None
    
    def update_tournament_player_status(self, tournament_id: int, user_id: int, 
                                       status: str, approved_by: int = None):
        self.cursor.execute('''
            UPDATE tournament_players 
            SET status = ?, approved_by = ?
            WHERE tournament_id = ? AND user_id = ?
        ''', (status, approved_by, tournament_id, user_id))
        self.conn.commit()
    
    def remove_player_from_tournament(self, tournament_id: int, user_id: int):
        self.cursor.execute('''
            DELETE FROM tournament_players WHERE tournament_id = ? AND user_id = ?
        ''', (tournament_id, user_id))
        self.conn.commit()
    
    def create_match(self, tournament_id: int, player1_id: int, player2_id: int,
                   round_num: int = 1, group_name: str = None, 
                   match_type: str = 'round', deadline_days: int = 3) -> int:
        deadline = datetime.now() + timedelta(days=deadline_days)
        
        self.cursor.execute('''
            INSERT INTO matches (tournament_id, player1_id, player2_id, round_num,
                               group_name, match_type, deadline_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (tournament_id, player1_id, player2_id, round_num, 
              group_name, match_type, deadline.isoformat()))
        self.conn.commit()
        return self.cursor.lastrowid
    
    def get_match(self, match_id: int) -> Optional[Dict]:
        self.cursor.execute('SELECT * FROM matches WHERE id = ?', (match_id,))
        row = self.cursor.fetchone()
        if row:
            return self._row_to_match(row)
        return None
    
    def get_tournament_matches(self, tournament_id: int, status: str = None) -> List[Dict]:
        query = 'SELECT * FROM matches WHERE tournament_id = ?'
        params = [tournament_id]
        if status:
            query += ' AND status = ?'
            params.append(status)
        query += ' ORDER BY round_num, group_name, id'
        
        self.cursor.execute(query, params)
        return [self._row_to_match(row) for row in self.cursor.fetchall()]
    
    def get_player_matches(self, user_id: int, tournament_id: int = None, 
                          status: str = 'pending') -> List[Dict]:
        query = 'SELECT * FROM matches WHERE (player1_id = ? OR player2_id = ?)'
        params = [user_id, user_id]
        if tournament_id:
            query += ' AND tournament_id = ?'
            params.append(tournament_id)
        if status:
            query += ' AND status = ?'
            params.append(status)
        query += ' ORDER BY deadline_at'
        
        self.cursor.execute(query, params)
        return [self._row_to_match(row) for row in self.cursor.fetchall()]
    
    def find_match_between_players(self, tournament_id: int, user1_id: int, user2_id: int, 
                                  status: str = 'pending') -> Optional[Dict]:
        self.cursor.execute('''
            SELECT * FROM matches 
            WHERE tournament_id = ? 
            AND status = ?
            AND ((player1_id = ? AND player2_id = ?) OR (player1_id = ? AND player2_id = ?))
            LIMIT 1
        ''', (tournament_id, status, user1_id, user2_id, user2_id, user1_id))
        row = self.cursor.fetchone()
        if row:
            return self._row_to_match(row)
        return None
    
    def _row_to_match(self, row) -> Dict:
        return {
            'id': row[0],
            'tournament_id': row[1],
            'player1_id': row[2],
            'player2_id': row[3],
            'player1_score': row[4],
            'player2_score': row[5],
            'winner_id': row[6],
            'round_num': row[7],
            'group_name': row[8],
            'match_type': row[9],
            'status': row[10],
            'screenshot_id': row[11],
            'reported_by': row[12],
            'reported_at': row[13],
            'deadline_at': row[14],
            'created_at': row[15]
        }
    
    def update_match_result(self, match_id: int, score1: int, score2: int, 
                          winner_id: int, reported_by: int, screenshot_id: str = None):
        self.cursor.execute('''
            UPDATE matches 
            SET player1_score = ?, player2_score = ?, winner_id = ?,
                status = 'completed', reported_by = ?, 
                reported_at = CURRENT_TIMESTAMP, screenshot_id = ?
            WHERE id = ?
        ''', (score1, score2, winner_id, reported_by, screenshot_id, match_id))
        self.conn.commit()
    
    def update_match_status(self, match_id: int, status: str):
        self.cursor.execute('UPDATE matches SET status = ? WHERE id = ?', (status, match_id))
        self.conn.commit()
    
    def update_player_stats(self, user_id: int, result: str, goals_scored: int, 
                           goals_conceded: int, rating_change: int = 0):
        if result == 'win':
            self.cursor.execute('''
                UPDATE players SET wins = wins + 1, goals_scored = goals_scored + ?,
                goals_conceded = goals_conceded + ?, rating = rating + ?
                WHERE user_id = ?
            ''', (goals_scored, goals_conceded, rating_change, user_id))
        elif result == 'loss':
            self.cursor.execute('''
                UPDATE players SET losses = losses + 1, goals_scored = goals_scored + ?,
                goals_conceded = goals_conceded + ?, rating = rating + ?
                WHERE user_id = ?
            ''', (goals_scored, goals_conceded, rating_change, user_id))
        elif result == 'draw':
            self.cursor.execute('''
                UPDATE players SET draws = draws + 1, goals_scored = goals_scored + ?,
                goals_conceded = goals_conceded + ?
                WHERE user_id = ?
            ''', (goals_scored, goals_conceded, user_id))
        self.conn.commit()
    
    def get_top_players(self, limit: int = 20) -> List[Dict]:
        self.cursor.execute('''
            SELECT * FROM players 
            ORDER BY rating DESC, wins DESC
            LIMIT ?
        ''', (limit,))
        return [self._row_to_player(row) for row in self.cursor.fetchall()]
    
    def get_group_standings(self, tournament_id: int, group_name: str) -> List[Dict]:
        self.cursor.execute('''
            SELECT p.*, tp.group_name
            FROM tournament_players tp
            JOIN players p ON tp.user_id = p.user_id
            WHERE tp.tournament_id = ? AND tp.group_name = ? AND tp.status = 'approved'
        ''', (tournament_id, group_name))
        
        standings = []
        for row in self.cursor.fetchall():
            p = self._row_to_player(row[:10])
            p['group_name'] = row[10]
            
            matches = self.get_tournament_matches(tournament_id)
            group_matches = [m for m in matches if m['group_name'] == group_name 
                          and (m['player1_id'] == p['user_id'] or m['player2_id'] == p['user_id'])
                          and m['status'] == 'completed']
            
            p['matches_played'] = len(group_matches)
            p['points'] = p['wins'] * 3 + p['draws']
            
            standings.append(p)
        
        standings.sort(key=lambda x: (-x['points'], -(x['goals_scored'] - x['goals_conceded']), -x['goals_scored']))
        return standings
    
    def get_playoff_matches(self, tournament_id: int, stage: str = None) -> List[Dict]:
        if stage:
            self.cursor.execute('''
                SELECT * FROM playoff_matches 
                WHERE tournament_id = ? AND stage = ?
                ORDER BY match_num
            ''', (tournament_id, stage))
        else:
            self.cursor.execute('''
                SELECT * FROM playoff_matches 
                WHERE tournament_id = ?
                ORDER BY 
                    CASE stage 
                        WHEN '1/8' THEN 1 
                        WHEN '1/4' THEN 2 
                        WHEN '1/2' THEN 3 
                        WHEN 'final' THEN 4 
                    END, match_num
            ''', (tournament_id,))
        
        columns = ['id', 'tournament_id', 'stage', 'match_num', 'player1_nick', 'player2_nick',
                   'player1_wins', 'player2_wins', 'status', 'message_id', 'created_at']
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
    
    def add_playoff_match(self, tournament_id: int, stage: str, match_num: int, 
                          player1_nick: str = None, player2_nick: str = None) -> int:
        try:
            self.cursor.execute('''
                INSERT INTO playoff_matches (tournament_id, stage, match_num, player1_nick, player2_nick)
                VALUES (?, ?, ?, ?, ?)
            ''', (tournament_id, stage, match_num, player1_nick, player2_nick))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.IntegrityError:
            self.cursor.execute('''
                UPDATE playoff_matches SET player1_nick = ?, player2_nick = ?
                WHERE tournament_id = ? AND stage = ? AND match_num = ?
            ''', (player1_nick, player2_nick, tournament_id, stage, match_num))
            self.conn.commit()
            return None
    
    def update_playoff_match(self, match_id: int, player1_wins: int = None, player2_wins: int = None,
                             status: str = None, message_id: int = None):
        updates = []
        params = []
        if player1_wins is not None:
            updates.append('player1_wins = ?')
            params.append(player1_wins)
        if player2_wins is not None:
            updates.append('player2_wins = ?')
            params.append(player2_wins)
        if status:
            updates.append('status = ?')
            params.append(status)
        if message_id:
            updates.append('message_id = ?')
            params.append(message_id)
        
        if updates:
            params.append(match_id)
            self.cursor.execute(f'UPDATE playoff_matches SET {", ".join(updates)} WHERE id = ?', params)
            self.conn.commit()
    
    def clear_playoff_matches(self, tournament_id: int):
        self.cursor.execute('DELETE FROM playoff_matches WHERE tournament_id = ?', (tournament_id,))
        self.conn.commit()


class EloCalculator:
    K_FACTOR = 32
    
    def calculate(self, rating_a: int, rating_b: int, score_a: float) -> Tuple[int, int, int]:
        expected_a = 1 / (1 + 10 ** ((rating_b - rating_a) / 400))
        expected_b = 1 - expected_a
        
        actual_b = 1 - score_a
        
        new_rating_a = rating_a + int(self.K_FACTOR * (score_a - expected_a))
        new_rating_b = rating_b + int(self.K_FACTOR * (actual_b - expected_b))
        
        change = new_rating_a - rating_a
        return new_rating_a, new_rating_b, change


class ScreenshotAnalyzer:
    def __init__(self):
        self.ocr_available = False
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
    
    def extract_scores(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        lines = text.strip().split('\n')
        
        for line in lines[:5]:
            line = line.strip()
            if not line:
                continue
            
            numbers = re.findall(r'\d', line)
            if len(numbers) == 4:
                s1 = int(numbers[0] + numbers[1])
                s2 = int(numbers[2] + numbers[3])
                if 0 <= s1 <= 99 and 0 <= s2 <= 99:
                    return s1, s2
            
            match = re.search(r'(\d)\s+(\d)\s*[^\w\s]\s*(\d{1,2})', line)
            if match:
                try:
                    s1 = int(match.group(1) + match.group(2))
                    s2 = int(match.group(3))
                    if 0 <= s1 <= 99 and 0 <= s2 <= 99:
                        return s1, s2
                except ValueError:
                    pass
            
            match = re.search(r'(\d{1,2})\s*[^\w\s]+\s*(\d{1,2})', line)
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


class TournamentBot:
    def __init__(self, token: str):
        self.token = token
        self.db = Database()
        self.elo = EloCalculator()
        self.screenshot_analyzer = ScreenshotAnalyzer()
        self.application = Application.builder().token(token).build()
        self.admin_notifications = {}
        self.join_message_id = None
        self.join_chat_id = None
        self.cooldowns = {}
        self.setup_handlers()
    
    def setup_handlers(self):
        self.application.add_handler(CommandHandler("admin", self.cmd_admin))
        self.application.add_handler(CommandHandler("tournament_create", self.cmd_create_tournament))
        self.application.add_handler(CommandHandler("tournament_start", self.cmd_start_tournament))
        self.application.add_handler(CommandHandler("tournament_end", self.cmd_end_tournament))
        self.application.add_handler(CommandHandler("nextround", self.cmd_next_round))
        self.application.add_handler(CommandHandler("allmatches", self.cmd_matches))
        self.application.add_handler(CommandHandler("standings", self.cmd_standings))
        self.application.add_handler(CommandHandler("group", self.cmd_group))
        self.application.add_handler(CommandHandler("playoff", self.cmd_playoff))
        self.application.add_handler(CommandHandler("pw", self.cmd_playoff_win))
        self.application.add_handler(CommandHandler("elo", self.cmd_elo))
        self.application.add_handler(CommandHandler("tp", self.cmd_tech_loss))
        self.application.add_handler(CommandHandler("replace", self.cmd_replace))
        self.application.add_handler(CommandHandler("cancelmatch", self.cmd_cancel_match))
        
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
            filters.Regex(r'^!help'), 
            self.cmd_help
        ))
        self.application.add_handler(MessageHandler(
            filters.Regex(r'^!commands'), 
            self.cmd_commands
        ))
        
        self.application.add_handler(MessageHandler(filters.PHOTO, self.handle_photo))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
    
    def notify_admin(self, chat_id: int, message: str):
        try:
            self.application.bot.send_message(chat_id=chat_id, text=message)
        except Exception as e:
            logger.error(f"Failed to send admin notification: {e}")
    
    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (
            "📖 Команды бота:\n\n"
            "!nick [ник] - установить игровой ник\n"
            "!profile - твой профиль и статистика\n"
            "!matches - твои матчи"
        )
        await update.message.reply_text(text)
    
    async def cmd_commands(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (
            "📋 КОМАНДЫ:\n\n"
            "!nick [ник] - установить игровой ник\n"
            "!profile - твой профиль и статистика\n"
            "!matches - твои матчи\n"
            "/standings [A/B/C/D] - таблица группы\n"
            "/group - все группы\n"
            "/elo - таблица рейтинга\n\n"
            "📸 Отправь скриншот с результатом:\n"
            "@Player1 - @Player2"
        )
        await update.message.reply_text(text)
    
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
    
    async def cmd_rating(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        top = self.db.get_top_players(20)
        
        if not top:
            await update.message.reply_text("Пока нет игроков в рейтинге.")
            return
        
        text = "🏆 ТАБЛИЦА ЛИДЕРОВ\n\n"
        medals = ["🥇", "🥈", "🥉"]
        
        for i, p in enumerate(top[:15], 1):
            medal = medals[i-1] if i <= 3 else f"{i}."
            name = p['ingame_nick'] or p['username'] or "?"
            text += f"{medal} {name}\n"
            text += f"   ELO: {p['rating']} | В:{p['wins']} П:{p['losses']} Н:{p['draws']}\n"
        
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
    
    async def cmd_standings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        players = self.db.get_tournament_players(tournament['id'], 'approved')
        
        if not players:
            await update.message.reply_text("Нет участников.")
            return
        
        text = f"📊 ТАБЛИЦА '{tournament['name']}':\n\n"
        
        sorted_players = sorted(players, key=lambda x: (-x['rating'], -x['wins']))
        
        for i, p in enumerate(sorted_players[:15], 1):
            text += f"{i}. {p['ingame_nick']}\n"
            text += f"   ELO: {p['rating']} | В:{p['wins']} П:{p['losses']} Н:{p['draws']}\n"
            text += f"   Голы: {p['goals_scored']}:{p['goals_conceded']}\n\n"
        
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
        import time
        user_id = update.effective_user.id
        player = self.db.get_player(user_id)
        
        if not player:
            return
        
        current_time = time.time()
        if user_id in self.cooldowns:
            last_submission = self.cooldowns[user_id]
            if current_time - last_submission < 180:
                remaining = int(180 - (current_time - last_submission))
                await update.message.reply_text(
                    f"⏳ Подожди {remaining} сек. перед следующей отправкой результата."
                )
                return
        
        tournament = self.db.get_tournament_by_chat(update.effective_chat.id)
        if not tournament:
            return
        
        photos = update.message.photo
        
        caption = update.message.caption or ""
        
        if not caption:
            await update.message.reply_text(
                "❌ Укажи ники игроков под скриншотом:\n"
                "player1 - player2"
            )
            return
        
        nick_match = re.match(r'@?(\S+)\s*[-–]\s*@?(\S+)', caption)
        if not nick_match:
            await update.message.reply_text(
                "❌ Неверный формат. Используй:\n"
                "player1 - player2"
            )
            return
        
        nick1 = nick_match.group(1).replace('@', '')
        nick2 = nick_match.group(2).replace('@', '')
        
        p1 = self.db.get_player_by_nick(nick1)
        p2 = self.db.get_player_by_nick(nick2)
        
        if not p1:
            await update.message.reply_text(f"❌ Игрок '{nick1}' не найден в базе.")
            return
        if not p2:
            await update.message.reply_text(f"❌ Игрок '{nick2}' не найден в базе.")
            return
        
        match = self.db.find_match_between_players(tournament['id'], p1['user_id'], p2['user_id'])
        if not match:
            await update.message.reply_text("❌ Нет ожидающего матча между этими игроками.")
            return
        
        results = []
        
        for photo in photos:
            try:
                photo_file = await context.bot.get_file(photo.file_id)
                photo_path = f"screenshots/match_{photo.file_id}.jpg"
                await photo_file.download_to_drive(photo_path)
                
                screenshot_text = self.screenshot_analyzer.extract_text(photo_path)
                score1, score2 = self.screenshot_analyzer.extract_scores(screenshot_text)
                
                if score1 is not None and score2 is not None:
                    if score1 > score2:
                        winner_id = p1['user_id']
                    elif score2 > score1:
                        winner_id = p2['user_id']
                    else:
                        winner_id = None
                    
                    await self.process_match_result(match, score1, score2, winner_id, user_id, photo.file_id)
                    
                    winner_text = f"{nick1}" if winner_id == p1['user_id'] else f"{nick2}" if winner_id == p2['user_id'] else "Ничья"
                    results.append(f"📸 {nick1} {score1}:{score2} {nick2} - {winner_text}")
            except Exception as e:
                logger.error(f"Error processing photo {photo.file_id}: {e}")
        
        if results:
            self.cooldowns[user_id] = current_time
            await update.message.reply_text(
                f"✅ Результаты ({len(results)}/{len(photos)}):\n" + "\n".join(results)
            )
        else:
            await update.message.reply_text(
                "❌ Не удалось распознать счёт ни на одном скриншоте.\n"
                "Убедись что счёт виден на фото."
            )
    
    async def process_match_result(self, match: Dict, score1: int, score2: int,
                                  winner_id: int, reported_by: int, screenshot_id: str = None):
        self.db.update_match_result(match['id'], score1, score2, winner_id, 
                                   reported_by, screenshot_id)
        
        p1 = self.db.get_player(match['player1_id'])
        p2 = self.db.get_player(match['player2_id'])
        
        if winner_id is None:
            result1 = result2 = 'draw'
            goals1 = goals2 = score1
            rating_change = 0
        elif winner_id == match['player1_id']:
            result1, result2 = 'win', 'loss'
            goals1, goals2 = score1, score2
            new_r1, new_r2, rating_change = self.elo.calculate(p1['rating'], p2['rating'], 1.0)
        else:
            result1, result2 = 'loss', 'win'
            goals1, goals2 = score1, score2
            new_r1, new_r2, rating_change = self.elo.calculate(p1['rating'], p2['rating'], 0.0)
        
        change1 = rating_change if result1 == 'win' else -abs(rating_change)
        change2 = -rating_change if result2 == 'win' else abs(rating_change)
        
        self.db.update_player_stats(match['player1_id'], result1, goals1, goals2, change1)
        self.db.update_player_stats(match['player2_id'], result2, goals2, goals1, change2)
        
        p1_new = self.db.get_player(match['player1_id'])
        p2_new = self.db.get_player(match['player2_id'])
        
        winner_name = p1_new['ingame_nick'] if winner_id == match['player1_id'] else p2_new['ingame_nick'] if winner_id else "Ничья"
        
        notification = (
            f"📊 Результат матча #{match['id']}\n\n"
            f"{p1['ingame_nick']} {score1}:{score2} {p2['ingame_nick']}\n"
            f"Победитель: {winner_name}\n\n"
            f"📈 Изменение ELO:\n"
            f"{p1['ingame_nick']}: {p1['rating']} → {p1_new['rating']} ({'+' if p1_new['rating'] > p1['rating'] else ''}{p1_new['rating'] - p1['rating']})\n"
            f"{p2['ingame_nick']}: {p2['rating']} → {p2_new['rating']} ({'+' if p2_new['rating'] > p2['rating'] else ''}{p2_new['rating'] - p2['rating']})"
        )
        
        tournament = self.db.get_tournament(match['tournament_id'])
        if tournament:
            self.notify_admin(tournament['chat_id'], notification)
    
    async def cmd_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("У тебя нет прав админа.")
            return
        
        text = (
            "👑 ПАНЕЛЬ АДМИНИСТРАТОРА\n\n"
            "/tournament_create Название - создать турнир\n"
            "/tournament_start - начать турнир\n"
            "/tournament_end - завершить турнир\n"
            "/standings [A/B/C/D] - таблица группы\n"
            "/group - все 4 группы\n"
            "/elo - таблица рейтинга\n"
            "/tp [ник] - тех. поражение\n"
            "/replace [old] [new] - замена\n"
            "/cancelmatch [ник1] [ник2] - отмена\n"
            "/playoff - генерация плей-офф\n"
            "/pw [стадия] [№] [ник] [счёт] - результат\n"
            "/allmatches - все матчи"
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
                groups_count=groups_count
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
                text += f"  {i}. {p['ingame_nick']} (ELO: {p['rating']})\n"
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
                reply_markup=reply_markup
            )
            self.join_message_id = msg.message_id
            self.join_chat_id = chat_id
        except Exception as e:
            logger.error(f"Error sending join message: {e}")
    
    async def update_join_message(self, chat_id: int, tournament_id: int):
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
                text += f"  {i}. {p['ingame_nick']} (ELO: {p['rating']})\n"
        else:
            text += "  Пока никто не присоединился\n"
        
        text += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        text += "Нажмите кнопку ниже чтобы присоединиться!"
        
        keyboard = [
            [InlineKeyboardButton("✅ ПРИСОЕДИНИТЬСЯ", callback_data="join_tournament")],
            [InlineKeyboardButton("❌ ОТМЕНА УЧАСТИЯ", callback_data="leave_tournament")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        target_chat_id = chat_id if chat_id else self.join_chat_id
        
        if self.join_message_id and target_chat_id:
            try:
                await self.application.bot.edit_message_text(
                    chat_id=target_chat_id,
                    message_id=self.join_message_id,
                    text=text,
                    reply_markup=reply_markup
                )
            except Exception as e:
                logger.error(f"Error updating join message: {e}")
    
    async def cmd_start_tournament(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
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
        
        approved = [p for p in tournament['players'] if p['tournament_status'] == 'approved']
        
        if len(approved) < tournament['min_players']:
            await update.message.reply_text(
                f"Недостаточно игроков. Нужно минимум {tournament['min_players']}, "
                f"одобрено {len(approved)}"
            )
            return
        
        self.db.update_tournament_status(tournament['id'], 'in_progress')
        
        format_obj = AVAILABLE_FORMATS.get(tournament['format'], AVAILABLE_FORMATS['single_elimination'])
        
        if format_obj.has_groups:
            self.create_group_stage(tournament, approved)
            message = f"Групповой этап создан!"
        else:
            self.create_knockout_bracket(tournament, approved)
            message = "Плей-офф создан!"
        
        await update.message.reply_text(
            f"🏆 Турнир '{tournament['name']}' начат!\n\n{message}"
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
            
            self.db.cursor.execute('''
                UPDATE tournament_players SET group_name = ?
                WHERE tournament_id = ? AND user_id = ?
            ''', (group_name, tournament['id'], player['user_id']))
            
            if group_name not in groups:
                groups[group_name] = []
            groups[group_name].append(player)
        
        self.db.conn.commit()
        
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
        
        await update.message.reply_text(
            f"🏆 Турнир '{tournament['name']}' завершён!"
        )
    
    async def cmd_list_tournaments(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
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
    
    async def cmd_matches(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
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
        completed = [m for m in matches if m['status'] == 'completed']
        
        text = f"⚽ МАТЧИ '{tournament['name']}':\n\n"
        
        if pending:
            text += f"⏳ Ожидают ({len(pending)}):\n"
            for m in pending[:5]:
                p1 = self.db.get_player(m['player1_id'])
                p2 = self.db.get_player(m['player2_id'])
                text += f"  #{m['id']} {p1['ingame_nick']} vs {p2['ingame_nick']}\n"
        
        if completed:
            text += f"\n✅ Завершены ({len(completed)}):\n"
            for m in completed[-5:]:
                p1 = self.db.get_player(m['player1_id'])
                p2 = self.db.get_player(m['player2_id'])
                text += f"  #{m['id']} {p1['ingame_nick']} {m['player1_score']}:{m['player2_score']} {p2['ingame_nick']}\n"
        
        await update.message.reply_text(text)
    
    async def cmd_next_round(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
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
        
        current_round = tournament['current_round']
        matches = self.db.get_tournament_matches(tournament['id'], 'pending')
        
        pending_in_round = [m for m in matches if m['round_num'] == current_round + 1]
        
        if pending_in_round:
            await update.message.reply_text(
                f"⚠️ Есть незавершенные матчи раунда {current_round + 1}\n"
                f"Завершите их перед следующим раундом."
            )
            return
        
        completed_in_round = self.db.get_tournament_matches(tournament['id'], 'completed')
        completed_in_round = [m for m in completed_in_round if m['round_num'] == current_round]
        
        self.db.update_tournament_round(tournament['id'], current_round + 1)
        
        format_obj = AVAILABLE_FORMATS.get(tournament['format'])
        
        if format_obj and format_obj.has_groups and current_round == 1:
            self.create_playoffs_from_groups(tournament)
            await update.message.reply_text(f"✅ Раунд {current_round + 1} создан!\nСозданы стыковые матчи.")
        elif completed_in_round:
            self.create_next_knockout_round(tournament, current_round)
            await update.message.reply_text(f"✅ Раунд {current_round + 1} создан!")
        else:
            await update.message.reply_text(f"✅ Раунд {current_round + 1} стартовал!")
    
    def create_playoffs_from_groups(self, tournament: Dict):
        groups = {}
        for player in tournament['players']:
            if player['tournament_status'] == 'approved' and player['group_name']:
                if player['group_name'] not in groups:
                    groups[player['group_name']] = []
                groups[player['group_name']].append(player)
        
        for group_name, group_players in groups.items():
            standings = self.db.get_group_standings(tournament['id'], group_name)
            
            if len(standings) >= 2:
                self.db.create_match(
                    tournament['id'],
                    standings[0]['user_id'],
                    standings[1]['user_id'],
                    round_num=2,
                    match_type='quarterfinal',
                    deadline_days=tournament['deadline_days']
                )
    
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
                    await self.update_join_message(self.join_chat_id, tournament['id'])
                    await query.answer("Вы присоединились!")
            else:
                self.db.add_player_to_tournament(tournament['id'], user_id, 'joined')
                await self.update_join_message(self.join_chat_id, tournament['id'])
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
                await self.update_join_message(self.join_chat_id, tournament['id'])
                await query.answer("Вы покинули турнир!")
            else:
                await query.answer("Вы не были участником", show_alert=True)
    
    async def cmd_standings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Использование: /standings [A/B/C/D]")
            return
        
        group_key = context.args[0].upper()
        if group_key not in ['A', 'B', 'C', 'D']:
            await update.message.reply_text("Использование: /standings [A/B/C/D]")
            return
        
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        standings = self.db.get_group_standings(tournament['id'], f"Группа {group_key}")
        
        if not standings:
            await update.message.reply_text(f"Группа {group_key} пуста.")
            return
        
        text = f"📊 ГРУППА {group_key}\n\n"
        text += "<pre>\n"
        text += f"{'#':<3} {'Ник':<18} {'И':>2} {'В':>2} {'Н':>2} {'П':>2} {'О':>2} {'Голы':>8}\n"
        text += "─" * 45 + "\n"
        
        for i, p in enumerate(standings, 1):
            goals = f"{p['goals_scored']}-{p['goals_conceded']}"
            nick = (p['ingame_nick'] or p.get('nick', '?'))[:16]
            text += f"{i:<3} {nick:<18} {p.get('games', 0):>2} {p.get('wins', 0):>2} {p.get('draws', 0):>2} {p.get('losses', 0):>2} {p.get('points', 0):>2} {goals:>8}\n"
        
        text += "</pre>"
        
        await update.message.reply_text(text, parse_mode='HTML')
    
    async def cmd_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        text = ""
        
        for group_key in ['A', 'B', 'C', 'D']:
            standings = self.db.get_group_standings(tournament['id'], f"Группа {group_key}")
            
            text += f"📊 ГРУППА {group_key}\n\n"
            text += "<pre>\n"
            text += f"{'#':<3} {'Ник':<18} {'И':>2} {'В':>2} {'Н':>2} {'П':>2} {'О':>2} {'Голы':>8}\n"
            text += "─" * 45 + "\n"
            
            if standings:
                for i, p in enumerate(standings, 1):
                    goals = f"{p['goals_scored']}-{p['goals_conceded']}"
                    nick = (p['ingame_nick'] or p.get('nick', '?'))[:16]
                    text += f"{i:<3} {nick:<18} {p.get('games', 0):>2} {p.get('wins', 0):>2} {p.get('draws', 0):>2} {p.get('losses', 0):>2} {p.get('points', 0):>2} {goals:>8}\n"
            else:
                text += "Пусто\n"
            
            text += "</pre>\n\n"
        
        await update.message.reply_text(text, parse_mode='HTML')
    
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
            return
        
        if not context.args:
            await update.message.reply_text("Использование: /tp [ник]")
            return
        
        nick = context.args[0]
        player = self.db.get_player_by_nick(nick)
        
        if not player:
            await update.message.reply_text(f"Игрок '{nick}' не найден.")
            return
        
        await update.message.reply_text(f"Техническое поражение для {nick}.")
    
    async def cmd_replace(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            return
        
        if len(context.args) < 2:
            await update.message.reply_text("Использование: /replace [old_nick] [new_nick]")
            return
        
        await update.message.reply_text("Замена игрока выполнена.")
    
    async def cmd_cancel_match(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            return
        
        if len(context.args) < 2:
            await update.message.reply_text("Использование: /cancelmatch [ник1] [ник2]")
            return
        
        await update.message.reply_text("Матч отменён.")
    
    async def cmd_playoff(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            return
        
        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        
        if not tournament:
            await update.message.reply_text("Нет активного турнира.")
            return
        
        top_16 = []
        for group_key in ['A', 'B', 'C', 'D']:
            standings = self.db.get_group_standings(tournament['id'], f"Группа {group_key}")
            if standings:
                for p in standings[:4]:
                    top_16.append(p)
        
        if len(top_16) < 16:
            await update.message.reply_text(f"Недостаточно игроков. Нужно 16, есть {len(top_16)}.")
            return
        
        self.db.clear_playoff_matches(tournament['id'])
        
        for i in range(8):
            p1 = top_16[i * 2]
            p2 = top_16[i * 2 + 1]
            self.db.add_playoff_match(tournament['id'], '1/8', i + 1, 
                                     p1['ingame_nick'], p2['ingame_nick'])
        
        bracket_text = self.format_playoff_bracket(tournament['id'])
        
        msg = await update.message.reply_text(bracket_text, parse_mode='HTML', disable_web_page_preview=True)
        
        self.db.cursor.execute(
            'UPDATE tournaments SET playoff_message_id = ? WHERE id = ?',
            (msg.message_id, tournament['id'])
        )
        self.db.conn.commit()
    
    def format_playoff_bracket(self, tournament_id: int) -> str:
        stages = [('1/8', 8, 3), ('1/4', 4, 3), ('1/2', 2, 4), ('final', 1, 4)]
        
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
            return
        
        if not context.args or len(context.args) < 3:
            await update.message.reply_text(
                "Использование: /pw [стадия] [номер] [ник_победителя] [счёт1] [счёт2]\n"
                "Пример: /pw 1/8 1 Player1 3 1\n"
                "Стадии: 1/8, 1/4, 1/2, final"
            )
            return
        
        stage = context.args[0]
        if stage not in ['1/8', '1/4', '1/2', 'final']:
            await update.message.reply_text("Неверная стадия. Используйте: 1/8, 1/4, 1/2, final")
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
            except Exception:
                pass
        
        result_text = f"✅ Записан результат:\n"
        result_text += f"{match['player1_nick']} {player1_wins}-{player2_wins} {match['player2_nick']}\n"
        if status == 'completed':
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
