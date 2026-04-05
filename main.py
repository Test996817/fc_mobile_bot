# main.py - FC Mobile Tournament Bot v2
import logging
import sqlite3
import json
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
from dataclasses import dataclass
from enum import Enum

USE_POSTGRES = bool(os.getenv("DATABASE_URL"))

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

from ai_service import AIService
from graphics_renderer import GraphicsRenderer

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
        self.conn.row_factory = sqlite3.Row
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
                topic_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        try:
            self.cursor.execute('ALTER TABLE tournaments ADD COLUMN playoff_message_id INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        
        try:
            self.cursor.execute('ALTER TABLE tournaments ADD COLUMN topic_id INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        
        try:
            self.cursor.execute('ALTER TABLE tournaments ADD COLUMN groups_topic_id INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        
        try:
            self.cursor.execute('ALTER TABLE tournaments ADD COLUMN groups_message_id INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        
        try:
            self.cursor.execute('ALTER TABLE tournaments ADD COLUMN results_topic_id INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        
        try:
            self.cursor.execute('ALTER TABLE tournaments ADD COLUMN reg_message_id INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

        try:
            self.cursor.execute('ALTER TABLE tournaments ADD COLUMN groups_graphic_message_id INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

        try:
            self.cursor.execute('ALTER TABLE tournaments ADD COLUMN playoff_graphic_message_id INTEGER')
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

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_rating_snapshots (
                tournament_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                rating_start INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (tournament_id, user_id)
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
                         groups_count: int = 0, topic_id: int = None) -> int:
        self.cursor.execute('''
            INSERT INTO tournaments (name, format, chat_id, created_by, 
                                   max_players, min_players, deadline_days, groups_count, topic_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (name, format, chat_id, created_by, max_players, min_players, deadline_days, groups_count, topic_id))
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
            WHERE chat_id = ? AND status = 'registration'
            ORDER BY created_at DESC LIMIT 1
        ''', (chat_id,))
        row = self.cursor.fetchone()
        if row:
            return self._row_to_tournament(row)
        
        self.cursor.execute('''
            SELECT * FROM tournaments 
            WHERE chat_id = ? AND status = 'in_progress'
            ORDER BY created_at DESC LIMIT 1
        ''', (chat_id,))
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
            'players': players,
            'topic_id': row[11] if len(row) > 11 else None,
            'playoff_message_id': row[12] if len(row) > 12 else None,
            'groups_topic_id': row[13] if len(row) > 13 else None,
            'groups_message_id': row[14] if len(row) > 14 else None,
            'results_topic_id': row[15] if len(row) > 15 else None,
            'reg_message_id': row[16] if len(row) > 16 else None,
            'groups_graphic_message_id': row[17] if len(row) > 17 else None,
            'playoff_graphic_message_id': row[18] if len(row) > 18 else None,
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
    
    def get_all_players(self) -> List[Dict]:
        self.cursor.execute('SELECT * FROM players WHERE ingame_nick IS NOT NULL ORDER BY rating DESC')
        return [self._row_to_player(row) for row in self.cursor.fetchall()]

    def delete_tournament_matches(self, tournament_id: int):
        self.cursor.execute('DELETE FROM matches WHERE tournament_id = ?', (tournament_id,))
        self.conn.commit()
    
    def update_tournament_groups_info(self, tournament_id: int, topic_id: int, message_id: int):
        self.cursor.execute('''
            UPDATE tournaments SET groups_topic_id = ?, groups_message_id = ?
            WHERE id = ?
        ''', (topic_id, message_id, tournament_id))
        self.conn.commit()
    
    def update_tournament_results_topic(self, tournament_id: int, topic_id: int):
        self.cursor.execute('''
            UPDATE tournaments SET results_topic_id = ? WHERE id = ?
        ''', (topic_id, tournament_id))
        self.conn.commit()
    
    def update_tournament_reg_message(self, tournament_id: int, message_id: int):
        self.cursor.execute('''
            UPDATE tournaments SET reg_message_id = ? WHERE id = ?
        ''', (message_id, tournament_id))
        self.conn.commit()

    def update_tournament_groups_graphic_message(self, tournament_id: int, message_id: int):
        self.cursor.execute('''
            UPDATE tournaments SET groups_graphic_message_id = ? WHERE id = ?
        ''', (message_id, tournament_id))
        self.conn.commit()

    def update_tournament_playoff_graphic_message(self, tournament_id: int, message_id: int):
        self.cursor.execute('''
            UPDATE tournaments SET playoff_graphic_message_id = ? WHERE id = ?
        ''', (message_id, tournament_id))
        self.conn.commit()
    
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
            WHERE tp.tournament_id = ? AND tp.group_name = ? AND tp.status = 'joined'
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
                        WHEN 'bronze' THEN 4
                        WHEN 'final' THEN 5 
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

    def snapshot_tournament_ratings(self, tournament_id: int, user_ids: List[int]):
        self.cursor.execute('DELETE FROM tournament_rating_snapshots WHERE tournament_id = ?', (tournament_id,))
        for user_id in user_ids:
            self.cursor.execute('SELECT rating FROM players WHERE user_id = ?', (user_id,))
            row = self.cursor.fetchone()
            if row:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO tournament_rating_snapshots (tournament_id, user_id, rating_start)
                    VALUES (?, ?, ?)
                ''', (tournament_id, user_id, row[0]))
        self.conn.commit()

    def get_tournament_rating_gains(self, tournament_id: int) -> List[Dict]:
        self.cursor.execute('''
            SELECT s.user_id, p.ingame_nick, s.rating_start, p.rating AS rating_end,
                   (p.rating - s.rating_start) AS gain
            FROM tournament_rating_snapshots s
            JOIN players p ON p.user_id = s.user_id
            WHERE s.tournament_id = ?
            ORDER BY gain DESC, p.ingame_nick ASC
        ''', (tournament_id,))
        rows = self.cursor.fetchall()
        return [
            {
                'user_id': row[0],
                'ingame_nick': row[1],
                'rating_start': row[2],
                'rating_end': row[3],
                'gain': row[4],
            }
            for row in rows
        ]

    def delete_tournament(self, tournament_id: int):
        self.cursor.execute('DELETE FROM matches WHERE tournament_id = ?', (tournament_id,))
        self.cursor.execute('DELETE FROM playoff_matches WHERE tournament_id = ?', (tournament_id,))
        self.cursor.execute('DELETE FROM tournament_rating_snapshots WHERE tournament_id = ?', (tournament_id,))
        self.cursor.execute('DELETE FROM tournament_players WHERE tournament_id = ?', (tournament_id,))
        self.cursor.execute('DELETE FROM tournaments WHERE id = ?', (tournament_id,))
        self.conn.commit()


if USE_POSTGRES:
    from db_postgres import Database
    from db_postgres import AVAILABLE_FORMATS as AVAILABLE_FORMATS


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


class TournamentBot:
    def __init__(self, token: str):
        self.token = token
        self.db = Database()
        self.elo = EloCalculator()
        self.ai_service = AIService()
        self.graphics = GraphicsRenderer()
        self.screenshot_analyzer = ScreenshotAnalyzer()
        self.application = Application.builder().token(token).build()
        self.admin_notifications = {}
        self.cooldowns = {}
        self.ai_cooldowns = {}
        self.media_groups_buffer = {}
        self.media_groups_tasks = {}
        self.pending_match_hints = {}
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
        self.application.add_handler(CommandHandler("cancelmatch", self.cmd_cancel_match))
        self.application.add_handler(CommandHandler("notifyall", self.cmd_notify_all))
        self.application.add_handler(CommandHandler("gresult", self.cmd_gresult))
        self.application.add_handler(CommandHandler("refreshreg", self.cmd_refresh_reg))
        self.application.add_handler(CommandHandler("regen_matches", self.cmd_regen_matches))
        self.application.add_handler(CommandHandler("tinfo", self.cmd_tinfo))
        self.application.add_handler(CommandHandler("dbstats", self.cmd_dbstats))
        self.application.add_handler(CommandHandler("finalpost", self.cmd_finalpost))
        self.application.add_handler(CommandHandler("ai", self.cmd_ai))
        self.application.add_handler(CommandHandler("aihealth", self.cmd_aihealth))
        self.application.add_handler(CommandHandler("gtable", self.cmd_groups_graphic))
        self.application.add_handler(CommandHandler("pbracket", self.cmd_playoff_graphic))
        self.application.add_handler(CommandHandler("resend_groups", self.cmd_resend_groups))

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
        self.application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.Regex(r'^\s*@?\S.{0,40}\s*(?:-|–|—|vs|VS)\s*@?\S.{0,40}\s*$'),
            self.handle_match_hint_message
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

    async def handle_match_hint_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or not update.message.text:
            return

        user_id = update.effective_user.id
        is_admin = self.db.is_admin(user_id)
        player = self.db.get_player(user_id)
        if not player and not is_admin:
            return

        chat_id = update.effective_chat.id
        tournament = self.db.get_tournament_by_chat(chat_id)
        if not tournament:
            return

        results_topic_id = tournament.get('results_topic_id')
        thread_id = update.message.message_thread_id
        if results_topic_id and thread_id != results_topic_id:
            return

        key = (chat_id, thread_id or 0, user_id)
        self.pending_match_hints[key] = {
            "text": update.message.text.strip(),
            "ts": datetime.now().timestamp(),
        }
        await update.message.reply_text("✅ Пара принята. Теперь отправь скриншоты.")
    
    def generate_groups_table(self, tournament_id: int) -> str:
        text = "━━━━━━━━━━━━━━━━━━━━\n🏆 ГРУППОВОЙ ЭТАП\n━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for group_key in ['A', 'B', 'C', 'D']:
            standings = self.db.get_group_standings(tournament_id, f"Группа {group_key}")
            
            text += f"📊 ГРУППА {group_key}\n"
            text += "━━━━━━━━━━━━━━━━━━━━\n"
            text += "Игрок          | И | В | П | Н | Мячи\n"
            text += "━━━━━━━━━━━━━━━━━━━━\n"
            
            if standings:
                for p in standings:
                    nick = (p.get('ingame_nick') or '?')[:14]
                    nick = nick.ljust(14)
                    matches = p.get('matches_played', 0)
                    wins = p.get('wins', 0)
                    losses = p.get('losses', 0)
                    draws = p.get('draws', 0)
                    gs = p.get('goals_scored', 0)
                    gc = p.get('goals_conceded', 0)
                    text += f"{nick} | {matches} | {wins} | {losses} | {draws} | {gs}:{gc}\n"
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
            await update.message.reply_text("Нет ожидающего матча между этими игроками.")
            return
        
        if score1 > score2:
            winner_id = p1['user_id']
        elif score2 > score1:
            winner_id = p2['user_id']
        else:
            winner_id = None

        await self.process_match_result(
            match,
            score1,
            score2,
            winner_id,
            update.effective_user.id,
            send_notification=False,
        )

        p1_new = self.db.get_player(match['player1_id'])
        p2_new = self.db.get_player(match['player2_id'])

        p1_nick = self._copyable_nick(p1.get('ingame_nick'))
        p2_nick = self._copyable_nick(p2.get('ingame_nick'))
        if winner_id == match['player1_id']:
            winner_name = p1_nick
        elif winner_id == match['player2_id']:
            winner_name = p2_nick
        else:
            winner_name = "Ничья"

        p1_delta = p1_new['rating'] - p1['rating']
        p2_delta = p2_new['rating'] - p2['rating']
        text = (
            f"✅ Матч #{match['id']}: {p1_nick} {score1}:{score2} {p2_nick}\n"
            f"🏆 {winner_name}\n"
            f"📈 ELO: {p1_nick} {p1['rating']}→{p1_new['rating']} ({p1_delta:+d}) | "
            f"{p2_nick} {p2['rating']}→{p2_new['rating']} ({p2_delta:+d})"
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
        msg = update.message
        if not msg or not msg.photo:
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

        screenshots_dir = "screenshots"
        os.makedirs(screenshots_dir, exist_ok=True)

        is_admin = self.db.is_admin(user_id)
        player = self.db.get_player(user_id)
        if not player and not is_admin:
            return

        tournament = self.db.get_tournament_by_chat(chat_id)
        if not tournament:
            return

        results_topic_id = tournament.get('results_topic_id')
        output_thread_id = results_topic_id or thread_id
        if results_topic_id and thread_id != results_topic_id:
            return

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

                if best_candidate and best_total >= 1.20:
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

        caption_match = resolve_match_by_text(caption)
        hint_key_used = None
        if not caption_match:
            now_ts = datetime.now().timestamp()
            candidate_keys = [
                (chat_id, thread_id or 0, user_id),
                (chat_id, results_topic_id or 0, user_id),
            ]

            for key in candidate_keys:
                hint = self.pending_match_hints.get(key)
                if not hint:
                    continue
                if now_ts - hint.get("ts", 0) > 900:
                    self.pending_match_hints.pop(key, None)
                    continue
                caption_match = resolve_match_by_text(hint.get("text", ""))
                if caption_match:
                    hint_key_used = key
                    break

            if not caption_match:
                # fallback: any свежая подсказка этого пользователя в чате
                for key, hint in list(self.pending_match_hints.items()):
                    k_chat, _k_thread, k_user = key
                    if k_chat != chat_id or k_user != user_id:
                        continue
                    if now_ts - hint.get("ts", 0) > 900:
                        self.pending_match_hints.pop(key, None)
                        continue
                    caption_match = resolve_match_by_text(hint.get("text", ""))
                    if caption_match:
                        hint_key_used = key
                        break

        if not caption_match:
            await self._send_results_reply(
                context,
                chat_id,
                output_thread_id,
                "❌ Не удалось определить матч по подписи.\n"
                "Укажи подпись в формате: player1 - player2",
            )
            return

        self.pending_match_hints.pop((chat_id, thread_id or 0, user_id), None)
        if results_topic_id:
            self.pending_match_hints.pop((chat_id, results_topic_id, user_id), None)
        if hint_key_used:
            self.pending_match_hints.pop(hint_key_used, None)

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

        p1_nick = self._copyable_nick(p1.get('ingame_nick'))
        p2_nick = self._copyable_nick(p2.get('ingame_nick'))
        p1_new_nick = self._copyable_nick(p1_new.get('ingame_nick'))
        p2_new_nick = self._copyable_nick(p2_new.get('ingame_nick'))
        
        winner_name = p1_new_nick if winner_id == match['player1_id'] else p2_new_nick if winner_id else "Ничья"
        
        notification = (
            f"📊 Результат матча #{match['id']}\n\n"
            f"{p1_nick} {score1}:{score2} {p2_nick}\n"
            f"Победитель: {winner_name}\n\n"
            f"📈 Изменение ELO:\n"
            f"{p1_nick}: {p1['rating']} → {p1_new['rating']} ({'+' if p1_new['rating'] > p1['rating'] else ''}{p1_new['rating'] - p1['rating']})\n"
            f"{p2_nick}: {p2['rating']} → {p2_new['rating']} ({'+' if p2_new['rating'] > p2['rating'] else ''}{p2_new['rating'] - p2['rating']})"
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
            "⚙️ Команды для запуска турнира:\n"
            "/tournament_create Название - создать турнир\n"
            "/refreshreg - обновить пост регистрации\n"
            "/tournament_start - начать турнир\n"
            "/tournament_end - завершить турнир\n\n"
            "🎮 Команды управления матчами:\n"
            "/allmatches - все матчи\n"
            "/gresult Player1 13-10 Player2 - вручную результат\n"
            "/tp [ник] - тех. поражение\n"
            "/replace [old] [new] - замена\n"
            "/cancelmatch [ник1] [ник2] - отмена\n"
            "/notifyall - пинг по регистрации\n\n"
            "🏆 Плей-офф и визуал:\n"
            "/playoff - генерация плей-офф\n"
            "/pw [стадия] [№] [ник] [счёт] - результат\n"
            "/gtable - таблица групп (моноширинный текст)\n"
            "/pbracket - сетка плей-офф (моноширинный текст)\n\n"
            "📊 Аналитика и сервис:\n"
            "/elo - таблица рейтинга\n"
            "/tinfo [ID] - информация по турниру\n"
            "/finalpost [ID] - отправить финальный пост\n"
            "/dbstats - статистика базы\n"
            "/ai [вопрос] - общий AI ассистент\n"
            "/aihealth - диагностика AI"
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
            
            self.db.cursor.execute('''
                UPDATE tournament_players SET group_name = %s
                WHERE tournament_id = %s AND user_id = %s
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

    def build_ai_tournament_context(self, tournament: Dict) -> str:
        matches = self.db.get_tournament_matches(tournament['id'])
        pending = len([m for m in matches if m['status'] == 'pending'])
        in_progress = len([m for m in matches if m['status'] == 'in_progress'])
        completed = len([m for m in matches if m['status'] == 'completed'])
        joined = [p for p in tournament['players'] if p['tournament_status'] == 'joined']

        playoff_matches = self.db.get_playoff_matches(tournament['id'])
        playoff_completed = len([m for m in playoff_matches if m.get('status') == 'completed'])

        gains = self.db.get_tournament_rating_gains(tournament['id'])
        top_gains = gains[:5]
        gains_text = "\n".join(
            [f"- {row['ingame_nick']}: {row['gain']:+d}" for row in top_gains]
        ) if top_gains else "- нет данных"

        return (
            f"Турнир #{tournament['id']}\n"
            f"Название: {tournament['name']}\n"
            f"Формат: {tournament['format']}\n"
            f"Статус: {tournament['status']}\n"
            f"Раунд: {tournament.get('current_round', 0)}\n"
            f"Участников: {len(joined)}\n"
            f"Матчи pending: {pending}\n"
            f"Матчи in_progress: {in_progress}\n"
            f"Матчи completed: {completed}\n"
            f"Плей-офф завершено: {playoff_completed}/{len(playoff_matches)}\n"
            f"Топ прирост ELO:\n{gains_text}"
        )

    def build_ai_fallback_response(self, tournament: Dict, question: str) -> str:
        matches = self.db.get_tournament_matches(tournament['id'])
        pending = len([m for m in matches if m['status'] == 'pending'])
        in_progress = len([m for m in matches if m['status'] == 'in_progress'])
        completed = len([m for m in matches if m['status'] == 'completed'])
        joined = [p for p in tournament['players'] if p['tournament_status'] == 'joined']

        playoff_matches = self.db.get_playoff_matches(tournament['id'])
        playoff_completed = len([m for m in playoff_matches if m.get('status') == 'completed'])

        lines = [
            "⚠️ ИИ временно недоступен, даю локальную сводку:",
            "",
            f"📌 Турнир #{tournament['id']} - {tournament['name']}",
            f"Статус: {tournament['status']} | Формат: {tournament['format']}",
            f"Участников: {len(joined)}/{tournament.get('max_players') or '∞'}",
            f"Матчи: pending {pending}, in_progress {in_progress}, completed {completed}",
            f"Плей-офф: {playoff_completed}/{len(playoff_matches)} завершено",
        ]

        if pending > 0:
            lines.extend([
                "",
                "Рекомендации:",
                "1) Дайте напоминание игрокам с pending-матчами (/notifyall).",
                "2) Проверьте корректность топика результатов (RESULTS_TOPIC).",
                "3) Для нераспознанных скринов используйте /gresult.",
            ])

        if question:
            lines.extend(["", f"Ваш вопрос: {question}"])

        return "\n".join(lines)

    def build_ai_general_fallback_response(self, question: str) -> str:
        lines = [
            "⚠️ ИИ временно недоступен.",
            "",
            "Что можно сделать сейчас:",
            "1) Сформулируй вопрос короче и конкретнее.",
            "2) Добавь контекст: цель, ограничения, желаемый результат.",
            "3) Повтори запрос через 10-20 секунд.",
        ]

        if question:
            lines.extend(["", f"Ваш вопрос: {question}"])

        return "\n".join(lines)

    async def cmd_ai(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        if not context.args:
            await update.message.reply_text("Использование: /ai [вопрос]")
            return

        chat_id = update.effective_chat.id
        now = datetime.now().timestamp()
        last_used = self.ai_cooldowns.get(chat_id, 0)
        if now - last_used < 10:
            await update.message.reply_text("⏳ Подожди несколько секунд перед следующим AI-запросом.")
            return

        available, reason = self.ai_service.is_available()
        if not available:
            await update.message.reply_text(f"❌ AI недоступен: {reason}")
            return

        tournament = self.db.get_tournament_by_chat(chat_id)

        user_question = " ".join(context.args).strip()
        if len(user_question) > 600:
            user_question = user_question[:600]

        system_prompt = (
            "Ты ассистент администратора FC Mobile. "
            "Отвечай только на русском, кратко и по делу. "
            "Дай конкретные шаги/рекомендации, если уместно."
        )
        if tournament:
            tournament_context = self.build_ai_tournament_context(tournament)
            user_prompt = (
                f"Контекст турнира:\n{tournament_context}\n\n"
                f"Вопрос администратора: {user_question}"
            )
        else:
            user_prompt = f"Вопрос администратора: {user_question}"

        try:
            await update.message.reply_text("🤖 Анализирую...")
            answer = await asyncio.to_thread(
                self.ai_service.ask,
                system_prompt,
                user_prompt,
                500
            )
            self.ai_cooldowns[chat_id] = now
            provider_used = self.ai_service.last_provider_used
            model_used = self.ai_service.last_model_used
            answer_with_meta = f"{answer}\n\nℹ️ Источник AI: {provider_used} ({model_used})"
            await update.message.reply_text(answer_with_meta[:3800])
        except Exception as e:
            logger.error(f"AI command error: {e}")
            if tournament:
                fallback_text = self.build_ai_fallback_response(tournament, user_question)
            else:
                fallback_text = self.build_ai_general_fallback_response(user_question)
            await update.message.reply_text(fallback_text[:3200])
            await update.message.reply_text(f"🔎 Диагностика AI: {str(e)[:700]}")

    async def cmd_aihealth(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return

        available, reason = self.ai_service.is_available()
        provider = self.ai_service.provider
        groq_set = "yes" if bool(self.ai_service.groq_key) else "no"
        openrouter_set = "yes" if bool(self.ai_service.openrouter_key) else "no"

        text = (
            "🩺 AI HEALTH\n\n"
            f"enabled: {self.ai_service.enabled}\n"
            f"provider mode: {provider}\n"
            f"available: {available}\n"
            f"reason: {reason}\n"
            f"groq key set: {groq_set}\n"
            f"groq model: {self.ai_service.groq_model}\n"
            f"openrouter key set: {openrouter_set}\n"
            f"openrouter model: {self.ai_service.openrouter_model}\n"
            f"timeout: {self.ai_service.timeout_sec}s\n"
            f"last provider used: {self.ai_service.last_provider_used}\n"
            f"last model used: {self.ai_service.last_model_used}"
        )
        await update.message.reply_text(text)

        if not available:
            return

        try:
            answer = await asyncio.to_thread(
                self.ai_service.ask,
                "Ответь одним словом OK.",
                "Проверка связи",
                20,
            )
            provider_used = self.ai_service.last_provider_used
            model_used = self.ai_service.last_model_used
            await update.message.reply_text(
                f"✅ AI test passed: {answer[:200]}\n"
                f"Источник: {provider_used} ({model_used})"
            )
        except Exception as e:
            logger.error(f"AI health check error: {e}")
            await update.message.reply_text(f"❌ AI test failed: {str(e)[:900]}")

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
        
        await update.message.reply_text("Замена игрока выполнена.")
    
    async def cmd_cancel_match(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.db.is_admin(update.effective_user.id):
            await update.message.reply_text("❌ Команда доступна только админам.")
            return
        
        if len(context.args) < 2:
            await update.message.reply_text("Использование: /cancelmatch [ник1] [ник2]")
            return
        
        await update.message.reply_text("Матч отменён.")

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
            'UPDATE tournaments SET playoff_message_id = %s WHERE id = %s',
            (msg.message_id, tournament['id'])
        )
        self.db.conn.commit()

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
