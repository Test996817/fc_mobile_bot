"""
FC Mobile Tournament Bot - PostgreSQL версия
Используйте этот файл если хостинг требует PostgreSQL
"""

import os
import logging
import sqlite3
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    def __init__(self):
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            self.conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
        else:
            self.conn = sqlite3.connect(
                os.getenv("DB_PATH", "tournament_bot.db"), 
                check_same_thread=False
            )
        self.cursor = self.conn.cursor()
        self.create_tables()
    
    def create_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                user_id BIGINT PRIMARY KEY,
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
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                format TEXT NOT NULL,
                max_players INTEGER,
                min_players INTEGER DEFAULT 4,
                chat_id BIGINT NOT NULL,
                status TEXT DEFAULT 'registration',
                deadline_days INTEGER DEFAULT 3,
                current_round INTEGER DEFAULT 0,
                groups_count INTEGER DEFAULT 0,
                created_by BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_players (
                id SERIAL PRIMARY KEY,
                tournament_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                status TEXT DEFAULT 'pending',
                group_name TEXT,
                approved_by BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tournament_id, user_id)
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                tournament_id INTEGER NOT NULL,
                player1_id BIGINT NOT NULL,
                player2_id BIGINT NOT NULL,
                player1_score INTEGER,
                player2_score INTEGER,
                winner_id BIGINT,
                round_num INTEGER DEFAULT 1,
                group_name TEXT,
                match_type TEXT DEFAULT 'round',
                status TEXT DEFAULT 'pending',
                screenshot_id TEXT,
                reported_by BIGINT,
                reported_at TIMESTAMP,
                deadline_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS playoff_matches (
                id SERIAL PRIMARY KEY,
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
            DO $$ BEGIN
                ALTER TABLE tournaments ADD COLUMN playoff_message_id INTEGER;
            EXCEPTION WHEN others THEN NULL;
            END $$;
        ''')
        
        self.conn.commit()
    
    def add_player(self, user_id: int, username: str, ingame_nick: str = None) -> bool:
        try:
            self.cursor.execute('''
                INSERT INTO players (user_id, username, ingame_nick)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                ingame_nick = COALESCE(EXCLUDED.ingame_nick, players.ingame_nick)
            ''', (user_id, username, ingame_nick))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding player: {e}")
            return False
    
    def get_player(self, user_id: int) -> Optional[Dict]:
        self.cursor.execute('SELECT * FROM players WHERE user_id = %s', (user_id,))
        row = self.cursor.fetchone()
        return dict(row) if row else None
    
    def get_player_by_nick(self, ingame_nick: str) -> Optional[Dict]:
        self.cursor.execute('SELECT * FROM players WHERE ingame_nick = %s', (ingame_nick,))
        row = self.cursor.fetchone()
        return dict(row) if row else None
    
    def update_player_nick(self, user_id: int, ingame_nick: str):
        self.cursor.execute('UPDATE players SET ingame_nick = %s WHERE user_id = %s', 
                          (ingame_nick, user_id))
        self.conn.commit()
    
    def add_admin(self, user_id: int):
        self.cursor.execute('INSERT INTO admins (user_id) VALUES (%s) ON CONFLICT DO NOTHING', (user_id,))
        self.conn.commit()
    
    def is_admin(self, user_id: int) -> bool:
        self.cursor.execute('SELECT 1 FROM admins WHERE user_id = %s', (user_id,))
        return self.cursor.fetchone() is not None
    
    def create_tournament(self, name: str, format: str, chat_id: int, 
                         created_by: int, max_players: int = None, 
                         min_players: int = 4, deadline_days: int = 3,
                         groups_count: int = 0) -> int:
        self.cursor.execute('''
            INSERT INTO tournaments (name, format, chat_id, created_by, 
                                   max_players, min_players, deadline_days, groups_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (name, format, chat_id, created_by, max_players, min_players, deadline_days, groups_count))
        return self.cursor.fetchone()['id']
    
    def get_tournament(self, tournament_id: int) -> Optional[Dict]:
        self.cursor.execute('SELECT * FROM tournaments WHERE id = %s', (tournament_id,))
        row = self.cursor.fetchone()
        if not row:
            return None
        result = dict(row)
        result['players'] = self.get_tournament_players(tournament_id)
        return result
    
    def get_tournament_by_chat(self, chat_id: int) -> Optional[Dict]:
        self.cursor.execute('''
            SELECT * FROM tournaments 
            WHERE status = 'registration'
            ORDER BY created_at DESC LIMIT 1
        ''')
        row = self.cursor.fetchone()
        if row:
            return dict(row)
        
        self.cursor.execute('''
            SELECT * FROM tournaments 
            WHERE status = 'in_progress'
            ORDER BY created_at DESC LIMIT 1
        ''')
        row = self.cursor.fetchone()
        return dict(row) if row else None
    
    def update_tournament_status(self, tournament_id: int, status: str):
        self.cursor.execute('UPDATE tournaments SET status = %s WHERE id = %s', 
                          (status, tournament_id))
        self.conn.commit()
    
    def add_player_to_tournament(self, tournament_id: int, user_id: int, status: str = 'joined') -> bool:
        try:
            self.cursor.execute('''
                INSERT INTO tournament_players (tournament_id, user_id, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (tournament_id, user_id) DO UPDATE SET status = %s
            ''', (tournament_id, user_id, status, status))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding player to tournament: {e}")
            return False
    
    def get_player_tournament_status(self, tournament_id: int, user_id: int) -> Optional[Dict]:
        self.cursor.execute('''
            SELECT p.*, tp.status as tournament_status, tp.group_name 
            FROM tournament_players tp
            JOIN players p ON tp.user_id = p.user_id
            WHERE tp.tournament_id = %s AND tp.user_id = %s
        ''', (tournament_id, user_id))
        row = self.cursor.fetchone()
        return dict(row) if row else None
    
    def get_tournament_players(self, tournament_id: int, status: str = None) -> List[Dict]:
        query = '''
            SELECT p.*, tp.status as tournament_status, tp.group_name 
            FROM tournament_players tp
            JOIN players p ON tp.user_id = p.user_id
            WHERE tp.tournament_id = %s
        '''
        params = [tournament_id]
        if status:
            query += ' AND tp.status = %s'
            params.append(status)
        
        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]
    
    def update_tournament_player_status(self, tournament_id: int, user_id: int, 
                                       status: str, approved_by: int = None):
        self.cursor.execute('''
            UPDATE tournament_players 
            SET status = %s, approved_by = %s
            WHERE tournament_id = %s AND user_id = %s
        ''', (status, approved_by, tournament_id, user_id))
        self.conn.commit()
    
    def get_tournaments_by_chat(self, chat_id: int) -> List[Dict]:
        self.cursor.execute('''
            SELECT * FROM tournaments 
            WHERE chat_id = %s
            ORDER BY created_at DESC
        ''', (chat_id,))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def update_tournament_round(self, tournament_id: int, round_num: int):
        self.cursor.execute('UPDATE tournaments SET current_round = %s WHERE id = %s', 
                          (round_num, tournament_id))
        self.conn.commit()
    
    def get_group_standings(self, tournament_id: int, group_name: str) -> List[Dict]:
        players = self.get_tournament_players(tournament_id)
        players = [p for p in players if p.get('group_name') == group_name]
        
        matches = self.get_tournament_matches(tournament_id)
        group_matches = [m for m in matches if m.get('group_name') == group_name]
        
        for p in players:
            p['matches_played'] = 0
            p['wins'] = 0
            p['draws'] = 0
            p['losses'] = 0
            p['goals_scored'] = 0
            p['goals_conceded'] = 0
            p['points'] = 0
            
            for m in group_matches:
                if m['status'] != 'completed':
                    continue
                if m['player1_id'] == p['user_id']:
                    p['matches_played'] += 1
                    p['goals_scored'] += m['player1_score'] or 0
                    p['goals_conceded'] += m['player2_score'] or 0
                    if m['winner_id'] == p['user_id']:
                        p['wins'] += 1
                    elif m['player1_score'] == m['player2_score']:
                        p['draws'] += 1
                    else:
                        p['losses'] += 1
                elif m['player2_id'] == p['user_id']:
                    p['matches_played'] += 1
                    p['goals_scored'] += m['player2_score'] or 0
                    p['goals_conceded'] += m['player1_score'] or 0
                    if m['winner_id'] == p['user_id']:
                        p['wins'] += 1
                    elif m['player1_score'] == m['player2_score']:
                        p['draws'] += 1
                    else:
                        p['losses'] += 1
            
            p['points'] = p['wins'] * 3 + p['draws']
        
        players.sort(key=lambda x: (-x['points'], -(x['goals_scored'] - x['goals_conceded']), -x['goals_scored']))
        return players
    
    def get_tournament_matches(self, tournament_id: int, status: str = None) -> List[Dict]:
        query = 'SELECT * FROM matches WHERE tournament_id = %s'
        params = [tournament_id]
        if status:
            query += ' AND status = %s'
            params.append(status)
        query += ' ORDER BY round_num, group_name, id'
        
        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]
    
    def remove_player_from_tournament(self, tournament_id: int, user_id: int):
        self.cursor.execute('''
            DELETE FROM tournament_players WHERE tournament_id = %s AND user_id = %s
        ''', (tournament_id, user_id))
        self.conn.commit()
    
    def update_match_result(self, match_id: int, score1: int, score2: int,
                          winner_id: int, reported_by: int, screenshot_id: str = None):
        self.cursor.execute('''
            UPDATE matches SET 
            player1_score = %s, player2_score = %s, winner_id = %s,
            status = 'completed', reported_by = %s, screenshot_id = %s,
            reported_at = CURRENT_TIMESTAMP
            WHERE id = %s
        ''', (score1, score2, winner_id, reported_by, screenshot_id, match_id))
        self.conn.commit()
    
    def find_match_between_players(self, tournament_id: int, user1_id: int, user2_id: int, 
                                  status: str = 'pending') -> Optional[Dict]:
        self.cursor.execute('''
            SELECT * FROM matches 
            WHERE tournament_id = %s 
            AND status = %s
            AND ((player1_id = %s AND player2_id = %s) OR (player1_id = %s AND player2_id = %s))
            LIMIT 1
        ''', (tournament_id, status, user1_id, user2_id, user2_id, user1_id))
        row = self.cursor.fetchone()
        return dict(row) if row else None
    
    def get_player_matches(self, user_id: int, tournament_id: int = None, 
                          status: str = 'pending') -> List[Dict]:
        query = 'SELECT * FROM matches WHERE (player1_id = %s OR player2_id = %s)'
        params = [user_id, user_id]
        if tournament_id:
            query += ' AND tournament_id = %s'
            params.append(tournament_id)
        if status:
            query += ' AND status = %s'
            params.append(status)
        
        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]
    
    def create_match(self, tournament_id: int, player1_id: int, player2_id: int,
                    round_num: int = 1) -> int:
        self.cursor.execute('''
            INSERT INTO matches (tournament_id, player1_id, player2_id, round_num)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        ''', (tournament_id, player1_id, player2_id, round_num))
        self.conn.commit()
        return self.cursor.fetchone()['id']
    
    def update_player_stats(self, user_id: int, result: str, goals_scored: int, 
                           goals_conceded: int, rating_change: int):
        self.cursor.execute('''
            UPDATE players SET
            rating = rating + %s,
            wins = wins + CASE WHEN %s = 'win' THEN 1 ELSE 0 END,
            losses = losses + CASE WHEN %s = 'loss' THEN 1 ELSE 0 END,
            draws = draws + CASE WHEN %s = 'draw' THEN 1 ELSE 0 END,
            goals_scored = goals_scored + %s,
            goals_conceded = goals_conceded + %s
            WHERE user_id = %s
        ''', (rating_change, result, result, result, goals_scored, goals_conceded, user_id))
        self.conn.commit()
    
    def get_top_players(self, limit: int = 20) -> List[Dict]:
        self.cursor.execute('''
            SELECT * FROM players 
            WHERE ingame_nick IS NOT NULL
            ORDER BY rating DESC 
            LIMIT %s
        ''', (limit,))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_playoff_matches(self, tournament_id: int, stage: str = None) -> List[Dict]:
        if stage:
            self.cursor.execute('''
                SELECT * FROM playoff_matches 
                WHERE tournament_id = %s AND stage = %s
                ORDER BY match_num
            ''', (tournament_id, stage))
        else:
            self.cursor.execute('''
                SELECT * FROM playoff_matches 
                WHERE tournament_id = %s
                ORDER BY 
                    CASE stage 
                        WHEN '1/8' THEN 1 
                        WHEN '1/4' THEN 2 
                        WHEN '1/2' THEN 3 
                        WHEN 'final' THEN 4 
                    END, match_num
            ''', (tournament_id,))
        
        return [dict(row) for row in self.cursor.fetchall()]
    
    def add_playoff_match(self, tournament_id: int, stage: str, match_num: int, 
                          player1_nick: str = None, player2_nick: str = None) -> int:
        try:
            self.cursor.execute('''
                INSERT INTO playoff_matches (tournament_id, stage, match_num, player1_nick, player2_nick)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            ''', (tournament_id, stage, match_num, player1_nick, player2_nick))
            self.conn.commit()
            return self.cursor.fetchone()['id']
        except Exception:
            self.cursor.execute('''
                UPDATE playoff_matches SET player1_nick = %s, player2_nick = %s
                WHERE tournament_id = %s AND stage = %s AND match_num = %s
            ''', (player1_nick, player2_nick, tournament_id, stage, match_num))
            self.conn.commit()
            return None
    
    def update_playoff_match(self, match_id: int, player1_wins: int = None, player2_wins: int = None,
                             status: str = None, message_id: int = None):
        updates = []
        params = []
        if player1_wins is not None:
            updates.append('player1_wins = %s')
            params.append(player1_wins)
        if player2_wins is not None:
            updates.append('player2_wins = %s')
            params.append(player2_wins)
        if status:
            updates.append('status = %s')
            params.append(status)
        if message_id:
            updates.append('message_id = %s')
            params.append(message_id)
        
        if updates:
            params.append(match_id)
            self.cursor.execute(f"UPDATE playoff_matches SET {', '.join(updates)} WHERE id = %s", params)
            self.conn.commit()
    
    def clear_playoff_matches(self, tournament_id: int):
        self.cursor.execute('DELETE FROM playoff_matches WHERE tournament_id = %s', (tournament_id,))
        self.conn.commit()
    
    def close(self):
        self.conn.close()


class EloCalculator:
    K_FACTOR = 32
    
    def calculate(self, rating_a: int, rating_b: int, score_a: float) -> Tuple[int, int, int]:
        expected_a = 1 / (1 + 10 ** ((rating_b - rating_a) / 400))
        change = int(self.K_FACTOR * (score_a - expected_a))
        new_a = rating_a + change
        new_b = rating_b - change
        return new_a, new_b, abs(change)


class ScreenshotAnalyzer:
    def __init__(self):
        self.tesseract_path = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        self.logger = logging.getLogger(__name__)
        self.logger.info("OCR module initialized")
    
    def extract_text(self, image_path: str) -> str:
        import pytesseract
        from PIL import Image
        
        try:
            pytesseract.pytesseract.tesseract_cmd = self.tesseract_path
            image = Image.open(image_path)
            text = pytesseract.image_to_string(image, lang='eng+rus')
            return text
        except Exception as e:
            self.logger.error(f"OCR error: {e}")
            return ""
    
    def extract_scores(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        import re
        
        patterns = [
            r'(\d{1,2})\s*[-:]\s*(\d{1,2})',
            r'(\d{1,2})\s*[-:]\s*(\d{1,2})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return int(match.group(1)), int(match.group(2))
        
        return None, None

    def delete_tournament(self, tournament_id: int):
        self.cursor.execute('DELETE FROM matches WHERE tournament_id = %s', (tournament_id,))
        self.cursor.execute('DELETE FROM playoff_matches WHERE tournament_id = %s', (tournament_id,))
        self.cursor.execute('DELETE FROM tournament_players WHERE tournament_id = %s', (tournament_id,))
        self.cursor.execute('DELETE FROM tournaments WHERE id = %s', (tournament_id,))
        self.conn.commit()
