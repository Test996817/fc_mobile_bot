"""
Универсальный адаптер базы данных
Автоматически выбирает: PostgreSQL (если DATABASE_URL) или SQLite
"""

import os
import sqlite3
import logging
from typing import Dict, List, Optional, Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: str = "tournament_bot.db"):
        self.db_path = db_path
        self.is_postgres = bool(os.getenv("DATABASE_URL"))
        
        if self.is_postgres:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            self._conn = psycopg2.connect(os.getenv("DATABASE_URL"), cursor_factory=RealDictCursor)
            self._dict_conn = True
            logger.info("Using PostgreSQL database")
        else:
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._dict_conn = False
            logger.info(f"Using SQLite database: {db_path}")
    
    @property
    def cursor(self):
        return self._conn.cursor()
    
    def create_tables(self):
        if self.is_postgres:
            self._create_tables_postgres()
        else:
            self._create_tables_sqlite()
    
    def _create_tables_sqlite(self):
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
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
        
        self._conn.commit()
    
    def _create_tables_postgres(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                user_id BIGSERIAL PRIMARY KEY,
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
                user_id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self._conn.commit()
    
    def add_player(self, user_id: int, username: str, ingame_nick: str = None) -> bool:
        try:
            if self.is_postgres:
                self.cursor.execute('''
                    INSERT INTO players (user_id, username, ingame_nick)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    ingame_nick = COALESCE(EXCLUDED.ingame_nick, players.ingame_nick)
                ''', (user_id, username, ingame_nick))
            else:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO players (user_id, username, ingame_nick)
                    VALUES (?, ?, ?)
                ''', (user_id, username, ingame_nick))
            self._conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding player: {e}")
            return False
    
    def get_player(self, user_id: int) -> Optional[Dict]:
        if self.is_postgres:
            self.cursor.execute('SELECT * FROM players WHERE user_id = %s', (user_id,))
            row = self.cursor.fetchone()
            return dict(row) if row else None
        else:
            self.cursor.execute('SELECT * FROM players WHERE user_id = ?', (user_id,))
            row = self.cursor.fetchone()
            return self._row_to_player(row) if row else None
    
    def get_player_by_nick(self, ingame_nick: str) -> Optional[Dict]:
        if self.is_postgres:
            self.cursor.execute('SELECT * FROM players WHERE ingame_nick = %s', (ingame_nick,))
            row = self.cursor.fetchone()
            return dict(row) if row else None
        else:
            self.cursor.execute('SELECT * FROM players WHERE ingame_nick = ?', (ingame_nick,))
            row = self.cursor.fetchone()
            return self._row_to_player(row) if row else None
    
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
        if self.is_postgres:
            self.cursor.execute('UPDATE players SET ingame_nick = %s WHERE user_id = %s', 
                              (ingame_nick, user_id))
        else:
            self.cursor.execute('UPDATE players SET ingame_nick = ? WHERE user_id = ?', 
                              (ingame_nick, user_id))
        self._conn.commit()
    
    def add_admin(self, user_id: int):
        if self.is_postgres:
            self.cursor.execute('INSERT INTO admins (user_id) VALUES (%s) ON CONFLICT DO NOTHING', (user_id,))
        else:
            self.cursor.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (user_id,))
        self._conn.commit()
    
    def is_admin(self, user_id: int) -> bool:
        if self.is_postgres:
            self.cursor.execute('SELECT 1 FROM admins WHERE user_id = %s', (user_id,))
        else:
            self.cursor.execute('SELECT 1 FROM admins WHERE user_id = ?', (user_id,))
        return self.cursor.fetchone() is not None
    
    def create_tournament(self, name: str, format: str, chat_id: int, 
                         created_by: int, max_players: int = None, 
                         min_players: int = 4, deadline_days: int = 3,
                         groups_count: int = 0) -> int:
        if self.is_postgres:
            self.cursor.execute('''
                INSERT INTO tournaments (name, format, chat_id, created_by, 
                                       max_players, min_players, deadline_days, groups_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (name, format, chat_id, created_by, max_players, min_players, deadline_days, groups_count))
            result = self.cursor.fetchone()
            self._conn.commit()
            return result[0]
        else:
            self.cursor.execute('''
                INSERT INTO tournaments (name, format, chat_id, created_by, 
                                       max_players, min_players, deadline_days, groups_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (name, format, chat_id, created_by, max_players, min_players, deadline_days, groups_count))
            self._conn.commit()
            return self.cursor.lastrowid
    
    def get_tournament(self, tournament_id: int) -> Optional[Dict]:
        if self.is_postgres:
            self.cursor.execute('SELECT * FROM tournaments WHERE id = %s', (tournament_id,))
            row = self.cursor.fetchone()
            if row:
                result = dict(row)
                result['players'] = self.get_tournament_players(row['id'])
                return result
            return None
        else:
            self.cursor.execute('SELECT * FROM tournaments WHERE id = ?', (tournament_id,))
            row = self.cursor.fetchone()
            if row:
                result = self._row_to_tournament(row)
                result['players'] = self.get_tournament_players(row[0])
                return result
            return None
    
    def get_tournament_by_chat(self, chat_id: int) -> Optional[Dict]:
        if self.is_postgres:
            self.cursor.execute('''
                SELECT * FROM tournaments 
                WHERE chat_id = %s AND status = 'registration'
                ORDER BY created_at DESC LIMIT 1
            ''', (chat_id,))
            row = self.cursor.fetchone()
            if row:
                result = dict(row)
                result['players'] = self.get_tournament_players(row['id'])
                return result
            
            self.cursor.execute('''
                SELECT * FROM tournaments 
                WHERE chat_id = %s AND status = 'in_progress'
                ORDER BY created_at DESC LIMIT 1
            ''', (chat_id,))
            row = self.cursor.fetchone()
            if row:
                result = dict(row)
                result['players'] = self.get_tournament_players(row['id'])
                return result
            return None
        else:
            self.cursor.execute('''
                SELECT * FROM tournaments 
                WHERE chat_id = ? AND status = 'registration'
                ORDER BY created_at DESC LIMIT 1
            ''', (chat_id,))
            row = self.cursor.fetchone()
            if row:
                result = self._row_to_tournament(row)
                result['players'] = self.get_tournament_players(row[0])
                return result
            
            self.cursor.execute('''
                SELECT * FROM tournaments 
                WHERE chat_id = ? AND status = 'in_progress'
                ORDER BY created_at DESC LIMIT 1
            ''', (chat_id,))
            row = self.cursor.fetchone()
            if row:
                result = self._row_to_tournament(row)
                result['players'] = self.get_tournament_players(row[0])
                return result
            return None
    
    def _row_to_tournament(self, row) -> Dict:
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
            'players': []
        }
    
    def update_tournament_status(self, tournament_id: int, status: str):
        if self.is_postgres:
            self.cursor.execute('UPDATE tournaments SET status = %s WHERE id = %s', 
                              (status, tournament_id))
        else:
            self.cursor.execute('UPDATE tournaments SET status = ? WHERE id = ?', 
                              (status, tournament_id))
        self._conn.commit()
    
    def update_tournament_round(self, tournament_id: int, round_num: int):
        if self.is_postgres:
            self.cursor.execute('UPDATE tournaments SET current_round = %s WHERE id = %s', 
                              (round_num, tournament_id))
        else:
            self.cursor.execute('UPDATE tournaments SET current_round = ? WHERE id = ?', 
                              (round_num, tournament_id))
        self._conn.commit()
    
    def get_tournament_players(self, tournament_id: int, status: str = None) -> List[Dict]:
        if self.is_postgres:
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
            players = []
            for row in self.cursor.fetchall():
                p = dict(row)
                p['tournament_status'] = p.pop('tournament_status')
                p['group_name'] = p.pop('group_name')
                players.append(p)
            return players
        else:
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
    
    def add_player_to_tournament(self, tournament_id: int, user_id: int, status: str = 'joined') -> bool:
        try:
            if self.is_postgres:
                self.cursor.execute('''
                    INSERT INTO tournament_players (tournament_id, user_id, status)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (tournament_id, user_id) DO UPDATE SET status = %s
                ''', (tournament_id, user_id, status, status))
            else:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO tournament_players (tournament_id, user_id, status)
                    VALUES (?, ?, ?)
                ''', (tournament_id, user_id, status))
            self._conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding player to tournament: {e}")
            return False
    
    def get_player_tournament_status(self, tournament_id: int, user_id: int) -> Optional[Dict]:
        if self.is_postgres:
            self.cursor.execute('''
                SELECT p.*, tp.status as tournament_status, tp.group_name 
                FROM tournament_players tp
                JOIN players p ON tp.user_id = p.user_id
                WHERE tp.tournament_id = %s AND tp.user_id = %s
            ''', (tournament_id, user_id))
            row = self.cursor.fetchone()
            if row:
                p = dict(row)
                p['tournament_status'] = p.pop('tournament_status')
                p['group_name'] = p.pop('group_name')
                return p
            return None
        else:
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
    
    def remove_player_from_tournament(self, tournament_id: int, user_id: int):
        if self.is_postgres:
            self.cursor.execute('''
                DELETE FROM tournament_players WHERE tournament_id = %s AND user_id = %s
            ''', (tournament_id, user_id))
        else:
            self.cursor.execute('''
                DELETE FROM tournament_players WHERE tournament_id = ? AND user_id = ?
            ''', (tournament_id, user_id))
        self._conn.commit()
    
    def update_tournament_player_status(self, tournament_id: int, user_id: int, 
                                       status: str, approved_by: int = None):
        if self.is_postgres:
            self.cursor.execute('''
                UPDATE tournament_players 
                SET status = %s, approved_by = %s
                WHERE tournament_id = %s AND user_id = %s
            ''', (status, approved_by, tournament_id, user_id))
        else:
            self.cursor.execute('''
                UPDATE tournament_players 
                SET status = ?, approved_by = ?
                WHERE tournament_id = ? AND user_id = ?
            ''', (status, approved_by, tournament_id, user_id))
        self._conn.commit()
    
    def update_match_result(self, match_id: int, score1: int, score2: int,
                          winner_id: int, reported_by: int, screenshot_id: str = None):
        if self.is_postgres:
            self.cursor.execute('''
                UPDATE matches SET 
                player1_score = %s, player2_score = %s, winner_id = %s,
                status = 'completed', reported_by = %s, screenshot_id = %s,
                reported_at = CURRENT_TIMESTAMP
                WHERE id = %s
            ''', (score1, score2, winner_id, reported_by, screenshot_id, match_id))
        else:
            from datetime import datetime
            self.cursor.execute('''
                UPDATE matches SET 
                player1_score = ?, player2_score = ?, winner_id = ?,
                status = 'completed', reported_by = ?, screenshot_id = ?,
                reported_at = ?
                WHERE id = ?
            ''', (score1, score2, winner_id, reported_by, screenshot_id, datetime.now(), match_id))
        self._conn.commit()
    
    def find_match_between_players(self, tournament_id: int, user1_id: int, user2_id: int, 
                                  status: str = 'pending') -> Optional[Dict]:
        if self.is_postgres:
            self.cursor.execute('''
                SELECT * FROM matches 
                WHERE tournament_id = %s 
                AND status = %s
                AND ((player1_id = %s AND player2_id = %s) OR (player1_id = %s AND player2_id = %s))
                LIMIT 1
            ''', (tournament_id, status, user1_id, user2_id, user2_id, user1_id))
            row = self.cursor.fetchone()
            return dict(row) if row else None
        else:
            self.cursor.execute('''
                SELECT * FROM matches 
                WHERE tournament_id = ? 
                AND status = ?
                AND ((player1_id = ? AND player2_id = ?) OR (player1_id = ? AND player2_id = ?))
                LIMIT 1
            ''', (tournament_id, status, user1_id, user2_id, user2_id, user1_id))
            row = self.cursor.fetchone()
            return self._row_to_match(row) if row else None
    
    def get_player_matches(self, user_id: int, tournament_id: int = None, 
                          status: str = 'pending') -> List[Dict]:
        if self.is_postgres:
            query = 'SELECT * FROM matches WHERE (player1_id = %s OR player2_id = %s)'
            params = [user_id, user_id]
            if tournament_id:
                query += ' AND tournament_id = %s'
                params.append(tournament_id)
            if status:
                query += ' AND status = %s'
                params.append(status)
            query += ' ORDER BY created_at'
            
            self.cursor.execute(query, params)
            return [dict(row) for row in self.cursor.fetchall()]
        else:
            query = 'SELECT * FROM matches WHERE (player1_id = ? OR player2_id = ?)'
            params = [user_id, user_id]
            if tournament_id:
                query += ' AND tournament_id = ?'
                params.append(tournament_id)
            if status:
                query += ' AND status = ?'
                params.append(status)
            
            self.cursor.execute(query, params)
            return [self._row_to_match(row) for row in self.cursor.fetchall()]
    
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
    
    def create_match(self, tournament_id: int, player1_id: int, player2_id: int,
                    round_num: int = 1) -> int:
        if self.is_postgres:
            self.cursor.execute('''
                INSERT INTO matches (tournament_id, player1_id, player2_id, round_num)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            ''', (tournament_id, player1_id, player2_id, round_num))
            result = self.cursor.fetchone()
            self._conn.commit()
            return result[0]
        else:
            self.cursor.execute('''
                INSERT INTO matches (tournament_id, player1_id, player2_id, round_num)
                VALUES (?, ?, ?, ?)
            ''', (tournament_id, player1_id, player2_id, round_num))
            self._conn.commit()
            return self.cursor.lastrowid
    
    def update_player_stats(self, user_id: int, result: str, goals_scored: int, 
                           goals_conceded: int, rating_change: int):
        if self.is_postgres:
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
        else:
            wins_inc = 1 if result == 'win' else 0
            losses_inc = 1 if result == 'loss' else 0
            draws_inc = 1 if result == 'draw' else 0
            
            self.cursor.execute('''
                UPDATE players SET
                rating = rating + ?,
                wins = wins + ?,
                losses = losses + ?,
                draws = draws + ?,
                goals_scored = goals_scored + ?,
                goals_conceded = goals_conceded + ?
                WHERE user_id = ?
            ''', (rating_change, wins_inc, losses_inc, draws_inc, goals_scored, goals_conceded, user_id))
        self._conn.commit()
    
    def get_top_players(self, limit: int = 20) -> List[Dict]:
        if self.is_postgres:
            self.cursor.execute('''
                SELECT * FROM players 
                WHERE ingame_nick IS NOT NULL
                ORDER BY rating DESC 
                LIMIT %s
            ''', (limit,))
            return [dict(row) for row in self.cursor.fetchall()]
        else:
            self.cursor.execute('''
                SELECT * FROM players 
                WHERE ingame_nick IS NOT NULL
                ORDER BY rating DESC 
                LIMIT ?
            ''', (limit,))
            return [self._row_to_player(row) for row in self.cursor.fetchall()]
    
    def close(self):
        self._conn.close()
