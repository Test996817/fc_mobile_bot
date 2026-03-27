"""
UNIVERSE OF HEROES - Tournament Bot Database
Supports PostgreSQL (Railway) and SQLite
"""

import os
import sqlite3
import logging
from typing import Dict, List, Optional, Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str = "universe_heroes.db"):
        self.db_path = db_path
        database_url = os.getenv("DATABASE_URL")
        self.is_postgres = bool(database_url)

        if self.is_postgres:
            self._connect_postgres(database_url)
        else:
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._dict_conn = False
            logger.info(f"Using SQLite database: {db_path}")
        
        self._cursor = self._conn.cursor()
        self.create_tables()

    def _connect_postgres(self, database_url: str):
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
        except ImportError as e:
            logger.critical("psycopg2 is not installed but DATABASE_URL is set.")
            raise RuntimeError("psycopg2 is required when DATABASE_URL is set") from e

        if "sslmode=" not in database_url:
            separator = "&" if "?" in database_url else "?"
            connect_url = f"{database_url}{separator}sslmode=require"
        else:
            connect_url = database_url

        logger.info("Connecting to PostgreSQL...")
        try:
            self._conn = psycopg2.connect(connect_url, cursor_factory=RealDictCursor)
            self._conn.autocommit = False
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
            self._dict_conn = True
            logger.info("PostgreSQL connection established.")
        except Exception as e:
            logger.critical(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    @property
    def cursor(self):
        return self._cursor
    
    def create_tables(self):
        if self.is_postgres:
            self._create_tables_postgres()
        else:
            self._create_tables_sqlite()
        self._conn.commit()
    
    def _create_tables_sqlite(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nick TEXT UNIQUE NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS elo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_nick TEXT UNIQUE NOT NULL,
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
            CREATE TABLE IF NOT EXISTS group_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_name TEXT NOT NULL,
                player1_nick TEXT NOT NULL,
                player2_nick TEXT NOT NULL,
                player1_home INTEGER DEFAULT 0,
                round_num INTEGER DEFAULT 1,
                match_num INTEGER DEFAULT 1,
                player1_score INTEGER,
                player2_score INTEGER,
                status TEXT DEFAULT 'pending',
                reported_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_standings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_name TEXT NOT NULL,
                player_nick TEXT UNIQUE NOT NULL,
                games INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                draws INTEGER DEFAULT 0,
                points INTEGER DEFAULT 0,
                goals_scored INTEGER DEFAULT 0,
                goals_conceded INTEGER DEFAULT 0,
                goal_diff INTEGER DEFAULT 0,
                avg_goals REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS playoff_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stage TEXT NOT NULL,
                match_num INTEGER NOT NULL,
                player1_nick TEXT,
                player2_nick TEXT,
                player1_wins INTEGER DEFAULT 0,
                player2_wins INTEGER DEFAULT 0,
                player1_goals INTEGER DEFAULT 0,
                player2_goals INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                message_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    
    def _create_tables_postgres(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                id SERIAL PRIMARY KEY,
                nick TEXT UNIQUE NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS elo (
                id SERIAL PRIMARY KEY,
                player_nick TEXT UNIQUE NOT NULL,
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
            CREATE TABLE IF NOT EXISTS group_matches (
                id SERIAL PRIMARY KEY,
                group_name TEXT NOT NULL,
                player1_nick TEXT NOT NULL,
                player2_nick TEXT NOT NULL,
                player1_home INTEGER DEFAULT 0,
                round_num INTEGER DEFAULT 1,
                match_num INTEGER DEFAULT 1,
                player1_score INTEGER,
                player2_score INTEGER,
                status TEXT DEFAULT 'pending',
                reported_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_standings (
                id SERIAL PRIMARY KEY,
                group_name TEXT NOT NULL,
                player_nick TEXT UNIQUE NOT NULL,
                games INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                draws INTEGER DEFAULT 0,
                points INTEGER DEFAULT 0,
                goals_scored INTEGER DEFAULT 0,
                goals_conceded INTEGER DEFAULT 0,
                goal_diff INTEGER DEFAULT 0,
                avg_goals REAL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS playoff_matches (
                id SERIAL PRIMARY KEY,
                stage TEXT NOT NULL,
                match_num INTEGER NOT NULL,
                player1_nick TEXT,
                player2_nick TEXT,
                player1_wins INTEGER DEFAULT 0,
                player2_wins INTEGER DEFAULT 0,
                player1_goals INTEGER DEFAULT 0,
                player2_goals INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                message_id BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    
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
    
    def add_player(self, nick: str) -> bool:
        try:
            if self.is_postgres:
                self.cursor.execute('INSERT INTO players (nick) VALUES (%s) ON CONFLICT DO NOTHING', (nick,))
                self.cursor.execute('INSERT INTO elo (player_nick) VALUES (%s) ON CONFLICT DO NOTHING', (nick,))
            else:
                self.cursor.execute('INSERT OR IGNORE INTO players (nick) VALUES (?)', (nick,))
                self.cursor.execute('INSERT OR IGNORE INTO elo (player_nick) VALUES (?)', (nick,))
            self._conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding player: {e}")
            return False
    
    def get_player(self, nick: str) -> Optional[Dict]:
        if self.is_postgres:
            self.cursor.execute('SELECT * FROM players WHERE nick = %s', (nick,))
        else:
            self.cursor.execute('SELECT * FROM players WHERE nick = ?', (nick,))
        row = self.cursor.fetchone()
        return dict(row) if row else None
    
    def get_all_active_players(self) -> List[Dict]:
        if self.is_postgres:
            self.cursor.execute('SELECT * FROM players WHERE is_active = 1 ORDER BY nick')
        else:
            self.cursor.execute('SELECT * FROM players WHERE is_active = 1 ORDER BY nick')
        return [dict(row) for row in self.cursor.fetchall()]
    
    def deactivate_player(self, old_nick: str) -> bool:
        try:
            if self.is_postgres:
                self.cursor.execute('UPDATE players SET is_active = 0 WHERE nick = %s', (old_nick,))
            else:
                self.cursor.execute('UPDATE players SET is_active = 0 WHERE nick = ?', (old_nick,))
            self._conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error deactivating player: {e}")
            return False
    
    def replace_player(self, old_nick: str, new_nick: str) -> bool:
        try:
            self.add_player(new_nick)
            if self.is_postgres:
                self.cursor.execute('UPDATE group_standings SET player_nick = %s WHERE player_nick = %s', (new_nick, old_nick))
                self.cursor.execute('UPDATE group_matches SET player1_nick = %s WHERE player1_nick = %s', (new_nick, old_nick))
                self.cursor.execute('UPDATE group_matches SET player2_nick = %s WHERE player2_nick = %s', (new_nick, old_nick))
                self.cursor.execute('UPDATE playoff_matches SET player1_nick = %s WHERE player1_nick = %s', (new_nick, old_nick))
                self.cursor.execute('UPDATE playoff_matches SET player2_nick = %s WHERE player2_nick = %s', (new_nick, old_nick))
            else:
                self.cursor.execute('UPDATE group_standings SET player_nick = ? WHERE player_nick = ?', (new_nick, old_nick))
                self.cursor.execute('UPDATE group_matches SET player1_nick = ? WHERE player1_nick = ?', (new_nick, old_nick))
                self.cursor.execute('UPDATE group_matches SET player2_nick = ? WHERE player2_nick = ?', (new_nick, old_nick))
                self.cursor.execute('UPDATE playoff_matches SET player1_nick = ? WHERE player1_nick = ?', (new_nick, old_nick))
                self.cursor.execute('UPDATE playoff_matches SET player2_nick = ? WHERE player2_nick = ?', (new_nick, old_nick))
            self.deactivate_player(old_nick)
            self._conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error replacing player: {e}")
            return False
    
    def get_elo(self, nick: str) -> Optional[Dict]:
        if self.is_postgres:
            self.cursor.execute('SELECT * FROM elo WHERE player_nick = %s', (nick,))
        else:
            self.cursor.execute('SELECT * FROM elo WHERE player_nick = ?', (nick,))
        row = self.cursor.fetchone()
        return dict(row) if row else None
    
    def update_elo(self, nick: str, rating_change: int, result: str, goals_scored: int, goals_conceded: int):
        if self.is_postgres:
            self.cursor.execute('''
                UPDATE elo SET
                rating = rating + %s,
                wins = wins + CASE WHEN %s = 'win' THEN 1 ELSE 0 END,
                losses = losses + CASE WHEN %s = 'loss' THEN 1 ELSE 0 END,
                draws = draws + CASE WHEN %s = 'draw' THEN 1 ELSE 0 END,
                goals_scored = goals_scored + %s,
                goals_conceded = goals_conceded + %s
                WHERE player_nick = %s
            ''', (rating_change, result, result, result, goals_scored, goals_conceded, nick))
        else:
            wins_inc = 1 if result == 'win' else 0
            losses_inc = 1 if result == 'loss' else 0
            draws_inc = 1 if result == 'draw' else 0
            self.cursor.execute('''
                UPDATE elo SET
                rating = rating + ?,
                wins = wins + ?,
                losses = losses + ?,
                draws = draws + ?,
                goals_scored = goals_scored + ?,
                goals_conceded = goals_conceded + ?
                WHERE player_nick = ?
            ''', (rating_change, wins_inc, losses_inc, draws_inc, goals_scored, goals_conceded, nick))
        self._conn.commit()
    
    def get_elo_table(self, limit: int = 50) -> List[Dict]:
        if self.is_postgres:
            self.cursor.execute('SELECT * FROM elo ORDER BY rating DESC LIMIT %s', (limit,))
        else:
            self.cursor.execute('SELECT * FROM elo ORDER BY rating DESC LIMIT ?', (limit,))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_group_standings(self, group_name: str) -> List[Dict]:
        if self.is_postgres:
            self.cursor.execute('''
                SELECT * FROM group_standings WHERE group_name = %s
                ORDER BY points DESC, goal_diff DESC, avg_goals DESC, goals_scored DESC
            ''', (group_name,))
        else:
            self.cursor.execute('''
                SELECT * FROM group_standings WHERE group_name = ?
                ORDER BY points DESC, goal_diff DESC, avg_goals DESC, goals_scored DESC
            ''', (group_name,))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def update_group_standings(self, nick: str, group_name: str):
        matches = self.get_group_matches(nick, group_name, status='completed')
        games = len(matches)
        wins = losses = draws = goals_scored = goals_conceded = 0
        
        for m in matches:
            if m['player1_nick'] == nick:
                goals_scored += m['player1_score'] or 0
                goals_conceded += m['player2_score'] or 0
                if m['player1_score'] > m['player2_score']:
                    wins += 1
                elif m['player1_score'] < m['player2_score']:
                    losses += 1
                else:
                    draws += 1
            else:
                goals_scored += m['player2_score'] or 0
                goals_conceded += m['player1_score'] or 0
                if m['player2_score'] > m['player1_score']:
                    wins += 1
                elif m['player2_score'] < m['player1_score']:
                    losses += 1
                else:
                    draws += 1
        
        points = wins * 3 + draws
        goal_diff = goals_scored - goals_conceded
        avg_goals = round(goals_scored / games, 2) if games > 0 else 0
        
        if self.is_postgres:
            self.cursor.execute('''
                INSERT INTO group_standings (group_name, player_nick, games, wins, losses, draws, 
                    points, goals_scored, goals_conceded, goal_diff, avg_goals)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_nick) DO UPDATE SET
                games = EXCLUDED.games, wins = EXCLUDED.wins, losses = EXCLUDED.losses,
                draws = EXCLUDED.draws, points = EXCLUDED.points, goals_scored = EXCLUDED.goals_scored,
                goals_conceded = EXCLUDED.goals_conceded, goal_diff = EXCLUDED.goal_diff,
                avg_goals = EXCLUDED.avg_goals, updated_at = CURRENT_TIMESTAMP
            ''', (group_name, nick, games, wins, losses, draws, points, goals_scored, goals_conceded, goal_diff, avg_goals))
        else:
            self.cursor.execute('''
                INSERT OR REPLACE INTO group_standings 
                (group_name, player_nick, games, wins, losses, draws, points, goals_scored, goals_conceded, goal_diff, avg_goals)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (group_name, nick, games, wins, losses, draws, points, goals_scored, goals_conceded, goal_diff, avg_goals))
        self._conn.commit()
    
    def get_group_matches(self, nick: str = None, group_name: str = None, status: str = None) -> List[Dict]:
        conditions = []
        params = []
        if nick:
            conditions.append("(player1_nick = %s OR player2_nick = %s)")
            params.extend([nick, nick])
        if group_name:
            conditions.append("group_name = %s")
            params.append(group_name)
        if status:
            conditions.append("status = %s")
            params.append(status)
        
        if self.is_postgres:
            query = "SELECT * FROM group_matches"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY round_num, match_num"
            self.cursor.execute(query, params)
        else:
            query = "SELECT * FROM group_matches WHERE " + " AND ".join(conditions).replace('%s', '?') if conditions else "SELECT * FROM group_matches"
            query += " ORDER BY round_num, match_num"
            self.cursor.execute(query, params if params else [])
        
        return [dict(row) for row in self.cursor.fetchall()]
    
    def add_group_match(self, group_name: str, player1: str, player2: str, round_num: int, match_num: int, player1_home: int) -> int:
        if self.is_postgres:
            self.cursor.execute('''
                INSERT INTO group_matches (group_name, player1_nick, player2_nick, round_num, match_num, player1_home)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            ''', (group_name, player1, player2, round_num, match_num, player1_home))
            result = self.cursor.fetchone()
            self._conn.commit()
            return result[0]
        else:
            self.cursor.execute('''
                INSERT INTO group_matches (group_name, player1_nick, player2_nick, round_num, match_num, player1_home)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (group_name, player1, player2, round_num, match_num, player1_home))
            self._conn.commit()
            return self.cursor.lastrowid
    
    def update_group_match_result(self, match_id: int, score1: int, score2: int, reported_by: str):
        if self.is_postgres:
            self.cursor.execute('''
                UPDATE group_matches SET player1_score = %s, player2_score = %s, 
                status = 'completed', reported_by = %s WHERE id = %s
            ''', (score1, score2, reported_by, match_id))
        else:
            self.cursor.execute('''
                UPDATE group_matches SET player1_score = ?, player2_score = ?,
                status = 'completed', reported_by = ? WHERE id = ?
            ''', (score1, score2, reported_by, match_id))
        self._conn.commit()
    
    def cancel_group_match(self, match_id: int):
        if self.is_postgres:
            self.cursor.execute('DELETE FROM group_matches WHERE id = %s', (match_id,))
        else:
            self.cursor.execute('DELETE FROM group_matches WHERE id = ?', (match_id,))
        self._conn.commit()
    
    def get_playoff_matches(self, stage: str = None) -> List[Dict]:
        if self.is_postgres:
            if stage:
                self.cursor.execute('SELECT * FROM playoff_matches WHERE stage = %s ORDER BY match_num', (stage,))
            else:
                self.cursor.execute('SELECT * FROM playoff_matches ORDER BY match_num')
        else:
            if stage:
                self.cursor.execute('SELECT * FROM playoff_matches WHERE stage = ? ORDER BY match_num', (stage,))
            else:
                self.cursor.execute('SELECT * FROM playoff_matches ORDER BY match_num')
        
        stages_order = ['1/8', '1/4', '1/2', 'final']
        results = sorted([dict(row) for row in self.cursor.fetchall()], 
                        key=lambda x: (stages_order.index(x['stage']), x['match_num']))
        return results
    
    def add_playoff_match(self, stage: str, match_num: int, player1_nick: str = None, player2_nick: str = None) -> int:
        if self.is_postgres:
            self.cursor.execute('''
                INSERT INTO playoff_matches (stage, match_num, player1_nick, player2_nick)
                VALUES (%s, %s, %s, %s) RETURNING id
            ''', (stage, match_num, player1_nick, player2_nick))
            result = self.cursor.fetchone()
            self._conn.commit()
            return result[0]
        else:
            self.cursor.execute('''
                INSERT INTO playoff_matches (stage, match_num, player1_nick, player2_nick)
                VALUES (?, ?, ?, ?)
            ''', (stage, match_num, player1_nick, player2_nick))
            self._conn.commit()
            return self.cursor.lastrowid
    
    def update_playoff_match(self, match_id: int, player1_wins: int = None, player2_wins: int = None,
                           player1_goals: int = None, player2_goals: int = None,
                           status: str = None, message_id: int = None):
        updates = []
        params = []
        if player1_wins is not None:
            updates.append("player1_wins = %s")
            params.append(player1_wins)
        if player2_wins is not None:
            updates.append("player2_wins = %s")
            params.append(player2_wins)
        if player1_goals is not None:
            updates.append("player1_goals = %s")
            params.append(player1_goals)
        if player2_goals is not None:
            updates.append("player2_goals = %s")
            params.append(player2_goals)
        if status:
            updates.append("status = %s")
            params.append(status)
        if message_id:
            updates.append("message_id = %s")
            params.append(message_id)
        
        if updates:
            params.append(match_id)
            if self.is_postgres:
                query = f"UPDATE playoff_matches SET {', '.join(updates)} WHERE id = %s"
            else:
                query = f"UPDATE playoff_matches SET {', '.join(updates)} WHERE id = ?"
            self.cursor.execute(query, params)
            self._conn.commit()
    
    def clear_playoff_matches(self):
        if self.is_postgres:
            self.cursor.execute('DELETE FROM playoff_matches')
        else:
            self.cursor.execute('DELETE FROM playoff_matches')
        self._conn.commit()
    
    def clear_group_matches(self, group_name: str = None):
        if group_name:
            if self.is_postgres:
                self.cursor.execute('DELETE FROM group_matches WHERE group_name = %s', (group_name,))
            else:
                self.cursor.execute('DELETE FROM group_matches WHERE group_name = ?', (group_name,))
        else:
            if self.is_postgres:
                self.cursor.execute('DELETE FROM group_matches')
            else:
                self.cursor.execute('DELETE FROM group_matches')
        self._conn.commit()
    
    def clear_group_standings(self, group_name: str = None):
        if group_name:
            if self.is_postgres:
                self.cursor.execute('DELETE FROM group_standings WHERE group_name = %s', (group_name,))
            else:
                self.cursor.execute('DELETE FROM group_standings WHERE group_name = ?', (group_name,))
        else:
            if self.is_postgres:
                self.cursor.execute('DELETE FROM group_standings')
            else:
                self.cursor.execute('DELETE FROM group_standings')
        self._conn.commit()
    
    def close(self):
        self._conn.close()
