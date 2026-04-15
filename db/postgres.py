"""
FC Mobile Tournament Bot - PostgreSQL версия
Используйте этот файл если хостинг требует PostgreSQL
"""

import os
import logging
import sqlite3
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta

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
        self._database_url = os.getenv("DATABASE_URL")
        self._use_postgres = bool(self._database_url)
        if self._use_postgres:
            self._reconnect()
        else:
            self.conn = sqlite3.connect(
                os.getenv("DB_PATH", "tournament_bot.db"), 
                check_same_thread=False
            )
        self._raw_cursor = self.conn.cursor()
        self.create_tables()
    
    def _reconnect(self):
        if self._use_postgres:
            self.conn = psycopg2.connect(self._database_url, cursor_factory=RealDictCursor)
            self._raw_cursor = self.conn.cursor()
            logger.info("Database reconnected successfully")
    
    def _execute_with_retry(self, query: str, params: tuple = None, retry_count: int = 3):
        for attempt in range(retry_count):
            try:
                if params:
                    self.cursor.execute(query, params)
                else:
                    self.cursor.execute(query)
                return
            except psycopg2.OperationalError as e:
                if attempt < retry_count - 1:
                    logger.warning(f"Database connection error: {e}. Reconnecting...")
                    self._reconnect()
                else:
                    raise
    
    def __getattr__(self, name):
        if name == 'cursor':
            return self._cursor_with_retry
        return super().__getattribute__(name)
    
    @property
    def _cursor_with_retry(self):
        class RetryCursor:
            def __init__(self, db):
                self._db = db
            
            @property
            def _raw_cursor(self):
                return self._db._raw_cursor
            
            def execute(self, query: str, params: tuple = None):
                for attempt in range(3):
                    try:
                        if params:
                            self._raw_cursor.execute(query, params)
                        else:
                            self._raw_cursor.execute(query)
                        return
                    except psycopg2.OperationalError as e:
                        if attempt < 2:
                            logger.warning(f"Database connection error: {e}. Reconnecting...")
                            self._db._reconnect()
                        else:
                            try:
                                self._db.conn.rollback()
                            except Exception:
                                pass
                            raise
                    except psycopg2.Error:
                        try:
                            self._db.conn.rollback()
                        except Exception:
                            pass
                        raise
            
            def fetchone(self):
                return self._raw_cursor.fetchone()
            
            def fetchall(self):
                return self._raw_cursor.fetchall()
        
        return RetryCursor(self)
    
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
                playoff_message_id INTEGER,
                topic_id BIGINT,
                groups_topic_id BIGINT,
                groups_message_id INTEGER,
                results_topic_id BIGINT,
                reg_message_id INTEGER,
                groups_graphic_message_id INTEGER,
                playoff_graphic_message_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='tournaments' AND column_name='topic_id') THEN
                    ALTER TABLE tournaments ADD COLUMN topic_id BIGINT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='tournaments' AND column_name='playoff_message_id') THEN
                    ALTER TABLE tournaments ADD COLUMN playoff_message_id INTEGER;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='tournaments' AND column_name='groups_topic_id') THEN
                    ALTER TABLE tournaments ADD COLUMN groups_topic_id BIGINT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='tournaments' AND column_name='groups_message_id') THEN
                    ALTER TABLE tournaments ADD COLUMN groups_message_id INTEGER;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='tournaments' AND column_name='results_topic_id') THEN
                    ALTER TABLE tournaments ADD COLUMN results_topic_id BIGINT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='tournaments' AND column_name='reg_message_id') THEN
                    ALTER TABLE tournaments ADD COLUMN reg_message_id INTEGER;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='tournaments' AND column_name='groups_graphic_message_id') THEN
                    ALTER TABLE tournaments ADD COLUMN groups_graphic_message_id INTEGER;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='tournaments' AND column_name='playoff_graphic_message_id') THEN
                    ALTER TABLE tournaments ADD COLUMN playoff_graphic_message_id INTEGER;
                END IF;
            END
            $$;
        """)

        self.cursor.execute("""
            DO $$
            BEGIN
                BEGIN
                    ALTER TABLE tournaments ALTER COLUMN topic_id TYPE BIGINT;
                EXCEPTION WHEN others THEN NULL;
                END;
                BEGIN
                    ALTER TABLE tournaments ALTER COLUMN groups_topic_id TYPE BIGINT;
                EXCEPTION WHEN others THEN NULL;
                END;
                BEGIN
                    ALTER TABLE tournaments ALTER COLUMN results_topic_id TYPE BIGINT;
                EXCEPTION WHEN others THEN NULL;
                END;
            END
            $$;
        """)
        
        self.cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='matches' AND column_name='player1_elo_before') THEN
                    ALTER TABLE matches ADD COLUMN player1_elo_before INTEGER;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='matches' AND column_name='player2_elo_before') THEN
                    ALTER TABLE matches ADD COLUMN player2_elo_before INTEGER;
                END IF;
            END
            $$;
        """)
        
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                player1_elo_before INTEGER,
                player2_elo_before INTEGER
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
                player1_elo_before INTEGER,
                player2_elo_before INTEGER,
                elo_applied BOOLEAN DEFAULT FALSE,
                UNIQUE(tournament_id, stage, match_num)
            )
        ''')

        self.cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='playoff_matches' AND column_name='player1_elo_before') THEN
                    ALTER TABLE playoff_matches ADD COLUMN player1_elo_before INTEGER;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='playoff_matches' AND column_name='player2_elo_before') THEN
                    ALTER TABLE playoff_matches ADD COLUMN player2_elo_before INTEGER;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='playoff_matches' AND column_name='elo_applied') THEN
                    ALTER TABLE playoff_matches ADD COLUMN elo_applied BOOLEAN DEFAULT FALSE;
                END IF;
            END
            $$;
        """)

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tournament_rating_snapshots (
                tournament_id INTEGER NOT NULL,
                user_id BIGINT NOT NULL,
                rating_start INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (tournament_id, user_id)
            )
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

    def reset_all_ratings(self, rating: int = 1000) -> int:
        self.cursor.execute('UPDATE players SET rating = %s', (rating,))
        affected = self._raw_cursor.rowcount if self._raw_cursor.rowcount is not None else 0
        self.conn.commit()
        return affected

    def reset_all_player_stats_and_ratings(self, rating: int = 1000) -> int:
        self.cursor.execute('''
            UPDATE players
            SET rating = %s,
                wins = 0,
                losses = 0,
                draws = 0,
                goals_scored = 0,
                goals_conceded = 0
        ''', (rating,))
        affected = self._raw_cursor.rowcount if self._raw_cursor.rowcount is not None else 0
        self.conn.commit()
        return affected

    def get_completed_matches_ordered(self) -> List[Dict]:
        self.cursor.execute('''
            SELECT * FROM matches
            WHERE status = 'completed'
            ORDER BY COALESCE(reported_at, created_at), id
        ''')
        return [dict(row) for row in self.cursor.fetchall()]
    
    def add_admin(self, user_id: int):
        self.cursor.execute('INSERT INTO admins (user_id) VALUES (%s) ON CONFLICT DO NOTHING', (user_id,))
        self.conn.commit()
    
    def is_admin(self, user_id: int) -> bool:
        self.cursor.execute('SELECT 1 FROM admins WHERE user_id = %s', (user_id,))
        return self.cursor.fetchone() is not None
    
    def create_tournament(self, name: str, format: str, chat_id: int, 
                         created_by: int, max_players: int = None, 
                         min_players: int = 4, deadline_days: int = 3,
                         groups_count: int = 0, topic_id: int = None) -> int:
        self.cursor.execute('''
            INSERT INTO tournaments (name, format, chat_id, created_by, 
                                   max_players, min_players, deadline_days, groups_count, topic_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (name, format, chat_id, created_by, max_players, min_players, deadline_days, groups_count, topic_id))
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
    
    def get_all_players(self) -> List[Dict]:
        self.cursor.execute('SELECT * FROM players WHERE ingame_nick IS NOT NULL ORDER BY rating DESC')
        return [dict(row) for row in self.cursor.fetchall()]

    def delete_tournament_matches(self, tournament_id: int):
        self.cursor.execute('DELETE FROM matches WHERE tournament_id = %s', (tournament_id,))
        self.conn.commit()
    
    def get_match_by_id(self, match_id: int) -> Optional[Dict]:
        self.cursor.execute('SELECT * FROM matches WHERE id = %s', (match_id,))
        row = self.cursor.fetchone()
        return dict(row) if row else None
    
    def cancel_match(self, match_id: int) -> Tuple[bool, Optional[Dict], Optional[Dict]]:
        match = self.get_match_by_id(match_id)
        if not match:
            return False, None, None

        was_completed = (match.get('status') == 'completed')
        p1_score = int(match.get('player1_score') or 0)
        p2_score = int(match.get('player2_score') or 0)
        p1_id = match['player1_id']
        p2_id = match['player2_id']

        if was_completed:
            winner_id = match.get('winner_id')
            if winner_id is None or p1_score == p2_score:
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET draws = GREATEST(draws - 1, 0),
                        goals_scored = GREATEST(goals_scored - %s, 0),
                        goals_conceded = GREATEST(goals_conceded - %s, 0)
                    WHERE user_id = %s
                    ''',
                    (p1_score, p2_score, p1_id),
                )
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET draws = GREATEST(draws - 1, 0),
                        goals_scored = GREATEST(goals_scored - %s, 0),
                        goals_conceded = GREATEST(goals_conceded - %s, 0)
                    WHERE user_id = %s
                    ''',
                    (p2_score, p1_score, p2_id),
                )
            elif winner_id == p1_id:
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET wins = GREATEST(wins - 1, 0),
                        goals_scored = GREATEST(goals_scored - %s, 0),
                        goals_conceded = GREATEST(goals_conceded - %s, 0)
                    WHERE user_id = %s
                    ''',
                    (p1_score, p2_score, p1_id),
                )
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET losses = GREATEST(losses - 1, 0),
                        goals_scored = GREATEST(goals_scored - %s, 0),
                        goals_conceded = GREATEST(goals_conceded - %s, 0)
                    WHERE user_id = %s
                    ''',
                    (p2_score, p1_score, p2_id),
                )
            else:
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET losses = GREATEST(losses - 1, 0),
                        goals_scored = GREATEST(goals_scored - %s, 0),
                        goals_conceded = GREATEST(goals_conceded - %s, 0)
                    WHERE user_id = %s
                    ''',
                    (p1_score, p2_score, p1_id),
                )
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET wins = GREATEST(wins - 1, 0),
                        goals_scored = GREATEST(goals_scored - %s, 0),
                        goals_conceded = GREATEST(goals_conceded - %s, 0)
                    WHERE user_id = %s
                    ''',
                    (p2_score, p1_score, p2_id),
                )

            p1_elo_before = match.get('player1_elo_before')
            p2_elo_before = match.get('player2_elo_before')
            if p1_elo_before is not None:
                self.cursor.execute(
                    'UPDATE players SET rating = %s WHERE user_id = %s',
                    (p1_elo_before, p1_id)
                )
            if p2_elo_before is not None:
                self.cursor.execute(
                    'UPDATE players SET rating = %s WHERE user_id = %s',
                    (p2_elo_before, p2_id)
                )

        self.cursor.execute('''
            UPDATE matches
            SET player1_score = NULL,
                player2_score = NULL,
                winner_id = NULL,
                status = 'pending',
                screenshot_id = NULL,
                reported_by = NULL,
                reported_at = NULL
            WHERE id = %s
        ''', (match_id,))

        self.conn.commit()

        p1 = self.get_player(p1_id)
        p2 = self.get_player(p2_id)
        return True, p1, p2
    
    def update_tournament_groups_info(self, tournament_id: int, topic_id: int, message_id: int):
        self.cursor.execute('''
            UPDATE tournaments SET groups_topic_id = %s, groups_message_id = %s
            WHERE id = %s
        ''', (topic_id, message_id, tournament_id))
        self.conn.commit()
    
    def update_tournament_results_topic(self, tournament_id: int, topic_id: int):
        self.cursor.execute('''
            UPDATE tournaments SET results_topic_id = %s WHERE id = %s
        ''', (topic_id, tournament_id))
        self.conn.commit()
    
    def update_tournament_reg_message(self, tournament_id: int, message_id: int):
        self.cursor.execute('''
            UPDATE tournaments SET reg_message_id = %s WHERE id = %s
        ''', (message_id, tournament_id))
        self.conn.commit()

    def update_tournament_groups_graphic_message(self, tournament_id: int, message_id: int):
        self.cursor.execute('''
            UPDATE tournaments SET groups_graphic_message_id = %s WHERE id = %s
        ''', (message_id, tournament_id))
        self.conn.commit()

    def update_tournament_playoff_graphic_message(self, tournament_id: int, message_id: int):
        self.cursor.execute('''
            UPDATE tournaments SET playoff_graphic_message_id = %s WHERE id = %s
        ''', (message_id, tournament_id))
        self.conn.commit()

    def set_playoff_message_id(self, tournament_id: int, message_id: int):
        self.cursor.execute(
            'UPDATE tournaments SET playoff_message_id = %s WHERE id = %s',
            (message_id, tournament_id),
        )
        self.conn.commit()
    
    def set_player_group(self, tournament_id: int, user_id: int, group_name: str):
        self.cursor.execute(
            'UPDATE tournament_players SET group_name = %s WHERE tournament_id = %s AND user_id = %s',
            (group_name, tournament_id, user_id),
        )
        self.conn.commit()

    def update_tournament_player_status(self, tournament_id: int, user_id: int, 
                                       status: str, approved_by: int = None):
        self.cursor.execute('''
            UPDATE tournament_players 
            SET status = %s, approved_by = %s
            WHERE tournament_id = %s AND user_id = %s
        ''', (status, approved_by, tournament_id, user_id))
        self.conn.commit()

    def replace_tournament_player(self, tournament_id: int, old_user_id: int, new_user_id: int) -> bool:
        self.cursor.execute('''
            UPDATE tournament_players
            SET user_id = %s
            WHERE tournament_id = %s AND user_id = %s
        ''', (new_user_id, tournament_id, old_user_id))
        changed = self._raw_cursor.rowcount if self._raw_cursor.rowcount is not None else 0
        self.conn.commit()
        return changed > 0

    def reassign_open_matches_player(self, tournament_id: int, old_user_id: int, new_user_id: int) -> int:
        self.cursor.execute('''
            UPDATE matches
            SET player1_id = %s
            WHERE tournament_id = %s AND player1_id = %s
        ''', (new_user_id, tournament_id, old_user_id))
        changed_p1 = self._raw_cursor.rowcount if self._raw_cursor.rowcount is not None else 0

        self.cursor.execute('''
            UPDATE matches
            SET player2_id = %s
            WHERE tournament_id = %s AND player2_id = %s
        ''', (new_user_id, tournament_id, old_user_id))
        changed_p2 = self._raw_cursor.rowcount if self._raw_cursor.rowcount is not None else 0

        self.conn.commit()
        return changed_p1 + changed_p2

    def delete_open_matches_for_player(self, tournament_id: int, user_id: int) -> int:
        self.cursor.execute('''
            DELETE FROM matches
            WHERE tournament_id = %s
              AND (player1_id = %s OR player2_id = %s)
              AND status IN ('pending', 'in_progress')
        ''', (tournament_id, user_id, user_id))
        deleted = self._raw_cursor.rowcount if self._raw_cursor.rowcount is not None else 0
        self.conn.commit()
        return deleted
    
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
        players = self.get_tournament_players(tournament_id, status='joined')
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
        match = self.get_match_by_id(match_id)
        if match:
            p1_elo = match.get('player1_elo_before')
            p2_elo = match.get('player2_elo_before')
            if p1_elo is None:
                p1 = self.get_player(match['player1_id'])
                p2 = self.get_player(match['player2_id'])
                p1_elo = p1.get('rating', 0) if p1 else 0
                p2_elo = p2.get('rating', 0) if p2 else 0
            self.cursor.execute('''
                UPDATE matches SET 
                player1_score = %s, player2_score = %s, winner_id = %s,
                status = 'completed', reported_by = %s, screenshot_id = %s,
                reported_at = CURRENT_TIMESTAMP,
                player1_elo_before = %s, player2_elo_before = %s
                WHERE id = %s
            ''', (score1, score2, winner_id, reported_by, screenshot_id, p1_elo, p2_elo, match_id))
        else:
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
                    round_num: int = 1, group_name: str = None,
                    match_type: str = 'round', deadline_days: int = 3) -> int:
        deadline = datetime.now() + timedelta(days=deadline_days)
        self.cursor.execute('''
            INSERT INTO matches (tournament_id, player1_id, player2_id, round_num,
                               group_name, match_type, deadline_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (tournament_id, player1_id, player2_id, round_num,
              group_name, match_type, deadline.isoformat()))
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
                        WHEN 'bronze' THEN 4
                        WHEN 'final' THEN 5 
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
                             status: str = None, message_id: int = None,
                             player1_elo_before: int = None, player2_elo_before: int = None,
                             elo_applied: bool = None, player1_nick: str = None, player2_nick: str = None):
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
        if player1_elo_before is not None:
            updates.append('player1_elo_before = %s')
            params.append(player1_elo_before)
        if player2_elo_before is not None:
            updates.append('player2_elo_before = %s')
            params.append(player2_elo_before)
        if elo_applied is not None:
            updates.append('elo_applied = %s')
            params.append(elo_applied)
        if player1_nick is not None:
            updates.append('player1_nick = %s')
            params.append(player1_nick)
        if player2_nick is not None:
            updates.append('player2_nick = %s')
            params.append(player2_nick)

        if updates:
            params.append(match_id)
            self.cursor.execute(f"UPDATE playoff_matches SET {', '.join(updates)} WHERE id = %s", params)
            self.conn.commit()

    def revert_playoff_match_elo(self, match_id: int) -> Optional[Dict]:
        self.cursor.execute('SELECT * FROM playoff_matches WHERE id = %s', (match_id,))
        row = self.cursor.fetchone()
        if not row:
            return None
        match = dict(row)

        if match.get('elo_applied') and match.get('player1_elo_before') is not None and match.get('player2_elo_before') is not None:
            p1_id = self.get_player_id_by_nick(match['player1_nick'])
            p2_id = self.get_player_id_by_nick(match['player2_nick'])
            p1_wins = int(match['player1_wins'] or 0)
            p2_wins = int(match['player2_wins'] or 0)

            if p1_id:
                self.cursor.execute(
                    'UPDATE players SET rating = %s, wins = GREATEST(wins - %s, 0), losses = GREATEST(losses - %s, 0) WHERE user_id = %s',
                    (match['player1_elo_before'], p2_wins, p1_wins, p1_id)
                )
            if p2_id:
                self.cursor.execute(
                    'UPDATE players SET rating = %s, wins = GREATEST(wins - %s, 0), losses = GREATEST(losses - %s, 0) WHERE user_id = %s',
                    (match['player2_elo_before'], p1_wins, p2_wins, p2_id)
                )

        self.cursor.execute(
            'UPDATE playoff_matches SET player1_wins = 0, player2_wins = 0, status = %s, elo_applied = FALSE WHERE id = %s',
            ('pending', match_id)
        )
        self.conn.commit()
        return match

    def get_player_id_by_nick(self, nick: str) -> Optional[int]:
        if not nick:
            return None
        self.cursor.execute('SELECT user_id FROM players WHERE ingame_nick = %s', (nick,))
        row = self.cursor.fetchone()
        return row['user_id'] if row else None
    
    def clear_playoff_matches(self, tournament_id: int):
        self.cursor.execute('DELETE FROM playoff_matches WHERE tournament_id = %s', (tournament_id,))
        self.conn.commit()

    def clear_playoff_match_slot(self, tournament_id: int, stage: str, match_num: int):
        self.cursor.execute(
            'UPDATE playoff_matches SET player1_nick = NULL, player2_nick = NULL WHERE tournament_id = %s AND stage = %s AND match_num = %s',
            (tournament_id, stage, match_num)
        )
        self.conn.commit()

    def snapshot_tournament_ratings(self, tournament_id: int, user_ids: List[int]):
        self.cursor.execute('DELETE FROM tournament_rating_snapshots WHERE tournament_id = %s', (tournament_id,))
        for user_id in user_ids:
            self.cursor.execute('SELECT rating FROM players WHERE user_id = %s', (user_id,))
            row = self.cursor.fetchone()
            if row:
                rating = row['rating'] if isinstance(row, dict) else row[0]
                self.cursor.execute('''
                    INSERT INTO tournament_rating_snapshots (tournament_id, user_id, rating_start)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (tournament_id, user_id) DO UPDATE SET rating_start = EXCLUDED.rating_start
                ''', (tournament_id, user_id, rating))
        self.conn.commit()

    def get_tournament_rating_gains(self, tournament_id: int) -> List[Dict]:
        self.cursor.execute('''
            SELECT s.user_id, p.ingame_nick, s.rating_start, p.rating AS rating_end,
                   (p.rating - s.rating_start) AS gain
            FROM tournament_rating_snapshots s
            JOIN players p ON p.user_id = s.user_id
            WHERE s.tournament_id = %s
            ORDER BY gain DESC, p.ingame_nick ASC
        ''', (tournament_id,))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def close(self):
        self.conn.close()

    def delete_tournament(self, tournament_id: int):
        self.cursor.execute('DELETE FROM matches WHERE tournament_id = %s', (tournament_id,))
        self.cursor.execute('DELETE FROM playoff_matches WHERE tournament_id = %s', (tournament_id,))
        self.cursor.execute('DELETE FROM tournament_rating_snapshots WHERE tournament_id = %s', (tournament_id,))
        self.cursor.execute('DELETE FROM tournament_players WHERE tournament_id = %s', (tournament_id,))
        self.cursor.execute('DELETE FROM tournaments WHERE id = %s', (tournament_id,))
        self.conn.commit()

    def clear_all_tournaments(self):
        self.cursor.execute('DELETE FROM matches')
        self.cursor.execute('DELETE FROM playoff_matches')
        self.cursor.execute('DELETE FROM tournament_players')
        self.cursor.execute('DELETE FROM tournaments')
        self.conn.commit()
