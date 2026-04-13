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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                player1_elo_before INTEGER,
                player2_elo_before INTEGER
            )
        ''')

        try:
            self.cursor.execute('ALTER TABLE matches ADD COLUMN player1_elo_before INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

        try:
            self.cursor.execute('ALTER TABLE matches ADD COLUMN player2_elo_before INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        
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
                player1_elo_before INTEGER,
                player2_elo_before INTEGER,
                elo_applied INTEGER DEFAULT 0,
                UNIQUE(tournament_id, stage, match_num)
            )
        ''')

        try:
            self.cursor.execute('ALTER TABLE playoff_matches ADD COLUMN player1_elo_before INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self.cursor.execute('ALTER TABLE playoff_matches ADD COLUMN player2_elo_before INTEGER')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        try:
            self.cursor.execute('ALTER TABLE playoff_matches ADD COLUMN elo_applied INTEGER DEFAULT 0')
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

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

    def reset_all_ratings(self, rating: int = 1000) -> int:
        self.cursor.execute('UPDATE players SET rating = ?', (rating,))
        affected = self.cursor.rowcount if self.cursor.rowcount is not None else 0
        self.conn.commit()
        return affected

    def reset_all_player_stats_and_ratings(self, rating: int = 1000) -> int:
        self.cursor.execute('''
            UPDATE players
            SET rating = ?,
                wins = 0,
                losses = 0,
                draws = 0,
                goals_scored = 0,
                goals_conceded = 0
        ''', (rating,))
        affected = self.cursor.rowcount if self.cursor.rowcount is not None else 0
        self.conn.commit()
        return affected

    def get_completed_matches_ordered(self) -> List[Dict]:
        self.cursor.execute('''
            SELECT * FROM matches
            WHERE status = 'completed'
            ORDER BY COALESCE(reported_at, created_at), id
        ''')
        return [self._row_to_match(row) for row in self.cursor.fetchall()]
    
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

    def replace_tournament_player(self, tournament_id: int, old_user_id: int, new_user_id: int) -> bool:
        self.cursor.execute('''
            UPDATE tournament_players
            SET user_id = ?
            WHERE tournament_id = ? AND user_id = ?
        ''', (new_user_id, tournament_id, old_user_id))
        changed = self.cursor.rowcount if self.cursor.rowcount is not None else 0
        self.conn.commit()
        return changed > 0

    def reassign_open_matches_player(self, tournament_id: int, old_user_id: int, new_user_id: int) -> int:
        self.cursor.execute('''
            UPDATE matches
            SET player1_id = ?
            WHERE tournament_id = ? AND player1_id = ?
        ''', (new_user_id, tournament_id, old_user_id))
        changed_p1 = self.cursor.rowcount if self.cursor.rowcount is not None else 0

        self.cursor.execute('''
            UPDATE matches
            SET player2_id = ?
            WHERE tournament_id = ? AND player2_id = ?
        ''', (new_user_id, tournament_id, old_user_id))
        changed_p2 = self.cursor.rowcount if self.cursor.rowcount is not None else 0

        self.conn.commit()
        return changed_p1 + changed_p2

    def delete_open_matches_for_player(self, tournament_id: int, user_id: int) -> int:
        self.cursor.execute('''
            DELETE FROM matches
            WHERE tournament_id = ?
              AND (player1_id = ? OR player2_id = ?)
              AND status IN ('pending', 'in_progress')
        ''', (tournament_id, user_id, user_id))
        deleted = self.cursor.rowcount if self.cursor.rowcount is not None else 0
        self.conn.commit()
        return deleted
    
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

    def get_match_by_id(self, match_id: int) -> Optional[Dict]:
        return self.get_match(match_id)
    
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
            'created_at': row[15],
            'player1_elo_before': row[16] if len(row) > 16 else None,
            'player2_elo_before': row[17] if len(row) > 17 else None,
        }

    def _row_to_playoff_match(self, row) -> Dict:
        return {
            'id': row[0],
            'tournament_id': row[1],
            'stage': row[2],
            'match_num': row[3],
            'player1_nick': row[4],
            'player2_nick': row[5],
            'player1_wins': row[6],
            'player2_wins': row[7],
            'status': row[8],
            'message_id': row[9],
            'created_at': row[10],
            'player1_elo_before': row[11] if len(row) > 11 else None,
            'player2_elo_before': row[12] if len(row) > 12 else None,
            'elo_applied': bool(row[13]) if len(row) > 13 else False,
        }
    
    def update_match_result(self, match_id: int, score1: int, score2: int, 
                          winner_id: int, reported_by: int, screenshot_id: str = None):
        match = self.get_match(match_id)
        p1_elo_before = None
        p2_elo_before = None

        if match:
            p1_elo_before = match.get('player1_elo_before')
            p2_elo_before = match.get('player2_elo_before')
            if p1_elo_before is None:
                p1 = self.get_player(match['player1_id'])
                p2 = self.get_player(match['player2_id'])
                p1_elo_before = p1.get('rating', 0) if p1 else 0
                p2_elo_before = p2.get('rating', 0) if p2 else 0

        self.cursor.execute('''
            UPDATE matches 
            SET player1_score = ?, player2_score = ?, winner_id = ?,
                status = 'completed', reported_by = ?, 
                reported_at = CURRENT_TIMESTAMP, screenshot_id = ?,
                player1_elo_before = ?, player2_elo_before = ?
            WHERE id = ?
        ''', (score1, score2, winner_id, reported_by, screenshot_id, p1_elo_before, p2_elo_before, match_id))
        self.conn.commit()

    def cancel_match(self, match_id: int) -> Tuple[bool, Optional[Dict], Optional[Dict]]:
        match = self.get_match(match_id)
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
                    SET draws = MAX(draws - 1, 0),
                        goals_scored = MAX(goals_scored - ?, 0),
                        goals_conceded = MAX(goals_conceded - ?, 0)
                    WHERE user_id = ?
                    ''',
                    (p1_score, p2_score, p1_id),
                )
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET draws = MAX(draws - 1, 0),
                        goals_scored = MAX(goals_scored - ?, 0),
                        goals_conceded = MAX(goals_conceded - ?, 0)
                    WHERE user_id = ?
                    ''',
                    (p2_score, p1_score, p2_id),
                )
            elif winner_id == p1_id:
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET wins = MAX(wins - 1, 0),
                        goals_scored = MAX(goals_scored - ?, 0),
                        goals_conceded = MAX(goals_conceded - ?, 0)
                    WHERE user_id = ?
                    ''',
                    (p1_score, p2_score, p1_id),
                )
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET losses = MAX(losses - 1, 0),
                        goals_scored = MAX(goals_scored - ?, 0),
                        goals_conceded = MAX(goals_conceded - ?, 0)
                    WHERE user_id = ?
                    ''',
                    (p2_score, p1_score, p2_id),
                )
            else:
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET losses = MAX(losses - 1, 0),
                        goals_scored = MAX(goals_scored - ?, 0),
                        goals_conceded = MAX(goals_conceded - ?, 0)
                    WHERE user_id = ?
                    ''',
                    (p1_score, p2_score, p1_id),
                )
                self.cursor.execute(
                    '''
                    UPDATE players
                    SET wins = MAX(wins - 1, 0),
                        goals_scored = MAX(goals_scored - ?, 0),
                        goals_conceded = MAX(goals_conceded - ?, 0)
                    WHERE user_id = ?
                    ''',
                    (p2_score, p1_score, p2_id),
                )

            p1_elo_before = match.get('player1_elo_before')
            p2_elo_before = match.get('player2_elo_before')
            if p1_elo_before is not None:
                self.cursor.execute('UPDATE players SET rating = ? WHERE user_id = ?', (p1_elo_before, p1_id))
            if p2_elo_before is not None:
                self.cursor.execute('UPDATE players SET rating = ? WHERE user_id = ?', (p2_elo_before, p2_id))

        self.cursor.execute('''
            UPDATE matches
            SET player1_score = NULL,
                player2_score = NULL,
                winner_id = NULL,
                status = 'pending',
                screenshot_id = NULL,
                reported_by = NULL,
                reported_at = NULL
            WHERE id = ?
        ''', (match_id,))
        self.conn.commit()

        return True, self.get_player(p1_id), self.get_player(p2_id)
    
    def update_match_status(self, match_id: int, status: str):
        self.cursor.execute('UPDATE matches SET status = ? WHERE id = ?', (status, match_id))
        self.conn.commit()
    
    def update_player_stats(self, user_id: int, result: str, goals_scored: int, 
                           goals_conceded: int, rating_change: int = 0):
        if result == 'win':
            self.cursor.execute('''
                UPDATE players SET wins = wins + 1, goals_scored = goals_scored + ?,
                goals_conceded = goals_conceded + ?
                WHERE user_id = ?
            ''', (goals_scored, goals_conceded, user_id))
        elif result == 'loss':
            self.cursor.execute('''
                UPDATE players SET losses = losses + 1, goals_scored = goals_scored + ?,
                goals_conceded = goals_conceded + ?
                WHERE user_id = ?
            ''', (goals_scored, goals_conceded, user_id))
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
                   'player1_wins', 'player2_wins', 'status', 'message_id', 'created_at',
                   'player1_elo_before', 'player2_elo_before', 'elo_applied']
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
                             status: str = None, message_id: int = None,
                             player1_elo_before: int = None, player2_elo_before: int = None,
                             elo_applied: int = None):
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
        if player1_elo_before is not None:
            updates.append('player1_elo_before = ?')
            params.append(player1_elo_before)
        if player2_elo_before is not None:
            updates.append('player2_elo_before = ?')
            params.append(player2_elo_before)
        if elo_applied is not None:
            updates.append('elo_applied = ?')
            params.append(elo_applied)

        if updates:
            params.append(match_id)
            self.cursor.execute(f'UPDATE playoff_matches SET {", ".join(updates)} WHERE id = ?', params)
            self.conn.commit()

    def revert_playoff_match_elo(self, match_id: int) -> Optional[Dict]:
        self.cursor.execute('SELECT * FROM playoff_matches WHERE id = ?', (match_id,))
        row = self.cursor.fetchone()
        if not row:
            return None
        match = self._row_to_playoff_match(row)

        if match.get('elo_applied') and match.get('player1_elo_before') is not None and match.get('player2_elo_before') is not None:
            p1_id = self.get_player_id_by_nick(match['player1_nick'])
            p2_id = self.get_player_id_by_nick(match['player2_nick'])
            p1_wins = int(match['player1_wins'] or 0)
            p2_wins = int(match['player2_wins'] or 0)

            if p1_id:
                self.cursor.execute(
                    'UPDATE players SET rating = ?, wins = MAX(wins - ?, 0), losses = MAX(losses - ?, 0) WHERE user_id = ?',
                    (match['player1_elo_before'], p2_wins, p1_wins, p1_id)
                )
            if p2_id:
                self.cursor.execute(
                    'UPDATE players SET rating = ?, wins = MAX(wins - ?, 0), losses = MAX(losses - ?, 0) WHERE user_id = ?',
                    (match['player2_elo_before'], p1_wins, p2_wins, p2_id)
                )

        self.cursor.execute(
            'UPDATE playoff_matches SET player1_wins = 0, player2_wins = 0, status = ?, elo_applied = 0 WHERE id = ?',
            ('pending', match_id)
        )
        self.conn.commit()
        return match

    def get_player_id_by_nick(self, nick: str) -> Optional[int]:
        if not nick:
            return None
        self.cursor.execute('SELECT user_id FROM players WHERE ingame_nick = ?', (nick,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def clear_playoff_matches(self, tournament_id: int):
        self.cursor.execute('DELETE FROM playoff_matches WHERE tournament_id = ?', (tournament_id,))
        self.conn.commit()

    def clear_playoff_match_slot(self, tournament_id: int, stage: str, match_num: int):
        self.cursor.execute(
            'UPDATE playoff_matches SET player1_nick = NULL, player2_nick = NULL WHERE tournament_id = ? AND stage = ? AND match_num = ?',
            (tournament_id, stage, match_num)
        )
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
    from db.postgres import Database
    from db.postgres import AVAILABLE_FORMATS as AVAILABLE_FORMATS









