import os
base = os.path.join(os.environ['USERPROFILE'], 'Desktop', 'fc_mobile_bot')
os.chdir(base)

with open('main.py', 'r', encoding='utf-8') as f:
    content = f.read()

db_start = content.find('class Database:')
tb_import = content.find('from bot.app import TournamentBot')

# Extract everything before Database class (imports etc)
before_db = content[:db_start]

# Extract Database class
db_class = content[db_start:tb_import].strip()

# Write db/sqlite.py
lines = []
lines.append('import logging')
lines.append('import sqlite3')
lines.append('from datetime import datetime, timedelta')
lines.append('from typing import Dict, List, Optional, Tuple')
lines.append('from dataclasses import dataclass')
lines.append('from enum import Enum')
lines.append('')
lines.append('class TournamentStatus(Enum):')
lines.append('    REGISTRATION = "registration"')
lines.append('    IN_PROGRESS = "in_progress"')
lines.append('    COMPLETED = "completed"')
lines.append('    CANCELLED = "cancelled"')
lines.append('')
lines.append('@dataclass')
lines.append('class TournamentFormat:')
lines.append('    name: str')
lines.append('    has_groups: bool')
lines.append('    description: str')
lines.append('')
lines.append('AVAILABLE_FORMATS = {")
lines.append('    "single_elimination": TournamentFormat(')
lines.append('        name="Single Elimination",')
lines.append('        has_groups=False,')
lines.append('        description="пָноварсой потовар")')
lines.append('    ),')
lines.append('    "classical": TournamentFormat(')
lines.append('        name="Кинорсатеони",')
lines.append('        has_groups=True,')
lines.append('        description="4 порев ант, посани"')')
lines.append('    },')
lines.append('}')
lines.append('')
lines.append("logging.basicConfig(")
lines.append("    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',")
lines.append("    level=logging.INFO")
lines.append(")")
lines.append("logger = logging.getLogger(__name__)")
lines.append('')
lines.append('')
lines.append(db_class)
lines.append('')

with open('db/sqlite.py', 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))

print('Created db/sqlite.py')
