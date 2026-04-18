import logging
import os
import sys

from config.runtime import load_runtime_env
from config.bootstrap_health import check_db_health
from bot.app import TournamentBot

load_runtime_env()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    if not check_db_health():
        logger.error("Database health check failed during startup. Exiting.")
        sys.exit(1)

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN not found in .env")

    bot = TournamentBot(token)
    admin_ids = os.getenv("ADMIN_IDS")
    if admin_ids:
        for admin_id in admin_ids.split(","):
            try:
                bot.db.add_admin(int(admin_id.strip()))
            except ValueError:
                logger.warning("Invalid admin id in ADMIN_IDS: %s", admin_id)

    bot.run()
