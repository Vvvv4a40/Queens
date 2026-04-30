import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN    = os.getenv("BOT_TOKEN")
DATABASE     = "lectures.db"
HEADLESS     = os.getenv("HEADLESS", os.getenv("HEADLESS_MODE", "true")).lower() == "true"
TIMEZONE     = "Asia/Tomsk"   # UTC+7
TSU_API_BASE = os.getenv("TSU_API_BASE", "https://intime.tsu.ru/api/web")
SCHEDULE_LOOKAHEAD_DAYS = int(os.getenv("SCHEDULE_LOOKAHEAD_DAYS", "14"))
SCHEDULE_SYNC_INTERVAL_MINUTES = int(os.getenv("SCHEDULE_SYNC_INTERVAL_MINUTES", "30"))
CHAT_EXIT_MESSAGE_THRESHOLD = int(os.getenv("CHAT_EXIT_MESSAGE_THRESHOLD", "5"))
CHAT_MONITOR_INTERVAL_SECONDS = int(os.getenv("CHAT_MONITOR_INTERVAL_SECONDS", "15"))
