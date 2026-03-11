import logging
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


LOG_FILE = LOG_DIR / "app.log" 

def setup_logging(level=logging.INFO):
    root = logging.getLogger()
    root.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )

    # Console output
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)

     # File output - rotates daily, keeps 30 days
    file_handler = TimedRotatingFileHandler(
        LOG_DIR / "app.log",
        when="midnight",    # rotate at midnight
        interval=1,         # every 1 day
        backupCount=30,     # keep 30 days of logs
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    root.addHandler(console)
    root.addHandler(file_handler)