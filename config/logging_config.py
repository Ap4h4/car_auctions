import logging
import sys
from pathlib import Path

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

    # File output
    file_handler = logging.FileHandler(LOG_FILE)
    
    file_handler.setFormatter(formatter)

    root.addHandler(console)
    root.addHandler(file_handler)