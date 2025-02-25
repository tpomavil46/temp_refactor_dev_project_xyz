import logging
import sys
from pathlib import Path

# Ensure logs directory exists
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Set up logging configuration
LOG_FILE_PATH = LOGS_DIR / "app.log"

logging.basicConfig(
    level=logging.INFO,  # Default log level
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),  # Save logs to file
        logging.StreamHandler(sys.stdout),   # Print logs to console
    ],
)

# Create logger instance
logger = logging.getLogger("itv_asset_tree")

def log_info(message: str):
    """Logs an informational message."""
    logger.info(message)

def log_warning(message: str):
    """Logs a warning message."""
    logger.warning(message)

def log_error(message: str):
    """Logs an error message."""
    logger.error(message)

def log_debug(message: str):
    """Logs a debug message (useful for development)."""
    logger.debug(message)
    
# Examples:

# from fastapi import FastAPI, Request
# from itv_asset_tree.utils.logger import log_info, log_error

# app = FastAPI()

# @app.middleware("http")
# async def log_requests(request: Request, call_next):
#     log_info(f"📥 Incoming request: {request.method} {request.url}")
#     response = await call_next(request)
#     log_info(f"📤 Response status: {response.status_code}")
#     return response

# ----------------------------------------------------

# from itv_asset_tree.utils.logger import log_info, log_error

# class TreeBuilder:
#     def build_tree(self):
#         try:
#             log_info("🌳 Starting tree-building process...")
#             # Tree-building logic
#             log_info("✅ Tree successfully built!")
#         except Exception as e:
#             log_error(f"❌ Error while building tree: {e}")

# ----------------------------------------------------

# from itv_asset_tree.utils.logger import log_info, log_warning

# def get_item(item_id: int):
#     log_info(f"Fetching item with ID: {item_id}")
#     # Logic to retrieve item
#     log_warning("⚠️ Item not found!")  # Example of warning log