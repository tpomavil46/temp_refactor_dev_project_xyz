import os
from dotenv import load_dotenv
from seeq import spy
from itv_asset_tree.utils.logger import log_info, log_error

# Load environment variables
load_dotenv()

def connect_to_seeq():
    """Handles authentication to Seeq on startup."""
    SERVER_HOST = os.getenv("SERVER_HOST")
    SERVER_USERNAME = os.getenv("SERVER_USERNAME")
    SERVER_PASSWORD = os.getenv("SERVER_PASSWORD")

    log_info(f"🔌 Attempting to connect to Seeq at {SERVER_HOST} as {SERVER_USERNAME}...")

    try:
        spy.login(url=SERVER_HOST, username=SERVER_USERNAME, password=SERVER_PASSWORD)
        log_info("✅ Successfully logged into Seeq.")
    except Exception as e:
        log_error(f"❌ Seeq login failed: {e}")