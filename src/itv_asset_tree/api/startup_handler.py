import os
from dotenv import load_dotenv
from seeq import spy
from itv_asset_tree.config import settings
from itv_asset_tree.utils.logger import log_info, log_error

# Load environment variables
load_dotenv()

def connect_to_seeq():
    """Handles authentication to Seeq."""
    log_info(f"🔌 Connecting to Seeq at {settings.SERVER_HOST} as {settings.SERVER_USERNAME}...")
    
    try:
        spy.login(url=settings.SERVER_HOST, username=settings.SERVER_USERNAME, password=settings.SERVER_PASSWORD)
        log_info("✅ Successfully logged into Seeq.")
    except Exception as e:
        log_error(f"❌ Seeq login failed: {e}")