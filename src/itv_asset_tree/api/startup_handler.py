import os
from dotenv import load_dotenv
from seeq import spy

# Load environment variables
load_dotenv()
HOST = os.getenv("SERVER_HOST")
USERNAME = os.getenv("SERVER_USERNAME")
PASSWORD = os.getenv("SERVER_PASSWORD")

def connect_to_seeq():
    """Handles Seeq login on app startup."""
    print(f"🔌 Connecting to Seeq at {HOST} as {USERNAME}...")
    try:
        spy.options.compatibility = 193
        spy.options.friendly_exceptions = False
        spy.login(url=HOST, username=USERNAME, password=PASSWORD)
        print("✅ Successfully logged into Seeq.")
    except Exception as e:
        print(f"❌ Seeq login failed: {e}")