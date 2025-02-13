# conftest.py
import pytest
from seeq import spy
import os
from dotenv import load_dotenv

@pytest.fixture(scope="session", autouse=True)
def seeq_login():
    """Automatically logs into Seeq before running any tests."""
    load_dotenv()

    # ✅ Apply the required Seeq options (matching api.py)
    spy.options.compatibility = 193
    spy.options.friendly_exceptions = False

    try:
        spy.login(
            url=os.getenv("SERVER_HOST"),
            username=os.getenv("SERVER_USERNAME"),
            password=os.getenv("SERVER_PASSWORD")
        )
        print("✅ Successfully logged into Seeq for test session.")
    except Exception as e:
        pytest.exit(f"❌ Failed to log into Seeq: {e}")