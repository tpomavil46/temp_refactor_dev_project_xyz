# conftest.py
import pytest
from unittest.mock import patch
import os
from dotenv import load_dotenv
from seeq import spy

@pytest.fixture(scope="session", autouse=True)
def seeq_login():
    """Automatically logs into Seeq for local tests or mocks it for CI."""
    if os.getenv("CI") == "true":
        # ✅ Use mock login in CI environment
        with patch('seeq.spy.login') as mock_login, patch('seeq.spy.workbooks.push') as mock_push:
            mock_login.return_value = None
            mock_push.return_value = None
            print("✅ Using mocked Seeq login for CI pipeline.")
            yield  # ✅ Correctly yield to pytest
    else:
        # ✅ Use real login for local testing
        load_dotenv()
        try:
            spy.options.compatibility = 193
            spy.options.friendly_exceptions = False
            spy.login(
                url=os.getenv("SERVER_HOST"),
                username=os.getenv("SERVER_USERNAME"),
                password=os.getenv("SERVER_PASSWORD"),
                request_origin_label="Asset Tree Tests"
            )
            print("✅ Successfully logged into Seeq for local test session.")
            yield  # ✅ Correctly yield to pytest
        finally:
            print("🔒 Logging out from Seeq after local tests...")
            spy.logout()  # ✅ Optional, clean up session