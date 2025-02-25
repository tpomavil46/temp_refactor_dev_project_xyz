import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings  

class Settings(BaseSettings):
    app_name: str = "ITV Asset Tree API"
    debug: bool = False

    SERVER_USERNAME: str = os.getenv("SERVER_USERNAME")
    SERVER_PASSWORD: str = os.getenv("SERVER_PASSWORD")
    SERVER_HOST: str = os.getenv("SERVER_HOST")

# Load environment variables
load_dotenv()

# ✅ Create a single `Settings` instance
settings = Settings()