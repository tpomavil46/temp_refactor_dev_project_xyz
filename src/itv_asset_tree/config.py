import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    SERVER_USERNAME: str = os.getenv("SERVER_USERNAME")
    SERVER_PASSWORD: str = os.getenv("SERVER_PASSWORD")
    SERVER_HOST: str = os.getenv("SERVER_HOST")

settings = Settings()