# src/itv_asset_tree/config.py
from pydantic import BaseSettings

class Settings(BaseSettings):
    app_name: str = "ITV Asset Tree"
    debug: bool = False
    database_url: str

    class Config:
        env_file = ".env"

settings = Settings()