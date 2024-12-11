from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    CACHE_TTL: int = 60
    DEFAULT_LIMIT: int = 100
    API_RATE_LIMIT: int = 1200
    binance_api_key: str = ""
    binance_api_secret: str = ""

    class Config:
        env_file = ".env"


settings = Settings()
