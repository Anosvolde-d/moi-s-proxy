import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# Get the project root directory (one level up from backend)
BASE_DIR = Path(__file__).parent.parent

class Config:
    # Default target API settings (used when key doesn't have custom settings)
    DEFAULT_TARGET_URL = os.getenv("DEFAULT_TARGET_URL", "https://api.openai.com/v1")
    DEFAULT_TARGET_API_KEY = os.getenv("DEFAULT_TARGET_API_KEY", "")
    
    # Database - store in project_root/data/proxy.db
    DATABASE_PATH = os.getenv("DATABASE_PATH", str(BASE_DIR / "data" / "proxy.db"))
    
    # Turso / libSQL (for cloud deployment) - DEPRECATED, use PostgreSQL instead
    TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "")
    TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")
    
    # PostgreSQL (for Zeabur cloud deployment)
    POSTGRES_URL = os.getenv("POSTGRES_URL", "")
    
    # Ensure data directory exists
    data_dir = BASE_DIR / "data"
    data_dir.mkdir(exist_ok=True)
    
    # Server settings
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "3000"))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Security - ADMIN_PASSWORD must be set in environment variables
    ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
    if not ADMIN_PASSWORD:
        raise ValueError("ADMIN_PASSWORD environment variable must be set for security")
    
    # CORS
    CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")
    
    # WebScrapingAPI settings for IP rotation proxy
    WEBSCRAPINGAPI_ENABLED = os.getenv("WEBSCRAPINGAPI_ENABLED", "false").lower() == "true"
    WEBSCRAPINGAPI_KEY = os.getenv("WEBSCRAPINGAPI_KEY", "")
    WEBSCRAPINGAPI_URL = os.getenv("WEBSCRAPINGAPI_URL", "https://api.webscrapingapi.com/v2")
    
    # Airforce ad filtering - number of characters to remove from end of response
    AIRFORCE_AD_CHARS = int(os.getenv("AIRFORCE_AD_CHARS", "54"))
    
    # OpenRouter default headers (can be overridden per-key)
    DEFAULT_HTTP_REFERER = os.getenv("DEFAULT_HTTP_REFERER", "https://mois-proxy.netlify.app")
    DEFAULT_X_TITLE = os.getenv("DEFAULT_X_TITLE", "MOI's Proxy")
    
    # Keep-alive settings (to prevent server from sleeping due to inactivity)
    KEEP_ALIVE_ENABLED = os.getenv("KEEP_ALIVE_ENABLED", "true").lower() == "true"
    KEEP_ALIVE_INTERVAL = int(os.getenv("KEEP_ALIVE_INTERVAL", "300"))  # 5 minutes default
    
    # Chatbot Assistant Configuration (must be set in environment)
    CHATBOT_API_URL = os.getenv("CHATBOT_API_URL", "")
    CHATBOT_API_KEY = os.getenv("CHATBOT_API_KEY", "")
    CHATBOT_MODEL = os.getenv("CHATBOT_MODEL", "")
    
    # Background Sync Interval (seconds)
    # Default to 60 seconds (1 minute) for frequent backups
    SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "60"))

config = Config()