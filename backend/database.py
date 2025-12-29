import hashlib
import secrets
import string
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
import asyncio
import logging
import os
import re

# Configure logging for database
logger = logging.getLogger(__name__)

# Try to import asyncpg for PostgreSQL
try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
    logger.info("asyncpg loaded successfully")
except ImportError:
    ASYNCPG_AVAILABLE = False
    logger.warning("asyncpg not installed. PostgreSQL support disabled.")

# Try to import aiosqlite for SQLite fallback
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False
    logger.warning("aiosqlite not installed.")

from config import config
from contextlib import asynccontextmanager

# Constants
MAX_RETRY_ATTEMPTS = 3
INITIAL_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 30.0
RETRY_BACKOFF_MULTIPLIER = 2.0

# Sub-key format pattern: Moi-sub-XXXXXYYYYY (5 digits + 5 lowercase letters)
SUB_KEY_PREFIX = "Moi-sub-"
SUB_KEY_PATTERN = re.compile(r'^Moi-sub-[0-9]{5}[a-z]{5}$')


def generate_sub_key() -> str:
    """
    Generate a sub-key in format: Moi-sub-XXXXXYYYYY
    
    Where:
    - X is a digit (0-9)
    - Y is a lowercase letter (a-z)
    
    **Validates: Requirements 2.1, 2.2**
    
    Returns:
        str: A sub-key string in the format Moi-sub-XXXXXYYYYY
    """
    digits = ''.join(secrets.choice(string.digits) for _ in range(5))
    letters = ''.join(secrets.choice(string.ascii_lowercase) for _ in range(5))
    return f"{SUB_KEY_PREFIX}{digits}{letters}"


def validate_sub_key_format(sub_key: str) -> bool:
    """
    Validate that a sub-key matches the expected format.
    
    **Validates: Requirements 2.1, 2.2**
    
    Args:
        sub_key: The sub-key string to validate
        
    Returns:
        bool: True if the sub-key matches the format Moi-sub-XXXXXYYYYY
    """
    return bool(SUB_KEY_PATTERN.match(sub_key))


class Database:
    """Database abstraction layer supporting PostgreSQL (primary) and SQLite (fallback)."""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DATABASE_PATH
        self._pool = None  # PostgreSQL connection pool
        self._pg_connected = False
        
        # For SQLite fallback / in-memory testing
        self._shared_conn = None
        self._is_memory_db = self.db_path == ":memory:"
        
        # Force SQLite mode for in-memory databases (used in testing)
        # Otherwise, use PostgreSQL if available and configured
        self._use_postgres = bool(config.POSTGRES_URL) and ASYNCPG_AVAILABLE and not self._is_memory_db
        
        if self._use_postgres:
            logger.info(f"PostgreSQL mode enabled")
        else:
            logger.info(f"SQLite mode - using {self.db_path}")
            # Ensure directory exists for SQLite
            if not self._is_memory_db:
                db_dir = os.path.dirname(self.db_path)
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir, exist_ok=True)
    
    async def init_db(self):
        """Initialize database connection and tables."""
        if self._use_postgres:
            await self._init_postgres()
        else:
            await self._init_sqlite()
    
    async def _init_postgres(self):
        """Initialize PostgreSQL connection pool and tables."""
        retry_delay = INITIAL_RETRY_DELAY
        
        for attempt in range(MAX_RETRY_ATTEMPTS):
            try:
                logger.info(f"Connecting to PostgreSQL (attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS})...")
                self._pool = await asyncpg.create_pool(
                    config.POSTGRES_URL,
                    min_size=2,
                    max_size=10,
                    command_timeout=60
                )
                self._pg_connected = True
                logger.info("PostgreSQL connection pool created")
                
                # Initialize tables
                await self._init_postgres_tables()
                return
                
            except Exception as e:
                logger.error(f"PostgreSQL connection failed (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * RETRY_BACKOFF_MULTIPLIER, MAX_RETRY_DELAY)
        
        # Fallback to SQLite if PostgreSQL fails
        logger.warning("PostgreSQL connection failed, falling back to SQLite")
        self._use_postgres = False
        await self._init_sqlite()
    
    async def _init_postgres_tables(self):
        """Create PostgreSQL tables."""
        async with self._pool.acquire() as conn:
            # Create api_keys table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS api_keys (
                    id SERIAL PRIMARY KEY,
                    key_hash TEXT UNIQUE NOT NULL,
                    key_prefix TEXT NOT NULL,
                    name TEXT,
                    max_rpm INTEGER DEFAULT 60,
                    max_rpd INTEGER DEFAULT 1000,
                    current_rpm INTEGER DEFAULT 0,
                    current_rpd INTEGER DEFAULT 0,
                    last_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    refresh_hour INTEGER DEFAULT NULL,
                    enabled BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used_at TIMESTAMP,
                    target_url TEXT DEFAULT NULL,
                    target_api_key TEXT DEFAULT NULL,
                    use_proxy BOOLEAN DEFAULT TRUE,
                    no_auth BOOLEAN DEFAULT FALSE,
                    model_mappings TEXT DEFAULT NULL,
                    expires_at TIMESTAMP DEFAULT NULL,
                    ip_whitelist TEXT DEFAULT NULL,
                    ip_blacklist TEXT DEFAULT NULL,
                    providers TEXT DEFAULT NULL,
                    provider_rotation_index INTEGER DEFAULT 0,
                    provider_rotation_frequency INTEGER DEFAULT 1,
                    disable_model_fetch BOOLEAN DEFAULT FALSE,
                    http_referer TEXT DEFAULT NULL,
                    max_total_tokens INTEGER DEFAULT NULL,
                    total_tokens_used INTEGER DEFAULT 0,
                    max_context_tokens INTEGER DEFAULT NULL,
                    custom_prefills TEXT DEFAULT NULL,
                    budget_limit REAL DEFAULT NULL,
                    budget_used REAL DEFAULT 0,
                    budget_reset_date TEXT DEFAULT NULL,
                    claim_code TEXT DEFAULT NULL
                )
            ''')
            
            # Create usage_logs table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS usage_logs (
                    id SERIAL PRIMARY KEY,
                    api_key_id INTEGER REFERENCES api_keys(id),
                    model TEXT,
                    tokens_used INTEGER,
                    input_tokens INTEGER DEFAULT 0,
                    output_tokens INTEGER DEFAULT 0,
                    cost REAL DEFAULT 0,
                    success BOOLEAN,
                    is_cache_hit BOOLEAN DEFAULT FALSE,
                    error_message TEXT,
                    client_ip TEXT,
                    client_app TEXT DEFAULT NULL,
                    request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sub_key_id INTEGER DEFAULT NULL
                )
            ''')
            
            # Create client_blacklist table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS client_blacklist (
                    id SERIAL PRIMARY KEY,
                    client_app TEXT UNIQUE NOT NULL,
                    reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Add client_app column if it doesn't exist (migration)
            try:
                await conn.execute('ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS client_app TEXT DEFAULT NULL')
            except:
                pass
            
            # Create model_costs table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS model_costs (
                    id SERIAL PRIMARY KEY,
                    model_pattern TEXT UNIQUE NOT NULL,
                    input_cost_per_1m REAL DEFAULT 0,
                    output_cost_per_1m REAL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Seed default costs if empty
            count = await conn.fetchval('SELECT COUNT(*) FROM model_costs')
            if count == 0:
                default_costs = [
                    # OpenAI models
                    ('gpt-4o', 5.0, 15.0),
                    ('gpt-4o-mini', 0.15, 0.6),
                    ('gpt-4-turbo', 10.0, 30.0),
                    ('gpt-4', 30.0, 60.0),
                    ('gpt-3.5-turbo', 0.5, 1.5),
                    ('o1-preview', 15.0, 60.0),
                    ('o1-mini', 3.0, 12.0),
                    # Anthropic models
                    ('claude-3-5-sonnet', 3.0, 15.0),
                    ('claude-3-opus', 15.0, 75.0),
                    ('claude-3-haiku', 0.25, 1.25),
                    ('claude-3-sonnet', 3.0, 15.0),
                    ('claude-opus-4', 15.0, 75.0),
                    ('claude-sonnet-4', 3.0, 15.0),
                    # Google models
                    ('gemini-pro', 0.5, 1.5),
                    ('gemini-1.5-pro', 3.5, 10.5),
                    ('gemini-1.5-flash', 0.075, 0.3),
                    # Meta models
                    ('llama-3', 0.2, 0.2),
                    ('llama-3.1', 0.2, 0.2),
                    ('llama-3.2', 0.2, 0.2),
                    # Mistral models
                    ('mistral', 0.2, 0.6),
                    ('mixtral', 0.5, 0.5),
                ]
                for pattern, input_cost, output_cost in default_costs:
                    await conn.execute(
                        'INSERT INTO model_costs (model_pattern, input_cost_per_1m, output_cost_per_1m) VALUES ($1, $2, $3)',
                        pattern, input_cost, output_cost
                    )
            
            # Create indexes
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_usage_logs_key ON usage_logs(api_key_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_usage_logs_time ON usage_logs(request_time)')
            
            # Discord users table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS discord_users (
                    id TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    avatar TEXT,
                    global_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP
                )
            ''')
            
            # Discord sessions table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS discord_sessions (
                    session_token TEXT PRIMARY KEY,
                    discord_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP NOT NULL,
                    FOREIGN KEY (discord_id) REFERENCES discord_users(id)
                )
            ''')
            
            # Banned Discord users table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS banned_discord_users (
                    discord_id TEXT PRIMARY KEY,
                    banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    banned_by TEXT,
                    reason TEXT
                )
            ''')
            
            # Key variations mapping table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS key_variations (
                    variation_hash TEXT PRIMARY KEY,
                    original_key_id INTEGER NOT NULL,
                    discord_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (original_key_id) REFERENCES api_keys(id),
                    FOREIGN KEY (discord_id) REFERENCES discord_users(id)
                )
            ''')
            
            # Sub-keys table for code-based key claiming system
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS sub_keys (
                    id SERIAL PRIMARY KEY,
                    sub_key_hash TEXT UNIQUE NOT NULL,
                    sub_key_prefix TEXT NOT NULL,
                    master_key_id INTEGER NOT NULL,
                    discord_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used_at TIMESTAMP,
                    enabled BOOLEAN DEFAULT TRUE,
                    FOREIGN KEY (master_key_id) REFERENCES api_keys(id),
                    FOREIGN KEY (discord_id) REFERENCES discord_users(id),
                    UNIQUE(master_key_id, discord_id)
                )
            ''')
            
            # Add discord_id column to usage_logs if it doesn't exist
            try:
                await conn.execute('ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS discord_id TEXT DEFAULT NULL')
            except:
                pass
            
            # Add claim_code column to api_keys if it doesn't exist (migration for code-based key claiming)
            try:
                await conn.execute('ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS claim_code TEXT DEFAULT NULL')
            except:
                pass
            
            # Add sub_key_id column to usage_logs if it doesn't exist (for tracking sub-key usage)
            try:
                await conn.execute('ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS sub_key_id INTEGER DEFAULT NULL')
            except:
                pass
            
            # Create indexes for Discord tables
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_discord_sessions_discord_id ON discord_sessions(discord_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_discord_sessions_expires ON discord_sessions(expires_at)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_key_variations_discord_id ON key_variations(discord_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_key_variations_original_key ON key_variations(original_key_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_usage_logs_discord_id ON usage_logs(discord_id)')
            
            # Create indexes for sub_keys table
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_sub_keys_master_key ON sub_keys(master_key_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_sub_keys_discord_id ON sub_keys(discord_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_api_keys_claim_code ON api_keys(claim_code)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_usage_logs_sub_key_id ON usage_logs(sub_key_id)')
            
        logger.info("PostgreSQL tables initialized")

    
    async def _init_sqlite(self):
        """Initialize SQLite database and tables."""
        if self._is_memory_db:
            self._shared_conn = await aiosqlite.connect(":memory:")
            self._shared_conn.row_factory = aiosqlite.Row
            await self._init_sqlite_tables(self._shared_conn)
        else:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                await self._init_sqlite_tables(db)
                await db.commit()
        logger.info("SQLite database initialized")
    
    async def _init_sqlite_tables(self, db):
        """Create SQLite tables."""
        await db.execute('''
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_hash TEXT UNIQUE NOT NULL,
                key_prefix TEXT NOT NULL,
                name TEXT,
                max_rpm INTEGER DEFAULT 60,
                max_rpd INTEGER DEFAULT 1000,
                current_rpm INTEGER DEFAULT 0,
                current_rpd INTEGER DEFAULT 0,
                last_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                refresh_hour INTEGER DEFAULT NULL,
                enabled BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP,
                target_url TEXT DEFAULT NULL,
                target_api_key TEXT DEFAULT NULL,
                use_proxy BOOLEAN DEFAULT 1,
                no_auth BOOLEAN DEFAULT 0,
                model_mappings TEXT DEFAULT NULL,
                expires_at TIMESTAMP DEFAULT NULL,
                ip_whitelist TEXT DEFAULT NULL,
                ip_blacklist TEXT DEFAULT NULL,
                providers TEXT DEFAULT NULL,
                provider_rotation_index INTEGER DEFAULT 0,
                provider_rotation_frequency INTEGER DEFAULT 1,
                disable_model_fetch BOOLEAN DEFAULT 0,
                http_referer TEXT DEFAULT NULL,
                max_total_tokens INTEGER DEFAULT NULL,
                total_tokens_used INTEGER DEFAULT 0,
                max_context_tokens INTEGER DEFAULT NULL,
                custom_prefills TEXT DEFAULT NULL,
                budget_limit REAL DEFAULT NULL,
                budget_used REAL DEFAULT 0,
                budget_reset_date TEXT DEFAULT NULL,
                claim_code TEXT DEFAULT NULL
            )
        ''')
        
        await db.execute('''
            CREATE TABLE IF NOT EXISTS usage_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key_id INTEGER,
                model TEXT,
                tokens_used INTEGER,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                cost REAL DEFAULT 0,
                success BOOLEAN,
                is_cache_hit BOOLEAN DEFAULT 0,
                error_message TEXT,
                client_ip TEXT,
                client_app TEXT DEFAULT NULL,
                request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sub_key_id INTEGER DEFAULT NULL,
                FOREIGN KEY (api_key_id) REFERENCES api_keys (id)
            )
        ''')
        
        # Create client_blacklist table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS client_blacklist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_app TEXT UNIQUE NOT NULL,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        await db.execute('''
            CREATE TABLE IF NOT EXISTS model_costs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_pattern TEXT UNIQUE NOT NULL,
                input_cost_per_1m REAL DEFAULT 0,
                output_cost_per_1m REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Seed default costs
        cursor = await db.execute('SELECT COUNT(*) as count FROM model_costs')
        row = await cursor.fetchone()
        if row and row[0] == 0:
            default_costs = [
                # OpenAI models
                ('gpt-4o', 5.0, 15.0),
                ('gpt-4o-mini', 0.15, 0.6),
                ('gpt-4-turbo', 10.0, 30.0),
                ('gpt-4', 30.0, 60.0),
                ('gpt-3.5-turbo', 0.5, 1.5),
                ('o1-preview', 15.0, 60.0),
                ('o1-mini', 3.0, 12.0),
                # Anthropic models
                ('claude-3-5-sonnet', 3.0, 15.0),
                ('claude-3-opus', 15.0, 75.0),
                ('claude-3-haiku', 0.25, 1.25),
                ('claude-3-sonnet', 3.0, 15.0),
                ('claude-opus-4', 15.0, 75.0),
                ('claude-sonnet-4', 3.0, 15.0),
                # Google models
                ('gemini-pro', 0.5, 1.5),
                ('gemini-1.5-pro', 3.5, 10.5),
                ('gemini-1.5-flash', 0.075, 0.3),
                # Meta models
                ('llama-3', 0.2, 0.2),
                ('llama-3.1', 0.2, 0.2),
                ('llama-3.2', 0.2, 0.2),
                # Mistral models
                ('mistral', 0.2, 0.6),
                ('mixtral', 0.5, 0.5),
            ]
            for pattern, input_cost, output_cost in default_costs:
                await db.execute(
                    'INSERT INTO model_costs (model_pattern, input_cost_per_1m, output_cost_per_1m) VALUES (?, ?, ?)',
                    (pattern, input_cost, output_cost)
                )
        
        # Migration: Add client_app column if it doesn't exist
        try:
            await db.execute('ALTER TABLE usage_logs ADD COLUMN client_app TEXT DEFAULT NULL')
        except:
            pass  # Column already exists
        
        # Discord users table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS discord_users (
                id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                avatar TEXT,
                global_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
        ''')
        
        # Discord sessions table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS discord_sessions (
                session_token TEXT PRIMARY KEY,
                discord_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                FOREIGN KEY (discord_id) REFERENCES discord_users(id)
            )
        ''')
        
        # Banned Discord users table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS banned_discord_users (
                discord_id TEXT PRIMARY KEY,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                banned_by TEXT,
                reason TEXT
            )
        ''')
        
        # Key variations mapping table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS key_variations (
                variation_hash TEXT PRIMARY KEY,
                original_key_id INTEGER NOT NULL,
                discord_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (original_key_id) REFERENCES api_keys(id),
                FOREIGN KEY (discord_id) REFERENCES discord_users(id)
            )
        ''')
        
        # Sub-keys table for code-based key claiming system
        await db.execute('''
            CREATE TABLE IF NOT EXISTS sub_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sub_key_hash TEXT UNIQUE NOT NULL,
                sub_key_prefix TEXT NOT NULL,
                master_key_id INTEGER NOT NULL,
                discord_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used_at TIMESTAMP,
                enabled BOOLEAN DEFAULT 1,
                FOREIGN KEY (master_key_id) REFERENCES api_keys(id),
                FOREIGN KEY (discord_id) REFERENCES discord_users(id),
                UNIQUE(master_key_id, discord_id)
            )
        ''')
        
        # Migration: Add discord_id column to usage_logs if it doesn't exist
        try:
            await db.execute('ALTER TABLE usage_logs ADD COLUMN discord_id TEXT DEFAULT NULL')
        except:
            pass  # Column already exists
        
        # Migration: Add claim_code column to api_keys if it doesn't exist (for code-based key claiming)
        try:
            await db.execute('ALTER TABLE api_keys ADD COLUMN claim_code TEXT DEFAULT NULL')
        except:
            pass  # Column already exists
        
        # Migration: Add sub_key_id column to usage_logs if it doesn't exist (for tracking sub-key usage)
        try:
            await db.execute('ALTER TABLE usage_logs ADD COLUMN sub_key_id INTEGER DEFAULT NULL')
        except:
            pass  # Column already exists
        
        # Create indexes for sub_keys table
        try:
            await db.execute('CREATE INDEX IF NOT EXISTS idx_sub_keys_master_key ON sub_keys(master_key_id)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_sub_keys_discord_id ON sub_keys(discord_id)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_api_keys_claim_code ON api_keys(claim_code)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_usage_logs_sub_key_id ON usage_logs(sub_key_id)')
        except:
            pass  # Indexes may already exist
        
        await db.commit()

    # ==================== API KEY OPERATIONS ====================
    
    async def create_key(self, name: str = None, max_rpm: int = 60, max_rpd: int = 1000,
                        target_url: str = None, target_api_key: str = None,
                        no_auth: bool = False, use_proxy: bool = True,
                        model_mappings: str = None, expires_at: str = None,
                        ip_whitelist: str = None, ip_blacklist: str = None,
                        providers: str = None, provider_rotation_frequency: int = 1,
                        disable_model_fetch: bool = False, http_referer: str = None,
                        max_total_tokens: int = None, max_context_tokens: int = None,
                        custom_prefills: str = None, claim_code: str = None) -> Dict[str, Any]:
        """Create a new API key."""
        # Always use "Moi" as prefix, with the name in parentheses for display
        raw_key = f"Moi-{secrets.token_hex(4)}"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        # Store display name with key name if provided
        key_prefix = f"{raw_key} ({name})" if name else raw_key
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow('''
                    INSERT INTO api_keys (key_hash, key_prefix, name, max_rpm, max_rpd,
                        target_url, target_api_key, no_auth, use_proxy, model_mappings,
                        expires_at, ip_whitelist, ip_blacklist, providers,
                        provider_rotation_frequency, disable_model_fetch, http_referer,
                        max_total_tokens, max_context_tokens, custom_prefills, claim_code)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                    RETURNING id
                ''', key_hash, key_prefix, name, max_rpm, max_rpd,
                    target_url, target_api_key, no_auth, use_proxy, model_mappings,
                    expires_at, ip_whitelist, ip_blacklist, providers,
                    provider_rotation_frequency, disable_model_fetch, http_referer,
                    max_total_tokens, max_context_tokens, custom_prefills, claim_code)
                key_id = row['id']
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    INSERT INTO api_keys (key_hash, key_prefix, name, max_rpm, max_rpd,
                        target_url, target_api_key, no_auth, use_proxy, model_mappings,
                        expires_at, ip_whitelist, ip_blacklist, providers,
                        provider_rotation_frequency, disable_model_fetch, http_referer,
                        max_total_tokens, max_context_tokens, custom_prefills, claim_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (key_hash, key_prefix, name, max_rpm, max_rpd,
                    target_url, target_api_key, no_auth, use_proxy, model_mappings,
                    expires_at, ip_whitelist, ip_blacklist, providers,
                    provider_rotation_frequency, disable_model_fetch, http_referer,
                    max_total_tokens, max_context_tokens, custom_prefills, claim_code))
                await db.commit()
                key_id = cursor.lastrowid
            finally:
                if not self._is_memory_db:
                    await db.close()
        
        return {"id": key_id, "api_key": raw_key, "prefix": key_prefix, "name": name}


    async def validate_key(self, api_key: str) -> Optional[Dict[str, Any]]:
        """Validate an API key and return its info."""
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT * FROM api_keys WHERE key_hash = $1 AND enabled = TRUE',
                    key_hash
                )
                if row:
                    # Update last_used_at
                    await conn.execute(
                        'UPDATE api_keys SET last_used_at = CURRENT_TIMESTAMP WHERE id = $1',
                        row['id']
                    )
                    return dict(row)
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    'SELECT * FROM api_keys WHERE key_hash = ? AND enabled = 1',
                    (key_hash,)
                )
                row = await cursor.fetchone()
                if row:
                    await db.execute(
                        'UPDATE api_keys SET last_used_at = CURRENT_TIMESTAMP WHERE id = ?',
                        (row['id'],)
                    )
                    await db.commit()
                    return dict(row)
            finally:
                if not self._is_memory_db:
                    await db.close()
        return None

    async def validate_key_extended(self, api_key: str) -> Optional[Dict[str, Any]]:
        """
        Validate an API key (master or sub-key) and return extended info.
        
        This function checks both the api_keys table (for master keys) and the 
        sub_keys table (for sub-keys). If a sub-key is found, it returns the 
        master key configuration along with sub-key metadata.
        
        **Validates: Requirements 4.2**
        
        Args:
            api_key: The API key string to validate (can be master key or sub-key)
            
        Returns:
            Dict with key info including:
            - All master key fields (id, name, max_rpm, max_rpd, etc.)
            - is_sub_key: bool indicating if this is a sub-key
            - sub_key_id: int sub-key ID (if is_sub_key is True)
            - discord_id: str Discord user ID (if is_sub_key is True)
            - discord_username: str Discord username (if available)
            Returns None if key is invalid or disabled.
        """
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                # First, check if it's a master key
                row = await conn.fetchrow(
                    'SELECT * FROM api_keys WHERE key_hash = $1 AND enabled = TRUE',
                    key_hash
                )
                if row:
                    # Update last_used_at for master key
                    await conn.execute(
                        'UPDATE api_keys SET last_used_at = CURRENT_TIMESTAMP WHERE id = $1',
                        row['id']
                    )
                    result = dict(row)
                    result['is_sub_key'] = False
                    result['sub_key_id'] = None
                    result['discord_id'] = None
                    result['discord_username'] = None
                    return result
                
                # Not a master key, check if it's a sub-key
                sub_key_row = await conn.fetchrow('''
                    SELECT sk.id as sub_key_id, sk.discord_id, sk.enabled as sub_key_enabled,
                           sk.master_key_id, du.username as discord_username,
                           ak.*
                    FROM sub_keys sk
                    JOIN api_keys ak ON sk.master_key_id = ak.id
                    LEFT JOIN discord_users du ON sk.discord_id = du.id
                    WHERE sk.sub_key_hash = $1
                ''', key_hash)
                
                if sub_key_row:
                    sub_key_dict = dict(sub_key_row)
                    # Check if both sub-key and master key are enabled
                    if not sub_key_dict.get('sub_key_enabled') or not sub_key_dict.get('enabled'):
                        return None
                    
                    # Update last_used_at for sub-key
                    await conn.execute(
                        'UPDATE sub_keys SET last_used_at = CURRENT_TIMESTAMP WHERE id = $1',
                        sub_key_dict['sub_key_id']
                    )
                    
                    # Build result with master key config + sub-key metadata
                    result = {k: v for k, v in sub_key_dict.items() 
                              if k not in ('sub_key_enabled', 'master_key_id')}
                    result['is_sub_key'] = True
                    return result
                
                return None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                
                # First, check if it's a master key
                cursor = await db.execute(
                    'SELECT * FROM api_keys WHERE key_hash = ? AND enabled = 1',
                    (key_hash,)
                )
                row = await cursor.fetchone()
                if row:
                    await db.execute(
                        'UPDATE api_keys SET last_used_at = CURRENT_TIMESTAMP WHERE id = ?',
                        (row['id'],)
                    )
                    await db.commit()
                    result = dict(row)
                    result['is_sub_key'] = False
                    result['sub_key_id'] = None
                    result['discord_id'] = None
                    result['discord_username'] = None
                    return result
                
                # Not a master key, check if it's a sub-key
                cursor = await db.execute('''
                    SELECT sk.id as sub_key_id, sk.discord_id, sk.enabled as sub_key_enabled,
                           sk.master_key_id, du.username as discord_username,
                           ak.*
                    FROM sub_keys sk
                    JOIN api_keys ak ON sk.master_key_id = ak.id
                    LEFT JOIN discord_users du ON sk.discord_id = du.id
                    WHERE sk.sub_key_hash = ?
                ''', (key_hash,))
                sub_key_row = await cursor.fetchone()
                
                if sub_key_row:
                    sub_key_dict = dict(sub_key_row)
                    # Check if both sub-key and master key are enabled
                    if not sub_key_dict.get('sub_key_enabled') or not sub_key_dict.get('enabled'):
                        return None
                    
                    # Update last_used_at for sub-key
                    await db.execute(
                        'UPDATE sub_keys SET last_used_at = CURRENT_TIMESTAMP WHERE id = ?',
                        (sub_key_dict['sub_key_id'],)
                    )
                    await db.commit()
                    
                    # Build result with master key config + sub-key metadata
                    result = {k: v for k, v in sub_key_dict.items() 
                              if k not in ('sub_key_enabled', 'master_key_id')}
                    result['is_sub_key'] = True
                    return result
                
                return None
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_all_keys(self) -> List[Dict[str, Any]]:
        """Get all API keys."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('SELECT * FROM api_keys ORDER BY created_at DESC')
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('SELECT * FROM api_keys ORDER BY created_at DESC')
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_api_key_by_id(self, key_id: int) -> Optional[Dict[str, Any]]:
        """Get API key by ID."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow('SELECT * FROM api_keys WHERE id = $1', key_id)
                return dict(row) if row else None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('SELECT * FROM api_keys WHERE id = ?', (key_id,))
                row = await cursor.fetchone()
                return dict(row) if row else None
            finally:
                if not self._is_memory_db:
                    await db.close()

    # Alias for get_api_key_by_id
    async def get_key_by_id(self, key_id: int) -> Optional[Dict[str, Any]]:
        """Get API key by ID (alias for get_api_key_by_id)."""
        return await self.get_api_key_by_id(key_id)

    async def get_sub_key_by_id(self, sub_key_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a sub-key by its ID.
        
        Args:
            sub_key_id: The sub-key ID
            
        Returns:
            Dict with sub-key info or None if not found
        """
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow('''
                    SELECT sk.*, ak.name as master_key_name, ak.enabled as master_key_enabled
                    FROM sub_keys sk
                    JOIN api_keys ak ON sk.master_key_id = ak.id
                    WHERE sk.id = $1
                ''', sub_key_id)
                return dict(row) if row else None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT sk.*, ak.name as master_key_name, ak.enabled as master_key_enabled
                    FROM sub_keys sk
                    JOIN api_keys ak ON sk.master_key_id = ak.id
                    WHERE sk.id = ?
                ''', (sub_key_id,))
                row = await cursor.fetchone()
                return dict(row) if row else None
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def update_key_limits(self, key_id: int, **kwargs) -> bool:
        """Update API key settings."""
        if not kwargs:
            return False
        
        if self._use_postgres:
            sets = ', '.join([f"{k} = ${i+2}" for i, k in enumerate(kwargs.keys())])
            values = [key_id] + list(kwargs.values())
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    f'UPDATE api_keys SET {sets} WHERE id = $1',
                    *values
                )
                return 'UPDATE' in result
        else:
            sets = ', '.join([f"{k} = ?" for k in kwargs.keys()])
            values = list(kwargs.values()) + [key_id]
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute(f'UPDATE api_keys SET {sets} WHERE id = ?', values)
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()

    # Alias for compatibility
    async def update_api_key(self, key_id: int, **kwargs) -> bool:
        return await self.update_key_limits(key_id, **kwargs)

    async def toggle_key(self, key_id: int) -> bool:
        """
        Toggle API key enabled/disabled status.
        
        When a master key is disabled, all associated sub-keys are also disabled
        (cascade disable). When a master key is enabled, sub-keys are also enabled.
        
        **Validates: Requirements 4.5** - Master Key Cascade Disable
        """
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                # Get current status
                row = await conn.fetchrow('SELECT enabled FROM api_keys WHERE id = $1', key_id)
                if not row:
                    return False
                
                new_status = not row['enabled']
                
                # Update master key status
                await conn.execute(
                    'UPDATE api_keys SET enabled = $1 WHERE id = $2',
                    new_status, key_id
                )
                
                # Cascade: Update all sub-keys for this master key
                # When master is disabled, disable all sub-keys
                # When master is enabled, enable all sub-keys
                await conn.execute(
                    'UPDATE sub_keys SET enabled = $1 WHERE master_key_id = $2',
                    new_status, key_id
                )
                
                return True
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('SELECT enabled FROM api_keys WHERE id = ?', (key_id,))
                row = await cursor.fetchone()
                if not row:
                    return False
                
                new_status = not row['enabled']
                
                # Update master key status
                await db.execute(
                    'UPDATE api_keys SET enabled = ? WHERE id = ?',
                    (new_status, key_id)
                )
                
                # Cascade: Update all sub-keys for this master key
                # When master is disabled, disable all sub-keys
                # When master is enabled, enable all sub-keys
                await db.execute(
                    'UPDATE sub_keys SET enabled = ? WHERE master_key_id = ?',
                    (new_status, key_id)
                )
                
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def delete_key(self, key_id: int) -> bool:
        """Delete an API key and its sub-keys. Usage logs are preserved for analytics."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                # Use a transaction to ensure all deletes happen atomically
                async with conn.transaction():
                    # First, set sub_key_id to NULL in usage_logs for sub-keys of this master
                    await conn.execute('''
                        UPDATE usage_logs SET sub_key_id = NULL 
                        WHERE sub_key_id IN (SELECT id FROM sub_keys WHERE master_key_id = $1)
                    ''', key_id)
                    # Delete all sub-keys associated with this master key
                    await conn.execute('DELETE FROM sub_keys WHERE master_key_id = $1', key_id)
                    # Also delete key variations
                    await conn.execute('DELETE FROM key_variations WHERE original_key_id = $1', key_id)
                    # Set api_key_id to NULL in usage_logs to preserve analytics
                    await conn.execute('UPDATE usage_logs SET api_key_id = NULL WHERE api_key_id = $1', key_id)
                    # Finally delete the master key
                    result = await conn.execute('DELETE FROM api_keys WHERE id = $1', key_id)
                    return 'DELETE' in result
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                # First, set sub_key_id to NULL in usage_logs for sub-keys of this master
                await db.execute('''
                    UPDATE usage_logs SET sub_key_id = NULL 
                    WHERE sub_key_id IN (SELECT id FROM sub_keys WHERE master_key_id = ?)
                ''', (key_id,))
                # Delete all sub-keys associated with this master key
                await db.execute('DELETE FROM sub_keys WHERE master_key_id = ?', (key_id,))
                # Also delete key variations
                await db.execute('DELETE FROM key_variations WHERE original_key_id = ?', (key_id,))
                # Set api_key_id to NULL in usage_logs to preserve analytics
                await db.execute('UPDATE usage_logs SET api_key_id = NULL WHERE api_key_id = ?', (key_id,))
                # Finally delete the master key
                await db.execute('DELETE FROM api_keys WHERE id = ?', (key_id,))
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()

    # Alias for compatibility
    async def delete_api_key(self, key_id: int) -> bool:
        return await self.delete_key(key_id)

    async def refresh_key_limits(self, key_id: int) -> bool:
        """Reset rate limit counters (RPM and RPD) for a specific key."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                # Check if key exists
                row = await conn.fetchrow('SELECT id FROM api_keys WHERE id = $1', key_id)
                if not row:
                    return False
                
                # Reset the rate limit counters
                await conn.execute('''
                    UPDATE api_keys 
                    SET current_rpm = 0, 
                        current_rpd = 0, 
                        last_reset = NOW()
                    WHERE id = $1
                ''', key_id)
                return True
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                # Check if key exists
                cursor = await db.execute('SELECT id FROM api_keys WHERE id = ?', (key_id,))
                row = await cursor.fetchone()
                if not row:
                    return False
                
                # Reset the rate limit counters
                from datetime import timezone
                now = datetime.now(timezone.utc).isoformat()
                await db.execute('''
                    UPDATE api_keys 
                    SET current_rpm = 0, 
                        current_rpd = 0, 
                        last_reset = ?
                    WHERE id = ?
                ''', (now, key_id))
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def set_refresh_schedule(self, key_id: int, refresh_hour: int) -> bool:
        """Set the auto-refresh schedule hour for a key."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                # Check if key exists
                row = await conn.fetchrow('SELECT id FROM api_keys WHERE id = $1', key_id)
                if not row:
                    return False
                
                await conn.execute('''
                    UPDATE api_keys 
                    SET refresh_hour = $1
                    WHERE id = $2
                ''', refresh_hour, key_id)
                return True
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                cursor = await db.execute('SELECT id FROM api_keys WHERE id = ?', (key_id,))
                row = await cursor.fetchone()
                if not row:
                    return False
                
                await db.execute('''
                    UPDATE api_keys 
                    SET refresh_hour = ?
                    WHERE id = ?
                ''', (refresh_hour, key_id))
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def toggle_key_proxy(self, key_id: int) -> bool:
        """Toggle the use_proxy setting for a key."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                # Check if key exists and get current value
                row = await conn.fetchrow('SELECT id, use_proxy FROM api_keys WHERE id = $1', key_id)
                if not row:
                    return False
                
                new_value = not row['use_proxy']
                await conn.execute('''
                    UPDATE api_keys 
                    SET use_proxy = $1
                    WHERE id = $2
                ''', new_value, key_id)
                return True
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                cursor = await db.execute('SELECT id, use_proxy FROM api_keys WHERE id = ?', (key_id,))
                row = await cursor.fetchone()
                if not row:
                    return False
                
                new_value = 0 if row[1] else 1
                await db.execute('''
                    UPDATE api_keys 
                    SET use_proxy = ?
                    WHERE id = ?
                ''', (new_value, key_id))
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()


    # ==================== RATE LIMITING ====================
    
    async def check_rate_limit(self, key_id: int, max_rpm: int, max_rpd: int) -> Dict[str, Any]:
        """Check if request is within rate limits. Returns dict with 'allowed' and 'error' keys.
        
        Implements time-based reset:
        - RPM resets every minute (tracked separately)
        - RPD resets every day
        """
        now = datetime.now()
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT current_rpm, current_rpd, last_reset FROM api_keys WHERE id = $1',
                    key_id
                )
                if not row:
                    return {"allowed": False, "error": "Key not found"}
                
                current_rpm = row['current_rpm'] or 0
                current_rpd = row['current_rpd'] or 0
                last_reset = row['last_reset']
                
                # Parse last_reset timestamp - handle timezone-aware datetimes from PostgreSQL
                last_reset_dt = None
                if last_reset:
                    if isinstance(last_reset, str):
                        try:
                            # Remove timezone info for comparison
                            clean_str = last_reset.replace('Z', '').replace('+00:00', '').split('+')[0].split('.')[0]
                            last_reset_dt = datetime.fromisoformat(clean_str)
                        except:
                            last_reset_dt = None
                    elif hasattr(last_reset, 'replace'):
                        # It's a datetime object - make it timezone-naive for comparison
                        try:
                            last_reset_dt = last_reset.replace(tzinfo=None)
                        except:
                            last_reset_dt = last_reset
                    else:
                        last_reset_dt = last_reset
                
                # Determine what needs to be reset
                reset_rpm = False
                reset_rpd = False
                
                if last_reset_dt:
                    # Reset RPM if more than 1 minute has passed
                    time_diff = (now - last_reset_dt).total_seconds()
                    if time_diff >= 60:
                        reset_rpm = True
                        current_rpm = 0
                    
                    # Reset RPD if it's a new day
                    if last_reset_dt.date() < now.date():
                        reset_rpd = True
                        current_rpd = 0
                else:
                    # No last_reset, initialize everything
                    reset_rpm = True
                    reset_rpd = True
                    current_rpm = 0
                    current_rpd = 0
                
                # Check limits after potential reset
                if current_rpm >= max_rpm:
                    return {"allowed": False, "error": f"Rate limit exceeded: {current_rpm}/{max_rpm} RPM"}
                if current_rpd >= max_rpd:
                    return {"allowed": False, "error": f"Daily limit exceeded: {current_rpd}/{max_rpd} RPD"}
                
                # Update counters - after reset, current_rpm/rpd are already 0, so +1 gives us 1
                new_rpm = current_rpm + 1
                new_rpd = current_rpd + 1
                
                # Only update last_reset when RPM resets (every minute)
                if reset_rpm:
                    await conn.execute('''
                        UPDATE api_keys 
                        SET current_rpm = $1, 
                            current_rpd = $2,
                            last_reset = $3
                        WHERE id = $4
                    ''', new_rpm, new_rpd, now, key_id)
                else:
                    await conn.execute('''
                        UPDATE api_keys 
                        SET current_rpm = $1, 
                            current_rpd = $2
                        WHERE id = $3
                    ''', new_rpm, new_rpd, key_id)
                
                return {"allowed": True, "error": ""}
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    'SELECT current_rpm, current_rpd, last_reset FROM api_keys WHERE id = ?',
                    (key_id,)
                )
                row = await cursor.fetchone()
                if not row:
                    return {"allowed": False, "error": "Key not found"}
                
                current_rpm = row['current_rpm'] or 0
                current_rpd = row['current_rpd'] or 0
                last_reset = row['last_reset']
                
                # Parse last_reset timestamp
                last_reset_dt = None
                if last_reset:
                    if isinstance(last_reset, str):
                        try:
                            # Remove timezone info and microseconds for comparison
                            clean_str = last_reset.replace('Z', '').replace('+00:00', '').split('+')[0].split('.')[0]
                            last_reset_dt = datetime.fromisoformat(clean_str)
                        except:
                            last_reset_dt = None
                    else:
                        last_reset_dt = last_reset
                
                # Determine what needs to be reset
                reset_rpm = False
                reset_rpd = False
                
                if last_reset_dt:
                    # Reset RPM if more than 1 minute has passed
                    time_diff = (now - last_reset_dt).total_seconds()
                    if time_diff >= 60:
                        reset_rpm = True
                        current_rpm = 0
                    
                    # Reset RPD if it's a new day
                    if last_reset_dt.date() < now.date():
                        reset_rpd = True
                        current_rpd = 0
                else:
                    # No last_reset, initialize everything
                    reset_rpm = True
                    reset_rpd = True
                    current_rpm = 0
                    current_rpd = 0
                
                # Check limits after potential reset
                if current_rpm >= max_rpm:
                    return {"allowed": False, "error": f"Rate limit exceeded: {current_rpm}/{max_rpm} RPM"}
                if current_rpd >= max_rpd:
                    return {"allowed": False, "error": f"Daily limit exceeded: {current_rpd}/{max_rpd} RPD"}
                
                # Update counters - after reset, current_rpm/rpd are already 0, so +1 gives us 1
                new_rpm = current_rpm + 1
                new_rpd = current_rpd + 1
                
                # Only update last_reset when RPM resets (every minute)
                if reset_rpm:
                    await db.execute('''
                        UPDATE api_keys 
                        SET current_rpm = ?, 
                            current_rpd = ?,
                            last_reset = ?
                        WHERE id = ?
                    ''', (new_rpm, new_rpd, now.isoformat(), key_id))
                else:
                    await db.execute('''
                        UPDATE api_keys 
                        SET current_rpm = ?, 
                            current_rpd = ?
                        WHERE id = ?
                    ''', (new_rpm, new_rpd, key_id))
                await db.commit()
                
                return {"allowed": True, "error": ""}
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def increment_rate_limit(self, key_id: int):
        """Increment rate limit counters."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                await conn.execute('''
                    UPDATE api_keys 
                    SET current_rpm = current_rpm + 1, 
                        current_rpd = current_rpd + 1
                    WHERE id = $1
                ''', key_id)
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('''
                    UPDATE api_keys 
                    SET current_rpm = current_rpm + 1, 
                        current_rpd = current_rpd + 1
                    WHERE id = ?
                ''', (key_id,))
                await db.commit()
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_aggregated_usage(self, main_key_id: int) -> Dict[str, Any]:
        """
        Get aggregated usage across all sub-keys for a main key.
        
        This function sums RPD, RPM, and tokens across all sub-keys of a main key.
        Usage is aggregated from usage_logs table based on discord_id linked to key variations.
        
        Args:
            main_key_id: The ID of the main API key
            
        Returns:
            Dict with:
            - rpm_used: requests in current minute from all sub-keys
            - rpd_used: requests today from all sub-keys
            - tokens_used: total tokens today from all sub-keys
            - sub_key_count: number of active sub-keys
            
        **Validates: Requirements 10.6**
        """
        # Use UTC time to match SQLite's CURRENT_TIMESTAMP which stores UTC
        from datetime import timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None)  # Get UTC time as naive datetime
        minute_ago = now - timedelta(minutes=1)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                # Get sub-key count
                sub_key_count = await conn.fetchval('''
                    SELECT COUNT(*) FROM key_variations WHERE original_key_id = $1
                ''', main_key_id)
                
                # Get RPM (requests in the last minute) from all sub-keys
                rpm_used = await conn.fetchval('''
                    SELECT COUNT(*) FROM usage_logs 
                    WHERE api_key_id = $1 
                    AND request_time >= $2
                ''', main_key_id, minute_ago)
                
                # Get RPD (requests today) from all sub-keys
                rpd_used = await conn.fetchval('''
                    SELECT COUNT(*) FROM usage_logs 
                    WHERE api_key_id = $1 
                    AND request_time >= $2
                ''', main_key_id, today_start)
                
                # Get total tokens used today from all sub-keys
                tokens_used = await conn.fetchval('''
                    SELECT COALESCE(SUM(tokens_used), 0) FROM usage_logs 
                    WHERE api_key_id = $1 
                    AND request_time >= $2
                ''', main_key_id, today_start)
                
                return {
                    "rpm_used": rpm_used or 0,
                    "rpd_used": rpd_used or 0,
                    "tokens_used": tokens_used or 0,
                    "sub_key_count": sub_key_count or 0
                }
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                # Get sub-key count
                cursor = await db.execute('''
                    SELECT COUNT(*) FROM key_variations WHERE original_key_id = ?
                ''', (main_key_id,))
                row = await cursor.fetchone()
                sub_key_count = row[0] if row else 0
                
                # Format timestamps for SQLite comparison (use space separator, no microseconds)
                minute_ago_str = minute_ago.strftime('%Y-%m-%d %H:%M:%S')
                today_start_str = today_start.strftime('%Y-%m-%d %H:%M:%S')
                
                # Get RPM (requests in the last minute) from all sub-keys
                cursor = await db.execute('''
                    SELECT COUNT(*) FROM usage_logs 
                    WHERE api_key_id = ? 
                    AND request_time >= ?
                ''', (main_key_id, minute_ago_str))
                row = await cursor.fetchone()
                rpm_used = row[0] if row else 0
                
                # Get RPD (requests today) from all sub-keys
                cursor = await db.execute('''
                    SELECT COUNT(*) FROM usage_logs 
                    WHERE api_key_id = ? 
                    AND request_time >= ?
                ''', (main_key_id, today_start_str))
                row = await cursor.fetchone()
                rpd_used = row[0] if row else 0
                
                # Get total tokens used today from all sub-keys
                cursor = await db.execute('''
                    SELECT COALESCE(SUM(tokens_used), 0) FROM usage_logs 
                    WHERE api_key_id = ? 
                    AND request_time >= ?
                ''', (main_key_id, today_start_str))
                row = await cursor.fetchone()
                tokens_used = row[0] if row else 0
                
                return {
                    "rpm_used": rpm_used,
                    "rpd_used": rpd_used,
                    "tokens_used": tokens_used,
                    "sub_key_count": sub_key_count
                }
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def check_rate_limit_aggregated(self, main_key_id: int, max_rpm: int, max_rpd: int, 
                                          max_total_tokens: int = None) -> Dict[str, Any]:
        """
        Check if request is within rate limits using aggregated usage from all sub-keys.
        
        This function checks rate limits against the main key's settings by aggregating
        usage from ALL sub-keys. When the main key's limit is reached, ALL sub-keys are blocked.
        
        Args:
            main_key_id: The ID of the main API key
            max_rpm: Maximum requests per minute for the main key
            max_rpd: Maximum requests per day for the main key
            max_total_tokens: Maximum total tokens per day (optional)
            
        Returns:
            Dict with:
            - allowed: True if request is allowed, False otherwise
            - error: Error message if not allowed
            - rpm_used: Current RPM usage
            - rpd_used: Current RPD usage
            - tokens_used: Current token usage
            - rpm_remaining: Remaining RPM
            - rpd_remaining: Remaining RPD
            
        **Validates: Requirements 10.1, 10.4, 10.5**
        """
        # Get aggregated usage from all sub-keys
        usage = await self.get_aggregated_usage(main_key_id)
        
        rpm_used = usage["rpm_used"]
        rpd_used = usage["rpd_used"]
        tokens_used = usage["tokens_used"]
        
        # Check RPM limit
        if rpm_used >= max_rpm:
            return {
                "allowed": False,
                "error": f"Rate limit exceeded: {rpm_used}/{max_rpm} RPM. All sub-keys blocked until minute resets.",
                "rpm_used": rpm_used,
                "rpd_used": rpd_used,
                "tokens_used": tokens_used,
                "rpm_remaining": 0,
                "rpd_remaining": max(0, max_rpd - rpd_used)
            }
        
        # Check RPD limit
        if rpd_used >= max_rpd:
            return {
                "allowed": False,
                "error": f"Daily limit exceeded: {rpd_used}/{max_rpd} RPD. All sub-keys blocked until daily reset.",
                "rpm_used": rpm_used,
                "rpd_used": rpd_used,
                "tokens_used": tokens_used,
                "rpm_remaining": max(0, max_rpm - rpm_used),
                "rpd_remaining": 0
            }
        
        # Check token limit if specified
        if max_total_tokens is not None and tokens_used >= max_total_tokens:
            return {
                "allowed": False,
                "error": f"Daily token limit exceeded: {tokens_used}/{max_total_tokens} tokens. All sub-keys blocked until daily reset.",
                "rpm_used": rpm_used,
                "rpd_used": rpd_used,
                "tokens_used": tokens_used,
                "rpm_remaining": max(0, max_rpm - rpm_used),
                "rpd_remaining": max(0, max_rpd - rpd_used)
            }
        
        return {
            "allowed": True,
            "error": "",
            "rpm_used": rpm_used,
            "rpd_used": rpd_used,
            "tokens_used": tokens_used,
            "rpm_remaining": max_rpm - rpm_used - 1,  # -1 for current request
            "rpd_remaining": max_rpd - rpd_used - 1   # -1 for current request
        }

    async def check_and_increment_rate_limit(self, api_key: str) -> tuple[bool, str, Optional[Dict]]:
        """Validate key and check rate limits in one call. Returns (allowed, error, key_info)."""
        key_info = await self.validate_key(api_key)
        if not key_info:
            return False, "Invalid API key", None
        
        result = await self.check_rate_limit(
            key_info['id'], 
            key_info.get('max_rpm', 60), 
            key_info.get('max_rpd', 1000)
        )
        if not result["allowed"]:
            return False, result["error"], key_info
        
        return True, "", key_info


    # ==================== USAGE LOGGING ====================
    
    async def log_usage(self, key_id: int, model: str, input_tokens: int, output_tokens: int,
                       tokens_used: int = None, success: bool = True, error_message: str = None,
                       client_ip: str = None, is_cache_hit: bool = False, client_app: str = None,
                       discord_id: str = None, sub_key_id: int = None):
        """
        Log API usage with optional Discord and sub-key tracking.
        
        **Validates: Requirements 5.1**
        
        Args:
            key_id: The master API key ID
            model: The model used for the request
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            tokens_used: Total tokens (defaults to input + output)
            success: Whether the request was successful
            error_message: Error message if request failed
            client_ip: Client IP address (used when no Discord ID)
            is_cache_hit: Whether this was a cache hit
            client_app: Client application identifier
            discord_id: Discord user ID (for sub-key usage tracking)
            sub_key_id: Sub-key ID (for sub-key usage tracking)
        """
        if tokens_used is None:
            tokens_used = input_tokens + output_tokens
        
        # Calculate cost
        cost = await self._calculate_cost(model, input_tokens, output_tokens)
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO usage_logs (api_key_id, model, tokens_used, input_tokens, 
                        output_tokens, cost, success, error_message, client_ip, is_cache_hit, client_app, discord_id, sub_key_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ''', key_id, model, tokens_used, input_tokens, output_tokens, cost,
                    success, error_message, client_ip, is_cache_hit, client_app, discord_id, sub_key_id)
                
                # Update total tokens and budget
                await conn.execute('''
                    UPDATE api_keys 
                    SET total_tokens_used = total_tokens_used + $1,
                        budget_used = budget_used + $2
                    WHERE id = $3
                ''', tokens_used, cost, key_id)
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('''
                    INSERT INTO usage_logs (api_key_id, model, tokens_used, input_tokens,
                        output_tokens, cost, success, error_message, client_ip, is_cache_hit, client_app, discord_id, sub_key_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (key_id, model, tokens_used, input_tokens, output_tokens, cost,
                    success, error_message, client_ip, is_cache_hit, client_app, discord_id, sub_key_id))
                
                await db.execute('''
                    UPDATE api_keys 
                    SET total_tokens_used = total_tokens_used + ?,
                        budget_used = budget_used + ?
                    WHERE id = ?
                ''', (tokens_used, cost, key_id))
                await db.commit()
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def _calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost based on model pricing."""
        costs = await self.get_model_costs()
        model_lower = model.lower()
        
        # Try exact match first, then partial match
        for cost_info in costs:
            pattern = cost_info['model_pattern'].lower()
            # Check if pattern matches the model (either exact or partial)
            if pattern == model_lower or pattern in model_lower or model_lower in pattern:
                input_cost = (input_tokens / 1_000_000) * cost_info['input_cost_per_1m']
                output_cost = (output_tokens / 1_000_000) * cost_info['output_cost_per_1m']
                return input_cost + output_cost
        
        # Default cost if no pattern matches (small default to track usage)
        return 0.0

    async def get_key_request_logs(self, key_id: int = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get usage logs, optionally filtered by key."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                if key_id:
                    rows = await conn.fetch(
                        'SELECT * FROM usage_logs WHERE api_key_id = $1 ORDER BY request_time DESC LIMIT $2',
                        key_id, limit
                    )
                else:
                    rows = await conn.fetch(
                        'SELECT * FROM usage_logs ORDER BY request_time DESC LIMIT $1',
                        limit
                    )
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                if key_id:
                    cursor = await db.execute(
                        'SELECT * FROM usage_logs WHERE api_key_id = ? ORDER BY request_time DESC LIMIT ?',
                        (key_id, limit)
                    )
                else:
                    cursor = await db.execute(
                        'SELECT * FROM usage_logs ORDER BY request_time DESC LIMIT ?',
                        (limit,)
                    )
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_analytics(self, key_id: int = None, days: int = 7) -> Dict[str, Any]:
        """Get analytics data."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                if key_id:
                    totals = await conn.fetchrow('''
                        SELECT COUNT(*) as total_requests,
                               SUM(tokens_used) as total_tokens,
                               SUM(input_tokens) as total_input_tokens,
                               SUM(output_tokens) as total_output_tokens,
                               SUM(cost) as total_cost,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs 
                        WHERE api_key_id = $1 
                        AND request_time >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                    ''' % days, key_id)
                else:
                    totals = await conn.fetchrow('''
                        SELECT COUNT(*) as total_requests,
                               SUM(tokens_used) as total_tokens,
                               SUM(input_tokens) as total_input_tokens,
                               SUM(output_tokens) as total_output_tokens,
                               SUM(cost) as total_cost,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs
                        WHERE request_time >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                    ''' % days)
                
                # Get model usage stats with proper field names
                models = await conn.fetch('''
                    SELECT model, 
                           COUNT(*) as request_count, 
                           COALESCE(SUM(tokens_used), 0) as tokens,
                           COALESCE(SUM(input_tokens), 0) as input_tokens,
                           COALESCE(SUM(output_tokens), 0) as output_tokens,
                           COALESCE(SUM(cost), 0) as cost
                    FROM usage_logs 
                    WHERE request_time >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                    GROUP BY model ORDER BY request_count DESC LIMIT 10
                ''' % days)
                
                # Get hourly usage for charts
                hourly = await conn.fetch('''
                    SELECT 
                        TO_CHAR(request_time, 'YYYY-MM-DD HH24') as hour,
                        COUNT(*) as request_count,
                        COALESCE(SUM(input_tokens), 0) as input_tokens,
                        COALESCE(SUM(output_tokens), 0) as output_tokens
                    FROM usage_logs 
                    WHERE request_time >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                    GROUP BY TO_CHAR(request_time, 'YYYY-MM-DD HH24')
                    ORDER BY hour
                ''' % days)
                
                return {
                    "totals": {
                        "total_requests": totals['total_requests'] or 0,
                        "total_tokens": totals['total_tokens'] or 0,
                        "total_input_tokens": totals['total_input_tokens'] or 0,
                        "total_output_tokens": totals['total_output_tokens'] or 0,
                        "total_cost": totals['total_cost'] or 0,
                        "successful_requests": totals['successful_requests'] or 0
                    },
                    "models": [dict(row) for row in models],
                    "hourly_usage": [dict(row) for row in hourly]
                }
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                if key_id:
                    cursor = await db.execute('''
                        SELECT COUNT(*) as total_requests,
                               SUM(tokens_used) as total_tokens,
                               SUM(input_tokens) as total_input_tokens,
                               SUM(output_tokens) as total_output_tokens,
                               SUM(cost) as total_cost,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs 
                        WHERE api_key_id = ?
                        AND request_time >= datetime('now', ?)
                    ''', (key_id, f'-{days} days'))
                else:
                    cursor = await db.execute('''
                        SELECT COUNT(*) as total_requests,
                               SUM(tokens_used) as total_tokens,
                               SUM(input_tokens) as total_input_tokens,
                               SUM(output_tokens) as total_output_tokens,
                               SUM(cost) as total_cost,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs
                        WHERE request_time >= datetime('now', ?)
                    ''', (f'-{days} days',))
                row = await cursor.fetchone()
                
                # Get model usage stats with proper field names
                cursor = await db.execute('''
                    SELECT model, 
                           COUNT(*) as request_count, 
                           COALESCE(SUM(tokens_used), 0) as tokens,
                           COALESCE(SUM(input_tokens), 0) as input_tokens,
                           COALESCE(SUM(output_tokens), 0) as output_tokens,
                           COALESCE(SUM(cost), 0) as cost
                    FROM usage_logs 
                    WHERE request_time >= datetime('now', ?)
                    GROUP BY model ORDER BY request_count DESC LIMIT 10
                ''', (f'-{days} days',))
                models = await cursor.fetchall()
                
                # Get hourly usage for charts
                cursor = await db.execute('''
                    SELECT 
                        strftime('%Y-%m-%d %H', request_time) as hour,
                        COUNT(*) as request_count,
                        COALESCE(SUM(input_tokens), 0) as input_tokens,
                        COALESCE(SUM(output_tokens), 0) as output_tokens
                    FROM usage_logs 
                    WHERE request_time >= datetime('now', ?)
                    GROUP BY strftime('%Y-%m-%d %H', request_time)
                    ORDER BY hour
                ''', (f'-{days} days',))
                hourly = await cursor.fetchall()
                
                return {
                    "totals": {
                        "total_requests": row['total_requests'] or 0,
                        "total_tokens": row['total_tokens'] or 0,
                        "total_input_tokens": row['total_input_tokens'] or 0,
                        "total_output_tokens": row['total_output_tokens'] or 0,
                        "total_cost": row['total_cost'] or 0,
                        "successful_requests": row['successful_requests'] or 0
                    },
                    "models": [dict(m) for m in models],
                    "hourly_usage": [dict(h) for h in hourly]
                }
            finally:
                if not self._is_memory_db:
                    await db.close()


    # ==================== MODEL COSTS ====================
    
    async def get_model_costs(self) -> List[Dict[str, Any]]:
        """Get all model costs."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('SELECT * FROM model_costs ORDER BY model_pattern')
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('SELECT * FROM model_costs ORDER BY model_pattern')
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def set_model_cost(self, model_pattern: str, input_cost: float, output_cost: float):
        """Set or update model cost."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO model_costs (model_pattern, input_cost_per_1m, output_cost_per_1m)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (model_pattern) DO UPDATE SET
                        input_cost_per_1m = $2, output_cost_per_1m = $3
                ''', model_pattern, input_cost, output_cost)
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('''
                    INSERT OR REPLACE INTO model_costs (model_pattern, input_cost_per_1m, output_cost_per_1m)
                    VALUES (?, ?, ?)
                ''', (model_pattern, input_cost, output_cost))
                await db.commit()
            finally:
                if not self._is_memory_db:
                    await db.close()

    # ==================== PROVIDER ROTATION ====================
    
    async def update_provider_rotation(self, key_id: int, new_index: int):
        """Update provider rotation index."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    'UPDATE api_keys SET provider_rotation_index = $1 WHERE id = $2',
                    new_index, key_id
                )
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute(
                    'UPDATE api_keys SET provider_rotation_index = ? WHERE id = ?',
                    (new_index, key_id)
                )
                await db.commit()
            finally:
                if not self._is_memory_db:
                    await db.close()

    # ==================== STATUS & CLEANUP ====================
    
    def get_turso_status(self) -> Dict[str, Any]:
        """Get database connection status (for compatibility)."""
        return {
            "available": self._use_postgres,
            "connected": self._pg_connected if self._use_postgres else True,
            "type": "postgresql" if self._use_postgres else "sqlite",
            "url_configured": bool(config.POSTGRES_URL),
            "retry_count": 0,
            "last_error": None,
            "lib_available": ASYNCPG_AVAILABLE
        }

    async def close(self):
        """Close database connections."""
        logger.info("Closing database connections...")
        
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._pg_connected = False
            logger.info("PostgreSQL pool closed")
        
        if self._shared_conn:
            await self._shared_conn.close()
            self._shared_conn = None
            logger.info("SQLite connection closed")

    # ==================== COMPATIBILITY STUBS ====================
    # These methods exist for backward compatibility with old code
    
    async def init_turso_client(self):
        """Stub for Turso compatibility - PostgreSQL doesn't need this."""
        pass
    
    async def load_from_turso(self):
        """Stub for Turso compatibility - PostgreSQL doesn't need this."""
        pass
    
    async def full_sync_to_turso(self):
        """Stub for Turso compatibility - PostgreSQL doesn't need this."""
        return True
    
    async def start_sync_task(self):
        """Stub for Turso compatibility - PostgreSQL doesn't need this."""
        pass


    # Alias for create_key (backward compatibility)
    async def create_api_key(self, name: str = None, max_rpm: int = 60, max_rpd: int = 1000,
                            target_url: str = None, target_api_key: str = None,
                            no_auth: bool = False, use_proxy: bool = True,
                            model_mappings: str = None, expires_at: str = None,
                            ip_whitelist: str = None, ip_blacklist: str = None,
                            providers: str = None, provider_rotation_frequency: int = 1,
                            disable_model_fetch: bool = False, http_referer: str = None,
                            max_total_tokens: int = None, max_context_tokens: int = None,
                            custom_prefills: str = None, claim_code: str = None) -> Dict[str, Any]:
        return await self.create_key(
            name=name, max_rpm=max_rpm, max_rpd=max_rpd,
            target_url=target_url, target_api_key=target_api_key,
            no_auth=no_auth, use_proxy=use_proxy,
            model_mappings=model_mappings, expires_at=expires_at,
            ip_whitelist=ip_whitelist, ip_blacklist=ip_blacklist,
            providers=providers, provider_rotation_frequency=provider_rotation_frequency,
            disable_model_fetch=disable_model_fetch, http_referer=http_referer,
            max_total_tokens=max_total_tokens, max_context_tokens=max_context_tokens,
            custom_prefills=custom_prefills, claim_code=claim_code
        )

    # Alias for get_key_request_logs (backward compatibility)
    async def get_usage_logs(self, key_id: int = None, limit: int = 100) -> List[Dict[str, Any]]:
        return await self.get_key_request_logs(key_id=key_id, limit=limit)

    # Alias for get_all_keys (backward compatibility)
    async def list_api_keys(self) -> List[Dict[str, Any]]:
        return await self.get_all_keys()

    @asynccontextmanager
    async def get_db(self):
        """Context manager for raw database access (for backward compatibility)."""
        if self._use_postgres:
            conn = await self._pool.acquire()
            try:
                yield conn
            finally:
                await self._pool.release(conn)
        else:
            if self._is_memory_db:
                yield self._shared_conn
            else:
                db = await aiosqlite.connect(self.db_path)
                db.row_factory = aiosqlite.Row
                try:
                    yield db
                finally:
                    await db.close()


    async def ensure_all_rate_limits_exist(self) -> int:
        """Stub for backward compatibility - rate limits are now in api_keys table."""
        return 0

    # Alias for validate_key (backward compatibility)
    async def validate_api_key(self, api_key: str) -> Optional[Dict[str, Any]]:
        """Alias for validate_key for backward compatibility."""
        return await self.validate_key(api_key)

    # Alias for update_provider_rotation (backward compatibility)
    async def update_provider_rotation_index(self, key_id: int, new_index: int):
        """Alias for update_provider_rotation for backward compatibility."""
        return await self.update_provider_rotation(key_id, new_index)

    async def log_large_context(self, key_id: int, model: str, input_tokens: int = 0,
                                output_tokens: int = 0, total_tokens: int = 0,
                                client_ip: str = None):
        """Log large context requests. Uses usage_logs with a marker."""
        # Just log as a regular usage entry - the large context is tracked via tokens
        await self.log_usage(
            key_id=key_id,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            tokens_used=total_tokens,
            success=True,
            client_ip=client_ip,
            error_message=f"Large context: {total_tokens} tokens"
        )

    async def get_cached_response(self, prompt_hash: str, embedding: Any = None) -> Optional[Dict[str, Any]]:
        """Stub for cache lookup - caching not implemented in PostgreSQL version."""
        # Return None to indicate no cache hit
        return None

    async def cache_response(self, prompt_hash: str, response_body: str, model: str = None,
                            prompt_text: str = None, embedding: Any = None):
        """Stub for caching responses - caching not implemented in PostgreSQL version."""
        # No-op - caching disabled for simplicity
        pass

    async def update_budget_used(self, key_id: int, tokens_used: int):
        """Update budget used for a key based on token usage."""
        # This is already handled in log_usage, but provide explicit method
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                await conn.execute('''
                    UPDATE api_keys 
                    SET total_tokens_used = total_tokens_used + $1
                    WHERE id = $2
                ''', tokens_used, key_id)
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('''
                    UPDATE api_keys 
                    SET total_tokens_used = total_tokens_used + ?
                    WHERE id = ?
                ''', (tokens_used, key_id))
                await db.commit()
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_key_usage(self, key_id: int) -> Optional[Dict[str, Any]]:
        """Get usage statistics for a specific key."""
        key = await self.get_api_key_by_id(key_id)
        if not key:
            return None
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                stats = await conn.fetchrow('''
                    SELECT COUNT(*) as total_requests,
                           SUM(tokens_used) as total_tokens,
                           SUM(cost) as total_cost,
                           SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                    FROM usage_logs WHERE api_key_id = $1
                ''', key_id)
                return {
                    "key": key,
                    "current_rpm": key.get('current_rpm', 0),
                    "current_rpd": key.get('current_rpd', 0),
                    "refresh_hour": key.get('refresh_hour'),
                    "max_rpm": key.get('max_rpm', 60),
                    "max_rpd": key.get('max_rpd', 1000),
                    "total_requests": stats['total_requests'] or 0,
                    "total_tokens": stats['total_tokens'] or 0,
                    "total_cost": stats['total_cost'] or 0,
                    "successful_requests": stats['successful_requests'] or 0
                }
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT COUNT(*) as total_requests,
                           SUM(tokens_used) as total_tokens,
                           SUM(cost) as total_cost,
                           SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                    FROM usage_logs WHERE api_key_id = ?
                ''', (key_id,))
                stats = await cursor.fetchone()
                return {
                    "key": key,
                    "current_rpm": key.get('current_rpm', 0),
                    "current_rpd": key.get('current_rpd', 0),
                    "refresh_hour": key.get('refresh_hour'),
                    "max_rpm": key.get('max_rpm', 60),
                    "max_rpd": key.get('max_rpd', 1000),
                    "total_requests": stats['total_requests'] or 0,
                    "total_tokens": stats['total_tokens'] or 0,
                    "total_cost": stats['total_cost'] or 0,
                    "successful_requests": stats['successful_requests'] or 0
                }
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_recent_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent usage logs across all keys."""
        return await self.get_key_request_logs(key_id=None, limit=limit)

    async def get_large_context_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get logs for large context requests (>40k tokens)."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT * FROM usage_logs 
                    WHERE tokens_used > 40000 
                    ORDER BY request_time DESC LIMIT $1
                ''', limit)
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT * FROM usage_logs 
                    WHERE tokens_used > 40000 
                    ORDER BY request_time DESC LIMIT ?
                ''', (limit,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_large_context_stats(self) -> Dict[str, Any]:
        """Get statistics about large context requests."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                stats = await conn.fetchrow('''
                    SELECT COUNT(*) as count,
                           AVG(tokens_used) as avg_tokens,
                           MAX(tokens_used) as max_tokens,
                           SUM(tokens_used) as total_tokens
                    FROM usage_logs WHERE tokens_used > 40000
                ''')
                return {
                    "count": stats['count'] or 0,
                    "avg_tokens": float(stats['avg_tokens'] or 0),
                    "max_tokens": stats['max_tokens'] or 0,
                    "total_tokens": stats['total_tokens'] or 0
                }
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT COUNT(*) as count,
                           AVG(tokens_used) as avg_tokens,
                           MAX(tokens_used) as max_tokens,
                           SUM(tokens_used) as total_tokens
                    FROM usage_logs WHERE tokens_used > 40000
                ''')
                stats = await cursor.fetchone()
                return {
                    "count": stats['count'] or 0,
                    "avg_tokens": float(stats['avg_tokens'] or 0),
                    "max_tokens": stats['max_tokens'] or 0,
                    "total_tokens": stats['total_tokens'] or 0
                }
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def update_model_cost(self, pattern: str, input_cost: float, output_cost: float):
        """Update or insert model cost - alias for set_model_cost."""
        return await self.set_model_cost(pattern, input_cost, output_cost)

    async def delete_model_cost(self, cost_id: int) -> bool:
        """Delete a model cost entry."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                result = await conn.execute('DELETE FROM model_costs WHERE id = $1', cost_id)
                return 'DELETE' in result
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('DELETE FROM model_costs WHERE id = ?', (cost_id,))
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_key_stats(self, key_id: int) -> Dict[str, Any]:
        """Get detailed stats for a specific key."""
        key = await self.get_api_key_by_id(key_id)
        if not key:
            return {}
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                stats = await conn.fetchrow('''
                    SELECT COUNT(*) as total_requests,
                           SUM(tokens_used) as total_tokens,
                           SUM(cost) as total_cost,
                           SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests,
                           MAX(request_time) as last_request
                    FROM usage_logs WHERE api_key_id = $1
                ''', key_id)
                
                # Get today's requests count
                today_stats = await conn.fetchrow('''
                    SELECT COUNT(*) as today_count
                    FROM usage_logs 
                    WHERE api_key_id = $1 AND DATE(request_time) = CURRENT_DATE
                ''', key_id)
                
                # Get top models
                top_models = await conn.fetch('''
                    SELECT model, COUNT(*) as request_count
                    FROM usage_logs WHERE api_key_id = $1
                    GROUP BY model ORDER BY request_count DESC LIMIT 5
                ''', key_id)
                
                current_rpd = key.get('current_rpd', 0)
                max_rpd = key.get('max_rpd', 1000)
                
                return {
                    "total_requests": stats['total_requests'] or 0,
                    "total_tokens": stats['total_tokens'] or 0,
                    "total_cost": float(stats['total_cost'] or 0),
                    "successful_requests": stats['successful_requests'] or 0,
                    "last_request": str(stats['last_request']) if stats['last_request'] else None,
                    "current_rpm": key.get('current_rpm', 0),
                    "current_rpd": current_rpd,
                    "max_rpm": key.get('max_rpm', 60),
                    "max_rpd": max_rpd,
                    "rpd_used": current_rpd,
                    "rpd_remaining": max(0, max_rpd - current_rpd),
                    "requests_today": today_stats['today_count'] or 0,
                    "top_models": [{"model": r['model'], "request_count": r['request_count']} for r in top_models]
                }
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT COUNT(*) as total_requests,
                           SUM(tokens_used) as total_tokens,
                           SUM(cost) as total_cost,
                           SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests,
                           MAX(request_time) as last_request
                    FROM usage_logs WHERE api_key_id = ?
                ''', (key_id,))
                stats = await cursor.fetchone()
                
                # Get today's requests count
                cursor = await db.execute('''
                    SELECT COUNT(*) as today_count
                    FROM usage_logs 
                    WHERE api_key_id = ? AND DATE(request_time) = DATE('now')
                ''', (key_id,))
                today_stats = await cursor.fetchone()
                
                # Get top models
                cursor = await db.execute('''
                    SELECT model, COUNT(*) as request_count
                    FROM usage_logs WHERE api_key_id = ?
                    GROUP BY model ORDER BY request_count DESC LIMIT 5
                ''', (key_id,))
                top_models = await cursor.fetchall()
                
                current_rpd = key.get('current_rpd', 0)
                max_rpd = key.get('max_rpd', 1000)
                
                return {
                    "total_requests": stats['total_requests'] or 0,
                    "total_tokens": stats['total_tokens'] or 0,
                    "total_cost": float(stats['total_cost'] or 0),
                    "successful_requests": stats['successful_requests'] or 0,
                    "last_request": str(stats['last_request']) if stats['last_request'] else None,
                    "current_rpm": key.get('current_rpm', 0),
                    "current_rpd": current_rpd,
                    "max_rpm": key.get('max_rpm', 60),
                    "max_rpd": max_rpd,
                    "rpd_used": current_rpd,
                    "rpd_remaining": max(0, max_rpd - current_rpd),
                    "requests_today": today_stats['today_count'] or 0,
                    "top_models": [{"model": r['model'], "request_count": r['request_count']} for r in top_models]
                }
            finally:
                if not self._is_memory_db:
                    await db.close()

    # ==================== IMPORT/EXPORT ====================

    async def export_data(self) -> Dict[str, Any]:
        """Export all database data for backup."""
        keys = await self.get_all_keys()
        logs = await self.get_key_request_logs(limit=10000)
        costs = await self.get_model_costs()
        
        return {
            "version": "2.0",
            "exported_at": datetime.now().isoformat(),
            "api_keys": keys,
            "usage_logs": logs,
            "model_costs": costs
        }

    async def import_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Import data from a backup JSON file."""
        imported = {"keys": 0, "logs": 0, "costs": 0, "skipped": 0, "errors": []}
        
        try:
            # Import API keys
            api_keys = data.get("api_keys", [])
            for key_data in api_keys:
                try:
                    # Skip if key already exists (by key_hash)
                    key_hash = key_data.get("key_hash")
                    if not key_hash:
                        imported["errors"].append("Key missing key_hash, skipped")
                        continue
                        
                    existing = None
                    if self._use_postgres:
                        async with self._pool.acquire() as conn:
                            existing = await conn.fetchrow(
                                'SELECT id FROM api_keys WHERE key_hash = $1', key_hash
                            )
                    else:
                        db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
                        try:
                            cursor = await db.execute(
                                'SELECT id FROM api_keys WHERE key_hash = ?', (key_hash,)
                            )
                            existing = await cursor.fetchone()
                        finally:
                            if not self._is_memory_db:
                                await db.close()
                    
                    if existing:
                        imported["skipped"] += 1
                        continue  # Skip existing keys
                    
                    # Insert the key
                    if self._use_postgres:
                        async with self._pool.acquire() as conn:
                            await conn.execute('''
                                INSERT INTO api_keys (key_hash, key_prefix, name, max_rpm, max_rpd,
                                    current_rpm, current_rpd, enabled, target_url, target_api_key,
                                    use_proxy, no_auth, model_mappings, expires_at, ip_whitelist,
                                    ip_blacklist, providers, provider_rotation_index,
                                    provider_rotation_frequency, disable_model_fetch, http_referer,
                                    max_total_tokens, total_tokens_used, max_context_tokens,
                                    custom_prefills, budget_limit, budget_used)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                                    $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)
                            ''', 
                                key_data.get('key_hash', ''),
                                key_data.get('key_prefix', ''),
                                key_data.get('name'),
                                key_data.get('max_rpm', 60),
                                key_data.get('max_rpd', 1000),
                                key_data.get('current_rpm', 0),
                                key_data.get('current_rpd', 0),
                                key_data.get('enabled', True),
                                key_data.get('target_url'),
                                key_data.get('target_api_key'),
                                key_data.get('use_proxy', True),
                                key_data.get('no_auth', False),
                                key_data.get('model_mappings'),
                                key_data.get('expires_at'),
                                key_data.get('ip_whitelist'),
                                key_data.get('ip_blacklist'),
                                key_data.get('providers'),
                                key_data.get('provider_rotation_index', 0),
                                key_data.get('provider_rotation_frequency', 1),
                                key_data.get('disable_model_fetch', False),
                                key_data.get('http_referer'),
                                key_data.get('max_total_tokens'),
                                key_data.get('total_tokens_used', 0),
                                key_data.get('max_context_tokens'),
                                key_data.get('custom_prefills'),
                                key_data.get('budget_limit'),
                                key_data.get('budget_used', 0)
                            )
                    else:
                        db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
                        try:
                            await db.execute('''
                                INSERT INTO api_keys (key_hash, key_prefix, name, max_rpm, max_rpd,
                                    current_rpm, current_rpd, enabled, target_url, target_api_key,
                                    use_proxy, no_auth, model_mappings, expires_at, ip_whitelist,
                                    ip_blacklist, providers, provider_rotation_index,
                                    provider_rotation_frequency, disable_model_fetch, http_referer,
                                    max_total_tokens, total_tokens_used, max_context_tokens,
                                    custom_prefills, budget_limit, budget_used)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                key_data.get('key_hash', ''),
                                key_data.get('key_prefix', ''),
                                key_data.get('name'),
                                key_data.get('max_rpm', 60),
                                key_data.get('max_rpd', 1000),
                                key_data.get('current_rpm', 0),
                                key_data.get('current_rpd', 0),
                                key_data.get('enabled', True),
                                key_data.get('target_url'),
                                key_data.get('target_api_key'),
                                key_data.get('use_proxy', True),
                                key_data.get('no_auth', False),
                                key_data.get('model_mappings'),
                                key_data.get('expires_at'),
                                key_data.get('ip_whitelist'),
                                key_data.get('ip_blacklist'),
                                key_data.get('providers'),
                                key_data.get('provider_rotation_index', 0),
                                key_data.get('provider_rotation_frequency', 1),
                                key_data.get('disable_model_fetch', False),
                                key_data.get('http_referer'),
                                key_data.get('max_total_tokens'),
                                key_data.get('total_tokens_used', 0),
                                key_data.get('max_context_tokens'),
                                key_data.get('custom_prefills'),
                                key_data.get('budget_limit'),
                                key_data.get('budget_used', 0)
                            ))
                            await db.commit()
                        finally:
                            if not self._is_memory_db:
                                await db.close()
                    
                    imported["keys"] += 1
                except Exception as e:
                    imported["errors"].append(f"Key import error: {str(e)}")
            
            # Import model costs
            model_costs = data.get("model_costs", [])
            for cost_data in model_costs:
                try:
                    pattern = cost_data.get("model_pattern")
                    if pattern:
                        await self.set_model_cost(
                            pattern,
                            cost_data.get("input_cost_per_1m", 0),
                            cost_data.get("output_cost_per_1m", 0)
                        )
                        imported["costs"] += 1
                except Exception as e:
                    imported["errors"].append(f"Cost import error: {str(e)}")
            
            # Import usage logs
            usage_logs = data.get("usage_logs", [])
            for log_data in usage_logs:
                try:
                    # Need to map old key_id to new key_id if keys were imported
                    # For now, we'll try to insert with the original api_key_id
                    # This works if the key IDs match (same database) or if we're restoring
                    api_key_id = log_data.get("api_key_id")
                    if not api_key_id:
                        continue
                    
                    # Check if the key exists
                    key_exists = False
                    if self._use_postgres:
                        async with self._pool.acquire() as conn:
                            row = await conn.fetchrow(
                                'SELECT id FROM api_keys WHERE id = $1', api_key_id
                            )
                            key_exists = row is not None
                    else:
                        db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
                        try:
                            cursor = await db.execute(
                                'SELECT id FROM api_keys WHERE id = ?', (api_key_id,)
                            )
                            row = await cursor.fetchone()
                            key_exists = row is not None
                        finally:
                            if not self._is_memory_db:
                                await db.close()
                    
                    if not key_exists:
                        continue  # Skip logs for non-existent keys
                    
                    # Insert the log
                    if self._use_postgres:
                        async with self._pool.acquire() as conn:
                            await conn.execute('''
                                INSERT INTO usage_logs (api_key_id, model, tokens_used, input_tokens,
                                    output_tokens, cost, success, is_cache_hit, error_message, client_ip, request_time)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                            ''',
                                api_key_id,
                                log_data.get('model', 'unknown'),
                                log_data.get('tokens_used', 0),
                                log_data.get('input_tokens', 0),
                                log_data.get('output_tokens', 0),
                                log_data.get('cost', 0),
                                log_data.get('success', True),
                                log_data.get('is_cache_hit', False),
                                log_data.get('error_message'),
                                log_data.get('client_ip'),
                                log_data.get('request_time', datetime.now())
                            )
                    else:
                        db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
                        try:
                            await db.execute('''
                                INSERT INTO usage_logs (api_key_id, model, tokens_used, input_tokens,
                                    output_tokens, cost, success, is_cache_hit, error_message, client_ip, request_time)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                api_key_id,
                                log_data.get('model', 'unknown'),
                                log_data.get('tokens_used', 0),
                                log_data.get('input_tokens', 0),
                                log_data.get('output_tokens', 0),
                                log_data.get('cost', 0),
                                log_data.get('success', True),
                                log_data.get('is_cache_hit', False),
                                log_data.get('error_message'),
                                log_data.get('client_ip'),
                                log_data.get('request_time', datetime.now().isoformat())
                            ))
                            await db.commit()
                        finally:
                            if not self._is_memory_db:
                                await db.close()
                    
                    imported["logs"] += 1
                except Exception as e:
                    imported["errors"].append(f"Log import error: {str(e)}")
            
            return imported
            
        except Exception as e:
            imported["errors"].append(f"Import error: {str(e)}")
            return imported

    async def check_scheduled_refreshes(self) -> List[str]:
        """Check for API keys that need scheduled rate limit refresh based on refresh_hour.
        
        Returns list of key prefixes that were refreshed.
        """
        from datetime import datetime
        
        current_hour = datetime.now().hour
        refreshed_keys = []
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                # Find keys with refresh_hour matching current hour
                rows = await conn.fetch(
                    'SELECT id, key_prefix, refresh_hour FROM api_keys WHERE refresh_hour = $1 AND enabled = TRUE',
                    current_hour
                )
                
                for row in rows:
                    # Reset rate limits for this key
                    await conn.execute('''
                        UPDATE api_keys 
                        SET current_rpm = 0, current_rpd = 0, last_reset = CURRENT_TIMESTAMP
                        WHERE id = $1
                    ''', row['id'])
                    refreshed_keys.append(row['key_prefix'])
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    'SELECT id, key_prefix, refresh_hour FROM api_keys WHERE refresh_hour = ? AND enabled = 1',
                    (current_hour,)
                )
                rows = await cursor.fetchall()
                
                for row in rows:
                    await db.execute('''
                        UPDATE api_keys 
                        SET current_rpm = 0, current_rpd = 0, last_reset = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (row['id'],))
                    refreshed_keys.append(row['key_prefix'])
                
                await db.commit()
            finally:
                if not self._is_memory_db:
                    await db.close()
        
        return refreshed_keys


    # ==================== CLIENT APP TRACKING ====================
    
    def detect_client_app(self, user_agent: str, referer: str = None, origin: str = None) -> str:
        """
        Detect the client application from User-Agent, Referer, and Origin headers.
        
        Priority:
        1. Check Referer/Origin for web-based clients (janitorai.com, etc.)
        2. Check User-Agent for desktop/CLI clients
        3. Fall back to truncated user agent
        
        Args:
            user_agent: User-Agent header value
            referer: Referer header value (optional)
            origin: Origin header value (optional)
        
        Returns:
            Detected client application name
        """
        # First, check Referer and Origin for web-based clients
        # This is more reliable for browser-based apps like JanitorAI
        referer_lower = (referer or "").lower()
        origin_lower = (origin or "").lower()
        
        # Web-based client patterns (check Referer/Origin first)
        web_client_patterns = {
            "JanitorAI": ["janitorai.com", "janitor.ai"],
            "Zeabur": ["zeabur.app", "zeabur.com"],
            "Tavo": ["tavo.ai", "tavo.app"],
            "Agnai": ["agnai.chat", "agnaistic.com"],
            "Risu AI": ["risuai.xyz", "risuai.net"],
            "Venus AI": ["venusai.chat", "venus.chub.ai"],
            "Chub AI": ["chub.ai"],
            "CAI Tools": ["caitools.app"],
            "Mancer": ["mancer.tech"],
            "NovelAI": ["novelai.net"],
            "Localhost": ["localhost", "127.0.0.1"],
        }
        
        for client_name, patterns in web_client_patterns.items():
            for pattern in patterns:
                if pattern in referer_lower or pattern in origin_lower:
                    # Special case: localhost likely means SillyTavern or similar local app
                    if client_name == "Localhost":
                        return "SillyTavern (Local)"
                    return client_name
        
        # If no web client match, check User-Agent
        if not user_agent:
            return "Unknown"
        
        ua_lower = user_agent.lower()
        
        # Known client patterns from User-Agent
        client_patterns = {
            "SillyTavern": ["sillytavern", "silly-tavern", "silly tavern"],
            "Janitor AI": ["janitorai", "janitor-ai", "janitor ai"],
            "Tavern AI": ["tavernai", "tavern-ai", "tavern ai", "tavo"],
            "Agnai": ["agnai", "agn-ai"],
            "Risu AI": ["risuai", "risu-ai", "risu ai"],
            "Kobold AI": ["koboldai", "kobold-ai", "kobold ai", "koboldcpp"],
            "Oobabooga": ["oobabooga", "text-generation-webui"],
            "LM Studio": ["lm-studio", "lmstudio"],
            "OpenAI Python": ["openai-python", "openai/python"],
            "OpenAI Node": ["openai-node", "openai/node"],
            "Langchain": ["langchain"],
            "ChatGPT": ["chatgpt"],
            "Cursor": ["cursor"],
            "Continue": ["continue"],
            "Cody": ["cody", "sourcegraph"],
            "Copilot": ["copilot", "github-copilot"],
            "Postman": ["postman"],
            "Insomnia": ["insomnia"],
            "cURL": ["curl/"],
            "Python Requests": ["python-requests", "python-urllib"],
            "Axios": ["axios"],
            "Node Fetch": ["node-fetch"],
            "httpx": ["python-httpx", "httpx/"],
            "aiohttp": ["aiohttp"],
        }
        
        for client_name, patterns in client_patterns.items():
            for pattern in patterns:
                if pattern in ua_lower:
                    return client_name
        
        # Check for generic browser - but only if we have a referer/origin that's not our own domain
        # This helps identify "unknown web client" vs "unknown CLI client"
        browser_patterns = ["mozilla", "chrome", "safari", "firefox", "edge", "opera"]
        is_browser = any(pattern in ua_lower for pattern in browser_patterns)
        
        if is_browser:
            # If it's a browser but we couldn't identify the web app, return "Web Client"
            # This is better than just "Browser" as it indicates it's a web-based request
            if referer or origin:
                # Try to extract domain from referer/origin for more context
                domain = None
                for url in [referer, origin]:
                    if url:
                        # Extract domain from URL
                        import re
                        match = re.search(r'https?://([^/]+)', url)
                        if match:
                            domain = match.group(1)
                            break
                if domain:
                    return f"Web ({domain})"
            return "Web Client"
        
        # If no match, return truncated user agent
        return user_agent[:50] if len(user_agent) > 50 else user_agent
    
    async def get_client_stats(self, key_id: int = None, days: int = 30) -> List[Dict[str, Any]]:
        """Get request statistics grouped by client app."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                if key_id:
                    rows = await conn.fetch('''
                        SELECT client_app, 
                               COUNT(*) as request_count,
                               SUM(tokens_used) as total_tokens,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs 
                        WHERE api_key_id = $1 
                          AND request_time > NOW() - INTERVAL '%s days'
                          AND client_app IS NOT NULL
                        GROUP BY client_app
                        ORDER BY request_count DESC
                    ''' % days, key_id)
                else:
                    rows = await conn.fetch('''
                        SELECT client_app, 
                               COUNT(*) as request_count,
                               SUM(tokens_used) as total_tokens,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs 
                        WHERE request_time > NOW() - INTERVAL '%s days'
                          AND client_app IS NOT NULL
                        GROUP BY client_app
                        ORDER BY request_count DESC
                    ''' % days)
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                if key_id:
                    cursor = await db.execute('''
                        SELECT client_app, 
                               COUNT(*) as request_count,
                               SUM(tokens_used) as total_tokens,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs 
                        WHERE api_key_id = ?
                          AND request_time > datetime('now', '-' || ? || ' days')
                          AND client_app IS NOT NULL
                        GROUP BY client_app
                        ORDER BY request_count DESC
                    ''', (key_id, days))
                else:
                    cursor = await db.execute('''
                        SELECT client_app, 
                               COUNT(*) as request_count,
                               SUM(tokens_used) as total_tokens,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs 
                        WHERE request_time > datetime('now', '-' || ? || ' days')
                          AND client_app IS NOT NULL
                        GROUP BY client_app
                        ORDER BY request_count DESC
                    ''', (days,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    # ==================== CLIENT BLACKLIST ====================
    
    async def add_to_blacklist(self, client_app: str, reason: str = None) -> bool:
        """Add a client app to the blacklist."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                try:
                    await conn.execute(
                        'INSERT INTO client_blacklist (client_app, reason) VALUES ($1, $2) ON CONFLICT (client_app) DO UPDATE SET reason = $2',
                        client_app, reason
                    )
                    return True
                except:
                    return False
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute(
                    'INSERT OR REPLACE INTO client_blacklist (client_app, reason) VALUES (?, ?)',
                    (client_app, reason)
                )
                await db.commit()
                return True
            except:
                return False
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def remove_from_blacklist(self, client_app: str) -> bool:
        """Remove a client app from the blacklist."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    'DELETE FROM client_blacklist WHERE client_app = $1',
                    client_app
                )
                return 'DELETE' in result
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('DELETE FROM client_blacklist WHERE client_app = ?', (client_app,))
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def get_blacklist(self) -> List[Dict[str, Any]]:
        """Get all blacklisted client apps."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('SELECT * FROM client_blacklist ORDER BY created_at DESC')
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('SELECT * FROM client_blacklist ORDER BY created_at DESC')
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def is_client_blacklisted(self, client_app: str) -> bool:
        """Check if a client app is blacklisted."""
        if not client_app:
            return False
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT id FROM client_blacklist WHERE client_app = $1',
                    client_app
                )
                return row is not None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                cursor = await db.execute(
                    'SELECT id FROM client_blacklist WHERE client_app = ?',
                    (client_app,)
                )
                row = await cursor.fetchone()
                return row is not None
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_top_ips_by_tokens(self, limit: int = 10, days: int = 30) -> List[Dict[str, Any]]:
        """Get top IPs by total tokens used, with their top 3 client apps."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                # First get top IPs by tokens
                ip_rows = await conn.fetch('''
                    SELECT client_ip, 
                           SUM(tokens_used) as total_tokens,
                           COUNT(*) as request_count
                    FROM usage_logs 
                    WHERE request_time > NOW() - INTERVAL '%s days'
                      AND client_ip IS NOT NULL
                    GROUP BY client_ip
                    ORDER BY total_tokens DESC
                    LIMIT $1
                ''' % days, limit)
                
                results = []
                for ip_row in ip_rows:
                    ip = ip_row['client_ip']
                    # Get top 3 client apps for this IP
                    app_rows = await conn.fetch('''
                        SELECT client_app, COUNT(*) as app_requests
                        FROM usage_logs 
                        WHERE client_ip = $1
                          AND request_time > NOW() - INTERVAL '%s days'
                          AND client_app IS NOT NULL
                        GROUP BY client_app
                        ORDER BY app_requests DESC
                        LIMIT 3
                    ''' % days, ip)
                    
                    results.append({
                        'client_ip': ip,
                        'total_tokens': ip_row['total_tokens'] or 0,
                        'request_count': ip_row['request_count'] or 0,
                        'top_apps': [{'app': r['client_app'], 'requests': r['app_requests']} for r in app_rows]
                    })
                return results
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                # First get top IPs by tokens
                cursor = await db.execute('''
                    SELECT client_ip, 
                           SUM(tokens_used) as total_tokens,
                           COUNT(*) as request_count
                    FROM usage_logs 
                    WHERE request_time > datetime('now', '-' || ? || ' days')
                      AND client_ip IS NOT NULL
                    GROUP BY client_ip
                    ORDER BY total_tokens DESC
                    LIMIT ?
                ''', (days, limit))
                ip_rows = await cursor.fetchall()
                
                results = []
                for ip_row in ip_rows:
                    ip = ip_row['client_ip']
                    # Get top 3 client apps for this IP
                    cursor = await db.execute('''
                        SELECT client_app, COUNT(*) as app_requests
                        FROM usage_logs 
                        WHERE client_ip = ?
                          AND request_time > datetime('now', '-' || ? || ' days')
                          AND client_app IS NOT NULL
                        GROUP BY client_app
                        ORDER BY app_requests DESC
                        LIMIT 3
                    ''', (ip, days))
                    app_rows = await cursor.fetchall()
                    
                    results.append({
                        'client_ip': ip,
                        'total_tokens': ip_row['total_tokens'] or 0,
                        'request_count': ip_row['request_count'] or 0,
                        'top_apps': [{'app': r['client_app'], 'requests': r['app_requests']} for r in app_rows]
                    })
                return results
            finally:
                if not self._is_memory_db:
                    await db.close()


    # ==================== DISCORD USER OPERATIONS ====================
    
    async def create_or_update_discord_user(self, discord_id: str, username: str, 
                                            avatar: str = None, global_name: str = None) -> Dict[str, Any]:
        """Create or update a Discord user record."""
        now = datetime.now()
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO discord_users (id, username, avatar, global_name, last_login)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (id) DO UPDATE SET
                        username = $2, avatar = $3, global_name = $4, last_login = $5
                ''', discord_id, username, avatar, global_name, now)
                
                row = await conn.fetchrow('SELECT * FROM discord_users WHERE id = $1', discord_id)
                return dict(row) if row else None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                await db.execute('''
                    INSERT OR REPLACE INTO discord_users (id, username, avatar, global_name, last_login, created_at)
                    VALUES (?, ?, ?, ?, ?, COALESCE(
                        (SELECT created_at FROM discord_users WHERE id = ?),
                        CURRENT_TIMESTAMP
                    ))
                ''', (discord_id, username, avatar, global_name, now.isoformat(), discord_id))
                await db.commit()
                
                cursor = await db.execute('SELECT * FROM discord_users WHERE id = ?', (discord_id,))
                row = await cursor.fetchone()
                return dict(row) if row else None
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def get_discord_user(self, discord_id: str) -> Optional[Dict[str, Any]]:
        """Get a Discord user by ID."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow('SELECT * FROM discord_users WHERE id = $1', discord_id)
                return dict(row) if row else None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('SELECT * FROM discord_users WHERE id = ?', (discord_id,))
                row = await cursor.fetchone()
                return dict(row) if row else None
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def get_all_discord_users(self) -> List[Dict[str, Any]]:
        """Get all Discord users."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('SELECT * FROM discord_users ORDER BY last_login DESC')
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('SELECT * FROM discord_users ORDER BY last_login DESC')
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()

    # Alias for get_all_discord_users
    async def get_discord_users(self) -> List[Dict[str, Any]]:
        """Alias for get_all_discord_users."""
        return await self.get_all_discord_users()

    async def delete_discord_user(self, discord_id: str) -> bool:
        """Delete a Discord user and all related data (sessions, bans, key variations)."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                try:
                    # Delete in order to respect foreign key constraints
                    await conn.execute('DELETE FROM discord_sessions WHERE discord_id = $1', discord_id)
                    await conn.execute('DELETE FROM banned_discord_users WHERE discord_id = $1', discord_id)
                    await conn.execute('DELETE FROM key_variations WHERE discord_id = $1', discord_id)
                    await conn.execute('DELETE FROM discord_users WHERE id = $1', discord_id)
                    return True
                except Exception as e:
                    logger.error(f"Error deleting Discord user {discord_id}: {e}")
                    return False
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                # Delete in order to respect foreign key constraints
                await db.execute('DELETE FROM discord_sessions WHERE discord_id = ?', (discord_id,))
                await db.execute('DELETE FROM banned_discord_users WHERE discord_id = ?', (discord_id,))
                await db.execute('DELETE FROM key_variations WHERE discord_id = ?', (discord_id,))
                await db.execute('DELETE FROM discord_users WHERE id = ?', (discord_id,))
                await db.commit()
                return True
            except Exception as e:
                logger.error(f"Error deleting Discord user {discord_id}: {e}")
                return False
            finally:
                if not self._is_memory_db:
                    await db.close()

    # ==================== DISCORD SESSION OPERATIONS ====================
    
    async def create_discord_session(self, discord_id: str, session_token: str, 
                                     expires_at: datetime) -> Dict[str, Any]:
        """Create a new Discord session."""
        now = datetime.now()
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO discord_sessions (session_token, discord_id, created_at, expires_at)
                    VALUES ($1, $2, $3, $4)
                ''', session_token, discord_id, now, expires_at)
                return {
                    "session_token": session_token,
                    "discord_id": discord_id,
                    "created_at": now,
                    "expires_at": expires_at
                }
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('''
                    INSERT INTO discord_sessions (session_token, discord_id, created_at, expires_at)
                    VALUES (?, ?, ?, ?)
                ''', (session_token, discord_id, now.isoformat(), expires_at.isoformat()))
                await db.commit()
                return {
                    "session_token": session_token,
                    "discord_id": discord_id,
                    "created_at": now,
                    "expires_at": expires_at
                }
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def validate_discord_session(self, session_token: str) -> Optional[Dict[str, Any]]:
        """Validate a Discord session and return user info if valid."""
        now = datetime.now()
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow('''
                    SELECT ds.*, du.username, du.avatar, du.global_name
                    FROM discord_sessions ds
                    JOIN discord_users du ON ds.discord_id = du.id
                    WHERE ds.session_token = $1 AND ds.expires_at > $2
                ''', session_token, now)
                return dict(row) if row else None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT ds.*, du.username, du.avatar, du.global_name
                    FROM discord_sessions ds
                    JOIN discord_users du ON ds.discord_id = du.id
                    WHERE ds.session_token = ? AND ds.expires_at > ?
                ''', (session_token, now.isoformat()))
                row = await cursor.fetchone()
                return dict(row) if row else None
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def destroy_discord_session(self, session_token: str) -> bool:
        """Destroy a Discord session."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    'DELETE FROM discord_sessions WHERE session_token = $1',
                    session_token
                )
                return 'DELETE' in result
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('DELETE FROM discord_sessions WHERE session_token = ?', (session_token,))
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def cleanup_expired_sessions(self) -> int:
        """Remove expired Discord sessions. Returns count of removed sessions."""
        now = datetime.now()
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                result = await conn.execute('DELETE FROM discord_sessions WHERE expires_at < $1', now)
                # Extract count from result string like "DELETE 5"
                try:
                    return int(result.split()[-1])
                except:
                    return 0
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                cursor = await db.execute('SELECT COUNT(*) FROM discord_sessions WHERE expires_at < ?', (now.isoformat(),))
                count_row = await cursor.fetchone()
                count = count_row[0] if count_row else 0
                
                await db.execute('DELETE FROM discord_sessions WHERE expires_at < ?', (now.isoformat(),))
                await db.commit()
                return count
            finally:
                if not self._is_memory_db:
                    await db.close()

    # ==================== DISCORD BAN OPERATIONS ====================
    
    async def ban_discord_user(self, discord_id: str, banned_by: str = None, reason: str = None) -> bool:
        """Ban a Discord user."""
        now = datetime.now()
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                try:
                    await conn.execute('''
                        INSERT INTO banned_discord_users (discord_id, banned_at, banned_by, reason)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (discord_id) DO UPDATE SET
                            banned_at = $2, banned_by = $3, reason = $4
                    ''', discord_id, now, banned_by, reason)
                    
                    # Also destroy any active sessions for this user
                    await conn.execute('DELETE FROM discord_sessions WHERE discord_id = $1', discord_id)
                    return True
                except Exception as e:
                    logger.error(f"Error banning Discord user: {e}")
                    return False
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('''
                    INSERT OR REPLACE INTO banned_discord_users (discord_id, banned_at, banned_by, reason)
                    VALUES (?, ?, ?, ?)
                ''', (discord_id, now.isoformat(), banned_by, reason))
                
                # Also destroy any active sessions for this user
                await db.execute('DELETE FROM discord_sessions WHERE discord_id = ?', (discord_id,))
                await db.commit()
                return True
            except Exception as e:
                logger.error(f"Error banning Discord user: {e}")
                return False
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def unban_discord_user(self, discord_id: str) -> bool:
        """Unban a Discord user."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    'DELETE FROM banned_discord_users WHERE discord_id = $1',
                    discord_id
                )
                return 'DELETE' in result
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('DELETE FROM banned_discord_users WHERE discord_id = ?', (discord_id,))
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def is_discord_user_banned(self, discord_id: str) -> bool:
        """Check if a Discord user is banned."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT discord_id FROM banned_discord_users WHERE discord_id = $1',
                    discord_id
                )
                return row is not None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                cursor = await db.execute(
                    'SELECT discord_id FROM banned_discord_users WHERE discord_id = ?',
                    (discord_id,)
                )
                row = await cursor.fetchone()
                return row is not None
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def get_banned_discord_users(self) -> List[Dict[str, Any]]:
        """Get all banned Discord users."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT b.*, u.username, u.avatar, u.global_name
                    FROM banned_discord_users b
                    LEFT JOIN discord_users u ON b.discord_id = u.id
                    ORDER BY b.banned_at DESC
                ''')
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT b.*, u.username, u.avatar, u.global_name
                    FROM banned_discord_users b
                    LEFT JOIN discord_users u ON b.discord_id = u.id
                    ORDER BY b.banned_at DESC
                ''')
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()

    # ==================== KEY VARIATION OPERATIONS ====================
    
    async def store_key_variation(self, variation_hash: str, original_key_id: int, 
                                  discord_id: str) -> Dict[str, Any]:
        """Store a key variation mapping."""
        now = datetime.now()
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO key_variations (variation_hash, original_key_id, discord_id, created_at)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (variation_hash) DO UPDATE SET
                        original_key_id = $2, discord_id = $3
                ''', variation_hash, original_key_id, discord_id, now)
                return {
                    "variation_hash": variation_hash,
                    "original_key_id": original_key_id,
                    "discord_id": discord_id,
                    "created_at": now
                }
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('''
                    INSERT OR REPLACE INTO key_variations (variation_hash, original_key_id, discord_id, created_at)
                    VALUES (?, ?, ?, ?)
                ''', (variation_hash, original_key_id, discord_id, now.isoformat()))
                await db.commit()
                return {
                    "variation_hash": variation_hash,
                    "original_key_id": original_key_id,
                    "discord_id": discord_id,
                    "created_at": now
                }
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def get_key_variation(self, variation_hash: str) -> Optional[Dict[str, Any]]:
        """Get key variation info by variation hash."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow('''
                    SELECT kv.*, ak.key_hash, ak.key_prefix, ak.name as key_name
                    FROM key_variations kv
                    JOIN api_keys ak ON kv.original_key_id = ak.id
                    WHERE kv.variation_hash = $1
                ''', variation_hash)
                return dict(row) if row else None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT kv.*, ak.key_hash, ak.key_prefix, ak.name as key_name
                    FROM key_variations kv
                    JOIN api_keys ak ON kv.original_key_id = ak.id
                    WHERE kv.variation_hash = ?
                ''', (variation_hash,))
                row = await cursor.fetchone()
                return dict(row) if row else None
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def get_user_key_variations(self, discord_id: str) -> List[Dict[str, Any]]:
        """Get all key variations for a Discord user."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT kv.*, ak.key_prefix, ak.name as key_name
                    FROM key_variations kv
                    JOIN api_keys ak ON kv.original_key_id = ak.id
                    WHERE kv.discord_id = $1
                    ORDER BY kv.created_at DESC
                ''', discord_id)
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT kv.*, ak.key_prefix, ak.name as key_name
                    FROM key_variations kv
                    JOIN api_keys ak ON kv.original_key_id = ak.id
                    WHERE kv.discord_id = ?
                    ORDER BY kv.created_at DESC
                ''', (discord_id,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()

    # ==================== DISCORD ANALYTICS ====================
    
    async def get_discord_user_stats(self, discord_id: str) -> Dict[str, Any]:
        """Get usage statistics for a Discord user."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                stats = await conn.fetchrow('''
                    SELECT COUNT(*) as total_requests,
                           SUM(tokens_used) as total_tokens,
                           SUM(cost) as total_cost,
                           SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests,
                           MAX(request_time) as last_request
                    FROM usage_logs WHERE discord_id = $1
                ''', discord_id)
                
                return {
                    "total_requests": stats['total_requests'] or 0,
                    "total_tokens": stats['total_tokens'] or 0,
                    "total_cost": float(stats['total_cost'] or 0),
                    "successful_requests": stats['successful_requests'] or 0,
                    "last_request": str(stats['last_request']) if stats['last_request'] else None
                }
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT COUNT(*) as total_requests,
                           SUM(tokens_used) as total_tokens,
                           SUM(cost) as total_cost,
                           SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests,
                           MAX(request_time) as last_request
                    FROM usage_logs WHERE discord_id = ?
                ''', (discord_id,))
                stats = await cursor.fetchone()
                
                return {
                    "total_requests": stats['total_requests'] or 0,
                    "total_tokens": stats['total_tokens'] or 0,
                    "total_cost": float(stats['total_cost'] or 0),
                    "successful_requests": stats['successful_requests'] or 0,
                    "last_request": str(stats['last_request']) if stats['last_request'] else None
                }
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def get_top_discord_users_by_tokens(self, limit: int = 10, days: int = 30) -> List[Dict[str, Any]]:
        """Get top Discord users by total tokens used."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT ul.discord_id, 
                           du.username, du.avatar, du.global_name,
                           SUM(ul.tokens_used) as total_tokens,
                           COUNT(*) as request_count
                    FROM usage_logs ul
                    JOIN discord_users du ON ul.discord_id = du.id
                    WHERE ul.request_time > NOW() - INTERVAL '%s days'
                      AND ul.discord_id IS NOT NULL
                    GROUP BY ul.discord_id, du.username, du.avatar, du.global_name
                    ORDER BY total_tokens DESC
                    LIMIT $1
                ''' % days, limit)
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT ul.discord_id, 
                           du.username, du.avatar, du.global_name,
                           SUM(ul.tokens_used) as total_tokens,
                           COUNT(*) as request_count
                    FROM usage_logs ul
                    JOIN discord_users du ON ul.discord_id = du.id
                    WHERE ul.request_time > datetime('now', '-' || ? || ' days')
                      AND ul.discord_id IS NOT NULL
                    GROUP BY ul.discord_id, du.username, du.avatar, du.global_name
                    ORDER BY total_tokens DESC
                    LIMIT ?
                ''', (days, limit))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()


    async def get_all_discord_users_with_stats(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get all Discord users with their usage statistics."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT du.id as discord_id, 
                           du.username, du.avatar, du.global_name,
                           du.created_at, du.last_login,
                           COALESCE(SUM(ul.tokens_used), 0) as total_tokens,
                           COALESCE(COUNT(ul.id), 0) as request_count,
                           COALESCE(SUM(ul.cost), 0) as total_cost,
                           MAX(ul.request_time) as last_request,
                           CASE WHEN b.discord_id IS NOT NULL THEN TRUE ELSE FALSE END as is_banned
                    FROM discord_users du
                    LEFT JOIN usage_logs ul ON du.id = ul.discord_id 
                        AND ul.request_time > NOW() - INTERVAL '%s days'
                    LEFT JOIN banned_discord_users b ON du.id = b.discord_id
                    GROUP BY du.id, du.username, du.avatar, du.global_name, 
                             du.created_at, du.last_login, b.discord_id
                    ORDER BY total_tokens DESC
                ''' % days)
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT du.id as discord_id, 
                           du.username, du.avatar, du.global_name,
                           du.created_at, du.last_login,
                           COALESCE(SUM(ul.tokens_used), 0) as total_tokens,
                           COALESCE(COUNT(ul.id), 0) as request_count,
                           COALESCE(SUM(ul.cost), 0) as total_cost,
                           MAX(ul.request_time) as last_request,
                           CASE WHEN b.discord_id IS NOT NULL THEN 1 ELSE 0 END as is_banned
                    FROM discord_users du
                    LEFT JOIN usage_logs ul ON du.id = ul.discord_id 
                        AND ul.request_time > datetime('now', '-' || ? || ' days')
                    LEFT JOIN banned_discord_users b ON du.id = b.discord_id
                    GROUP BY du.id, du.username, du.avatar, du.global_name, 
                             du.created_at, du.last_login, b.discord_id
                    ORDER BY total_tokens DESC
                ''', (days,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()


    async def get_sub_keys_for_main_key(self, main_key_id: int) -> List[Dict[str, Any]]:
        """
        Get all sub-keys for a main key with usage statistics.
        
        Returns sub-keys with Discord username, sub_key_prefix, usage count, and last used time.
        Queries the sub_keys table (from code-based key claiming system).
        
        **Validates: Requirements 9.3, 9.4**
        """
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT 
                        sk.id as sub_key_id,
                        sk.sub_key_prefix,
                        sk.discord_id,
                        sk.created_at,
                        sk.last_used_at,
                        sk.enabled,
                        du.username as discord_username,
                        du.avatar as discord_avatar,
                        du.global_name as discord_global_name,
                        COALESCE(COUNT(ul.id), 0) as usage_count,
                        COALESCE(MAX(ul.request_time), sk.last_used_at) as last_used
                    FROM sub_keys sk
                    JOIN discord_users du ON sk.discord_id = du.id
                    LEFT JOIN usage_logs ul ON ul.sub_key_id = sk.id
                    WHERE sk.master_key_id = $1
                    GROUP BY sk.id, sk.sub_key_prefix, sk.discord_id, sk.created_at,
                             sk.last_used_at, sk.enabled,
                             du.username, du.avatar, du.global_name
                    ORDER BY usage_count DESC, sk.created_at DESC
                ''', main_key_id)
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT 
                        sk.id as sub_key_id,
                        sk.sub_key_prefix,
                        sk.discord_id,
                        sk.created_at,
                        sk.last_used_at,
                        sk.enabled,
                        du.username as discord_username,
                        du.avatar as discord_avatar,
                        du.global_name as discord_global_name,
                        COALESCE(COUNT(ul.id), 0) as usage_count,
                        COALESCE(MAX(ul.request_time), sk.last_used_at) as last_used
                    FROM sub_keys sk
                    JOIN discord_users du ON sk.discord_id = du.id
                    LEFT JOIN usage_logs ul ON ul.sub_key_id = sk.id
                    WHERE sk.master_key_id = ?
                    GROUP BY sk.id, sk.sub_key_prefix, sk.discord_id, sk.created_at,
                             sk.last_used_at, sk.enabled,
                             du.username, du.avatar, du.global_name
                    ORDER BY usage_count DESC, sk.created_at DESC
                ''', (main_key_id,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_sub_key_count_for_main_key(self, main_key_id: int) -> int:
        """
        Get the count of sub-keys for a main key.
        Queries the sub_keys table (from code-based key claiming system).
        
        **Validates: Requirements 9.2**
        """
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                result = await conn.fetchval('''
                    SELECT COUNT(*) FROM sub_keys WHERE master_key_id = $1
                ''', main_key_id)
                return result or 0
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                cursor = await db.execute('''
                    SELECT COUNT(*) FROM sub_keys WHERE master_key_id = ?
                ''', (main_key_id,))
                row = await cursor.fetchone()
                return row[0] if row else 0
            finally:
                if not self._is_memory_db:
                    await db.close()

    # ==================== SUB-KEY OPERATIONS (Code-Based Key Claiming) ====================
    
    async def check_sub_key_exists(self, sub_key_hash: str) -> bool:
        """
        Check if a sub-key hash already exists in the database.
        
        **Validates: Requirements 2.3**
        
        Args:
            sub_key_hash: SHA-256 hash of the sub-key
            
        Returns:
            bool: True if the sub-key hash exists, False otherwise
        """
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                result = await conn.fetchval(
                    'SELECT 1 FROM sub_keys WHERE sub_key_hash = $1',
                    sub_key_hash
                )
                return result is not None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                cursor = await db.execute(
                    'SELECT 1 FROM sub_keys WHERE sub_key_hash = ?',
                    (sub_key_hash,)
                )
                row = await cursor.fetchone()
                return row is not None
            finally:
                if not self._is_memory_db:
                    await db.close()
    
    async def generate_unique_sub_key(self, max_attempts: int = 100) -> Tuple[str, str]:
        """
        Generate a unique sub-key that doesn't exist in the database.
        
        This function generates sub-keys in the format Moi-sub-XXXXXYYYYY and
        verifies uniqueness against the database before returning.
        
        **Validates: Requirements 1.2, 2.1, 2.2, 2.3, 4.1**
        
        Args:
            max_attempts: Maximum number of generation attempts before raising an error
            
        Returns:
            Tuple[str, str]: A tuple of (sub_key, sub_key_hash) where:
                - sub_key: The generated sub-key string
                - sub_key_hash: SHA-256 hash of the sub-key
                
        Raises:
            RuntimeError: If unable to generate a unique sub-key after max_attempts
        """
        for _ in range(max_attempts):
            sub_key = generate_sub_key()
            sub_key_hash = hashlib.sha256(sub_key.encode()).hexdigest()
            
            # Check if this hash already exists
            if not await self.check_sub_key_exists(sub_key_hash):
                return sub_key, sub_key_hash
        
        raise RuntimeError(f"Failed to generate unique sub-key after {max_attempts} attempts")


    async def get_master_key_by_claim_code(self, claim_code: str) -> Optional[Dict[str, Any]]:
        """
        Look up a master key by its claim code.
        
        **Validates: Requirements 1.2, 3.6**
        
        Args:
            claim_code: The claim code to look up
            
        Returns:
            Dict with master key info if found, None otherwise
        """
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow('''
                    SELECT id, name, key_prefix, enabled, max_rpm, max_rpd
                    FROM api_keys 
                    WHERE claim_code = $1 AND enabled = TRUE
                ''', claim_code)
                return dict(row) if row else None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT id, name, key_prefix, enabled, max_rpm, max_rpd
                    FROM api_keys 
                    WHERE claim_code = ? AND enabled = 1
                ''', (claim_code,))
                row = await cursor.fetchone()
                return dict(row) if row else None
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_user_sub_key_for_master(self, master_key_id: int, discord_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a user's existing sub-key for a specific master key.
        
        **Validates: Requirements 1.4, 4.4**
        
        Args:
            master_key_id: The master key ID
            discord_id: The Discord user ID
            
        Returns:
            Dict with sub-key info if found, None otherwise
        """
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow('''
                    SELECT sk.id, sk.sub_key_prefix, sk.master_key_id, sk.discord_id, 
                           sk.created_at, sk.last_used_at, sk.enabled,
                           ak.name as master_key_name
                    FROM sub_keys sk
                    JOIN api_keys ak ON sk.master_key_id = ak.id
                    WHERE sk.master_key_id = $1 AND sk.discord_id = $2
                ''', master_key_id, discord_id)
                return dict(row) if row else None
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT sk.id, sk.sub_key_prefix, sk.master_key_id, sk.discord_id, 
                           sk.created_at, sk.last_used_at, sk.enabled,
                           ak.name as master_key_name
                    FROM sub_keys sk
                    JOIN api_keys ak ON sk.master_key_id = ak.id
                    WHERE sk.master_key_id = ? AND sk.discord_id = ?
                ''', (master_key_id, discord_id))
                row = await cursor.fetchone()
                return dict(row) if row else None
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def create_sub_key(self, sub_key_hash: str, sub_key_prefix: str, 
                             master_key_id: int, discord_id: str) -> int:
        """
        Create a new sub-key in the database.
        
        **Validates: Requirements 1.2, 4.1**
        
        Args:
            sub_key_hash: SHA-256 hash of the sub-key
            sub_key_prefix: The full sub-key string (Moi-sub-XXXXXYYYYY)
            master_key_id: The master key ID this sub-key belongs to
            discord_id: The Discord user ID who claimed this key
            
        Returns:
            The ID of the newly created sub-key
        """
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                result = await conn.fetchval('''
                    INSERT INTO sub_keys (sub_key_hash, sub_key_prefix, master_key_id, discord_id)
                    VALUES ($1, $2, $3, $4)
                    RETURNING id
                ''', sub_key_hash, sub_key_prefix, master_key_id, discord_id)
                return result
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                cursor = await db.execute('''
                    INSERT INTO sub_keys (sub_key_hash, sub_key_prefix, master_key_id, discord_id)
                    VALUES (?, ?, ?, ?)
                ''', (sub_key_hash, sub_key_prefix, master_key_id, discord_id))
                await db.commit()
                return cursor.lastrowid
            finally:
                if not self._is_memory_db:
                    await db.close()

    async def get_user_sub_keys(self, discord_id: str) -> List[Dict[str, Any]]:
        """
        Get all sub-keys for a Discord user with usage statistics.
        
        **Validates: Requirements 6.1, 6.4, 6.5**
        
        Args:
            discord_id: The Discord user ID
            
        Returns:
            List of sub-key dicts with master key name, usage stats, and enabled status
        """
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch('''
                    SELECT 
                        sk.id,
                        sk.sub_key_prefix,
                        sk.master_key_id,
                        sk.discord_id,
                        sk.created_at,
                        sk.last_used_at,
                        sk.enabled as sub_key_enabled,
                        ak.name as master_key_name,
                        ak.enabled as master_key_enabled,
                        COALESCE(
                            (SELECT COUNT(*) FROM usage_logs ul 
                             WHERE ul.sub_key_id = sk.id 
                             AND DATE(ul.request_time) = CURRENT_DATE), 0
                        ) as requests_today,
                        COALESCE(
                            (SELECT SUM(ul.tokens_used) FROM usage_logs ul 
                             WHERE ul.sub_key_id = sk.id), 0
                        ) as total_tokens
                    FROM sub_keys sk
                    JOIN api_keys ak ON sk.master_key_id = ak.id
                    WHERE sk.discord_id = $1
                    ORDER BY sk.created_at DESC
                ''', discord_id)
                return [dict(row) for row in rows]
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute('''
                    SELECT 
                        sk.id,
                        sk.sub_key_prefix,
                        sk.master_key_id,
                        sk.discord_id,
                        sk.created_at,
                        sk.last_used_at,
                        sk.enabled as sub_key_enabled,
                        ak.name as master_key_name,
                        ak.enabled as master_key_enabled,
                        COALESCE(
                            (SELECT COUNT(*) FROM usage_logs ul 
                             WHERE ul.sub_key_id = sk.id 
                             AND DATE(ul.request_time) = DATE('now')), 0
                        ) as requests_today,
                        COALESCE(
                            (SELECT SUM(ul.tokens_used) FROM usage_logs ul 
                             WHERE ul.sub_key_id = sk.id), 0
                        ) as total_tokens
                    FROM sub_keys sk
                    JOIN api_keys ak ON sk.master_key_id = ak.id
                    WHERE sk.discord_id = ?
                    ORDER BY sk.created_at DESC
                ''', (discord_id,))
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            finally:
                if not self._is_memory_db:
                    await db.close()


    async def set_claim_code(self, key_id: int, claim_code: Optional[str]) -> bool:
        """
        Set or update the claim code for a master key.
        
        **Validates: Requirements 3.1, 3.2, 3.3**
        
        Args:
            key_id: The master key ID
            claim_code: The claim code to set (or None to remove)
            
        Returns:
            bool: True if the key was updated, False if key not found
        """
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                result = await conn.execute('''
                    UPDATE api_keys SET claim_code = $1 WHERE id = $2
                ''', claim_code, key_id)
                return result != "UPDATE 0"
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                cursor = await db.execute('''
                    UPDATE api_keys SET claim_code = ? WHERE id = ?
                ''', (claim_code, key_id))
                await db.commit()
                return cursor.rowcount > 0
            finally:
                if not self._is_memory_db:
                    await db.close()
