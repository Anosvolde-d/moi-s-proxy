import hashlib
import secrets
from datetime import datetime
from typing import Optional, List, Dict, Any
import asyncio
import logging
import os

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


class Database:
    """Database abstraction layer supporting PostgreSQL (primary) and SQLite (fallback)."""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DATABASE_PATH
        self._pool = None  # PostgreSQL connection pool
        self._pg_connected = False
        self._use_postgres = bool(config.POSTGRES_URL) and ASYNCPG_AVAILABLE
        
        # For SQLite fallback / in-memory testing
        self._shared_conn = None
        self._is_memory_db = self.db_path == ":memory:"
        
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
                    budget_reset_date TEXT DEFAULT NULL
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
                    request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
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
                    ('gpt-4o', 5.0, 15.0),
                    ('gpt-4-turbo', 10.0, 30.0),
                    ('gpt-3.5-turbo', 0.5, 1.5),
                    ('claude-3-5-sonnet', 3.0, 15.0),
                    ('claude-3-opus', 15.0, 75.0),
                    ('claude-3-haiku', 0.25, 1.25)
                ]
                for pattern, input_cost, output_cost in default_costs:
                    await conn.execute(
                        'INSERT INTO model_costs (model_pattern, input_cost_per_1m, output_cost_per_1m) VALUES ($1, $2, $3)',
                        pattern, input_cost, output_cost
                    )
            
            # Create indexes
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_usage_logs_key ON usage_logs(api_key_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_usage_logs_time ON usage_logs(request_time)')
            
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
                budget_reset_date TEXT DEFAULT NULL
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
                request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (api_key_id) REFERENCES api_keys (id)
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
                ('gpt-4o', 5.0, 15.0),
                ('gpt-4-turbo', 10.0, 30.0),
                ('gpt-3.5-turbo', 0.5, 1.5),
                ('claude-3-5-sonnet', 3.0, 15.0),
                ('claude-3-opus', 15.0, 75.0),
                ('claude-3-haiku', 0.25, 1.25)
            ]
            for pattern, input_cost, output_cost in default_costs:
                await db.execute(
                    'INSERT INTO model_costs (model_pattern, input_cost_per_1m, output_cost_per_1m) VALUES (?, ?, ?)',
                    (pattern, input_cost, output_cost)
                )
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
                        custom_prefills: str = None) -> Dict[str, Any]:
        """Create a new API key."""
        raw_key = f"{name or 'key'}-{secrets.token_hex(4)}"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        key_prefix = raw_key[:20] if len(raw_key) > 20 else raw_key
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow('''
                    INSERT INTO api_keys (key_hash, key_prefix, name, max_rpm, max_rpd,
                        target_url, target_api_key, no_auth, use_proxy, model_mappings,
                        expires_at, ip_whitelist, ip_blacklist, providers,
                        provider_rotation_frequency, disable_model_fetch, http_referer,
                        max_total_tokens, max_context_tokens, custom_prefills)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                    RETURNING id
                ''', key_hash, key_prefix, name, max_rpm, max_rpd,
                    target_url, target_api_key, no_auth, use_proxy, model_mappings,
                    expires_at, ip_whitelist, ip_blacklist, providers,
                    provider_rotation_frequency, disable_model_fetch, http_referer,
                    max_total_tokens, max_context_tokens, custom_prefills)
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
                        max_total_tokens, max_context_tokens, custom_prefills)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (key_hash, key_prefix, name, max_rpm, max_rpd,
                    target_url, target_api_key, no_auth, use_proxy, model_mappings,
                    expires_at, ip_whitelist, ip_blacklist, providers,
                    provider_rotation_frequency, disable_model_fetch, http_referer,
                    max_total_tokens, max_context_tokens, custom_prefills))
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

    async def delete_key(self, key_id: int) -> bool:
        """Delete an API key."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                # Delete usage logs first
                await conn.execute('DELETE FROM usage_logs WHERE api_key_id = $1', key_id)
                result = await conn.execute('DELETE FROM api_keys WHERE id = $1', key_id)
                return 'DELETE' in result
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                await db.execute('DELETE FROM usage_logs WHERE api_key_id = ?', (key_id,))
                await db.execute('DELETE FROM api_keys WHERE id = ?', (key_id,))
                await db.commit()
                return True
            finally:
                if not self._is_memory_db:
                    await db.close()

    # Alias for compatibility
    async def delete_api_key(self, key_id: int) -> bool:
        return await self.delete_key(key_id)


    # ==================== RATE LIMITING ====================
    
    async def check_rate_limit(self, key_id: int, max_rpm: int, max_rpd: int) -> tuple[bool, str]:
        """Check if request is within rate limits. Returns (allowed, error_message)."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    'SELECT current_rpm, current_rpd, last_reset FROM api_keys WHERE id = $1',
                    key_id
                )
                if not row:
                    return False, "Key not found"
                
                current_rpm = row['current_rpm'] or 0
                current_rpd = row['current_rpd'] or 0
                
                # Check limits (no reset logic needed for simple rate limiting)
                if current_rpm >= max_rpm:
                    return False, f"Rate limit exceeded: {current_rpm}/{max_rpm} RPM"
                if current_rpd >= max_rpd:
                    return False, f"Daily limit exceeded: {current_rpd}/{max_rpd} RPD"
                
                return True, ""
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
                    return False, "Key not found"
                
                current_rpm = row['current_rpm'] or 0
                current_rpd = row['current_rpd'] or 0
                
                # Check limits (no reset logic needed for simple rate limiting)
                if current_rpm >= max_rpm:
                    return False, f"Rate limit exceeded: {current_rpm}/{max_rpm} RPM"
                if current_rpd >= max_rpd:
                    return False, f"Daily limit exceeded: {current_rpd}/{max_rpd} RPD"
                
                return True, ""
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

    async def check_and_increment_rate_limit(self, api_key: str) -> tuple[bool, str, Optional[Dict]]:
        """Validate key and check rate limits in one call. Returns (allowed, error, key_info)."""
        key_info = await self.validate_key(api_key)
        if not key_info:
            return False, "Invalid API key", None
        
        allowed, error = await self.check_rate_limit(
            key_info['id'], 
            key_info.get('max_rpm', 60), 
            key_info.get('max_rpd', 1000)
        )
        if not allowed:
            return False, error, key_info
        
        await self.increment_rate_limit(key_info['id'])
        return True, "", key_info


    # ==================== USAGE LOGGING ====================
    
    async def log_usage(self, key_id: int, model: str, input_tokens: int, output_tokens: int,
                       tokens_used: int = None, success: bool = True, error_message: str = None,
                       client_ip: str = None, is_cache_hit: bool = False):
        """Log API usage."""
        if tokens_used is None:
            tokens_used = input_tokens + output_tokens
        
        # Calculate cost
        cost = await self._calculate_cost(model, input_tokens, output_tokens)
        
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO usage_logs (api_key_id, model, tokens_used, input_tokens, 
                        output_tokens, cost, success, error_message, client_ip, is_cache_hit)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ''', key_id, model, tokens_used, input_tokens, output_tokens, cost,
                    success, error_message, client_ip, is_cache_hit)
                
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
                        output_tokens, cost, success, error_message, client_ip, is_cache_hit)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (key_id, model, tokens_used, input_tokens, output_tokens, cost,
                    success, error_message, client_ip, is_cache_hit))
                
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
        
        for cost_info in costs:
            pattern = cost_info['model_pattern']
            if pattern in model.lower():
                input_cost = (input_tokens / 1_000_000) * cost_info['input_cost_per_1m']
                output_cost = (output_tokens / 1_000_000) * cost_info['output_cost_per_1m']
                return input_cost + output_cost
        
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

    async def get_analytics(self, key_id: int = None) -> Dict[str, Any]:
        """Get analytics data."""
        if self._use_postgres:
            async with self._pool.acquire() as conn:
                if key_id:
                    totals = await conn.fetchrow('''
                        SELECT COUNT(*) as total_requests,
                               SUM(tokens_used) as total_tokens,
                               SUM(cost) as total_cost,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs WHERE api_key_id = $1
                    ''', key_id)
                else:
                    totals = await conn.fetchrow('''
                        SELECT COUNT(*) as total_requests,
                               SUM(tokens_used) as total_tokens,
                               SUM(cost) as total_cost,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs
                    ''')
                
                return {
                    "totals": {
                        "total_requests": totals['total_requests'] or 0,
                        "total_tokens": totals['total_tokens'] or 0,
                        "total_cost": totals['total_cost'] or 0,
                        "successful_requests": totals['successful_requests'] or 0
                    }
                }
        else:
            db = self._shared_conn if self._is_memory_db else await aiosqlite.connect(self.db_path)
            try:
                db.row_factory = aiosqlite.Row
                if key_id:
                    cursor = await db.execute('''
                        SELECT COUNT(*) as total_requests,
                               SUM(tokens_used) as total_tokens,
                               SUM(cost) as total_cost,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs WHERE api_key_id = ?
                    ''', (key_id,))
                else:
                    cursor = await db.execute('''
                        SELECT COUNT(*) as total_requests,
                               SUM(tokens_used) as total_tokens,
                               SUM(cost) as total_cost,
                               SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_requests
                        FROM usage_logs
                    ''')
                row = await cursor.fetchone()
                
                return {
                    "totals": {
                        "total_requests": row['total_requests'] or 0,
                        "total_tokens": row['total_tokens'] or 0,
                        "total_cost": row['total_cost'] or 0,
                        "successful_requests": row['successful_requests'] or 0
                    }
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
                            custom_prefills: str = None) -> Dict[str, Any]:
        return await self.create_key(
            name=name, max_rpm=max_rpm, max_rpd=max_rpd,
            target_url=target_url, target_api_key=target_api_key,
            no_auth=no_auth, use_proxy=use_proxy,
            model_mappings=model_mappings, expires_at=expires_at,
            ip_whitelist=ip_whitelist, ip_blacklist=ip_blacklist,
            providers=providers, provider_rotation_frequency=provider_rotation_frequency,
            disable_model_fetch=disable_model_fetch, http_referer=http_referer,
            max_total_tokens=max_total_tokens, max_context_tokens=max_context_tokens,
            custom_prefills=custom_prefills
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
