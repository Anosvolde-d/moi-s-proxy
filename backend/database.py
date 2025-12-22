import sqlite3
import hashlib
import secrets
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import aiosqlite
import asyncio
import logging
import os

# Configure logging for database
logger = logging.getLogger(__name__)

try:
    import libsql_client
    LIB_SQL_AVAILABLE = True
    logger.info("libsql_client loaded successfully")
except ImportError:
    LIB_SQL_AVAILABLE = False
    logger.warning("libsql_client not installed. Turso support disabled. Install with: pip install libsql-client")

from config import config
from contextlib import asynccontextmanager

class CursorWrapper:
    def __init__(self, result_set=None, cursor=None, is_sync=False):
        self._result_set = result_set
        self._cursor = cursor
        self._is_sync = is_sync
        self._index = 0
        
    @property
    def rowcount(self):
        if self._cursor:
            return self._cursor.rowcount
        # For libsql sync, rowcount is available on the result
        if self._result_set:
            return getattr(self._result_set, 'rowcount', 0)
        return 0

    @property
    def lastrowid(self):
        if self._cursor:
            return self._cursor.lastrowid
        # For libsql sync, lastrowid is available on the result
        if self._result_set:
            return getattr(self._result_set, 'lastrowid', None)
        return None

    async def fetchone(self):
        if self._cursor:
            row = await self._cursor.fetchone()
            return dict(row) if row else None
        
        # For libsql-client, result_set is the result of execute()
        if self._result_set and self._result_set.rows:
            if self._index < len(self._result_set.rows):
                row = self._result_set.rows[self._index]
                self._index += 1
                return dict(zip(self._result_set.columns, row))
        return None

    async def fetchall(self):
        if self._cursor:
            rows = await self._cursor.fetchall()
            return [dict(row) for row in rows]
        
        # For libsql-client
        if self._result_set and self._result_set.rows:
            columns = self._result_set.columns
            return [dict(zip(columns, row)) for row in self._result_set.rows]
        return []

class ConnectionWrapper:
    def __init__(self, conn=None, client=None):
        self._conn = conn  # aiosqlite async connection
        self._client = client  # libsql sync connection
        
    @property
    def row_factory(self):
        return getattr(self._conn, 'row_factory', None)
    
    @row_factory.setter
    def row_factory(self, value):
        if self._conn:
            self._conn.row_factory = value

    async def execute(self, sql, params=None):
        if self._conn:
            # aiosqlite async connection
            cursor = await self._conn.execute(sql, params or ())
            return CursorWrapper(cursor=cursor)
        else:
            # libsql-client async execution
            result = await self._client.execute(sql, params or ())
            return CursorWrapper(result_set=result)
            
    async def commit(self):
        if self._conn:
            await self._conn.commit()
        # libsql-client handles commits implicitly or via transactions which we are not using here for simplicity

    async def close(self):
        if self._conn:
            await self._conn.close()

class Database:
    def __init__(self, db_path: str = None):
        self.turso_available = LIB_SQL_AVAILABLE and bool(config.TURSO_DATABASE_URL)
        self.db_path = db_path or config.DATABASE_PATH
        self._turso_client = None
        
        # Sync queue for background Turso writes
        self._sync_queue = asyncio.Queue()
        self._sync_task = None
        self._loaded_from_turso = False
        
        # In-memory caches
        self._key_cache = {}  # {key_hash: (key_info, expiry)}
        self._costs_cache = None  # (costs, expiry)
        self._cache_ttl = 60  # 1 minute for keys
        self._costs_ttl = 300  # 5 minutes for model costs
        
        # Ensure the directory exists for local SQLite (always used)
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        logger.info(f"Using local SQLite database at {self.db_path}")
        
        if self.turso_available:
            logger.info(f"Turso sync enabled: {config.TURSO_DATABASE_URL}")
            # Client will be created in async init_turso_client()
            
        self.init_db()
    
    async def init_turso_client(self):
        """Initialize Turso client (must be called from async context)"""
        if self.turso_available and not self._turso_client:
            self._turso_client = libsql_client.create_client(
                url=config.TURSO_DATABASE_URL,
                auth_token=config.TURSO_AUTH_TOKEN
            )
            logger.info("Turso client initialized")

        
    @asynccontextmanager
    async def get_db(self):
        """Always use local SQLite for speed"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            yield ConnectionWrapper(conn=db)
    
    async def start_sync_task(self):
        """Start the background sync task"""
        if self.turso_available and not self._sync_task:
            self._sync_task = asyncio.create_task(self._process_sync_queue())
            logger.info("Background Turso sync task started")
    
    async def _process_sync_queue(self):
        """Background task to sync local changes to Turso"""
        while True:
            try:
                # Get a batch of items (wait up to 5 seconds to batch)
                items = []
                try:
                    while len(items) < 50:
                        item = await asyncio.wait_for(self._sync_queue.get(), timeout=5.0)
                        items.append(item)
                except asyncio.TimeoutError:
                    pass
                
                if items and self._turso_client:
                    for sql, params in items:
                        try:
                            await self._turso_client.execute(sql, params or ())
                        except Exception as e:
                            logger.warning(f"Turso sync error: {e}")
                    logger.debug(f"Synced {len(items)} items to Turso")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Sync queue error: {e}")
                await asyncio.sleep(1)
    
    def queue_turso_sync(self, sql: str, params=None):
        """Queue a SQL statement for background Turso sync"""
        if self.turso_available:
            try:
                self._sync_queue.put_nowait((sql, params))
            except:
                pass  # Queue full, skip this sync
    
    async def load_from_turso(self):
        """Load all data from Turso into local SQLite (called once on startup)"""
        if not self.turso_available or self._loaded_from_turso:
            return
        
        try:
            logger.info("Loading data from Turso...")
            
            # Fetch api_keys from Turso
            result = await self._turso_client.execute("SELECT * FROM api_keys")
            if result.rows:
                async with aiosqlite.connect(self.db_path) as db:
                    # Clear existing and insert from Turso
                    await db.execute("DELETE FROM api_keys")
                    columns = result.columns
                    placeholders = ",".join(["?"] * len(columns))
                    col_names = ",".join(columns)
                    for row in result.rows:
                        await db.execute(f"INSERT OR REPLACE INTO api_keys ({col_names}) VALUES ({placeholders})", row)
                    await db.commit()
                logger.info(f"Loaded {len(result.rows)} API keys from Turso")
            
            # Fetch usage_logs (last 1000)
            result = await self._turso_client.execute("SELECT * FROM usage_logs ORDER BY id DESC LIMIT 1000")
            if result.rows:
                async with aiosqlite.connect(self.db_path) as db:
                    columns = result.columns
                    placeholders = ",".join(["?"] * len(columns))
                    col_names = ",".join(columns)
                    for row in result.rows:
                        try:
                            await db.execute(f"INSERT OR REPLACE INTO usage_logs ({col_names}) VALUES ({placeholders})", row)
                        except:
                            pass
                    await db.commit()
                logger.info(f"Loaded {len(result.rows)} usage logs from Turso")
            
            # Fetch model_costs
            result = await self._turso_client.execute("SELECT * FROM model_costs")
            if result.rows:
                async with aiosqlite.connect(self.db_path) as db:
                    columns = result.columns
                    placeholders = ",".join(["?"] * len(columns))
                    col_names = ",".join(columns)
                    for row in result.rows:
                        try:
                            await db.execute(f"INSERT OR REPLACE INTO model_costs ({col_names}) VALUES ({placeholders})", row)
                        except:
                            pass
                    await db.commit()
                logger.info(f"Loaded {len(result.rows)} model costs from Turso")
            
            self._loaded_from_turso = True
            logger.info("Data loaded from Turso successfully")
        except Exception as e:
            logger.error(f"Failed to load from Turso: {e}")
    
    async def full_sync_to_turso(self):
        """Full overwrite sync from local SQLite to Turso (hourly)"""
        if not self.turso_available or not self._turso_client:
            return False
        
        try:
            logger.info("Starting full sync to Turso...")
            
            # Sync api_keys
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute("SELECT * FROM api_keys")
                rows = await cursor.fetchall()
                
                if rows:
                    # Delete all in Turso and re-insert
                    await self._turso_client.execute("DELETE FROM api_keys")
                    for row in rows:
                        columns = row.keys()
                        values = list(dict(row).values())
                        placeholders = ",".join(["?"] * len(columns))
                        col_names = ",".join(columns)
                        await self._turso_client.execute(f"INSERT INTO api_keys ({col_names}) VALUES ({placeholders})", values)
                    logger.info(f"Synced {len(rows)} API keys to Turso")
                
                # Sync usage_logs (last 10000)
                cursor = await db.execute("SELECT * FROM usage_logs ORDER BY id DESC LIMIT 10000")
                rows = await cursor.fetchall()
                
                if rows:
                    await self._turso_client.execute("DELETE FROM usage_logs")
                    for row in rows:
                        columns = row.keys()
                        values = list(dict(row).values())
                        placeholders = ",".join(["?"] * len(columns))
                        col_names = ",".join(columns)
                        try:
                            await self._turso_client.execute(f"INSERT INTO usage_logs ({col_names}) VALUES ({placeholders})", values)
                        except:
                            pass
                    logger.info(f"Synced {len(rows)} usage logs to Turso")
            
            logger.info("Full sync to Turso completed")
            return True
        except Exception as e:
            logger.error(f"Full sync to Turso failed: {e}")
            return False
    
    async def close(self):
        """Close the database clients"""
        if self._sync_task:
            self._sync_task.cancel()
        if self._turso_client:
            await self._turso_client.close()
    
    def init_db(self):
        """Initialize database with required tables"""
        # Run initialization in a way that works for both sync and async
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is already running, create a task
                loop.create_task(self._async_init_db())
            else:
                loop.run_until_complete(self._async_init_db())
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")

    async def _async_init_db(self):
        """Asynchronous database initialization"""
        async with self.get_db() as db:
            # We need a cursor-like object to run initialization
            # Since _init_tables expects a sync connection, we'll adapt or rewrite it
            await self._init_tables_async(db)
        logger.info("Database tables initialized successfully")
    
    async def _init_tables_async(self, db):
        """Initialize tables using an async connection"""
        # Create api_keys table
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
        
        # Helper to execute safely for existing tables
        async def safe_alter(sql):
            try:
                await db.execute(sql)
            except:
                pass

        await safe_alter('ALTER TABLE api_keys ADD COLUMN current_rpm INTEGER DEFAULT 0')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN current_rpd INTEGER DEFAULT 0')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN last_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN refresh_hour INTEGER DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN target_url TEXT DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN target_api_key TEXT DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN use_proxy BOOLEAN DEFAULT 1')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN no_auth BOOLEAN DEFAULT 0')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN model_mappings TEXT DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN expires_at TIMESTAMP DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN ip_whitelist TEXT DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN ip_blacklist TEXT DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN providers TEXT DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN provider_rotation_index INTEGER DEFAULT 0')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN provider_rotation_frequency INTEGER DEFAULT 1')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN disable_model_fetch BOOLEAN DEFAULT 0')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN http_referer TEXT DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN max_total_tokens INTEGER DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN total_tokens_used INTEGER DEFAULT 0')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN max_context_tokens INTEGER DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN custom_prefills TEXT DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN budget_limit REAL DEFAULT NULL')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN budget_used REAL DEFAULT 0')
        await safe_alter('ALTER TABLE api_keys ADD COLUMN budget_reset_date TEXT DEFAULT NULL')
        
        # Create usage_logs table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS usage_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key_id INTEGER,
                model TEXT,
                tokens_used INTEGER,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cost REAL DEFAULT 0,
                success BOOLEAN,
                is_cache_hit BOOLEAN DEFAULT 0,
                error_message TEXT,
                client_ip TEXT,
                request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (api_key_id) REFERENCES api_keys (id)
            )
        ''')
        
        await safe_alter('ALTER TABLE usage_logs ADD COLUMN is_cache_hit BOOLEAN DEFAULT 0')
        await safe_alter('ALTER TABLE usage_logs ADD COLUMN input_tokens INTEGER DEFAULT 0')
        await safe_alter('ALTER TABLE usage_logs ADD COLUMN output_tokens INTEGER DEFAULT 0')
        await safe_alter('ALTER TABLE usage_logs ADD COLUMN client_ip TEXT DEFAULT NULL')
        await safe_alter('ALTER TABLE usage_logs ADD COLUMN cost REAL DEFAULT 0')
            
        # Create model_costs table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS model_costs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_pattern TEXT UNIQUE NOT NULL,
                input_cost_per_1m REAL DEFAULT 0,
                output_cost_per_1m REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Seed some default costs if table is empty
        cursor = await db.execute('SELECT COUNT(*) as count FROM model_costs')
        row = await cursor.fetchone()
        if row and row['count'] == 0:
            default_costs = [
                ('gpt-4o', 5.0, 15.0),
                ('gpt-4-turbo', 10.0, 30.0),
                ('gpt-3.5-turbo', 0.5, 1.5),
                ('claude-3-5-sonnet', 3.0, 15.0),
                ('claude-3-opus', 15.0, 75.0),
                ('claude-3-haiku', 0.25, 1.25)
            ]
            for pattern, input_cost, output_cost in default_costs:
                await db.execute('INSERT INTO model_costs (model_pattern, input_cost_per_1m, output_cost_per_1m) VALUES (?, ?, ?)', (pattern, input_cost, output_cost))
        
        # Create response_cache table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS response_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_hash TEXT UNIQUE NOT NULL,
                prompt_text TEXT,
                embedding BLOB,
                response_body TEXT NOT NULL,
                model TEXT,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                hit_count INTEGER DEFAULT 0
            )
        ''')
        
        # Create indexes
        await db.execute('CREATE INDEX IF NOT EXISTS idx_response_cache_hash ON response_cache(request_hash)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_response_cache_expires ON response_cache(expires_at)')
        
        # Create rate_limits table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS rate_limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key_id INTEGER UNIQUE,
                minute_bucket TEXT,
                day_bucket TEXT,
                minute_count INTEGER DEFAULT 0,
                day_count INTEGER DEFAULT 0,
                last_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
            )
        ''')
        
        # Create large_context_logs table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS large_context_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key_id INTEGER,
                model TEXT,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
                client_ip TEXT,
                request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                request_summary TEXT,
                FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
            )
        ''')
        
        await db.execute('CREATE INDEX IF NOT EXISTS idx_large_context_time ON large_context_logs(request_time)')
        
        await db.commit()

    async def backup_database(self):
        """Perform an hourly backup of the database"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = Path("backups")
            backup_dir.mkdir(exist_ok=True)
            
            if self.use_turso:
                # For Turso, we could perform a remote dump if supported, 
                # but a simple way is to fetch critical tables and save locally as json or sqlite
                logger.info(f"Starting Turso backup to local file...")
                backup_path = backup_dir / f"turso_backup_{timestamp}.db"
                
                # Create a local sqlite backup of Turso data
                local_conn = sqlite3.connect(backup_path)
                # This is a bit complex as we'd need to replicate all tables.
                # A better approach for Turso is to use their CLI for backups, 
                # but programmatically we can at least snapshot the api_keys and analytics
                
                async with self.get_db() as db:
                    # Backup api_keys
                    cursor = await db.execute("SELECT * FROM api_keys")
                    rows = await cursor.fetchall()
                    if rows:
                        columns = rows[0].keys()
                        placeholders = ",".join(["?"] * len(columns))
                        local_conn.execute(f"CREATE TABLE api_keys ({','.join(columns)})")
                        local_conn.executemany(f"INSERT INTO api_keys VALUES ({placeholders})", [tuple(row.values()) for row in rows])
                    
                    # Backup usage_logs (limit to last 1000 for quick backup)
                    cursor = await db.execute("SELECT * FROM usage_logs ORDER BY id DESC LIMIT 1000")
                    rows = await cursor.fetchall()
                    if rows:
                        columns = rows[0].keys()
                        placeholders = ",".join(["?"] * len(columns))
                        local_conn.execute(f"CREATE TABLE usage_logs ({','.join(columns)})")
                        local_conn.executemany(f"INSERT INTO usage_logs VALUES ({placeholders})", [tuple(row.values()) for row in rows])
                
                local_conn.commit()
                local_conn.close()
                logger.info(f"Turso backup completed: {backup_path}")
            else:
                # For local SQLite, just copy the file
                import shutil
                backup_path = backup_dir / f"local_backup_{timestamp}.db"
                shutil.copy2(self.db_path, backup_path)
                logger.info(f"Local database backup completed: {backup_path}")
                
            return True
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False

    def _init_tables(self, conn):
        """Initialize tables on the given connection (works with both sqlite3 and libsql)"""
        cursor = conn.cursor()
        
        # Create api_keys table
        cursor.execute('''
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
        
        # Add new columns if they don't exist (for backward compatibility)
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN current_rpm INTEGER DEFAULT 0')
        except:
            pass
        
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN current_rpd INTEGER DEFAULT 0')
        except:
            pass
            
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN last_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        except:
            pass
            
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN refresh_hour INTEGER DEFAULT NULL')
        except:
            pass
        
        # Add target_url and target_api_key columns for custom API key configuration
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN target_url TEXT DEFAULT NULL')
        except:
            pass
        
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN target_api_key TEXT DEFAULT NULL')
        except:
            pass
        
        # Add use_proxy column (default true = use WebScrapingAPI for IP rotation)
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN use_proxy BOOLEAN DEFAULT 1')
        except:
            pass
        
        # Add no_auth column (default false = send API key, true = don't send API key)
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN no_auth BOOLEAN DEFAULT 0')
        except:
            pass
        
        # Add model_mappings column for custom model aliases
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN model_mappings TEXT DEFAULT NULL')
        except:
            pass
        
        # Add expires_at column for key expiration
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN expires_at TIMESTAMP DEFAULT NULL')
        except:
            pass
        
        # Add ip_whitelist column for IP restrictions
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN ip_whitelist TEXT DEFAULT NULL')
        except:
            pass
        
        # Add ip_blacklist column for IP restrictions
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN ip_blacklist TEXT DEFAULT NULL')
        except:
            pass
        
        # Add providers column for multi-provider support
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN providers TEXT DEFAULT NULL')
        except:
            pass
        
        # Add provider_rotation_index column for tracking current provider
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN provider_rotation_index INTEGER DEFAULT 0')
        except:
            pass
        
        # Add provider_rotation_frequency column for rotation settings
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN provider_rotation_frequency INTEGER DEFAULT 1')
        except:
            pass
        
        # Add disable_model_fetch column to prevent model listing
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN disable_model_fetch BOOLEAN DEFAULT 0')
        except:
            pass
        
        # Add http_referer column for custom OpenRouter HTTP-Referer header
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN http_referer TEXT DEFAULT NULL')
        except:
            pass
        
        # Add quota and limit columns
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN max_total_tokens INTEGER DEFAULT NULL')
        except:
            pass
            
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN total_tokens_used INTEGER DEFAULT 0')
        except:
            pass
            
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN max_context_tokens INTEGER DEFAULT NULL')
        except:
            pass
            
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN custom_prefills TEXT DEFAULT NULL')
        except:
            pass
        
        # Add budget columns for spending limits
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN budget_limit REAL DEFAULT NULL')
        except:
            pass
            
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN budget_used REAL DEFAULT 0')
        except:
            pass
            
        try:
            cursor.execute('ALTER TABLE api_keys ADD COLUMN budget_reset_date TEXT DEFAULT NULL')
        except:
            pass
        
        # Create usage_logs table with is_cache_hit
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS usage_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key_id INTEGER,
                model TEXT,
                tokens_used INTEGER,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cost REAL DEFAULT 0,
                success BOOLEAN,
                is_cache_hit BOOLEAN DEFAULT 0,
                error_message TEXT,
                client_ip TEXT,
                request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (api_key_id) REFERENCES api_keys (id)
            )
        ''')
        
        try:
            cursor.execute('ALTER TABLE usage_logs ADD COLUMN is_cache_hit BOOLEAN DEFAULT 0')
        except:
            pass
        
        # Add new columns for token tracking if they don't exist
        try:
            cursor.execute('ALTER TABLE usage_logs ADD COLUMN input_tokens INTEGER DEFAULT 0')
        except:
            pass
        
        try:
            cursor.execute('ALTER TABLE usage_logs ADD COLUMN output_tokens INTEGER DEFAULT 0')
        except:
            pass
        
        try:
            cursor.execute('ALTER TABLE usage_logs ADD COLUMN client_ip TEXT DEFAULT NULL')
        except:
            pass
            
        try:
            cursor.execute('ALTER TABLE usage_logs ADD COLUMN cost REAL DEFAULT 0')
        except:
            pass
            
        # Create model_costs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS model_costs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_pattern TEXT UNIQUE NOT NULL,
                input_cost_per_1m REAL DEFAULT 0,
                output_cost_per_1m REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Remove temporary semantic_cache if exists
        try:
            cursor.execute('DROP TABLE IF EXISTS semantic_cache')
        except:
            pass
        
        # Seed some default costs if table is empty
        cursor.execute('SELECT COUNT(*) FROM model_costs')
        if cursor.fetchone()[0] == 0:
            default_costs = [
                ('gpt-4o', 5.0, 15.0),
                ('gpt-4-turbo', 10.0, 30.0),
                ('gpt-3.5-turbo', 0.5, 1.5),
                ('claude-3-5-sonnet', 3.0, 15.0),
                ('claude-3-opus', 15.0, 75.0),
                ('claude-3-haiku', 0.25, 1.25)
            ]
            cursor.executemany('INSERT INTO model_costs (model_pattern, input_cost_per_1m, output_cost_per_1m) VALUES (?, ?, ?)', default_costs)
        
        # Create response_cache table for caching identical requests
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS response_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_hash TEXT UNIQUE NOT NULL,
                prompt_text TEXT,
                embedding BLOB,
                response_body TEXT NOT NULL,
                model TEXT,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                hit_count INTEGER DEFAULT 0
            )
        ''')
        
        # Create indexes for response_cache
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_response_cache_hash ON response_cache(request_hash)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_response_cache_expires ON response_cache(expires_at)')
        
        # Create rate_limits table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rate_limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key_id INTEGER UNIQUE,
                minute_bucket TEXT,
                day_bucket TEXT,
                minute_count INTEGER DEFAULT 0,
                day_count INTEGER DEFAULT 0,
                last_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
            )
        ''')
        
        # Create large_context_logs table for requests exceeding token threshold
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS large_context_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key_id INTEGER,
                model TEXT,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
                client_ip TEXT,
                request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                request_summary TEXT,
                FOREIGN KEY (api_key_id) REFERENCES api_keys(id)
            )
        ''')
        
        # Create index for large_context_logs
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_large_context_time ON large_context_logs(request_time)')
        
        conn.commit()
    
    def generate_api_key(self) -> tuple[str, str]:
        """Generate a new API key and return (key, prefix)"""
        # Generate random key with 'Moi-' prefix
        # We need a random string of about 10 chars to match user's example
        random_part = secrets.token_hex(5) # 10 hex characters
        api_key = f"Moi-{random_part}"
        
        # Create prefix for display
        prefix = api_key[:20] + "..." if len(api_key) > 20 else api_key
        
        return api_key, prefix
    
    def hash_key(self, api_key: str) -> str:
        """Hash API key for secure storage"""
        return hashlib.sha256(api_key.encode()).hexdigest()
    
    async def create_api_key(self, name: str = None, max_rpm: int = 60, max_rpd: int = 1000,
                            target_url: str = None, target_api_key: str = None,
                            no_auth: bool = False, use_proxy: bool = True,
                            model_mappings: str = None, expires_at: str = None,
                            ip_whitelist: str = None, ip_blacklist: str = None,
                            providers: str = None, provider_rotation_frequency: int = 1,
                            disable_model_fetch: bool = False, http_referer: str = None,
                            max_total_tokens: int = None, max_context_tokens: int = None,
                            custom_prefills: str = None, budget_limit: float = None) -> Dict[str, Any]:
        """Create a new API key with advanced settings including quotas, limits, and budget"""
        api_key, prefix = self.generate_api_key()
        key_hash = self.hash_key(api_key)
        now = datetime.now()
        current_minute = now.strftime("%Y-%m-%d %H:%M")
        current_day = now.strftime("%Y-%m-%d")
        budget_reset_date = now.strftime("%Y-%m-%d") if budget_limit else None
        
        async with self.get_db() as db:
            cursor = await db.execute('''
                INSERT INTO api_keys (key_hash, key_prefix, name, max_rpm, max_rpd, enabled, target_url, target_api_key, no_auth, use_proxy, model_mappings, expires_at, ip_whitelist, ip_blacklist, providers, provider_rotation_index, provider_rotation_frequency, disable_model_fetch, http_referer, max_total_tokens, max_context_tokens, custom_prefills, budget_limit, budget_used, budget_reset_date)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            ''', (key_hash, prefix, name, max_rpm, max_rpd, target_url, target_api_key, 1 if no_auth else 0, 1 if use_proxy else 0, model_mappings, expires_at, ip_whitelist, ip_blacklist, providers, provider_rotation_frequency, 1 if disable_model_fetch else 0, http_referer, max_total_tokens, max_context_tokens, custom_prefills, budget_limit, budget_reset_date))
            
            await db.commit()
            key_id = cursor.lastrowid
            
            # Initialize rate limits with proper bucket values
            await db.execute('''
                INSERT INTO rate_limits (api_key_id, minute_bucket, day_bucket, minute_count, day_count)
                VALUES (?, ?, ?, 0, 0)
            ''', (key_id, current_minute, current_day))
            await db.commit()
        
        # Queue sync to Turso for persistence across restarts
        self.queue_turso_sync('''
            INSERT OR REPLACE INTO api_keys (id, key_hash, key_prefix, name, max_rpm, max_rpd, enabled, target_url, target_api_key, no_auth, use_proxy, model_mappings, expires_at, ip_whitelist, ip_blacklist, providers, provider_rotation_index, provider_rotation_frequency, disable_model_fetch, http_referer, max_total_tokens, max_context_tokens, custom_prefills, budget_limit, budget_used, budget_reset_date)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        ''', (key_id, key_hash, prefix, name, max_rpm, max_rpd, target_url, target_api_key, 1 if no_auth else 0, 1 if use_proxy else 0, model_mappings, expires_at, ip_whitelist, ip_blacklist, providers, provider_rotation_frequency, 1 if disable_model_fetch else 0, http_referer, max_total_tokens, max_context_tokens, custom_prefills, budget_limit, budget_reset_date))

        
        return {
            "id": key_id,
            "api_key": api_key,  # Only return once during creation
            "prefix": prefix,
            "name": name,
            "max_rpm": max_rpm,
            "max_rpd": max_rpd,
            "enabled": True,
            "target_url": target_url,
            "target_api_key": target_api_key,
            "no_auth": no_auth,
            "use_proxy": use_proxy,
            "model_mappings": model_mappings,
            "expires_at": expires_at,
            "ip_whitelist": ip_whitelist,
            "ip_blacklist": ip_blacklist,
            "providers": providers,
            "provider_rotation_frequency": provider_rotation_frequency,
            "disable_model_fetch": disable_model_fetch,
            "http_referer": http_referer,
            "budget_limit": budget_limit,
            "budget_used": 0,
            "budget_reset_date": budget_reset_date,
            "created_at": datetime.now().isoformat()
        }
    
    async def validate_api_key(self, api_key: str) -> Optional[Dict[str, Any]]:
        """Validate API key and return key info if valid (with caching)"""
        key_hash = self.hash_key(api_key)
        now_ts = datetime.now().timestamp()
        
        # Check cache
        if key_hash in self._key_cache:
            info, expiry = self._key_cache[key_hash]
            if now_ts < expiry:
                return info
        
        async with self.get_db() as db:
            if not self.use_turso:
                db.row_factory = aiosqlite.Row
                
            cursor = await db.execute('''
                SELECT id, key_prefix, name, max_rpm, max_rpd, enabled, last_used_at, target_url, target_api_key, use_proxy, no_auth, model_mappings, expires_at, ip_whitelist, ip_blacklist, providers, provider_rotation_index, provider_rotation_frequency, disable_model_fetch, http_referer, max_total_tokens, total_tokens_used, max_context_tokens, custom_prefills, budget_limit, budget_used, budget_reset_date
                FROM api_keys
                WHERE key_hash = ?
            ''', (key_hash,))
            
            row = await cursor.fetchone()
            
            if row:
                # Check if key has enabled
                if not bool(row['enabled']):
                    return None
                    
                # Check if key has expired
                if row['expires_at']:
                    try:
                        expires_at = datetime.fromisoformat(row['expires_at'].replace('Z', '+00:00')) if isinstance(row['expires_at'], str) else row['expires_at']
                        if datetime.now() > expires_at:
                            return None  # Key has expired
                    except:
                        pass
                
                # Update last_used_at (don't wait for it to return response faster)
                asyncio.create_task(self._update_last_used(row['id']))
                
                key_info = {
                    "id": row['id'],
                    "prefix": row['key_prefix'],
                    "name": row['name'],
                    "max_rpm": row['max_rpm'],
                    "max_rpd": row['max_rpd'],
                    "enabled": True,
                    "last_used_at": row['last_used_at'],
                    "target_url": row['target_url'],
                    "target_api_key": row['target_api_key'],
                    "use_proxy": bool(row['use_proxy']) if row['use_proxy'] is not None else True,
                    "no_auth": bool(row['no_auth']) if row['no_auth'] is not None else False,
                    "model_mappings": row['model_mappings'],
                    "expires_at": row['expires_at'],
                    "ip_whitelist": row['ip_whitelist'],
                    "ip_blacklist": row['ip_blacklist'],
                    "providers": row['providers'],
                    "provider_rotation_index": row['provider_rotation_index'] or 0,
                    "provider_rotation_frequency": row['provider_rotation_frequency'] or 1,
                    "disable_model_fetch": bool(row['disable_model_fetch']) if row['disable_model_fetch'] is not None else False,
                    "http_referer": row['http_referer'],
                    "max_total_tokens": row['max_total_tokens'],
                    "total_tokens_used": row['total_tokens_used'] or 0,
                    "max_context_tokens": row['max_context_tokens'],
                    "custom_prefills": row['custom_prefills'],
                    "budget_limit": row['budget_limit'],
                    "budget_used": row['budget_used'] or 0,
                    "budget_reset_date": row['budget_reset_date']
                }
                
                # Update cache
                self._key_cache[key_hash] = (key_info, now_ts + self._cache_ttl)
                return key_info
            
            return None

    async def _update_last_used(self, key_id: int):
        """Background task to update last_used_at"""
        try:
            async with self.get_db() as db:
                await db.execute('''
                    UPDATE api_keys
                    SET last_used_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (key_id,))
                await db.commit()
        except:
            pass
    
    async def get_all_keys(self) -> List[Dict[str, Any]]:
        """Get all API keys (without actual keys)"""
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT id, key_prefix, name, max_rpm, max_rpd, enabled, created_at, last_used_at, target_url, target_api_key, use_proxy, no_auth, model_mappings, expires_at, ip_whitelist, ip_blacklist, providers, provider_rotation_index, provider_rotation_frequency, disable_model_fetch, http_referer, max_total_tokens, total_tokens_used, max_context_tokens, custom_prefills, budget_limit, budget_used, budget_reset_date
                FROM api_keys
                ORDER BY created_at DESC
            ''')
            
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def toggle_key(self, key_id: int) -> bool:
        """Toggle API key enabled/disabled status"""
        async with self.get_db() as db:
            cursor = await db.execute('''
                UPDATE api_keys
                SET enabled = NOT enabled
                WHERE id = ?
            ''', (key_id,))
            
            await db.commit()
            return cursor.rowcount > 0
    
    async def update_key_limits(self, key_id: int, max_rpm: int = None, max_rpd: int = None,
                                target_url: str = None, target_api_key: str = None,
                                no_auth: bool = None, use_proxy: bool = True,
                                model_mappings: str = None, expires_at: str = None,
                                ip_whitelist: str = None, ip_blacklist: str = None,
                                providers: str = None, provider_rotation_frequency: int = None,
                                disable_model_fetch: bool = None, http_referer: str = None,
                                max_total_tokens: int = None, max_context_tokens: int = None,
                                custom_prefills: str = None, budget_limit: float = None) -> bool:
        """Update rate limits, target settings, providers, budget, and advanced options for an API key"""
        updates = []
        params = []
        
        if max_rpm is not None:
            updates.append("max_rpm = ?")
            params.append(max_rpm)
        
        if max_rpd is not None:
            updates.append("max_rpd = ?")
            params.append(max_rpd)
            
        if max_total_tokens is not None:
            updates.append("max_total_tokens = ?")
            params.append(max_total_tokens if max_total_tokens != -1 else None)
            
        if max_context_tokens is not None:
            updates.append("max_context_tokens = ?")
            params.append(max_context_tokens if max_context_tokens != -1 else None)
            
        if custom_prefills is not None:
            updates.append("custom_prefills = ?")
            params.append(custom_prefills if custom_prefills else None)
        
        if target_url is not None:
            updates.append("target_url = ?")
            params.append(target_url if target_url else None)
        
        if target_api_key is not None:
            updates.append("target_api_key = ?")
            params.append(target_api_key if target_api_key else None)
        
        if no_auth is not None:
            updates.append("no_auth = ?")
            params.append(1 if no_auth else 0)
        
        if use_proxy is not None:
            updates.append("use_proxy = ?")
            params.append(1 if use_proxy else 0)
        
        if model_mappings is not None:
            updates.append("model_mappings = ?")
            params.append(model_mappings if model_mappings else None)
        
        if expires_at is not None:
            updates.append("expires_at = ?")
            params.append(expires_at if expires_at else None)
        
        if ip_whitelist is not None:
            updates.append("ip_whitelist = ?")
            params.append(ip_whitelist if ip_whitelist else None)
        
        if ip_blacklist is not None:
            updates.append("ip_blacklist = ?")
            params.append(ip_blacklist if ip_blacklist else None)
        
        if providers is not None:
            updates.append("providers = ?")
            params.append(providers if providers else None)
        
        if provider_rotation_frequency is not None:
            updates.append("provider_rotation_frequency = ?")
            params.append(provider_rotation_frequency)
        
        if disable_model_fetch is not None:
            updates.append("disable_model_fetch = ?")
            params.append(1 if disable_model_fetch else 0)
        
        if http_referer is not None:
            updates.append("http_referer = ?")
            params.append(http_referer if http_referer else None)
        
        if budget_limit is not None:
            updates.append("budget_limit = ?")
            params.append(budget_limit if budget_limit > 0 else None)
            # If setting a new budget limit, also set the reset date to today
            if budget_limit > 0:
                updates.append("budget_reset_date = ?")
                params.append(datetime.now().strftime("%Y-%m-%d"))
        
        if not updates:
            return False
        
        params.append(key_id)
        query = f"UPDATE api_keys SET {', '.join(updates)} WHERE id = ?"
        
        async with self.get_db() as db:
            cursor = await db.execute(query, params)
            await db.commit()
            return cursor.rowcount > 0
    
    async def check_and_reset_budget(self, key_id: int, budget_reset_date: str) -> bool:
        """Check if budget needs to be reset (daily) and reset if needed"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        if budget_reset_date != today:
            # Reset budget for new day
            async with self.get_db() as db:
                await db.execute('''
                    UPDATE api_keys
                    SET budget_used = 0, budget_reset_date = ?
                    WHERE id = ?
                ''', (today, key_id))
                await db.commit()
                return True
        return False
    
    async def check_budget_limit(self, key_id: int, budget_limit: float, budget_used: float, budget_reset_date: str) -> Dict[str, Any]:
        """Check if request is within budget limit, reset if new day"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Reset budget if it's a new day
        if budget_reset_date and budget_reset_date != today:
            await self.check_and_reset_budget(key_id, budget_reset_date)
            budget_used = 0
        
        # Check if within budget
        if budget_limit and budget_used >= budget_limit:
            return {
                "allowed": False,
                "error": f"Daily budget limit exceeded (${budget_used:.4f} / ${budget_limit:.4f}). Resets at midnight.",
                "budget_remaining": 0,
                "budget_used": budget_used,
                "budget_limit": budget_limit
            }
        
        budget_remaining = (budget_limit - budget_used) if budget_limit else None
        return {
            "allowed": True,
            "budget_remaining": budget_remaining,
            "budget_used": budget_used,
            "budget_limit": budget_limit
        }
    
    async def update_budget_used(self, key_id: int, cost: float) -> bool:
        """Update the budget_used for an API key after a request"""
        if cost <= 0:
            return True
            
        async with self.get_db() as db:
            await db.execute('''
                UPDATE api_keys
                SET budget_used = budget_used + ?
                WHERE id = ?
            ''', (cost, key_id))
            await db.commit()
            return True
    
    async def reset_all_budgets(self) -> int:
        """Reset all budgets that need resetting (for scheduled task)"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        async with self.get_db() as db:
            cursor = await db.execute('''
                UPDATE api_keys
                SET budget_used = 0, budget_reset_date = ?
                WHERE budget_limit IS NOT NULL AND budget_reset_date != ?
            ''', (today, today))
            await db.commit()
            return cursor.rowcount
    
    async def update_provider_rotation_index(self, key_id: int, new_index: int) -> bool:
        """Update the provider rotation index for an API key"""
        async with self.get_db() as db:
            cursor = await db.execute('''
                UPDATE api_keys
                SET provider_rotation_index = ?
                WHERE id = ?
            ''', (new_index, key_id))
            await db.commit()
            return cursor.rowcount > 0
    
    async def delete_key(self, key_id: int) -> bool:
        """Delete an API key but PRESERVE related usage logs for analytics"""
        async with self.get_db() as db:
            # Delete rate limits
            await db.execute('DELETE FROM rate_limits WHERE api_key_id = ?', (key_id,))
            
            # NOTE: usage_logs and large_context_logs are PRESERVED for analytics
            # We don't delete from usage_logs or large_context_logs here
            
            # Delete key
            cursor = await db.execute('DELETE FROM api_keys WHERE id = ?', (key_id,))
            
            await db.commit()
            return cursor.rowcount > 0
    
    async def toggle_key_proxy(self, key_id: int) -> bool:
        """Toggle the use_proxy setting for an API key"""
        async with self.get_db() as db:
            # Toggle the use_proxy value (1 becomes 0, 0 becomes 1)
            cursor = await db.execute('''
                UPDATE api_keys
                SET use_proxy = CASE WHEN use_proxy = 1 THEN 0 ELSE 1 END
                WHERE id = ?
            ''', (key_id,))
            await db.commit()
            return cursor.rowcount > 0
    
    async def log_usage(self, api_key_id: int, model: str, tokens_used: int = 0,
                       input_tokens: int = 0, output_tokens: int = 0,
                       success: bool = True, error_message: str = None, 
                       client_ip: str = None, is_cache_hit: bool = False):
        """Log API usage with token breakdown, client IP, and cost calculation"""
        cost = await self.calculate_cost(model, input_tokens, output_tokens)
        
        async with self.get_db() as db:
            # Insert usage log with cost and cache hit info
            cursor = await db.execute('''
                INSERT INTO usage_logs (api_key_id, model, tokens_used, input_tokens, output_tokens, success, is_cache_hit, error_message, client_ip, cost)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (api_key_id, model, tokens_used, input_tokens, output_tokens, success, 1 if is_cache_hit else 0, error_message, client_ip, cost))
            
            # Update total tokens used for the key
            if success and tokens_used > 0:
                await db.execute('''
                    UPDATE api_keys
                    SET total_tokens_used = total_tokens_used + ?
                    WHERE id = ?
                ''', (tokens_used, api_key_id))
            
            await db.commit()
        
        # Queue sync to Turso for analytics persistence
        self.queue_turso_sync('''
            INSERT INTO usage_logs (api_key_id, model, tokens_used, input_tokens, output_tokens, success, is_cache_hit, error_message, client_ip, cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (api_key_id, model, tokens_used, input_tokens, output_tokens, success, 1 if is_cache_hit else 0, error_message, client_ip, cost))
        
        # Also sync the token update for the key
        if success and tokens_used > 0:
            self.queue_turso_sync('''
                UPDATE api_keys SET total_tokens_used = total_tokens_used + ? WHERE id = ?
            ''', (tokens_used, api_key_id))


    async def calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost based on model patterns from model_costs table"""
        if not model or (input_tokens == 0 and output_tokens == 0):
            return 0.0
            
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            # Try to find a matching pattern (simple LIKE check)
            cursor = await db.execute('''
                SELECT input_cost_per_1m, output_cost_per_1m
                FROM model_costs
                WHERE ? LIKE '%' || model_pattern || '%'
                ORDER BY length(model_pattern) DESC
                LIMIT 1
            ''', (model,))
            row = await cursor.fetchone()
            
            if row:
                input_cost = (input_tokens / 1_000_000.0) * row['input_cost_per_1m']
                output_cost = (output_tokens / 1_000_000.0) * row['output_cost_per_1m']
                return input_cost + output_cost
                
        return 0.0

    async def get_model_costs(self) -> List[Dict[str, Any]]:
        """Get all model cost configurations (with caching)"""
        now_ts = datetime.now().timestamp()
        
        if self._costs_cache:
            costs, expiry = self._costs_cache
            if now_ts < expiry:
                return costs
                
        async with self.get_db() as db:
            if not self.use_turso:
                db.row_factory = aiosqlite.Row
            cursor = await db.execute('SELECT * FROM model_costs ORDER BY model_pattern ASC')
            rows = await cursor.fetchall()
            costs = [dict(row) for row in rows]
            
            # Update cache
            self._costs_cache = (costs, now_ts + self._costs_ttl)
            return costs

    async def update_model_cost(self, pattern: str, input_cost: float, output_cost: float) -> bool:
        """Add or update a model cost pattern"""
        async with self.get_db() as db:
            await db.execute('''
                INSERT INTO model_costs (model_pattern, input_cost_per_1m, output_cost_per_1m)
                VALUES (?, ?, ?)
                ON CONFLICT(model_pattern) DO UPDATE SET
                    input_cost_per_1m = excluded.input_cost_per_1m,
                    output_cost_per_1m = excluded.output_cost_per_1m
            ''', (pattern, input_cost, output_cost))
            await db.commit()
            return True

    async def delete_model_cost(self, cost_id: int) -> bool:
        """Delete a model cost pattern"""
        async with self.get_db() as db:
            cursor = await db.execute('DELETE FROM model_costs WHERE id = ?', (cost_id,))
            await db.commit()
            return cursor.rowcount > 0
    
    async def log_large_context(self, api_key_id: int, model: str, input_tokens: int = 0,
                                output_tokens: int = 0, total_tokens: int = 0,
                                client_ip: str = None, request_summary: str = None):
        """Log a large context request (exceeding token threshold)"""
        async with self.get_db() as db:
            await db.execute('''
                INSERT INTO large_context_logs (api_key_id, model, input_tokens, output_tokens, total_tokens, client_ip, request_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (api_key_id, model, input_tokens, output_tokens, total_tokens, client_ip, request_summary))
            
            await db.commit()
    
    async def get_large_context_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get the most recent large context logs"""
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT lcl.id, lcl.api_key_id, ak.key_prefix, lcl.model,
                       lcl.input_tokens, lcl.output_tokens, lcl.total_tokens,
                       lcl.client_ip, lcl.request_time, lcl.request_summary
                FROM large_context_logs lcl
                LEFT JOIN api_keys ak ON lcl.api_key_id = ak.id
                ORDER BY lcl.request_time DESC
                LIMIT ?
            ''', (limit,))
            
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def get_large_context_stats(self) -> Dict[str, Any]:
        """Get statistics about large context requests"""
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            
            # Total count and token stats
            cursor = await db.execute('''
                SELECT
                    COUNT(*) as total_requests,
                    COALESCE(SUM(total_tokens), 0) as total_tokens,
                    COALESCE(AVG(total_tokens), 0) as avg_tokens,
                    COALESCE(MAX(total_tokens), 0) as max_tokens
                FROM large_context_logs
            ''')
            stats = dict(await cursor.fetchone())
            
            # Top models by large context usage
            cursor = await db.execute('''
                SELECT model, COUNT(*) as request_count,
                       COALESCE(SUM(total_tokens), 0) as total_tokens
                FROM large_context_logs
                GROUP BY model
                ORDER BY request_count DESC
                LIMIT 5
            ''')
            top_models = [dict(row) for row in await cursor.fetchall()]
            
            stats['top_models'] = top_models
            return stats
    
    async def get_recent_logs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the most recent usage logs"""
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT ul.id, ul.api_key_id, ak.key_prefix, ul.model, ul.tokens_used,
                       ul.input_tokens, ul.output_tokens, ul.request_time, ul.success, ul.error_message, ul.client_ip, ul.cost
                FROM usage_logs ul
                LEFT JOIN api_keys ak ON ul.api_key_id = ak.id
                ORDER BY ul.request_time DESC
                LIMIT ?
            ''', (limit,))
            
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def get_key_request_logs(self, api_key_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """Get the most recent usage logs for a specific API key (without IP addresses)"""
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT ul.id, ul.model, ul.tokens_used,
                       ul.input_tokens, ul.output_tokens, ul.request_time, ul.success, ul.error_message, ul.cost
                FROM usage_logs ul
                WHERE ul.api_key_id = ?
                ORDER BY ul.request_time DESC
                LIMIT ?
            ''', (api_key_id, limit))
            
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    # Response cache functions
    async def get_cached_response(self, request_hash: str, embedding: List[float] = None, threshold: float = 0.95) -> Optional[Dict[str, Any]]:
        """Get a cached response by request hash or semantic similarity"""
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            
            # 1. Try exact match first (hash)
            cursor = await db.execute('''
                SELECT id, response_body, model, input_tokens, output_tokens, created_at, hit_count
                FROM response_cache
                WHERE request_hash = ? AND expires_at > datetime('now')
            ''', (request_hash,))
            row = await cursor.fetchone()
            
            if row:
                await db.execute('UPDATE response_cache SET hit_count = hit_count + 1 WHERE id = ?', (row['id'],))
                await db.commit()
                return {
                    "response_body": row['response_body'],
                    "model": row['model'],
                    "input_tokens": row['input_tokens'],
                    "output_tokens": row['output_tokens'],
                    "created_at": row['created_at'],
                    "hit_count": row['hit_count'] + 1,
                    "is_semantic": False
                }
            
            # 2. Try semantic match if embedding provided
            if embedding:
                import struct
                import math
                
                def cosine_similarity(v1, v2):
                    sum_xx, sum_yy, sum_xy = 0, 0, 0
                    for x, y in zip(v1, v2):
                        sum_xx += x*x
                        sum_yy += y*y
                        sum_xy += x*y
                    return sum_xy / math.sqrt(sum_xx*sum_yy) if sum_xx and sum_yy else 0

                # Fetch recent items for similarity check
                # Note: In production, use a vector DB or sqlite-vec. This is for MVP.
                async with db.execute('''
                    SELECT id, embedding, response_body, model, input_tokens, output_tokens, created_at, hit_count
                    FROM response_cache
                    WHERE embedding IS NOT NULL AND expires_at > datetime('now')
                    ORDER BY created_at DESC LIMIT 100
                ''') as cursor:
                    rows = await cursor.fetchall()
                    for r in rows:
                        stored_blob = r['embedding']
                        if not stored_blob: continue
                        
                        # Unpack float32 embedding
                        stored_vec = struct.unpack(f'{len(stored_blob)//4}f', stored_blob)
                        sim = cosine_similarity(embedding, stored_vec)
                        
                        if sim >= threshold:
                            await db.execute('UPDATE response_cache SET hit_count = hit_count + 1 WHERE id = ?', (r['id'],))
                            await db.commit()
                            return {
                                "response_body": r['response_body'],
                                "model": r['model'],
                                "input_tokens": r['input_tokens'],
                                "output_tokens": r['output_tokens'],
                                "created_at": r['created_at'],
                                "hit_count": r['hit_count'] + 1,
                                "is_semantic": True,
                                "similarity": sim
                            }
            
            return None
    
    async def cache_response(self, request_hash: str, response_body: str, model: str = None,
                            input_tokens: int = 0, output_tokens: int = 0, ttl_seconds: int = 3600,
                            prompt_text: str = None, embedding: List[float] = None):
        """Cache a response with TTL and optional embedding for semantic search"""
        import struct
        embedding_blob = None
        if embedding:
            embedding_blob = struct.pack(f'{len(embedding)}f', *embedding)
            
        async with self.get_db() as db:
            try:
                await db.execute('''
                    INSERT OR REPLACE INTO response_cache (request_hash, response_body, model, input_tokens, output_tokens, prompt_text, embedding, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now', ?))
                ''', (request_hash, response_body, model, input_tokens, output_tokens, prompt_text, embedding_blob, f'+{ttl_seconds} seconds'))
                await db.commit()
                return True
            except Exception as e:
                logger.error(f"Error caching response: {e}")
                return False
    
    async def cleanup_expired_cache(self) -> int:
        """Remove expired cache entries"""
        async with self.get_db() as db:
            cursor = await db.execute('''
                DELETE FROM response_cache WHERE expires_at < datetime('now')
            ''')
            await db.commit()
            return cursor.rowcount
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            
            cursor = await db.execute('''
                SELECT COUNT(*) as total_entries, SUM(hit_count) as total_hits
                FROM response_cache
                WHERE expires_at > datetime('now')
            ''')
            row = await cursor.fetchone()
            
            return {
                "total_entries": row['total_entries'] or 0,
                "total_hits": row['total_hits'] or 0
            }
    
    async def get_analytics(self, days: int = 7) -> Dict[str, Any]:
        """Get analytics data for the dashboard"""
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            
            # Total requests and tokens
            cursor = await db.execute('''
                SELECT
                    COUNT(*) as total_requests,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_requests,
                    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed_requests,
                    SUM(tokens_used) as total_tokens,
                    SUM(input_tokens) as total_input_tokens,
                    SUM(output_tokens) as total_output_tokens,
                    SUM(COALESCE(cost, 0)) as total_cost
                FROM usage_logs
                WHERE request_time >= datetime('now', ?)
            ''', (f'-{days} days',))
            totals = dict(await cursor.fetchone())
            
            # Requests per model
            cursor = await db.execute('''
                SELECT model, COUNT(*) as request_count,
                       SUM(tokens_used) as tokens,
                       SUM(input_tokens) as input_tokens,
                       SUM(output_tokens) as output_tokens
                FROM usage_logs
                WHERE request_time >= datetime('now', ?)
                GROUP BY model
                ORDER BY request_count DESC
            ''', (f'-{days} days',))
            models = [dict(row) for row in await cursor.fetchall()]
            
            # Requests per day
            cursor = await db.execute('''
                SELECT DATE(request_time) as date, COUNT(*) as request_count,
                       SUM(tokens_used) as tokens
                FROM usage_logs
                WHERE request_time >= datetime('now', ?)
                GROUP BY DATE(request_time)
                ORDER BY date ASC
            ''', (f'-{days} days',))
            daily = [dict(row) for row in await cursor.fetchall()]
            
            # Requests per hour (last 24 hours) with token breakdown
            cursor = await db.execute('''
                SELECT strftime('%Y-%m-%d %H', request_time) as hour,
                       COUNT(*) as request_count,
                       SUM(COALESCE(input_tokens, 0)) as input_tokens,
                       SUM(COALESCE(output_tokens, 0)) as output_tokens
                FROM usage_logs
                WHERE request_time >= datetime('now', '-1 day')
                GROUP BY strftime('%Y-%m-%d %H', request_time)
                ORDER BY hour ASC
            ''')
            hourly = [dict(row) for row in await cursor.fetchall()]
            
            return {
                "totals": totals,
                "models": models,
                "daily": daily,
                "hourly_usage": hourly,
                "period_days": days
            }
    
    async def check_rate_limit(self, api_key_id: int, max_rpm: int, max_rpd: int) -> Dict[str, Any]:
        """Check if request is within rate limits"""
        now = datetime.now()
        current_minute = now.strftime("%Y-%m-%d %H:%M")
        current_day = now.strftime("%Y-%m-%d")
        
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT minute_bucket, day_bucket, minute_count, day_count
                FROM rate_limits
                WHERE api_key_id = ?
            ''', (api_key_id,))
            
            row = await cursor.fetchone()
            
            if not row:
                # Initialize rate limits if not exists
                await db.execute('''
                    INSERT INTO rate_limits (api_key_id, minute_bucket, day_bucket, minute_count, day_count)
                    VALUES (?, ?, ?, 0, 0)
                ''', (api_key_id, current_minute, current_day))
                await db.commit()
                return {"allowed": True, "rpm_remaining": max_rpm, "rpd_remaining": max_rpd}
            
            minute_bucket = row['minute_bucket']
            day_bucket = row['day_bucket']
            minute_count = row['minute_count']
            day_count = row['day_count']
            
            # Reset minute counter if new minute
            if minute_bucket != current_minute:
                minute_count = 0
                minute_bucket = current_minute
            
            # Reset day counter if new day
            if day_bucket != current_day:
                day_count = 0
                day_bucket = current_day
            
            # Check limits
            if minute_count >= max_rpm:
                return {
                    "allowed": False,
                    "error": "ratelimited (please wait until rpm reset)",
                    "rpm_remaining": 0,
                    "rpd_remaining": max_rpd - day_count
                }
            
            if day_count >= max_rpd:
                return {
                    "allowed": False,
                    "error": "ratelimited (please wait until rpd reset)",
                    "rpm_remaining": max_rpm - minute_count,
                    "rpd_remaining": 0
                }
            
            # Increment counters
            await db.execute('''
                UPDATE rate_limits
                SET minute_bucket = ?, day_bucket = ?,
                    minute_count = ?, day_count = ?
                WHERE api_key_id = ?
            ''', (minute_bucket, day_bucket, minute_count + 1, day_count + 1, api_key_id))
            
            await db.commit()
            
            return {
                "allowed": True,
                "rpm_remaining": max_rpm - minute_count - 1,
                "rpd_remaining": max_rpd - day_count - 1
            }
    
    async def refresh_key_limits(self, key_id: int) -> bool:
        """Refresh/reset the RPM and RPD counters for a specific key"""
        async with self.get_db() as db:
            # Reset rate limits
            cursor = await db.execute('''
                UPDATE rate_limits
                SET minute_count = 0, day_count = 0,
                    minute_bucket = ?, day_bucket = ?,
                    last_reset = CURRENT_TIMESTAMP
                WHERE api_key_id = ?
            ''', (
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                datetime.now().strftime("%Y-%m-%d"),
                key_id
            ))
            
            # Update api_keys table
            await db.execute('''
                UPDATE api_keys
                SET current_rpm = 0, current_rpd = 0, last_reset = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (key_id,))
            
            await db.commit()
            return cursor.rowcount > 0
    
    async def set_refresh_schedule(self, key_id: int, refresh_hour: int) -> bool:
        """Set the auto-refresh hour for a key"""
        async with self.get_db() as db:
            cursor = await db.execute('''
                UPDATE api_keys
                SET refresh_hour = ?
                WHERE id = ?
            ''', (refresh_hour, key_id))
            
            await db.commit()
            return cursor.rowcount > 0
    
    async def get_key_usage(self, key_id: int) -> Dict[str, Any]:
        """Get current usage statistics for a key"""
        now = datetime.now()
        current_minute = now.strftime("%Y-%m-%d %H:%M")
        current_day = now.strftime("%Y-%m-%d")
        
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            
            # Get key info
            cursor = await db.execute('''
                SELECT max_rpm, max_rpd, refresh_hour, current_rpm, current_rpd, last_reset
                FROM api_keys
                WHERE id = ?
            ''', (key_id,))
            
            key_row = await cursor.fetchone()
            if not key_row:
                return None
            
            # Get rate limit info
            cursor = await db.execute('''
                SELECT minute_count, day_count, minute_bucket, day_bucket
                FROM rate_limits
                WHERE api_key_id = ?
            ''', (key_id,))
            
            rate_row = await cursor.fetchone()
            
            # Create rate_limits entry if it doesn't exist
            if not rate_row:
                await db.execute('''
                    INSERT INTO rate_limits (api_key_id, minute_bucket, day_bucket, minute_count, day_count)
                    VALUES (?, ?, ?, 0, 0)
                ''', (key_id, current_minute, current_day))
                await db.commit()
                
                return {
                    "max_rpm": key_row['max_rpm'],
                    "max_rpd": key_row['max_rpd'],
                    "refresh_hour": key_row['refresh_hour'],
                    "current_rpm": 0,
                    "current_rpd": 0,
                    "last_reset": key_row['last_reset'],
                    "minute_bucket": current_minute,
                    "day_bucket": current_day
                }
            
            # Check bucket freshness - if bucket is stale, count should be 0
            stored_minute_bucket = rate_row['minute_bucket']
            stored_day_bucket = rate_row['day_bucket']
            
            # If minute bucket is from a different minute, RPM count is effectively 0
            actual_rpm = rate_row['minute_count'] if stored_minute_bucket == current_minute else 0
            
            # If day bucket is from a different day, RPD count is effectively 0
            actual_rpd = rate_row['day_count'] if stored_day_bucket == current_day else 0
            
            return {
                "max_rpm": key_row['max_rpm'],
                "max_rpd": key_row['max_rpd'],
                "refresh_hour": key_row['refresh_hour'],
                "current_rpm": actual_rpm,
                "current_rpd": actual_rpd,
                "last_reset": key_row['last_reset'],
                "minute_bucket": stored_minute_bucket,
                "day_bucket": stored_day_bucket
            }
    
    async def ensure_all_rate_limits_exist(self) -> int:
        """Ensure all API keys have corresponding rate_limits entries"""
        now = datetime.now()
        current_minute = now.strftime("%Y-%m-%d %H:%M")
        current_day = now.strftime("%Y-%m-%d")
        
        async with self.get_db() as db:
            # Find keys without rate_limits entries
            cursor = await db.execute('''
                SELECT id FROM api_keys
                WHERE id NOT IN (SELECT api_key_id FROM rate_limits)
            ''')
            keys_without_limits = await cursor.fetchall()
            
            if keys_without_limits:
                for (key_id,) in keys_without_limits:
                    await db.execute('''
                        INSERT INTO rate_limits (api_key_id, minute_bucket, day_bucket, minute_count, day_count)
                        VALUES (?, ?, ?, 0, 0)
                    ''', (key_id, current_minute, current_day))
                await db.commit()
                return len(keys_without_limits)
            return 0
    
    async def check_scheduled_refreshes(self):
        """Check all keys and refresh those scheduled for current hour"""
        current_hour = datetime.now().hour
        
        async with self.get_db() as db:
            cursor = await db.execute('''
                SELECT id FROM api_keys
                WHERE refresh_hour = ? AND enabled = 1
            ''', (current_hour,))
            
            rows = await cursor.fetchall()
            refreshed_keys = []
            
            for row in rows:
                key_id = row['id']
                success = await self.refresh_key_limits(key_id)
                if success:
                    refreshed_keys.append(key_id)
            
            return refreshed_keys
    
    async def get_key_stats(self, api_key_id: int) -> Dict[str, Any]:
        """Get comprehensive usage statistics for a specific API key"""
        now = datetime.now()
        current_day = now.strftime("%Y-%m-%d")
        
        async with self.get_db() as db:
            db.row_factory = aiosqlite.Row
            
            # Get key info including max_rpd
            cursor = await db.execute('''
                SELECT max_rpd FROM api_keys WHERE id = ?
            ''', (api_key_id,))
            key_row = await cursor.fetchone()
            
            if not key_row:
                return None
            
            max_rpd = key_row['max_rpd']
            
            # Get current day count from rate_limits
            cursor = await db.execute('''
                SELECT day_count, day_bucket FROM rate_limits WHERE api_key_id = ?
            ''', (api_key_id,))
            rate_row = await cursor.fetchone()
            
            # Calculate RPD remaining
            if rate_row:
                day_bucket = rate_row['day_bucket']
                day_count = rate_row['day_count'] if day_bucket == current_day else 0
            else:
                day_count = 0
            
            rpd_remaining = max(0, max_rpd - day_count)
            
            # Get total requests and tokens for this key (all time)
            cursor = await db.execute('''
                SELECT
                    COUNT(*) as total_requests,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_requests,
                    SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed_requests,
                    COALESCE(SUM(tokens_used), 0) as total_tokens,
                    COALESCE(SUM(input_tokens), 0) as total_input_tokens,
                    COALESCE(SUM(output_tokens), 0) as total_output_tokens
                FROM usage_logs
                WHERE api_key_id = ?
            ''', (api_key_id,))
            totals = dict(await cursor.fetchone())
            
            # Get most used model with request count
            cursor = await db.execute('''
                SELECT model, COUNT(*) as request_count, SUM(tokens_used) as tokens
                FROM usage_logs
                WHERE api_key_id = ? AND success = 1
                GROUP BY model
                ORDER BY request_count DESC
                LIMIT 5
            ''', (api_key_id,))
            top_models = [dict(row) for row in await cursor.fetchall()]
            
            # Get today's usage
            cursor = await db.execute('''
                SELECT
                    COUNT(*) as requests_today,
                    COALESCE(SUM(tokens_used), 0) as tokens_today
                FROM usage_logs
                WHERE api_key_id = ? AND DATE(request_time) = DATE('now')
            ''', (api_key_id,))
            today = dict(await cursor.fetchone())
            
            # Get last 7 days usage trend
            cursor = await db.execute('''
                SELECT
                    DATE(request_time) as date,
                    COUNT(*) as requests,
                    COALESCE(SUM(tokens_used), 0) as tokens
                FROM usage_logs
                WHERE api_key_id = ? AND request_time >= datetime('now', '-7 days')
                GROUP BY DATE(request_time)
                ORDER BY date ASC
            ''', (api_key_id,))
            weekly_trend = [dict(row) for row in await cursor.fetchall()]
            
            # Get last request time
            cursor = await db.execute('''
                SELECT request_time FROM usage_logs
                WHERE api_key_id = ?
                ORDER BY request_time DESC
                LIMIT 1
            ''', (api_key_id,))
            last_request_row = await cursor.fetchone()
            last_request = last_request_row['request_time'] if last_request_row else None
            
            return {
                "rpd_used": day_count,
                "rpd_remaining": rpd_remaining,
                "max_rpd": max_rpd,
                "total_requests": totals['total_requests'] or 0,
                "successful_requests": totals['successful_requests'] or 0,
                "failed_requests": totals['failed_requests'] or 0,
                "total_tokens": totals['total_tokens'] or 0,
                "total_input_tokens": totals['total_input_tokens'] or 0,
                "total_output_tokens": totals['total_output_tokens'] or 0,
                "top_models": top_models,
                "most_used_model": top_models[0] if top_models else None,
                "requests_today": today['requests_today'] or 0,
                "tokens_today": today['tokens_today'] or 0,
                "weekly_trend": weekly_trend,
                "last_request": last_request
            }
