"""
Integration tests for the MOI's Proxy system.

**Feature: system-stability-fixes**

These tests validate the complete system functionality end-to-end,
verifying that all reported issues are resolved and the system
operates correctly under various conditions.

**Validates: All requirements**
"""

import pytest
import asyncio
import os
import sys
import json
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime
from typing import Dict, Any, List

# Add backend to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MockRequest:
    """Mock FastAPI Request object for testing."""
    
    def __init__(self, headers: dict = None, client_ip: str = "127.0.0.1"):
        self._headers = headers or {}
        self._client_ip = client_ip
        self.client = MagicMock()
        self.client.host = client_ip
    
    @property
    def headers(self):
        return self._headers


class MockWebSocket:
    """Mock WebSocket for testing."""
    
    def __init__(self, should_fail: bool = False):
        self.messages: List[Dict[str, Any]] = []
        self.should_fail = should_fail
        self.closed = False
        self.accepted = False
    
    async def accept(self):
        self.accepted = True
    
    async def send_json(self, data: Dict[str, Any]):
        if self.should_fail:
            raise ConnectionError("WebSocket connection lost")
        self.messages.append(data)
    
    async def receive_json(self):
        return {"type": "auth", "password": "test_password"}
    
    async def close(self, code: int = 1000):
        self.closed = True


class TestAPIKeyWorkflow:
    """
    Integration tests for the API key creation, usage, and analytics workflow.
    
    **Validates: Requirements 1.2, 2.1, 2.2, 2.3, 2.4, 2.5**
    """
    
    @pytest.mark.asyncio
    async def test_api_key_creation_and_retrieval(self):
        """
        Test that API keys can be created and retrieved correctly.
        
        **Validates: Requirements 1.2, 2.1, 2.4**
        """
        from database import Database
        
        # Create a test database
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Create an API key
            key_data = await db.create_api_key(
                name="Integration Test Key",
                max_rpm=100,
                max_rpd=1000
            )
            
            assert key_data is not None, "API key should be created"
            assert "api_key" in key_data, "Response should contain the key"
            assert "id" in key_data, "Response should contain the id"
            assert key_data["name"] == "Integration Test Key", "Name should match"
            
            # Retrieve the key
            key_info = await db.get_api_key_by_id(key_data["id"])
            
            assert key_info is not None, "Key should be retrievable"
            assert key_info["name"] == "Integration Test Key", "Retrieved name should match"
            assert key_info["max_rpm"] == 100, "RPM limit should match"
            assert key_info["max_rpd"] == 1000, "RPD limit should match"
            
        finally:
            await db.close()
    
    @pytest.mark.asyncio
    async def test_api_key_update_and_persistence(self):
        """
        Test that API key updates are persisted correctly.
        
        **Validates: Requirements 2.2, 2.3**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Create an API key
            key_data = await db.create_api_key(
                name="Update Test Key",
                max_rpm=50,
                max_rpd=500
            )
            
            key_id = key_data["id"]
            
            # Update the key limits
            await db.update_api_key(
                key_id,
                max_rpm=200,
                max_rpd=2000
            )
            
            # Retrieve and verify
            updated_key = await db.get_api_key_by_id(key_id)
            
            assert updated_key["max_rpm"] == 200, "Updated RPM should be persisted"
            assert updated_key["max_rpd"] == 2000, "Updated RPD should be persisted"
            
        finally:
            await db.close()
    
    @pytest.mark.asyncio
    async def test_api_key_deletion(self):
        """
        Test that API key deletion removes the key from the database.
        
        **Validates: Requirements 2.5**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Create an API key
            key_data = await db.create_api_key(
                name="Delete Test Key",
                max_rpm=50,
                max_rpd=500
            )
            
            key_id = key_data["id"]
            
            # Verify key exists
            key_info = await db.get_api_key_by_id(key_id)
            assert key_info is not None, "Key should exist before deletion"
            
            # Delete the key
            await db.delete_api_key(key_id)
            
            # Verify key is deleted
            deleted_key = await db.get_api_key_by_id(key_id)
            assert deleted_key is None, "Key should not exist after deletion"
            
        finally:
            await db.close()


class TestDatabaseConnectivity:
    """
    Integration tests for database connectivity and operations.
    
    **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5**
    """
    
    @pytest.mark.asyncio
    async def test_database_initialization(self):
        """
        Test that the database initializes correctly with all required tables.
        
        **Validates: Requirements 1.1**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Verify tables exist by querying them
            async with db.get_db() as conn:
                # Check api_keys table
                cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='api_keys'")
                result = await cursor.fetchone()
                assert result is not None, "api_keys table should exist"
                
                # Check usage_logs table
                cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='usage_logs'")
                result = await cursor.fetchone()
                assert result is not None, "usage_logs table should exist"
                
                # Check model_costs table
                cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='model_costs'")
                result = await cursor.fetchone()
                assert result is not None, "model_costs table should exist"
                
        finally:
            await db.close()
    
    @pytest.mark.asyncio
    async def test_usage_logging(self):
        """
        Test that usage data is logged correctly.
        
        **Validates: Requirements 1.3**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Create an API key first
            key_data = await db.create_api_key(
                name="Usage Log Test Key",
                max_rpm=100,
                max_rpd=1000
            )
            
            key_id = key_data["id"]
            
            # Log usage
            await db.log_usage(
                key_id,
                model="gpt-4",
                tokens_used=1000,
                input_tokens=500,
                output_tokens=500,
                success=True,
                client_ip="127.0.0.1"
            )
            
            # Retrieve usage logs
            logs = await db.get_key_request_logs(key_id, limit=10)
            
            assert len(logs) >= 1, "Usage log should be recorded"
            assert logs[0]["model"] == "gpt-4", "Model should match"
            assert logs[0]["tokens_used"] == 1000, "Tokens should match"
            
        finally:
            await db.close()
    
    @pytest.mark.asyncio
    async def test_data_retrieval_accuracy(self):
        """
        Test that data retrieved from the database is accurate.
        
        **Validates: Requirements 1.4**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Create multiple API keys
            keys = []
            for i in range(3):
                key_data = await db.create_api_key(
                    name=f"Accuracy Test Key {i}",
                    max_rpm=100 + i * 10,
                    max_rpd=1000 + i * 100
                )
                keys.append(key_data)
            
            # Retrieve all keys
            all_keys = await db.get_all_keys()
            
            assert len(all_keys) >= 3, "All created keys should be retrievable"
            
            # Verify each key's data
            for i, key in enumerate(keys):
                retrieved = await db.get_api_key_by_id(key["id"])
                assert retrieved["name"] == f"Accuracy Test Key {i}", f"Key {i} name should match"
                assert retrieved["max_rpm"] == 100 + i * 10, f"Key {i} RPM should match"
                assert retrieved["max_rpd"] == 1000 + i * 100, f"Key {i} RPD should match"
                
        finally:
            await db.close()


class TestHeaderProcessing:
    """
    Integration tests for header processing and encoding.
    
    **Validates: Requirements 5.1, 5.2, 5.3, 5.5**
    """
    
    def test_bearer_token_processing(self):
        """
        Test that Bearer tokens are processed correctly.
        
        **Validates: Requirements 5.1**
        """
        from proxy_server import ensure_string_header, build_request_headers
        
        # Test string token
        token = "sk-test-token-12345"
        result = ensure_string_header(token)
        assert result == token, "String token should be unchanged"
        
        # Test bytes token
        token_bytes = b"sk-test-token-12345"
        result = ensure_string_header(token_bytes)
        assert result == "sk-test-token-12345", "Bytes token should be decoded"
        
        # Test in build_request_headers
        headers = build_request_headers(api_key=token, no_auth=False)
        assert headers["Authorization"] == f"Bearer {token}", "Authorization header should be correct"
    
    def test_header_validation(self):
        """
        Test that header validation works correctly.
        
        **Validates: Requirements 5.5**
        """
        from proxy_server import validate_header_value, sanitize_headers
        
        # Valid header
        is_valid, error = validate_header_value("Test-Header", "valid value")
        assert is_valid is True, "Valid header should pass"
        
        # Invalid header with newline
        is_valid, error = validate_header_value("Test-Header", "invalid\nvalue")
        assert is_valid is False, "Header with newline should fail"
        
        # Sanitize headers
        headers = {"Test": "value", "Bad": "bad\x00value"}
        sanitized = sanitize_headers(headers, log_errors=False)
        assert "\x00" not in sanitized.get("Bad", ""), "Control chars should be removed"


class TestContentFiltering:
    """
    Integration tests for content filtering.
    
    **Validates: Requirements 7.1, 7.2, 7.3, 7.4, 7.5**
    """
    
    def test_code_content_allowed(self):
        """
        Test that legitimate code content is allowed.
        
        **Validates: Requirements 7.1, 7.4**
        """
        from proxy_server import jailbreak_check, is_code_context
        
        # Code snippet should be detected as code context
        code_content = """
        ```python
        def hello_world():
            print("Hello, World!")
        ```
        """
        
        assert is_code_context(code_content) is True, "Code block should be detected"
        
        # Code content should pass jailbreak check
        messages = [{"role": "user", "content": code_content}]
        is_blocked, reason = jailbreak_check(messages)
        assert is_blocked is False, f"Code content should not be blocked: {reason}"
    
    def test_educational_content_allowed(self):
        """
        Test that educational content is allowed.
        
        **Validates: Requirements 7.3, 7.4**
        """
        from proxy_server import jailbreak_check, is_code_context
        
        educational_content = """
        This is a tutorial on how to prevent SQL injection attacks.
        Example of vulnerable code:
        SELECT * FROM users WHERE id = user_input
        
        Secure coding practice:
        Use parameterized queries to sanitize input.
        """
        
        assert is_code_context(educational_content) is True, "Educational content should be detected"
        
        messages = [{"role": "user", "content": educational_content}]
        is_blocked, reason = jailbreak_check(messages)
        assert is_blocked is False, f"Educational content should not be blocked: {reason}"
    
    def test_actual_attack_blocked(self):
        """
        Test that actual attack patterns are blocked.
        
        **Validates: Requirements 7.2, 7.5**
        """
        from proxy_server import is_actual_attack_pattern
        
        # Shell command injection
        attack_content = "; rm -rf /"
        is_attack, reason = is_actual_attack_pattern(attack_content)
        assert is_attack is True, "Shell injection should be detected"
        
        # Path traversal
        attack_content = "../../../../etc/passwd"
        is_attack, reason = is_actual_attack_pattern(attack_content)
        assert is_attack is True, "Path traversal should be detected"


class TestAsyncOperations:
    """
    Integration tests for async operations.
    
    **Validates: Requirements 6.1, 6.2, 6.3, 6.5**
    """
    
    @pytest.mark.asyncio
    async def test_database_async_context_manager(self):
        """
        Test that database async context manager works correctly.
        
        **Validates: Requirements 6.1, 6.2**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Use async context manager
            async with db.get_db() as conn:
                cursor = await conn.execute("SELECT 1")
                result = await cursor.fetchone()
                assert result is not None, "Query should return result"
                
        finally:
            await db.close()
    
    @pytest.mark.asyncio
    async def test_concurrent_database_operations(self):
        """
        Test that concurrent database operations work correctly.
        
        **Validates: Requirements 6.5**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Create multiple keys concurrently
            async def create_key(name: str):
                return await db.create_api_key(
                    name=name,
                    max_rpm=100,
                    max_rpd=1000
                )
            
            # Run concurrent operations
            tasks = [create_key(f"Concurrent Key {i}") for i in range(5)]
            results = await asyncio.gather(*tasks)
            
            # Verify all keys were created
            assert len(results) == 5, "All keys should be created"
            for i, result in enumerate(results):
                assert result is not None, f"Key {i} should be created"
                assert "id" in result, f"Key {i} should have an id"
                
        finally:
            await db.close()


class TestRateLimiting:
    """
    Integration tests for rate limiting functionality.
    
    **Validates: Requirements 8.1, 8.2, 8.5**
    """
    
    @pytest.mark.asyncio
    async def test_rate_limit_enforcement(self):
        """
        Test that rate limits are enforced correctly.
        
        **Validates: Requirements 8.1, 8.5**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Create a key with low limits
            key_data = await db.create_api_key(
                name="Rate Limit Test Key",
                max_rpm=2,
                max_rpd=5
            )
            
            key_id = key_data["id"]
            api_key = key_data["api_key"]
            
            # Simulate requests
            for i in range(3):
                result = await db.check_and_increment_rate_limit(api_key)
                if i < 2:
                    assert result[0] is True, f"Request {i} should be allowed"
                else:
                    # Third request should be rate limited (RPM exceeded)
                    assert result[0] is False, f"Request {i} should be rate limited"
                    
        finally:
            await db.close()


class TestAnalyticsSystem:
    """
    Integration tests for the analytics system.
    
    **Validates: Requirements 3.1, 3.4, 3.5**
    """
    
    @pytest.mark.asyncio
    async def test_request_counter_accuracy(self):
        """
        Test that request counters are accurate.
        
        **Validates: Requirements 3.1, 3.4**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Create a key
            key_data = await db.create_api_key(
                name="Analytics Test Key",
                max_rpm=100,
                max_rpd=1000
            )
            
            key_id = key_data["id"]
            
            # Log multiple usage events
            for i in range(5):
                await db.log_usage(
                    key_id,
                    model="gpt-4",
                    tokens_used=100,
                    input_tokens=50,
                    output_tokens=50,
                    success=True,
                    client_ip="127.0.0.1"
                )
            
            # Get analytics
            analytics = await db.get_analytics()
            
            assert analytics["totals"]["total_requests"] >= 5, "Request count should be at least 5"
            
        finally:
            await db.close()


class TestWebSocketLogging:
    """
    Integration tests for WebSocket logging.
    
    **Validates: Requirements 4.1, 4.2, 4.4**
    """
    
    @pytest.mark.asyncio
    async def test_log_broadcast(self):
        """
        Test that logs are broadcast to connected clients.
        
        **Validates: Requirements 4.1**
        """
        import proxy_server
        
        # Store original state
        original_websockets = proxy_server.active_websockets.copy()
        original_buffer = proxy_server._log_buffer.copy()
        
        try:
            # Clear state
            proxy_server.active_websockets.clear()
            proxy_server._log_buffer.clear()
            
            # Create mock client
            mock_client = MockWebSocket()
            proxy_server.active_websockets.append(mock_client)
            
            # Broadcast a log
            await proxy_server.broadcast_log("Test message", "INFO")
            
            # Verify client received the message
            assert len(mock_client.messages) == 1, "Client should receive one message"
            assert mock_client.messages[0]["message"] == "Test message", "Message content should match"
            
        finally:
            # Restore state
            proxy_server.active_websockets.clear()
            proxy_server.active_websockets.extend(original_websockets)
            proxy_server._log_buffer.clear()
            proxy_server._log_buffer.extend(original_buffer)
    
    @pytest.mark.asyncio
    async def test_log_buffer_management(self):
        """
        Test that log buffer manages memory correctly.
        
        **Validates: Requirements 4.4**
        """
        import proxy_server
        
        # Store original state
        original_buffer = proxy_server._log_buffer.copy()
        original_websockets = proxy_server.active_websockets.copy()
        
        try:
            # Clear state
            proxy_server._log_buffer.clear()
            proxy_server.active_websockets.clear()
            
            # Add more logs than buffer max size
            for i in range(proxy_server._LOG_BUFFER_MAX_SIZE + 50):
                await proxy_server.broadcast_log(f"Log {i}", "INFO")
            
            # Verify buffer size is capped
            buffer = await proxy_server.get_log_buffer()
            assert len(buffer) <= proxy_server._LOG_BUFFER_MAX_SIZE, \
                "Buffer should not exceed max size"
            
            # Verify most recent logs are kept
            assert buffer[-1]["message"] == f"Log {proxy_server._LOG_BUFFER_MAX_SIZE + 49}", \
                "Most recent log should be in buffer"
                
        finally:
            # Restore state
            proxy_server._log_buffer.clear()
            proxy_server._log_buffer.extend(original_buffer)
            proxy_server.active_websockets.clear()
            proxy_server.active_websockets.extend(original_websockets)


class TestErrorHandling:
    """
    Integration tests for error handling and recovery.
    
    **Validates: Requirements 1.5, 6.4**
    """
    
    @pytest.mark.asyncio
    async def test_database_error_handling(self):
        """
        Test that database errors are handled gracefully.
        
        **Validates: Requirements 1.5**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        await db.init_db()
        
        try:
            # Try to get a non-existent key
            result = await db.get_api_key_by_id(99999)
            assert result is None, "Non-existent key should return None, not raise error"
            
            # Try to delete a non-existent key
            try:
                await db.delete_api_key(99999)
                # Should not raise an error
            except Exception as e:
                pytest.fail(f"Deleting non-existent key should not raise error: {e}")
                
        finally:
            await db.close()
    
    @pytest.mark.asyncio
    async def test_turso_status_reporting(self):
        """
        Test that Turso connection status is reported correctly.
        
        **Validates: Requirements 1.5**
        """
        from database import Database
        
        db = Database(db_path=":memory:")
        
        try:
            # Get status without Turso configured
            status = db.get_turso_status()
            
            assert "available" in status, "Status should include 'available'"
            assert "connected" in status, "Status should include 'connected'"
            assert "last_error" in status, "Status should include 'last_error'"
            
        finally:
            await db.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])







