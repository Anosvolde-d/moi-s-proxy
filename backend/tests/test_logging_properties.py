"""
Property-based tests for the request logging system.

**Feature: system-stability-fixes**

These tests validate the correctness properties defined in the design document
for the WebSocket logging system, log display continuity, and memory management.
"""

import pytest
import asyncio
import os
import sys
from unittest.mock import patch, MagicMock, AsyncMock
from hypothesis import given, strategies as st, settings, assume
from datetime import datetime
from typing import List, Dict, Any

# Add backend to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Strategy for generating log levels
log_level_strategy = st.sampled_from(["INFO", "WARNING", "ERROR", "DEBUG"])

# Strategy for generating log messages
log_message_strategy = st.text(
    alphabet=st.characters(whitelist_categories=('L', 'N', 'P', 'S'), 
                           whitelist_characters=' '),
    min_size=1,
    max_size=500
).filter(lambda x: x.strip())  # Ensure non-empty after strip

# Strategy for generating sequences of log entries
log_entry_strategy = st.fixed_dictionaries({
    "level": log_level_strategy,
    "message": log_message_strategy,
    "timestamp": st.just(datetime.now().isoformat())
})


class MockWebSocket:
    """Mock WebSocket for testing broadcast functionality."""
    
    def __init__(self, should_fail: bool = False, fail_after: int = -1):
        self.messages: List[Dict[str, Any]] = []
        self.should_fail = should_fail
        self.fail_after = fail_after
        self._send_count = 0
        self.closed = False
    
    async def send_json(self, data: Dict[str, Any]):
        self._send_count += 1
        if self.should_fail or (self.fail_after >= 0 and self._send_count > self.fail_after):
            raise ConnectionError("WebSocket connection lost")
        self.messages.append(data)
    
    async def close(self, code: int = 1000):
        self.closed = True


class TestLogDisplayContinuity:
    """
    **Feature: system-stability-fixes, Property 12: Log display continuity**
    
    *For any* WebSocket log stream, the dashboard should display logs 
    continuously without unexpected clearing.
    
    **Validates: Requirements 4.1**
    """
    
    @pytest.mark.asyncio
    @settings(max_examples=100, deadline=None)  # Disable deadline due to module loading time
    @given(
        log_entries=st.lists(log_entry_strategy, min_size=1, max_size=50)
    )
    async def test_logs_broadcast_to_all_connected_clients(self, log_entries: List[Dict[str, Any]]):
        """
        Property: For any sequence of log entries, all connected WebSocket clients
        should receive all log entries in order.
        
        **Feature: system-stability-fixes, Property 12: Log display continuity**
        **Validates: Requirements 4.1**
        """
        # Import the module to test
        import proxy_server
        
        # Create mock WebSocket clients
        num_clients = 3
        mock_clients = [MockWebSocket() for _ in range(num_clients)]
        
        # Store original state
        original_websockets = proxy_server.active_websockets.copy()
        original_buffer = proxy_server._log_buffer.copy() if hasattr(proxy_server, '_log_buffer') else []
        
        try:
            # Clear and set up test state
            proxy_server.active_websockets.clear()
            proxy_server.active_websockets.extend(mock_clients)
            
            if hasattr(proxy_server, '_log_buffer'):
                proxy_server._log_buffer.clear()
            
            # Broadcast all log entries
            for entry in log_entries:
                await proxy_server.broadcast_log(entry["message"], entry["level"])
            
            # Property: All clients should receive all non-PING log entries
            for client in mock_clients:
                # Each client should have received all log entries
                assert len(client.messages) == len(log_entries), \
                    f"Client should receive {len(log_entries)} messages, got {len(client.messages)}"
                
                # Messages should be in order
                for i, (sent, received) in enumerate(zip(log_entries, client.messages)):
                    assert received["message"] == sent["message"], \
                        f"Message {i} content mismatch"
                    assert received["level"] == sent["level"], \
                        f"Message {i} level mismatch"
        
        finally:
            # Restore original state
            proxy_server.active_websockets.clear()
            proxy_server.active_websockets.extend(original_websockets)
            if hasattr(proxy_server, '_log_buffer'):
                proxy_server._log_buffer.clear()
                proxy_server._log_buffer.extend(original_buffer)
    
    @pytest.mark.asyncio
    @settings(max_examples=100, deadline=None)  # Disable deadline due to async operations
    @given(
        log_entries=st.lists(log_entry_strategy, min_size=1, max_size=30)
    )
    async def test_log_buffer_stores_recent_entries(self, log_entries: List[Dict[str, Any]]):
        """
        Property: For any sequence of log entries, the log buffer should store
        recent entries for new connections.
        
        **Feature: system-stability-fixes, Property 12: Log display continuity**
        **Validates: Requirements 4.1**
        """
        import proxy_server
        
        # Store original state
        original_buffer = proxy_server._log_buffer.copy() if hasattr(proxy_server, '_log_buffer') else []
        original_websockets = proxy_server.active_websockets.copy()
        
        try:
            # Clear state
            proxy_server.active_websockets.clear()
            if hasattr(proxy_server, '_log_buffer'):
                proxy_server._log_buffer.clear()
            
            # Broadcast all log entries (no clients connected)
            for entry in log_entries:
                await proxy_server.broadcast_log(entry["message"], entry["level"])
            
            # Property: Log buffer should contain all entries (up to max size)
            buffer = await proxy_server.get_log_buffer()
            expected_count = min(len(log_entries), proxy_server._LOG_BUFFER_MAX_SIZE)
            
            assert len(buffer) == expected_count, \
                f"Buffer should contain {expected_count} entries, got {len(buffer)}"
            
            # If we sent fewer than max, all should be in buffer
            if len(log_entries) <= proxy_server._LOG_BUFFER_MAX_SIZE:
                for i, entry in enumerate(log_entries):
                    assert buffer[i]["message"] == entry["message"], \
                        f"Buffer entry {i} message mismatch"
        
        finally:
            # Restore original state
            proxy_server.active_websockets.clear()
            proxy_server.active_websockets.extend(original_websockets)
            if hasattr(proxy_server, '_log_buffer'):
                proxy_server._log_buffer.clear()
                proxy_server._log_buffer.extend(original_buffer)
    
    @pytest.mark.asyncio
    @settings(max_examples=50, deadline=None)  # Disable deadline due to async operations
    @given(
        log_entries=st.lists(log_entry_strategy, min_size=1, max_size=20),
        failing_client_index=st.integers(min_value=0, max_value=2)
    )
    async def test_disconnected_client_removed_without_affecting_others(
        self, 
        log_entries: List[Dict[str, Any]], 
        failing_client_index: int
    ):
        """
        Property: For any WebSocket client that disconnects, other clients
        should continue receiving logs without interruption.
        
        **Feature: system-stability-fixes, Property 12: Log display continuity**
        **Validates: Requirements 4.1**
        """
        import proxy_server
        
        # Create mock clients - one will fail
        num_clients = 3
        mock_clients = [
            MockWebSocket(should_fail=(i == failing_client_index))
            for i in range(num_clients)
        ]
        
        # Store original state
        original_websockets = proxy_server.active_websockets.copy()
        original_buffer = proxy_server._log_buffer.copy() if hasattr(proxy_server, '_log_buffer') else []
        
        try:
            # Set up test state
            proxy_server.active_websockets.clear()
            proxy_server.active_websockets.extend(mock_clients)
            
            if hasattr(proxy_server, '_log_buffer'):
                proxy_server._log_buffer.clear()
            
            # Broadcast all log entries
            for entry in log_entries:
                await proxy_server.broadcast_log(entry["message"], entry["level"])
            
            # Property: Non-failing clients should receive all messages
            for i, client in enumerate(mock_clients):
                if i != failing_client_index:
                    assert len(client.messages) == len(log_entries), \
                        f"Non-failing client {i} should receive all {len(log_entries)} messages"
            
            # Property: Failing client should be removed from active list
            assert mock_clients[failing_client_index] not in proxy_server.active_websockets, \
                "Failing client should be removed from active websockets"
        
        finally:
            # Restore original state
            proxy_server.active_websockets.clear()
            proxy_server.active_websockets.extend(original_websockets)
            if hasattr(proxy_server, '_log_buffer'):
                proxy_server._log_buffer.clear()
                proxy_server._log_buffer.extend(original_buffer)


class TestLogBufferMemoryManagement:
    """
    **Feature: system-stability-fixes, Property 15: Memory management for logs**
    
    *For any* large accumulation of log data, the system should manage memory 
    usage while maintaining recent log visibility.
    
    **Validates: Requirements 4.4**
    """
    
    @pytest.mark.asyncio
    @settings(max_examples=50, deadline=None)  # Disable deadline due to async operations
    @given(
        num_entries=st.integers(min_value=150, max_value=300)
    )
    async def test_log_buffer_respects_max_size(self, num_entries: int):
        """
        Property: For any number of log entries exceeding the buffer max size,
        the buffer should only retain the most recent entries up to max size.
        
        **Feature: system-stability-fixes, Property 15: Memory management for logs**
        **Validates: Requirements 4.4**
        """
        import proxy_server
        
        # Store original state
        original_buffer = proxy_server._log_buffer.copy() if hasattr(proxy_server, '_log_buffer') else []
        original_websockets = proxy_server.active_websockets.copy()
        
        try:
            # Clear state
            proxy_server.active_websockets.clear()
            if hasattr(proxy_server, '_log_buffer'):
                proxy_server._log_buffer.clear()
            
            # Generate and broadcast many log entries
            for i in range(num_entries):
                await proxy_server.broadcast_log(f"Log entry {i}", "INFO")
            
            # Property: Buffer size should not exceed max
            buffer = await proxy_server.get_log_buffer()
            assert len(buffer) <= proxy_server._LOG_BUFFER_MAX_SIZE, \
                f"Buffer size {len(buffer)} exceeds max {proxy_server._LOG_BUFFER_MAX_SIZE}"
            
            # Property: Buffer should contain the most recent entries
            if num_entries > proxy_server._LOG_BUFFER_MAX_SIZE:
                # The last entry in buffer should be the last one we sent
                assert buffer[-1]["message"] == f"Log entry {num_entries - 1}", \
                    "Buffer should contain most recent entry"
                
                # The first entry should be from (num_entries - max_size)
                expected_first = num_entries - proxy_server._LOG_BUFFER_MAX_SIZE
                assert buffer[0]["message"] == f"Log entry {expected_first}", \
                    f"Buffer should start from entry {expected_first}"
        
        finally:
            # Restore original state
            proxy_server.active_websockets.clear()
            proxy_server.active_websockets.extend(original_websockets)
            if hasattr(proxy_server, '_log_buffer'):
                proxy_server._log_buffer.clear()
                proxy_server._log_buffer.extend(original_buffer)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
