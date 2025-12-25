"""
Property-based tests for header processing and encoding.

**Feature: system-stability-fixes**

These tests validate the correctness properties defined in the design document
for header processing, particularly Bearer token encoding and header validation.
"""

import pytest
import asyncio
import os
import sys
from unittest.mock import patch, MagicMock, AsyncMock
from hypothesis import given, strategies as st, settings, assume

# Add backend to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proxy_server import (
    ensure_string_header,
    validate_header_value,
    sanitize_headers,
    build_request_headers,
    extract_bearer_token
)


# Strategy for generating valid Bearer tokens (alphanumeric with some special chars)
bearer_token_strategy = st.text(
    alphabet=st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"),
    min_size=10,
    max_size=256
)

# Strategy for generating valid API keys
api_key_strategy = st.text(
    alphabet=st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"),
    min_size=10,
    max_size=128
)

# Strategy for generating valid header values (printable ASCII without control chars)
header_value_strategy = st.text(
    alphabet=st.characters(
        whitelist_categories=('L', 'N', 'P', 'S', 'Zs'),
        blacklist_characters='\r\n\x00\x7f'
    ),
    min_size=0,
    max_size=500
)

# Strategy for generating header names
header_name_strategy = st.text(
    alphabet=st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"),
    min_size=1,
    max_size=50
)


class MockRequest:
    """Mock FastAPI Request object for testing."""
    
    def __init__(self, headers: dict = None):
        self._headers = headers or {}
    
    @property
    def headers(self):
        return self._headers


class TestBearerTokenEncoding:
    """
    **Feature: system-stability-fixes, Property 17: Bearer token encoding**
    
    *For any* valid Bearer token, the system should process headers without 
    byte string encoding errors.
    
    **Validates: Requirements 5.1**
    """
    
    @settings(max_examples=100)
    @given(token=bearer_token_strategy)
    def test_ensure_string_header_with_string_token(self, token: str):
        """
        Property: For any valid string token, ensure_string_header should 
        return the same string.
        
        **Feature: system-stability-fixes, Property 17: Bearer token encoding**
        **Validates: Requirements 5.1**
        """
        result = ensure_string_header(token)
        assert isinstance(result, str), "Result should be a string"
        assert result == token, "String input should be returned unchanged"
    
    @settings(max_examples=100)
    @given(token=bearer_token_strategy)
    def test_ensure_string_header_with_bytes_token(self, token: str):
        """
        Property: For any valid token encoded as bytes, ensure_string_header 
        should decode it to the original string.
        
        **Feature: system-stability-fixes, Property 17: Bearer token encoding**
        **Validates: Requirements 5.1**
        """
        # Convert to bytes
        token_bytes = token.encode('utf-8')
        
        result = ensure_string_header(token_bytes)
        assert isinstance(result, str), "Result should be a string"
        assert result == token, "Bytes should be decoded to original string"
    
    @settings(max_examples=100)
    @given(token=bearer_token_strategy)
    def test_build_request_headers_produces_valid_authorization(self, token: str):
        """
        Property: For any valid API key, build_request_headers should produce
        a properly formatted Authorization header without encoding errors.
        
        **Feature: system-stability-fixes, Property 17: Bearer token encoding**
        **Validates: Requirements 5.1**
        """
        headers = build_request_headers(api_key=token, no_auth=False)
        
        assert "Authorization" in headers, "Authorization header should be present"
        auth_header = headers["Authorization"]
        assert isinstance(auth_header, str), "Authorization header should be a string"
        assert auth_header.startswith("Bearer "), "Should start with 'Bearer '"
        assert auth_header == f"Bearer {token}", "Should contain the original token"
    
    @settings(max_examples=100)
    @given(token=bearer_token_strategy)
    def test_build_request_headers_with_bytes_api_key(self, token: str):
        """
        Property: For any API key provided as bytes, build_request_headers 
        should handle encoding correctly.
        
        **Feature: system-stability-fixes, Property 17: Bearer token encoding**
        **Validates: Requirements 5.1**
        """
        # Pass token as bytes
        token_bytes = token.encode('utf-8')
        
        headers = build_request_headers(api_key=token_bytes, no_auth=False)
        
        assert "Authorization" in headers, "Authorization header should be present"
        auth_header = headers["Authorization"]
        assert isinstance(auth_header, str), "Authorization header should be a string"
        assert "Bearer " in auth_header, "Should contain 'Bearer '"
        # The token should be properly decoded
        assert auth_header == f"Bearer {token}", "Should contain the decoded token"
    
    @pytest.mark.asyncio
    @settings(max_examples=100)
    @given(token=bearer_token_strategy)
    async def test_extract_bearer_token_from_string_header(self, token: str):
        """
        Property: For any valid Bearer token in a string Authorization header,
        extract_bearer_token should return the token.
        
        **Feature: system-stability-fixes, Property 17: Bearer token encoding**
        **Validates: Requirements 5.1**
        """
        request = MockRequest(headers={"Authorization": f"Bearer {token}"})
        
        result = await extract_bearer_token(request)
        
        assert result is not None, "Should extract token"
        assert result == token, "Should return the original token"
    
    @pytest.mark.asyncio
    @settings(max_examples=100)
    @given(token=bearer_token_strategy)
    async def test_extract_bearer_token_case_insensitive_prefix(self, token: str):
        """
        Property: For any valid Bearer token with case variations in 'Bearer',
        extract_bearer_token should still extract the token.
        
        **Feature: system-stability-fixes, Property 17: Bearer token encoding**
        **Validates: Requirements 5.1**
        """
        # Test with lowercase 'bearer'
        request = MockRequest(headers={"Authorization": f"bearer {token}"})
        
        result = await extract_bearer_token(request)
        
        assert result is not None, "Should extract token with lowercase 'bearer'"
        assert result == token, "Should return the original token"
    
    @pytest.mark.asyncio
    @settings(max_examples=50)
    @given(token=bearer_token_strategy)
    async def test_extract_bearer_token_with_whitespace(self, token: str):
        """
        Property: For any valid Bearer token with extra whitespace,
        extract_bearer_token should handle it gracefully.
        
        **Feature: system-stability-fixes, Property 17: Bearer token encoding**
        **Validates: Requirements 5.1**
        """
        # Test with extra whitespace
        request = MockRequest(headers={"Authorization": f"  Bearer   {token}  "})
        
        result = await extract_bearer_token(request)
        
        assert result is not None, "Should extract token with whitespace"
        assert result == token, "Should return the trimmed token"


class TestHeaderValidation:
    """
    **Feature: system-stability-fixes, Property 20: Header validation**
    
    *For any* custom headers added, the system should validate and encode 
    them correctly.
    
    **Validates: Requirements 5.5**
    """
    
    @settings(max_examples=100)
    @given(value=header_value_strategy)
    def test_validate_header_value_accepts_valid_values(self, value: str):
        """
        Property: For any valid header value (no control chars or newlines),
        validate_header_value should return True.
        
        **Feature: system-stability-fixes, Property 20: Header validation**
        **Validates: Requirements 5.5**
        """
        is_valid, error_msg = validate_header_value("Test-Header", value)
        
        assert is_valid is True, f"Valid header value should be accepted: {error_msg}"
        assert error_msg == "", "No error message for valid values"
    
    @settings(max_examples=50)
    @given(value=st.text(min_size=1, max_size=100))
    def test_validate_header_value_rejects_newlines(self, value: str):
        """
        Property: For any header value containing newlines,
        validate_header_value should reject it (header injection prevention).
        
        **Feature: system-stability-fixes, Property 20: Header validation**
        **Validates: Requirements 5.5**
        """
        # Filter out values that already contain control characters
        # to ensure we're specifically testing newline rejection
        assume(all(ord(c) >= 32 and ord(c) != 127 for c in value))
        
        # Add newline to the value
        value_with_newline = value + "\n"
        
        is_valid, error_msg = validate_header_value("Test-Header", value_with_newline)
        
        assert is_valid is False, "Header with newline should be rejected"
        # Error message should mention newline or control character (newline is a control char)
        assert "newline" in error_msg.lower() or "control" in error_msg.lower(), \
            f"Error should mention newline or control character: {error_msg}"
    
    @settings(max_examples=50)
    @given(value=st.text(min_size=1, max_size=100))
    def test_validate_header_value_rejects_carriage_return(self, value: str):
        """
        Property: For any header value containing carriage return,
        validate_header_value should reject it.
        
        **Feature: system-stability-fixes, Property 20: Header validation**
        **Validates: Requirements 5.5**
        """
        value_with_cr = value + "\r"
        
        is_valid, error_msg = validate_header_value("Test-Header", value_with_cr)
        
        assert is_valid is False, "Header with carriage return should be rejected"
    
    @settings(max_examples=100)
    @given(
        headers=st.dictionaries(
            keys=header_name_strategy,
            values=header_value_strategy,
            min_size=1,
            max_size=10
        )
    )
    def test_sanitize_headers_produces_string_values(self, headers: dict):
        """
        Property: For any dictionary of headers, sanitize_headers should
        produce a dictionary with all string values.
        
        **Feature: system-stability-fixes, Property 20: Header validation**
        **Validates: Requirements 5.5**
        """
        result = sanitize_headers(headers, log_errors=False)
        
        assert isinstance(result, dict), "Result should be a dictionary"
        for name, value in result.items():
            assert isinstance(name, str), f"Header name should be string: {name}"
            assert isinstance(value, str), f"Header value should be string: {value}"
    
    @settings(max_examples=50)
    @given(
        name=header_name_strategy,
        value=header_value_strategy
    )
    def test_sanitize_headers_removes_control_characters(self, name: str, value: str):
        """
        Property: For any header with control characters, sanitize_headers
        should remove them.
        
        **Feature: system-stability-fixes, Property 20: Header validation**
        **Validates: Requirements 5.5**
        """
        # Add a control character
        value_with_control = value + "\x00"
        headers = {name: value_with_control}
        
        result = sanitize_headers(headers, log_errors=False)
        
        assert "\x00" not in result[name], "Control character should be removed"


class TestHeaderForwardingIntegrity:
    """
    **Feature: system-stability-fixes, Property 18: Header forwarding integrity**
    
    *For any* headers forwarded to target APIs, all header values should be 
    properly formatted.
    
    **Validates: Requirements 5.2**
    """
    
    @settings(max_examples=100)
    @given(
        api_key=api_key_strategy,
        http_referer=st.text(
            alphabet=st.characters(whitelist_categories=('L', 'N', 'P', 'S'), blacklist_characters='\r\n'),
            min_size=5,
            max_size=100
        )
    )
    def test_build_request_headers_all_values_are_strings(self, api_key: str, http_referer: str):
        """
        Property: For any combination of inputs, build_request_headers should
        produce headers where all values are properly formatted strings.
        
        **Feature: system-stability-fixes, Property 18: Header forwarding integrity**
        **Validates: Requirements 5.2**
        """
        headers = build_request_headers(
            api_key=api_key,
            no_auth=False,
            base_url="https://openrouter.ai/api/v1",
            http_referer=http_referer
        )
        
        for name, value in headers.items():
            assert isinstance(value, str), f"Header {name} value should be string"
            # Check no control characters
            for char in value:
                assert ord(char) >= 32 or char == '\t', f"Header {name} contains control char"
                assert ord(char) != 127, f"Header {name} contains DEL char"
    
    @settings(max_examples=50)
    @given(api_key=api_key_strategy)
    def test_build_request_headers_no_auth_skips_authorization(self, api_key: str):
        """
        Property: When no_auth is True, build_request_headers should not
        include an Authorization header.
        
        **Feature: system-stability-fixes, Property 18: Header forwarding integrity**
        **Validates: Requirements 5.2**
        """
        headers = build_request_headers(
            api_key=api_key,
            no_auth=True
        )
        
        assert "Authorization" not in headers, "Authorization should not be present when no_auth=True"
    
    @settings(max_examples=50)
    @given(api_key=api_key_strategy)
    def test_build_request_headers_always_has_content_type(self, api_key: str):
        """
        Property: For any input, build_request_headers should always include
        Content-Type header.
        
        **Feature: system-stability-fixes, Property 18: Header forwarding integrity**
        **Validates: Requirements 5.2**
        """
        headers = build_request_headers(api_key=api_key)
        
        assert "Content-Type" in headers, "Content-Type should always be present"
        assert headers["Content-Type"] == "application/json", "Content-Type should be application/json"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])
