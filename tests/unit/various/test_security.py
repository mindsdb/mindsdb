# NOTE: generated with llm
import pytest
from mindsdb.utilities.security import validate_urls


class TestValidateUrls:
    """Test cases for validate_urls function"""

    def test_single_url_allowed(self):
        """Test that a single allowed URL returns True"""
        urls = "https://example.com/file"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_single_url_not_allowed(self):
        """Test that a single not allowed URL returns False"""
        urls = "https://malicious.com/file"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is False

    def test_single_url_disallowed(self):
        """Test that a single disallowed URL returns False"""
        urls = "https://example.com/file"
        allowed_urls = ["https://example.com"]
        disallowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls, disallowed_urls) is False

    def test_url_allowed_but_disallowed(self):
        """Test that a URL is allowed but disallowed returns False"""
        urls = "https://example.com/file"
        allowed_urls = ["https://example.com"]
        disallowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls, disallowed_urls) is False

    def test_url_allowed_and_not_disallowed(self):
        """Test that a URL is allowed and not disallowed returns True"""
        urls = "https://example.com/file"
        allowed_urls = ["https://example.com"]
        disallowed_urls = ["https://malicious.com"]
        assert validate_urls(urls, allowed_urls, disallowed_urls) is True

    def test_multiple_urls_some_disallowed(self):
        """Test that multiple URLs with some disallowed returns False"""
        urls = ["https://example.com/file1", "https://malicious.com/file2"]
        allowed_urls = ["https://example.com", "https://malicious.com"]
        disallowed_urls = ["https://malicious.com"]
        assert validate_urls(urls, allowed_urls, disallowed_urls) is False

    def test_multiple_urls_all_disallowed(self):
        """Test that multiple URLs all disallowed returns False"""
        urls = ["https://example.com/file1", "https://malicious.com/file2"]
        allowed_urls = ["https://example.com", "https://malicious.com"]
        disallowed_urls = ["https://example.com", "https://malicious.com"]
        assert validate_urls(urls, allowed_urls, disallowed_urls) is False

    def test_multiple_urls_none_disallowed(self):
        """Test that multiple URLs none disallowed returns True"""
        urls = ["https://example.com/file1", "https://trusted.com/file2"]
        allowed_urls = ["https://example.com", "https://trusted.com"]
        disallowed_urls = ["https://malicious.com"]
        assert validate_urls(urls, allowed_urls, disallowed_urls) is True

    def test_empty_disallowed_urls(self):
        """Test that empty disallowed_urls list returns True (allows everything)"""
        urls = "https://example.com/file"
        allowed_urls = ["https://example.com"]
        disallowed_urls = []
        assert validate_urls(urls, allowed_urls, disallowed_urls) is True

    def test_disallowed_urls_none(self):
        """Test that None disallowed_urls returns True (allows everything)"""
        urls = "https://example.com/file"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls, None) is True

    def test_empty_allowed_urls_and_disallowed(self):
        """Test that empty allowed_urls and disallowed_urls returns True (allows everything)"""
        urls = "https://any.com/file"
        allowed_urls = []
        disallowed_urls = []
        assert validate_urls(urls, allowed_urls, disallowed_urls) is True

    def test_empty_allowed_urls_but_disallowed(self):
        """Test that empty allowed_urls but disallowed_urls returns False"""
        urls = "https://bad.com/file"
        allowed_urls = []
        disallowed_urls = ["https://bad.com"]
        assert validate_urls(urls, allowed_urls, disallowed_urls) is False

    def test_multiple_allowed_urls(self):
        """Test with multiple allowed URLs"""
        urls = "https://example.com/file"
        allowed_urls = ["https://example.com", "https://trusted.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_different_schemes(self):
        """Test that different schemes are treated as different URLs"""
        urls = "http://example.com/file"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is False

    def test_same_scheme_allowed(self):
        """Test that same scheme is allowed"""
        urls = "https://example.com/file"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_url_without_scheme_raises_exception(self):
        """Test that URL without scheme raises ValueError"""
        urls = "example.com/file"
        allowed_urls = ["https://example.com"]
        with pytest.raises(ValueError, match="URL must include protocol and host name"):
            validate_urls(urls, allowed_urls)

    def test_url_without_netloc_raises_exception(self):
        """Test that URL without netloc raises ValueError"""
        urls = "https:///file"
        allowed_urls = ["https://example.com"]
        with pytest.raises(ValueError, match="URL must include protocol and host name"):
            validate_urls(urls, allowed_urls)

    def test_allowed_url_without_scheme_raises_exception(self):
        """Test that allowed URL without scheme raises ValueError"""
        urls = "https://example.com/file"
        allowed_urls = ["example.com"]
        with pytest.raises(ValueError, match="URL must include protocol and host name"):
            validate_urls(urls, allowed_urls)

    def test_allowed_url_without_netloc_raises_exception(self):
        """Test that allowed URL without netloc raises ValueError"""
        urls = "https://example.com/file"
        allowed_urls = ["https://"]
        with pytest.raises(ValueError, match="URL must include protocol and host name"):
            validate_urls(urls, allowed_urls)

    def test_subdomain_not_allowed(self):
        """Test that subdomain is not allowed unless explicitly specified"""
        urls = "https://sub.example.com/file"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is False

    def test_subdomain_allowed(self):
        """Test that subdomain is allowed when explicitly specified"""
        urls = "https://sub.example.com/file"
        allowed_urls = ["https://sub.example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_port_in_url(self):
        """Test URLs with ports"""
        urls = "https://example.com:8080/file"
        allowed_urls = ["https://example.com:8080"]
        assert validate_urls(urls, allowed_urls) is True

    def test_port_mismatch(self):
        """Test that different ports are treated as different URLs"""
        urls = "https://example.com:8080/file"
        allowed_urls = ["https://example.com:3000"]
        assert validate_urls(urls, allowed_urls) is False

    def test_path_ignored(self):
        """Test that path is ignored in comparison"""
        urls = "https://example.com/different/path"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_query_params_ignored(self):
        """Test that query parameters are ignored in comparison"""
        urls = "https://example.com/file?param=value"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_fragment_ignored(self):
        """Test that fragment is ignored in comparison"""
        urls = "https://example.com/file#section"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_mixed_case_netloc_case_insensitive(self):
        """Test that netloc comparison is case-insensitive"""
        urls = "https://EXAMPLE.com/file"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_mixed_case_scheme_case_insensitive(self):
        """Test that scheme comparison is case-insensitive"""
        urls = "HTTPS://example.com/file"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_mixed_case_both_case_insensitive(self):
        """Test that both scheme and netloc comparison are case-insensitive"""
        urls = "HTTPS://EXAMPLE.COM/file"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_mixed_case_allowed_urls_case_insensitive(self):
        """Test that allowed URLs are also case-insensitive"""
        urls = "https://example.com/file"
        allowed_urls = ["HTTPS://EXAMPLE.COM"]
        assert validate_urls(urls, allowed_urls) is True

    def test_empty_string_url(self):
        """Test empty string URL raises exception"""
        urls = ""
        allowed_urls = ["https://example.com"]
        with pytest.raises(ValueError, match="URL must include protocol and host name"):
            validate_urls(urls, allowed_urls)

    def test_none_url(self):
        """Test None URL raises TypeError"""
        urls = None
        allowed_urls = ["https://example.com"]
        with pytest.raises(TypeError):
            validate_urls(urls, allowed_urls)

    def test_complex_url_with_all_components(self):
        """Test complex URL with path, query, and fragment"""
        urls = "https://example.com/path/to/file?param1=value1&param2=value2#section"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_ip_address_url(self):
        """Test URL with IP address"""
        urls = "https://192.168.1.1/file"
        allowed_urls = ["https://192.168.1.1"]
        assert validate_urls(urls, allowed_urls) is True

    def test_ip_address_mismatch(self):
        """Test that different IP addresses are treated as different URLs"""
        urls = "https://192.168.1.1/file"
        allowed_urls = ["https://192.168.1.2"]
        assert validate_urls(urls, allowed_urls) is False

    def test_empty_string_in_allowed_urls_raises_exception(self):
        """Test that empty string in allowed_urls raises ValueError"""
        urls = "https://example.com/file"
        allowed_urls = [""]
        with pytest.raises(ValueError, match="URL must include protocol and host name"):
            validate_urls(urls, allowed_urls)

    def test_url_without_scheme_in_allowed_urls_raises_exception(self):
        """Test that URL without scheme in allowed_urls raises ValueError"""
        urls = "https://example.com/file"
        allowed_urls = ["example.com"]
        with pytest.raises(ValueError, match="URL must include protocol and host name"):
            validate_urls(urls, allowed_urls)

    def test_both_http_and_https_allowed(self):
        """Test that both HTTP and HTTPS can be allowed for the same domain"""
        urls = ["http://example.com/file", "https://example.com/file"]
        allowed_urls = ["http://example.com", "https://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_only_http_allowed(self):
        """Test that only HTTP is allowed"""
        urls = "https://example.com/file"
        allowed_urls = ["http://example.com"]
        assert validate_urls(urls, allowed_urls) is False

    def test_only_https_allowed(self):
        """Test that only HTTPS is allowed"""
        urls = "http://example.com/file"
        allowed_urls = ["https://example.com"]
        assert validate_urls(urls, allowed_urls) is False

    def test_ftp_scheme(self):
        """Test FTP scheme"""
        urls = "ftp://example.com/file"
        allowed_urls = ["ftp://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_custom_scheme(self):
        """Test custom scheme"""
        urls = "custom://example.com/file"
        allowed_urls = ["custom://example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_case_insensitive_with_ports(self):
        """Test case-insensitive comparison with ports"""
        urls = "HTTPS://EXAMPLE.COM:8080/file"
        allowed_urls = ["https://example.com:8080"]
        assert validate_urls(urls, allowed_urls) is True

    def test_case_insensitive_with_subdomains(self):
        """Test case-insensitive comparison with subdomains"""
        urls = "https://SUB.EXAMPLE.COM/file"
        allowed_urls = ["https://sub.example.com"]
        assert validate_urls(urls, allowed_urls) is True

    def test_case_insensitive_ip_addresses(self):
        """Test case-insensitive comparison with IP addresses"""
        urls = "HTTPS://192.168.1.1/file"
        allowed_urls = ["https://192.168.1.1"]
        assert validate_urls(urls, allowed_urls) is True
