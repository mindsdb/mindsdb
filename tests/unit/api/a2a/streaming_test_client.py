import time
import requests
from typing import List, Dict, Any, Tuple


class StreamingTestClient:
    """A client that tracks timing information for streamed responses."""

    def __init__(self):
        self.session = requests.Session()

    def get_with_timing(
        self, url: str, **kwargs
    ) -> Tuple[List[Dict[str, Any]], List[float]]:
        """
        Make a GET request and track when each chunk is received.

        Args:
            url: The URL to request
            **kwargs: Additional arguments to pass to requests.get

        Returns:
            Tuple containing:
            - List of chunks (parsed as JSON if possible)
            - List of timestamps when each chunk was received
        """
        chunks = []
        timestamps = []

        with self.session.get(url, stream=True, **kwargs) as response:
            # Verify we're getting a chunked response
            if response.headers.get("Transfer-Encoding") != "chunked":
                print("Warning: Response is not using chunked transfer encoding")

            for chunk in response.iter_content(chunk_size=None, decode_unicode=True):
                if chunk:
                    timestamps.append(time.time())
                    try:
                        # Try to parse as JSON
                        import json

                        # The chunk might contain multiple JSON objects if they were sent together
                        # Split by newlines and parse each one
                        for line in chunk.strip().split("\n"):
                            if line:
                                chunks.append(json.loads(line))
                    except (ValueError, TypeError, json.JSONDecodeError):
                        # If not JSON, store as string
                        chunks.append(chunk)

        return chunks, timestamps

    def post_with_timing(
        self, url: str, **kwargs
    ) -> Tuple[List[Dict[str, Any]], List[float]]:
        """
        Make a POST request and track when each chunk is received.

        Args:
            url: The URL to request
            **kwargs: Additional arguments to pass to requests.post

        Returns:
            Tuple containing:
            - List of chunks (parsed as JSON if possible)
            - List of timestamps when each chunk was received
        """
        chunks = []
        timestamps = []

        with self.session.post(url, stream=True, **kwargs) as response:
            # Verify we're getting a chunked response
            if response.headers.get("Transfer-Encoding") != "chunked":
                print("Warning: Response is not using chunked transfer encoding")

            # Use a smaller chunk size to better detect streaming
            buffer = ""
            for chunk in response.iter_content(chunk_size=1, decode_unicode=True):
                if not chunk:
                    continue

                # Add to buffer and process complete lines
                buffer += chunk

                # Process complete lines if we have a newline
                if "\n" in buffer:
                    lines = buffer.split("\n")
                    # Keep the last part (might be incomplete)
                    buffer = lines.pop()

                    for line in lines:
                        if line.strip():
                            timestamps.append(time.time())
                            try:
                                # Try to parse as JSON
                                import json

                                chunks.append(json.loads(line))
                            except (ValueError, TypeError, json.JSONDecodeError):
                                # If not JSON, store as string
                                chunks.append(line)

            # Process any remaining content in the buffer
            if buffer.strip():
                timestamps.append(time.time())
                try:
                    # Try to parse as JSON
                    import json

                    chunks.append(json.loads(buffer))
                except (ValueError, TypeError, json.JSONDecodeError):
                    # If not JSON, store as string
                    chunks.append(buffer)

        return chunks, timestamps

    def analyze_timing(self, timestamps: List[float]) -> Dict[str, float]:
        """
        Analyze the timing information from a streamed response.

        Args:
            timestamps: List of timestamps when chunks were received

        Returns:
            Dictionary with timing statistics
        """
        if not timestamps or len(timestamps) < 2:
            return {"error": "Not enough chunks to analyze"}

        intervals = [
            timestamps[i] - timestamps[i - 1] for i in range(1, len(timestamps))
        ]

        return {
            "total_chunks": len(timestamps),
            "total_time": timestamps[-1] - timestamps[0],
            "average_interval": sum(intervals) / len(intervals),
            "min_interval": min(intervals),
            "max_interval": max(intervals),
            "intervals": intervals,
        }

    def post(self, url: str, **kwargs):
        """
        Make a POST request and return the response.

        Args:
            url: The URL to request
            **kwargs: Additional arguments to pass to requests.post

        Returns:
            The response object
        """
        return self.session.post(url, **kwargs)
