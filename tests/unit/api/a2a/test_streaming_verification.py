import unittest
import time
import json
import threading
import pytest
from http.server import HTTPServer, BaseHTTPRequestHandler

from tests.unit.api.a2a.streaming_test_client import StreamingTestClient


class StreamingHandler(BaseHTTPRequestHandler):
    """
    A simple HTTP handler that streams responses with controlled timing.
    """

    def do_GET(self):
        """Handle GET requests with streaming response."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        # Stream 5 chunks with 0.5 second delay between them
        for i in range(5):
            # Format the chunk properly for chunked transfer encoding
            # Format: [chunk size in hex]\r\n[chunk data]\r\n
            chunk_data = json.dumps({"chunk": i, "timestamp": time.time()}) + "\n"
            chunk_size = hex(len(chunk_data))[2:]  # Convert to hex and remove '0x' prefix

            self.wfile.write(f"{chunk_size}\r\n".encode("utf-8"))
            self.wfile.write(chunk_data.encode("utf-8"))
            self.wfile.write(b"\r\n")
            self.wfile.flush()  # Ensure the chunk is sent immediately
            time.sleep(0.5)  # Delay between chunks

        # End the chunked response with a zero-length chunk
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def do_POST(self):
        """Handle POST requests with streaming response."""
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")

        # Parse the request data
        try:
            data = json.loads(post_data)
            # Get the delay parameter or use default
            delay = data.get("delay", 0.5)
            chunks = data.get("chunks", 5)
        except (ValueError, TypeError, json.JSONDecodeError):
            delay = 0.5
            chunks = 5

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        # Stream chunks with the specified delay
        for i in range(chunks):
            # Format the chunk properly for chunked transfer encoding
            chunk_data = json.dumps({"chunk": i, "timestamp": time.time()}) + "\n"
            chunk_size = hex(len(chunk_data))[2:]  # Convert to hex and remove '0x' prefix

            self.wfile.write(f"{chunk_size}\r\n".encode("utf-8"))
            self.wfile.write(chunk_data.encode("utf-8"))
            self.wfile.write(b"\r\n")
            self.wfile.flush()  # Ensure the chunk is sent immediately
            time.sleep(delay)  # Delay between chunks

        # End the chunked response with a zero-length chunk
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()


class TestStreamingVerification(unittest.TestCase):
    """
    Test cases to verify that responses are properly streamed.
    """

    @classmethod
    def setUpClass(cls):
        """Start a test server in a separate thread."""
        cls.server = HTTPServer(("localhost", 0), StreamingHandler)
        cls.server_port = cls.server.server_port
        cls.server_thread = threading.Thread(target=cls.server.serve_forever)
        cls.server_thread.daemon = True
        cls.server_thread.start()
        cls.base_url = f"http://localhost:{cls.server_port}"

    @classmethod
    def tearDownClass(cls):
        """Shut down the test server."""
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join()

    def test_get_streaming(self):
        """Test that GET responses are properly streamed."""
        client = StreamingTestClient()
        chunks, timestamps = client.get_with_timing(f"{self.base_url}/stream")

        # Verify we got the expected number of chunks
        self.assertEqual(len(chunks), 5)

        # Analyze timing
        timing = client.analyze_timing(timestamps)

        # Verify that chunks were received over time, not all at once
        # The total time should be at least (chunks-1) * delay
        self.assertGreaterEqual(
            timing["total_time"], 0.5 * 4 * 0.9
        )  # 90% of expected time to account for timing variations

        # Verify that the average interval is close to our delay
        self.assertGreaterEqual(timing["average_interval"], 0.5 * 0.8)  # 80% of expected delay

        # Print timing information for debugging
        print(f"Streaming GET timing: {timing}")

    @pytest.mark.slow
    def test_post_streaming(self):
        """Test that POST responses are properly streamed."""
        client = StreamingTestClient()

        # Test with different delays
        for delay in [0.2, 0.5, 1.0]:
            chunks, timestamps = client.post_with_timing(f"{self.base_url}/stream", json={"delay": delay, "chunks": 5})

            # Verify we got the expected number of chunks
            self.assertEqual(len(chunks), 5)

            # Analyze timing
            timing = client.analyze_timing(timestamps)

            # Verify that chunks were received over time, not all at once
            # The total time should be at least (chunks-1) * delay
            self.assertGreaterEqual(timing["total_time"], delay * 4 * 0.9)  # 90% of expected time

            # Verify that the average interval is close to our delay
            self.assertGreaterEqual(timing["average_interval"], delay * 0.8)  # 80% of expected delay

            # Print timing information for debugging
            print(f"Streaming POST timing with delay {delay}: {timing}")

    def test_verify_not_batched(self):
        """
        Test to specifically verify that chunks aren't being batched together.

        This test uses a longer delay to make it more obvious if batching occurs.
        """
        client = StreamingTestClient()
        chunks, timestamps = client.post_with_timing(f"{self.base_url}/stream", json={"delay": 1.0, "chunks": 5})

        # Verify we got the expected number of chunks
        self.assertEqual(len(chunks), 5)

        # Analyze timing
        timing = client.analyze_timing(timestamps)

        # If chunks are batched, we'll see some very small intervals
        # and some very large ones. Check that all intervals are reasonably close
        # to our expected delay.
        for interval in timing["intervals"]:
            # Each interval should be at least 50% of the expected delay
            # and not more than 150% of the expected delay
            self.assertGreaterEqual(interval, 1.0 * 0.5)
            self.assertLessEqual(interval, 1.0 * 1.5)

        # Print timing information for debugging
        print(f"Batching verification timing: {timing}")


if __name__ == "__main__":
    unittest.main()
