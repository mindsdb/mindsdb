import unittest
from mindsdb.integrations.handlers.ibm_cos_handler.ibm_cos_handler import (
    IBMCloudObjectStorageHandler,
)


class IBMCloudObjectStorageHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "cos_hmac_access_key_id": "your-access-key-id",
            "cos_hmac_secret_access_key": "your-secret-access-key",
            "cos_endpoint_url": "https://s3.eu-gb.cloud-object-storage.appdomain.cloud",
            "bucket": "your-bucket-name",
        }
        cls.handler = IBMCloudObjectStorageHandler("test_ibm_cos_handler", cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()


if __name__ == "__main__":
    unittest.main()
