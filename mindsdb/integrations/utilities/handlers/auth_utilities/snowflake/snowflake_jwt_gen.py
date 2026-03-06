# Based on https://docs.snowflake.com/en/developer-guide/sql-api/authenticating

import time
import base64
import hashlib
import logging
from datetime import timedelta, timezone, datetime

from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PublicFormat
from cryptography.hazmat.backends import default_backend
import jwt

logger = logging.getLogger(__name__)

ISSUER = "iss"
EXPIRE_TIME = "exp"
ISSUE_TIME = "iat"
SUBJECT = "sub"


class JWTGenerator(object):
    """
    Creates and signs a JWT with the specified private key file, username, and account identifier. The JWTGenerator keeps the
    generated token and only regenerates the token if a specified period of time has passed.
    """

    LIFETIME = timedelta(minutes=60)  # The tokens will have a 59 minute lifetime
    ALGORITHM = "RS256"  # Tokens will be generated using RSA with SHA256

    def __init__(self, account: str, user: str, private_key: str, lifetime: timedelta = LIFETIME):
        """
        __init__ creates an object that generates JWTs for the specified user, account identifier, and private key.
        :param account: Your Snowflake account identifier. See https://docs.snowflake.com/en/user-guide/admin-account-identifier.html. Note that if you are using the account locator, exclude any region information from the account locator.
        :param user: The Snowflake username.
        :param private_key: The private key file used for signing the JWTs.
        :param lifetime: The number of minutes (as a timedelta) during which the key will be valid.
        """

        logger.info(
            """Creating JWTGenerator with arguments
            account : %s, user : %s, lifetime : %s""",
            account,
            user,
            lifetime,
        )

        # Construct the fully qualified name of the user in uppercase.
        self.account = self.prepare_account_name_for_jwt(account)
        self.user = user.upper()
        self.qualified_username = self.account + "." + self.user

        self.lifetime = lifetime
        self.renew_time = datetime.now(timezone.utc)
        self.token = None

        self.private_key = load_pem_private_key(private_key.encode(), None, default_backend())

    def prepare_account_name_for_jwt(self, raw_account: str) -> str:
        """
        Prepare the account identifier for use in the JWT.
        For the JWT, the account identifier must not include the subdomain or any region or cloud provider information.
        :param raw_account: The specified account identifier.
        :return: The account identifier in a form that can be used to generate JWT.
        """
        account = raw_account
        if ".global" not in account:
            # Handle the general case.
            idx = account.find(".")
            if idx > 0:
                account = account[0:idx]
        else:
            # Handle the replication case.
            idx = account.find("-")
            if idx > 0:
                account = account[0:idx]
        # Use uppercase for the account identifier.
        return account.upper()

    def get_token(self) -> str:
        """
        Generates a new JWT.
        :return: the new token
        """
        now = datetime.now(timezone.utc)  # Fetch the current time

        # Prepare the fields for the payload.
        # Generate the public key fingerprint for the issuer in the payload.
        public_key_fp = self.calculate_public_key_fingerprint(self.private_key)

        # Create our payload
        payload = {
            # Set the issuer to the fully qualified username concatenated with the public key fingerprint.
            ISSUER: self.qualified_username + "." + public_key_fp,
            # Set the subject to the fully qualified username.
            SUBJECT: self.qualified_username,
            # Set the issue time to now.
            ISSUE_TIME: now,
            # Set the expiration time, based on the lifetime specified for this object.
            EXPIRE_TIME: now + self.lifetime,
        }

        # Regenerate the actual token
        token = jwt.encode(payload, key=self.private_key, algorithm=JWTGenerator.ALGORITHM)
        # If you are using a version of PyJWT prior to 2.0, jwt.encode returns a byte string, rather than a string.
        # If the token is a byte string, convert it to a string.
        if isinstance(token, bytes):
            token = token.decode("utf-8")
        self.token = token

        return self.token

    def calculate_public_key_fingerprint(self, private_key: str) -> str:
        """
        Given a private key in PEM format, return the public key fingerprint.
        :param private_key: private key string
        :return: public key fingerprint
        """
        # Get the raw bytes of public key.
        public_key_raw = private_key.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)

        # Get the sha256 hash of the raw bytes.
        sha256hash = hashlib.sha256()
        sha256hash.update(public_key_raw)

        # Base64-encode the value and prepend the prefix 'SHA256:'.
        public_key_fp = "SHA256:" + base64.b64encode(sha256hash.digest()).decode("utf-8")
        logger.info("Public key fingerprint is %s", public_key_fp)

        return public_key_fp


def get_validated_jwt(token: str, account: str, user: str, private_key: str) -> str:
    try:
        content = jwt.decode(token, algorithms=[JWTGenerator.ALGORITHM], options={"verify_signature": False})

        expired = content.get("exp", 0)
        # add 5 seconds before limit
        if expired - 5 > time.time():
            # keep the same
            return token

    except jwt.DecodeError:
        # wrong key
        ...

    # generate new token
    if private_key is None:
        raise ValueError("Private key is missing")
    return JWTGenerator(account, user, private_key).get_token()
