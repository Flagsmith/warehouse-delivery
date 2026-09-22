import base64
import hashlib
import json
from typing import Any

from cryptography.fernet import Fernet


def fernet_from_secret(secret: str) -> Fernet:
    """Builds the same encryption key the API builds from
    WAREHOUSE_CREDENTIALS_SECRET, so credentials the API encrypted can be read
    here. The key is the SHA-256 of the secret, exactly as the API's
    EncryptedJSONField computes it."""
    digest = hashlib.sha256(secret.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def decrypt_json(fernet: Fernet, token: str) -> Any:
    return json.loads(fernet.decrypt(token.encode()))
