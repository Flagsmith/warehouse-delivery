import pytest
from cryptography.fernet import InvalidToken

from warehouse_delivery.crypto import decrypt_json, fernet_from_secret

# Produced by the API's EncryptedJSONField with WAREHOUSE_CREDENTIALS_SECRET set
# to "fernet-secret", so this test pins the two derivations to each other.
API_TOKEN = (
    "gAAAAABqq6gdveEPhCz427gngsKy5vl5u4ri0kR3mjVN3ST7dtVfVWW3GwaIjzxu_R_6HkdXyMOBn7bv"
    "fV_nAVMo2ikgWvtY2ZxzHpXb9Toss-pynFD93qs="
)


def test_decrypt_json__token_from_the_api__yields_the_credentials() -> None:
    # Given the secret the API encrypted with
    fernet = fernet_from_secret("fernet-secret")

    # When
    credentials = decrypt_json(fernet, API_TOKEN)

    # Then
    assert credentials == {"password": "hunter2"}


def test_decrypt_json__different_secret__raises_invalid_token() -> None:
    # Given a secret other than the one the API encrypted with
    fernet = fernet_from_secret("rotated-secret")

    # When / Then the driver's own error comes through for the caller to handle
    with pytest.raises(InvalidToken):
        decrypt_json(fernet, API_TOKEN)
