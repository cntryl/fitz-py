from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time

DEFAULT_PERMISSIONS = [
    "kv://**#*",
    "queue://**#*",
    "notice://**#*",
    "stream://**#*",
    "rpc://**#*",
    "lease://**#*",
    "schedule://**#*",
]


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _encode(payload: dict[str, object], secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    signing_input = f"{_b64url(json.dumps(header, separators=(',', ':')).encode())}.{_b64url(json.dumps(payload, separators=(',', ':')).encode())}"
    signature = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
    return f"{signing_input}.{_b64url(signature)}"


def make_valid_jwt(subject: str = "fitz-py-tests") -> str:
    secret = os.getenv("FITZ_BROKER_JWT_HMAC_SECRET", "test-secret-key")
    audience = os.getenv("FITZ_BROKER_JWT_AUDIENCE", "fitz")
    tenant = os.getenv("FITZ_BROKER_JWT_TENANT", "dev")
    now = int(time.time())
    return _encode(
        {
            "iss": "",
            "sub": subject,
            "aud": audience,
            "tid": tenant,
            "iat": now,
            "nbf": now,
            "exp": now + 300,
            "fitz": {"permissions": DEFAULT_PERMISSIONS},
        },
        secret,
    )


def make_expired_jwt(subject: str = "fitz-py-tests") -> str:
    secret = os.getenv("FITZ_BROKER_JWT_HMAC_SECRET", "test-secret-key")
    audience = os.getenv("FITZ_BROKER_JWT_AUDIENCE", "fitz")
    tenant = os.getenv("FITZ_BROKER_JWT_TENANT", "dev")
    now = int(time.time())
    return _encode(
        {
            "iss": "",
            "sub": subject,
            "aud": audience,
            "tid": tenant,
            "iat": now - 600,
            "nbf": now - 600,
            "exp": now - 300,
            "fitz": {"permissions": DEFAULT_PERMISSIONS},
        },
        secret,
    )
