from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _encode(payload: dict[str, object], secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    signing_input = f"{_b64url(json.dumps(header, separators=(',', ':')).encode())}.{_b64url(json.dumps(payload, separators=(',', ':')).encode())}"
    signature = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
    return f"{signing_input}.{_b64url(signature)}"


def make_valid_jwt(subject: str = "fitz-py-test") -> str:
    secret = os.getenv("FITZ_BROKER_JWT_HMAC_SECRET", "dev-secret")
    audience = os.getenv("FITZ_BROKER_JWT_AUDIENCE", "fitz")
    now = int(time.time())
    return _encode(
        {
            "sub": subject,
            "aud": audience,
            "iat": now,
            "nbf": now,
            "exp": now + 300,
        },
        secret,
    )


def make_expired_jwt(subject: str = "fitz-py-test") -> str:
    secret = os.getenv("FITZ_BROKER_JWT_HMAC_SECRET", "dev-secret")
    audience = os.getenv("FITZ_BROKER_JWT_AUDIENCE", "fitz")
    now = int(time.time())
    return _encode(
        {
            "sub": subject,
            "aud": audience,
            "iat": now - 600,
            "nbf": now - 600,
            "exp": now - 300,
        },
        secret,
    )
