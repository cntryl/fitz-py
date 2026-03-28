from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from typing import Literal

import pytest

from fitz_py import Client, ClientConfig
from tests.integration.fixture.jwt import make_valid_jwt

AuthMode = Literal["anonymous", "valid_jwt"]


def _broker_url(transport: str, auth_mode: AuthMode) -> str:
    if transport == "ws":
        if auth_mode == "valid_jwt":
            return os.getenv("FITZ_BROKER_AUTH_WS_ADDR", "ws://localhost:4090/ws")
        return os.getenv("FITZ_BROKER_ANON_WS_ADDR", "ws://localhost:4190/ws")

    if auth_mode == "valid_jwt":
        return os.getenv("FITZ_BROKER_AUTH_TCP_ADDR", "localhost:4091")
    return os.getenv("FITZ_BROKER_ANON_TCP_ADDR", "localhost:4191")


def unique_route(prefix: str) -> str:
    suffix = f"{int(time.time() * 1000)}-{uuid.uuid4().hex}"
    if prefix == "schedule":
        return f"{prefix}://integration-realm/{suffix}/res/run"
    return f"{prefix}://integration-realm/{suffix}/res"


@dataclass(slots=True)
class IntegrationFixture:
    transport: str
    auth_mode: AuthMode
    client: Client

    @classmethod
    async def connect_or_fail(cls, transport: str, auth_mode: AuthMode) -> "IntegrationFixture":
        token_provider = None
        if auth_mode == "valid_jwt":
            token_provider = make_valid_jwt
        client = Client(
            ClientConfig(
                url=_broker_url(transport, auth_mode),
                token_provider=token_provider,
                transport=transport,
            )
        )
        try:
            await client.connect()
        except Exception as exc:
            pytest.skip(f"Broker not reachable for integration test: {exc}")
        return cls(transport=transport, auth_mode=auth_mode, client=client)

    async def close(self) -> None:
        await self.client.close()
