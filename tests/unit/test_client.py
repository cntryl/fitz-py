from __future__ import annotations

import pytest

from fitz_py import Client, ClientConfig, ConnectionState


def test_client_starts_disconnected() -> None:
    client = Client(ClientConfig(url="ws://localhost:4190/ws"))
    assert client.state is ConnectionState.DISCONNECTED


@pytest.mark.asyncio
async def test_client_async_context_manager_connects_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Client(ClientConfig(url="ws://localhost:4190/ws"))
    calls: list[str] = []

    async def fake_connect(self: Client) -> None:
        calls.append("connect")

    async def fake_close(self: Client) -> None:
        calls.append("close")

    monkeypatch.setattr(Client, "connect", fake_connect)
    monkeypatch.setattr(Client, "close", fake_close)

    async with client as active:
        assert active is client

    assert calls == ["connect", "close"]
