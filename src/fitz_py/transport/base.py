from __future__ import annotations

from abc import ABC, abstractmethod


class Transport(ABC):
    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send(self, data: bytes) -> None:
        raise NotImplementedError

    @abstractmethod
    async def receive(self) -> bytes:
        raise NotImplementedError

    @abstractmethod
    async def heartbeat(self, timeout: float) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_url(self) -> str:
        raise NotImplementedError
