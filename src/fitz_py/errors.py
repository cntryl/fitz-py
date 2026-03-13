from __future__ import annotations


class FitzError(Exception):
    def __init__(
        self,
        message: str,
        code: str,
        domain_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.domain_code = domain_code


class TransportError(FitzError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "TRANSPORT_ERROR")


class ConnectionError(FitzError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "CONNECTION_ERROR")


class AuthenticationError(FitzError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "AUTH_ERROR")


class TimeoutError(FitzError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "TIMEOUT")


class ProtocolError(FitzError):
    def __init__(self, message: str, domain_code: int | None = None) -> None:
        super().__init__(message, "PROTOCOL_ERROR", domain_code)


class CodecError(FitzError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "CODEC_ERROR")


class KvError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"KV_{code}", domain_code)


class QueueError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"QUEUE_{code}", domain_code)


class NoticeError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"NOTICE_{code}", domain_code)


class RpcError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"RPC_{code}", domain_code)


class LeaseError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"LEASE_{code}", domain_code)


class StreamError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"STREAM_{code}", domain_code)


class ScheduleError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"SCHEDULE_{code}", domain_code)
