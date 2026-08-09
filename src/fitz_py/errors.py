"""Python-native Fitz exception hierarchy and retry classification."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any


class FitzError(Exception):
    def __init__(
        self,
        message: str,
        code: str,
        domain_code: int | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.domain_code = domain_code
        self.context = MappingProxyType(dict(context or {}))


class FitzTransportError(FitzError):
    def __init__(self, message: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message, "TRANSPORT_ERROR", context=context)


class FitzConnectionError(FitzError):
    def __init__(self, message: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message, "CONNECTION_ERROR", context=context)


class AuthenticationError(FitzError):
    def __init__(self, message: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message, "AUTH_ERROR", context=context)


class FitzTimeoutError(FitzError):
    def __init__(self, message: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message, "TIMEOUT", context=context)


class ProtocolError(FitzError):
    def __init__(self, message: str, domain_code: int | None = None) -> None:
        super().__init__(message, "PROTOCOL_ERROR", domain_code)


class CodecError(FitzError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "CODEC_ERROR")


class RequestQueueFullError(FitzError):
    def __init__(self) -> None:
        super().__init__("Request queue is full", "REQUEST_QUEUE_FULL")


class SubscriptionBackpressureError(FitzError):
    def __init__(self) -> None:
        super().__init__("Subscription consumer fell behind", "SUBSCRIPTION_BACKPRESSURE")


class StaleHandleError(FitzError):
    def __init__(self, kind: str) -> None:
        super().__init__(f"{kind} is stale after disconnect", "STALE_HANDLE")


class ReconnectRestoreError(FitzError):
    def __init__(self, domain: str, registration: str, cause: BaseException) -> None:
        super().__init__(
            f"Failed to restore {domain} registration {registration}: {cause}",
            "RECONNECT_RESTORE_FAILED",
            context={"domain": domain, "registration": registration},
        )


class LeaseLostError(FitzError):
    def __init__(self, message: str = "Lease ownership was lost") -> None:
        super().__init__(message, "LEASE_LOST")


class LeaseLifecycleError(ExceptionGroup, FitzError):
    def __new__(cls, causes: list[Exception]) -> LeaseLifecycleError:
        return super().__new__(
            cls,
            "Multiple failures occurred while managing a lease",
            causes,
        )

    def __init__(self, causes: list[Exception]) -> None:
        self.code = "LEASE_LIFECYCLE_MULTIPLE_FAILURES"
        self.domain_code = None
        self.context = MappingProxyType({"causes": tuple(causes)})
        self.causes = tuple(causes)

    def derive(self, exceptions: Sequence[BaseException]) -> LeaseLifecycleError:
        derived = [error for error in exceptions if isinstance(error, Exception)]
        if len(derived) != len(exceptions):
            raise TypeError("Lease lifecycle groups contain Exception instances only")
        return LeaseLifecycleError(derived)


class DomainError(FitzError):
    prefix = "DOMAIN"

    def __init__(self, message: str, reason: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"{self.prefix}_{reason}", domain_code)
        self.reason = reason


class KVError(DomainError):
    prefix = "KV"


class QueueError(DomainError):
    prefix = "QUEUE"


class NoticeError(DomainError):
    prefix = "NOTICE"


class RPCError(DomainError):
    prefix = "RPC"


class LeaseError(DomainError):
    prefix = "LEASE"


class StreamError(DomainError):
    prefix = "STREAM"


class ScheduleError(DomainError):
    prefix = "SCHEDULE"


_RETRYABLE = {
    ("KV", 1004),
    ("KV", 1009),
    ("QUEUE", 4005),
    ("LEASE", 5001),
    ("LEASE", 5006),
    ("LEASE", 5007),
    ("RPC", 6001),
    ("RPC", 6002),
    ("RPC", 6003),
    ("RPC", 6004),
}


def is_retryable(error: BaseException) -> bool:
    if isinstance(error, (FitzTransportError, FitzTimeoutError, RequestQueueFullError)):
        return True
    if not isinstance(error, DomainError) or error.domain_code is None:
        return False
    return (error.prefix, error.domain_code) in _RETRYABLE


def domain_error(
    error_type: type[DomainError], operation: str, status: int, message: str | None = None
) -> DomainError:
    reason = message or f"status {status}"
    return error_type(f"{operation} failed: {reason}", operation, status)


def kv_error(message: str, domain_code: int | None = None) -> KVError:
    return KVError(message, "ERROR", domain_code)


def queue_error(message: str, domain_code: int | None = None) -> QueueError:
    return QueueError(message, "ERROR", domain_code)


def rpc_error(message: str, domain_code: int | None = None) -> RPCError:
    return RPCError(message, "ERROR", domain_code)


def lease_error(message: str, domain_code: int | None = None) -> LeaseError:
    return LeaseError(message, "ERROR", domain_code)


def notice_error(message: str, domain_code: int | None = None) -> NoticeError:
    return NoticeError(message, "ERROR", domain_code)


def stream_error(message: str, domain_code: int | None = None) -> StreamError:
    return StreamError(message, "ERROR", domain_code)


def schedule_error(message: str, domain_code: int | None = None) -> ScheduleError:
    return ScheduleError(message, "ERROR", domain_code)
