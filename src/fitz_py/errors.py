from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar


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


TError = TypeVar("TError", bound=FitzError)


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


class ErrKvTransactionAborted(KvError):
    def __init__(self, message: str = "KV transaction aborted") -> None:
        super().__init__(message, "TRANSACTION_ABORTED", 1)


class ErrKvLeaseExpired(KvError):
    def __init__(self, message: str = "KV lease expired") -> None:
        super().__init__(message, "LEASE_EXPIRED", 2)


class ErrKvConflictingWrite(KvError):
    def __init__(self, message: str = "KV conflicting write") -> None:
        super().__init__(message, "CONFLICTING_WRITE", 3)


class ErrKvKeyNotFound(KvError):
    def __init__(self, message: str = "KV key not found") -> None:
        super().__init__(message, "KEY_NOT_FOUND", 4)


class ErrKvOperationNotAllowed(KvError):
    def __init__(self, message: str = "KV operation not allowed") -> None:
        super().__init__(message, "OPERATION_NOT_ALLOWED", 5)


class QueueError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"QUEUE_{code}", domain_code)


class ErrQueueNotFound(QueueError):
    def __init__(self, message: str = "Queue not found") -> None:
        super().__init__(message, "NOT_FOUND", 1)


class ErrQueueMessageNotFound(QueueError):
    def __init__(self, message: str = "Queue message not found") -> None:
        super().__init__(message, "MESSAGE_NOT_FOUND", 2)


class ErrQueueInvalidToken(QueueError):
    def __init__(self, message: str = "Queue invalid token") -> None:
        super().__init__(message, "INVALID_TOKEN", 3)


class ErrQueueFull(QueueError):
    def __init__(self, message: str = "Queue full") -> None:
        super().__init__(message, "FULL", 4)


class ErrQueueInvalidDelay(QueueError):
    def __init__(self, message: str = "Queue invalid delay") -> None:
        super().__init__(message, "INVALID_DELAY", 5)


class NoticeError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"NOTICE_{code}", domain_code)


class ErrNoticeGeneral(NoticeError):
    def __init__(self, message: str = "Notice error") -> None:
        super().__init__(message, "GENERAL", 1)


class RpcError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"RPC_{code}", domain_code)


class ErrRpcTimeout(RpcError):
    def __init__(self, message: str = "RPC timeout") -> None:
        super().__init__(message, "TIMEOUT", 1)


class ErrRpcHandlerNotFound(RpcError):
    def __init__(self, message: str = "RPC handler not found") -> None:
        super().__init__(message, "HANDLER_NOT_FOUND", 2)


class ErrRpcHandlerError(RpcError):
    def __init__(self, message: str = "RPC handler error") -> None:
        super().__init__(message, "HANDLER_ERROR", 3)


class ErrRpcInvalidRequest(RpcError):
    def __init__(self, message: str = "RPC invalid request") -> None:
        super().__init__(message, "INVALID_REQUEST", 4)


class LeaseError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"LEASE_{code}", domain_code)


class ErrLeaseHeld(LeaseError):
    def __init__(self, message: str = "Lease is already held") -> None:
        super().__init__(message, "HELD", 1)


class ErrLeaseNotFound(LeaseError):
    def __init__(self, message: str = "Lease not found") -> None:
        super().__init__(message, "NOT_FOUND", 2)


class ErrLeaseInvalidToken(LeaseError):
    def __init__(self, message: str = "Lease invalid token") -> None:
        super().__init__(message, "INVALID_TOKEN", 3)


class StreamError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"STREAM_{code}", domain_code)


class ErrStreamNotFound(StreamError):
    def __init__(self, message: str = "Stream not found") -> None:
        super().__init__(message, "NOT_FOUND", 1)


class ErrStreamOffsetOutOfRange(StreamError):
    def __init__(self, message: str = "Stream offset out of range") -> None:
        super().__init__(message, "OFFSET_OUT_OF_RANGE", 2)


class ErrStreamInvalidOffset(StreamError):
    def __init__(self, message: str = "Stream invalid offset") -> None:
        super().__init__(message, "INVALID_OFFSET", 3)


class ErrStreamFull(StreamError):
    def __init__(self, message: str = "Stream full") -> None:
        super().__init__(message, "FULL", 4)


class ErrStreamSessionNotFound(StreamError):
    def __init__(self, message: str = "Stream session not found") -> None:
        super().__init__(message, "SESSION_NOT_FOUND", 5)


class ErrStreamSessionClosed(StreamError):
    def __init__(self, message: str = "Stream session closed") -> None:
        super().__init__(message, "SESSION_CLOSED", 6)


class ErrStreamExpectedOffsetMismatch(StreamError):
    def __init__(self, message: str = "Stream expected offset mismatch") -> None:
        super().__init__(message, "EXPECTED_OFFSET_MISMATCH", 7)


class ScheduleError(FitzError):
    def __init__(self, message: str, code: str, domain_code: int | None = None) -> None:
        super().__init__(message, f"SCHEDULE_{code}", domain_code)


class ErrScheduleNotFound(ScheduleError):
    def __init__(self, message: str = "Schedule not found") -> None:
        super().__init__(message, "NOT_FOUND", 1)


class ErrScheduleTaskNotFound(ScheduleError):
    def __init__(self, message: str = "Schedule task not found") -> None:
        super().__init__(message, "TASK_NOT_FOUND", 2)


class ErrScheduleInvalidCron(ScheduleError):
    def __init__(self, message: str = "Schedule invalid cron") -> None:
        super().__init__(message, "INVALID_CRON", 3)


class ErrScheduleInvalidDelay(ScheduleError):
    def __init__(self, message: str = "Schedule invalid delay") -> None:
        super().__init__(message, "INVALID_DELAY", 4)


class ErrScheduleInvalidTimestamp(ScheduleError):
    def __init__(self, message: str = "Schedule invalid timestamp") -> None:
        super().__init__(message, "INVALID_TIMESTAMP", 5)


_KV_STATUS_MAP: dict[int, type[KvError]] = {
    1: ErrKvTransactionAborted,
    2: ErrKvLeaseExpired,
    3: ErrKvConflictingWrite,
    4: ErrKvKeyNotFound,
    5: ErrKvOperationNotAllowed,
}

_QUEUE_STATUS_MAP: dict[int, type[QueueError]] = {
    1: ErrQueueNotFound,
    2: ErrQueueMessageNotFound,
    3: ErrQueueInvalidToken,
    4: ErrQueueFull,
    5: ErrQueueInvalidDelay,
}

_RPC_STATUS_MAP: dict[int, type[RpcError]] = {
    1: ErrRpcTimeout,
    2: ErrRpcHandlerNotFound,
    3: ErrRpcHandlerError,
    4: ErrRpcInvalidRequest,
}

_LEASE_STATUS_MAP: dict[int, type[LeaseError]] = {
    1: ErrLeaseHeld,
    2: ErrLeaseNotFound,
    3: ErrLeaseInvalidToken,
}

_NOTICE_STATUS_MAP: dict[int, type[NoticeError]] = {
    1: ErrNoticeGeneral,
}

_STREAM_STATUS_MAP: dict[int, type[StreamError]] = {
    1: ErrStreamNotFound,
    2: ErrStreamOffsetOutOfRange,
    3: ErrStreamInvalidOffset,
    4: ErrStreamFull,
    5: ErrStreamSessionNotFound,
    6: ErrStreamSessionClosed,
    7: ErrStreamExpectedOffsetMismatch,
}

_SCHEDULE_STATUS_MAP: dict[int, type[ScheduleError]] = {
    1: ErrScheduleNotFound,
    2: ErrScheduleTaskNotFound,
    3: ErrScheduleInvalidCron,
    4: ErrScheduleInvalidDelay,
    5: ErrScheduleInvalidTimestamp,
}

_RETRYABLE_ERROR_CODES = {
    "KV_4",
    "QUEUE_4",
    "LEASE_1",
    "NOTICE_1",
    "STREAM_1",
    "STREAM_2",
    "STREAM_3",
    "STREAM_4",
    "RPC_1",
}


def _build_domain_error(
    mapping: dict[int, type[TError]],
    fallback: Callable[[str, int | None], TError],
    message: str,
    domain_code: int | None,
) -> TError:
    if domain_code is not None:
        error_type = mapping.get(domain_code)
        if error_type is not None:
            return error_type(message)
    return fallback(message, domain_code)


def kv_error(message: str, domain_code: int | None = None) -> KvError:
    return _build_domain_error(
        _KV_STATUS_MAP,
        lambda msg, code: KvError(msg, "ERROR", code),
        message,
        domain_code,
    )


def queue_error(message: str, domain_code: int | None = None) -> QueueError:
    return _build_domain_error(
        _QUEUE_STATUS_MAP,
        lambda msg, code: QueueError(msg, "ERROR", code),
        message,
        domain_code,
    )


def rpc_error(message: str, domain_code: int | None = None) -> RpcError:
    return _build_domain_error(
        _RPC_STATUS_MAP,
        lambda msg, code: RpcError(msg, "ERROR", code),
        message,
        domain_code,
    )


def lease_error(message: str, domain_code: int | None = None) -> LeaseError:
    return _build_domain_error(
        _LEASE_STATUS_MAP,
        lambda msg, code: LeaseError(msg, "ERROR", code),
        message,
        domain_code,
    )


def notice_error(message: str, domain_code: int | None = None) -> NoticeError:
    return _build_domain_error(
        _NOTICE_STATUS_MAP,
        lambda msg, code: NoticeError(msg, "ERROR", code),
        message,
        domain_code,
    )


def stream_error(message: str, domain_code: int | None = None) -> StreamError:
    return _build_domain_error(
        _STREAM_STATUS_MAP,
        lambda msg, code: StreamError(msg, "ERROR", code),
        message,
        domain_code,
    )


def schedule_error(message: str, domain_code: int | None = None) -> ScheduleError:
    return _build_domain_error(
        _SCHEDULE_STATUS_MAP,
        lambda msg, code: ScheduleError(msg, "ERROR", code),
        message,
        domain_code,
    )


def is_retryable(error: object) -> bool:
    if not isinstance(error, FitzError):
        return False
    if isinstance(error, (TimeoutError, TransportError)):
        return True
    if error.domain_code is None:
        return False
    prefix = error.code.split("_")[0]
    return f"{prefix}_{error.domain_code}" in _RETRYABLE_ERROR_CODES
