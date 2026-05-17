from fitz_py import __all__ as public_exports
from fitz_py.domains.lease import Lease, LeaseSubscription
from fitz_py.domains.queue import QueueItem, QueueSubscription
from fitz_py.domains.rpc import InboundRpcRequest


def test_public_exports_snapshot() -> None:
    assert public_exports == [
        "AuthenticationError",
        "Client",
        "ClientConfig",
        "CodecError",
        "ConnectionError",
        "ConnectionState",
        "ErrKvConflictingWrite",
        "ErrKvKeyNotFound",
        "ErrKvLeaseExpired",
        "ErrKvOperationNotAllowed",
        "ErrKvTransactionAborted",
        "ErrLeaseHeld",
        "ErrLeaseInvalidToken",
        "ErrLeaseNotFound",
        "ErrNoticeGeneral",
        "ErrQueueFull",
        "ErrQueueInvalidDelay",
        "ErrQueueInvalidToken",
        "ErrQueueMessageNotFound",
        "ErrQueueNotFound",
        "ErrRpcHandlerError",
        "ErrRpcHandlerNotFound",
        "ErrRpcInvalidRequest",
        "ErrRpcTimeout",
        "ErrScheduleInvalidCron",
        "ErrScheduleInvalidDelay",
        "ErrScheduleInvalidTimestamp",
        "ErrScheduleNotFound",
        "ErrScheduleTaskNotFound",
        "ErrStreamExpectedOffsetMismatch",
        "ErrStreamFull",
        "ErrStreamInvalidOffset",
        "ErrStreamNotFound",
        "ErrStreamOffsetOutOfRange",
        "ErrStreamSessionClosed",
        "ErrStreamSessionNotFound",
        "FitzError",
        "InboundRpcRequest",
        "KVDurability",
        "KVMode",
        "KvClient",
        "KvGetResult",
        "KvError",
        "KvPair",
        "KvScanResult",
        "KvTransaction",
        "Lease",
        "LeaseClient",
        "LeaseError",
        "LeaseHandler",
        "LeaseInfo",
        "LeaseSubscription",
        "NoticeClient",
        "NoticeError",
        "NoticeHandler",
        "NoticeMessage",
        "NoticeSubscription",
        "ProtocolError",
        "QueueAvailabilityHandler",
        "QueueClient",
        "QueueError",
        "QueueItem",
        "QueueSubscription",
        "ReconnectOptions",
        "ResponseFrame",
        "ResponseWriter",
        "RpcClient",
        "RpcError",
        "RpcHandler",
        "RpcSubscription",
        "ScheduleClient",
        "ScheduleEntry",
        "ScheduleError",
        "ScheduleHandler",
        "ScheduleNotification",
        "ScheduleSubscription",
        "StreamClient",
        "StreamFilterClause",
        "StreamFilterSet",
        "StreamCommitMode",
        "StreamCommitNotification",
        "StreamError",
        "StreamFilteredReason",
        "StreamHandler",
        "StreamMetadata",
        "StreamReadCursor",
        "StreamReadItem",
        "StreamReadItemKind",
        "StreamReadPage",
        "StreamRecord",
        "StreamSession",
        "StreamSubscription",
        "TimeoutError",
        "TokenProvider",
        "TransportError",
        "TransportType",
        "is_retryable",
    ]


def test_public_domain_types_hide_wire_identifiers() -> None:
    request = InboundRpcRequest(route="rpc://realm/area/task", reply_route="", body=b"ping")
    assert not hasattr(request, "correlation_id")

    queue_item = QueueItem(route="queue://realm/area/item", _id=1, _token=2, body=b"body", _client=object())
    assert not hasattr(queue_item, "id")
    assert not hasattr(queue_item, "token")

    lease = Lease(route="lease://realm/area/lock", _token=3, _client=object())
    assert lease.token == 3

    queue_subscription = QueueSubscription(4, "queue://realm/area/*", lambda _sub_id: None)
    assert not hasattr(queue_subscription, "sub_id")

    lease_subscription = LeaseSubscription(5, "lease://realm/area/*", lambda _sub_id: None)
    assert not hasattr(lease_subscription, "sub_id")
