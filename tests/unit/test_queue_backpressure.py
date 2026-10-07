from types import SimpleNamespace

import pytest

from fitz_py.domains.queue import QueueClient, QueueItem
from fitz_py.domains.schedule import ScheduleClient
from fitz_py.errors import ProtocolError, QueueError, ScheduleError, StaleHandleError, is_retryable
from fitz_py.protocol.buffer import BufferWriter
from fitz_py.protocol.response import parse_plain_or_coded_response


@pytest.mark.asyncio
async def test_should_preserve_coded_capacity_for_queue_completion() -> None:
    # Arrange
    writer = BufferWriter()
    writer.write_u8(1)
    writer.write_u32_be(4005)
    writer.write_string("not accepted")

    async def request(_message_type: int, _payload: bytes) -> bytes:
        return writer.build()

    client = object.__new__(QueueClient)
    client.connection = SimpleNamespace(request=request, generation=1)
    item = QueueItem("queue://prod/app/jobs", b"body", 7, 11, client, 1)
    # Act
    with pytest.raises(QueueError) as caught:
        await item.complete()
    # Assert
    assert caught.value.domain_code == 4005
    assert is_retryable(caught.value)
    assert not item._closed


@pytest.mark.asyncio
async def test_should_preserve_coded_capacity_for_schedule_cancel() -> None:
    # Arrange
    writer = BufferWriter()
    writer.write_u8(1)
    writer.write_u32_be(7010)
    writer.write_string("not accepted")

    async def request(_message_type: int, _payload: bytes) -> bytes:
        return writer.build()

    client = object.__new__(ScheduleClient)
    client.connection = SimpleNamespace(request=request)
    # Act
    with pytest.raises(ScheduleError) as caught:
        await client.cancel("schedule://prod/app/jobs/run")
    # Assert
    assert caught.value.domain_code == 7010
    assert is_retryable(caught.value)


@pytest.mark.parametrize("code, retryable", [(4005, True), (4007, False)])
@pytest.mark.parametrize("operation", ["complete", "extend"])
@pytest.mark.asyncio
async def test_should_surface_queue_rejection_without_automatic_replay(code, retryable, operation):
    # Arrange
    writer = BufferWriter()
    writer.write_u8(1)
    writer.write_u32_be(code)
    writer.write_string("broker rejection")
    calls = 0

    async def request(_message_type: int, _payload: bytes) -> bytes:
        nonlocal calls
        calls += 1
        return writer.build()

    client = object.__new__(QueueClient)
    client.connection = SimpleNamespace(request=request, generation=1)
    item = QueueItem("queue://prod/app/jobs", b"body", 7, 11, client, 1)
    # Act
    with pytest.raises(QueueError) as caught:
        if operation == "complete":
            await item.complete()
        else:
            await item.extend(30)
    # Assert
    assert caught.value.domain_code == code
    assert is_retryable(caught.value) is retryable
    assert calls == 1
    assert not item._closed


def test_should_preserve_long_plain_error_without_inferring_capacity():
    # Arrange
    writer = BufferWriter()
    writer.write_u8(1)
    writer.write_string("x" * 4005)
    # Act
    response = parse_plain_or_coded_response(writer.build())
    # Assert
    assert response.error_code is None
    assert response.error == "x" * 4005


def test_should_reject_malformed_capacity_response():
    # Arrange
    payload = bytes([1, 0, 0, 15, 165, 0, 0, 0, 2, 120])
    # Act / Assert
    with pytest.raises(ProtocolError):
        parse_plain_or_coded_response(payload)


@pytest.mark.asyncio
async def test_should_retain_reservation_until_explicit_successful_completion():
    # Arrange
    writer = BufferWriter()
    writer.write_u8(1)
    writer.write_u32_be(4005)
    writer.write_string("not accepted")
    payloads = []

    async def request(_message_type: int, payload: bytes) -> bytes:
        payloads.append(payload)
        return writer.build() if len(payloads) == 1 else b"\0"

    client = object.__new__(QueueClient)
    client.connection = SimpleNamespace(request=request, generation=1)
    item = QueueItem("queue://prod/app/jobs", b"body", 7, 11, client, 1)
    with pytest.raises(QueueError):
        await item.complete()
    # Act
    await item.complete()
    # Assert
    assert payloads == [payloads[0], payloads[0]]
    with pytest.raises(StaleHandleError):
        await item.complete()
    assert len(payloads) == 2


@pytest.mark.asyncio
async def test_should_not_replay_completion_after_uncertain_timeout():
    # Arrange
    calls = 0

    async def request(_message_type: int, _payload: bytes) -> bytes:
        nonlocal calls
        calls += 1
        raise TimeoutError("outcome unknown")

    client = object.__new__(QueueClient)
    client.connection = SimpleNamespace(request=request, generation=1)
    item = QueueItem("queue://prod/app/jobs", b"body", 7, 11, client, 1)
    # Act / Assert
    with pytest.raises(TimeoutError):
        await item.complete()
    assert calls == 1
