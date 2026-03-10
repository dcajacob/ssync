from __future__ import annotations

import socket
import threading
import time
from pathlib import Path

import pytest

from ssync.space_sync import sender as sender_module
from ssync.space_sync.frames import (
    TransferStatus,
    decode_data_chunk,
    decode_frame,
    decode_manifest,
    decode_repair_done,
    encode_file_info_response,
    encode_repair_request,
    encode_status,
    encode_transfer_complete,
)
from ssync.space_sync.manifest import RepairRequest, TransferManifest
from ssync.space_sync.sender import SpaceSyncSender
from ssync.space_sync.types import FrameType, RemoteFileInfo, SenderConfig, TransferState


def test_apply_rate_limit_treats_bps_as_bits_per_second(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sender = SpaceSyncSender(SenderConfig(max_data_rate_bps=8_000_000))
    sleeps: list[float] = []

    def _fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(sender_module.time, "monotonic", lambda: 10.25)
    monkeypatch.setattr(sender_module.time, "sleep", _fake_sleep)

    paced_total = sender._apply_rate_limit(
        paced_start_s=10.0,
        paced_data_bytes=0,
        just_sent_bytes=1_000_000,
    )

    assert paced_total == 1_000_000
    assert sleeps == [pytest.approx(0.75)]


def test_send_file_uses_single_read_for_manifest(tmp_path: Path) -> None:
    source_path = tmp_path / "payload.bin"
    source_payload = b"single-read-check" * 256
    source_path.write_bytes(source_payload)
    sender = SpaceSyncSender(SenderConfig(enable_feedback=False))

    original_from_bytes = TransferManifest.from_bytes
    captured_raw: list[bytes] = []

    def _wrapped_from_bytes(
        *,
        raw: bytes,
        file_name: str,
        chunk_size: int,
        metadata: dict[int, bytes] | None = None,
    ) -> TransferManifest:
        captured_raw.append(raw)
        return original_from_bytes(
            raw=raw,
            file_name=file_name,
            chunk_size=chunk_size,
            metadata=metadata,
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(sender_module.TransferManifest, "from_bytes", _wrapped_from_bytes)
    try:
        sender.send_file(source_path, "127.0.0.1", 9_999)
    finally:
        monkeypatch.undo()

    assert captured_raw == [source_payload]


def test_query_remote_file_times_out_on_non_matching_responses() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("127.0.0.1", 0))
    host, port = server.getsockname()
    stop_event = threading.Event()

    def _spam_wrong_path() -> None:
        try:
            request_raw, addr = server.recvfrom(65535)
            assert request_raw
            wrong = encode_file_info_response(RemoteFileInfo(path="wrong-name.bin", exists=False))
            while not stop_event.is_set():
                server.sendto(wrong, addr)
                time.sleep(0.01)
        finally:
            server.close()

    worker = threading.Thread(target=_spam_wrong_path, daemon=True)
    worker.start()
    sender = SpaceSyncSender(SenderConfig(enable_feedback=True, feedback_wait_s=0.2))
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        sender.query_remote_file(
            destination_host=str(host),
            destination_port=int(port),
            remote_name="expected-name.bin",
            include_checksum=False,
        )
    elapsed = time.monotonic() - start
    stop_event.set()
    worker.join(timeout=1.0)
    assert elapsed < 1.0


def test_drain_repair_requests_stops_on_transfer_complete_signal() -> None:
    sender = SpaceSyncSender(SenderConfig(enable_feedback=True))
    manifest = TransferManifest.from_bytes(raw=b"abcdef", file_name="sample.bin", chunk_size=2)
    transfer_complete_frame = encode_transfer_complete(manifest.transfer_id)

    class FakeSocket:
        def __init__(self) -> None:
            self._reads = 0

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            if self._reads == 0:
                self._reads += 1
                return transfer_complete_frame, ("127.0.0.1", 9000)
            raise BlockingIOError

        def sendto(self, _payload: bytes, _destination: tuple[str, int]) -> int:
            return 0

    repaired, rounds, paced, completed = sender._drain_repair_requests(
        sock=FakeSocket(),  # type: ignore[arg-type]
        manifest=manifest,
        chunks=[b"ab", b"cd", b"ef"],
        destination=("127.0.0.1", 9000),
        send_repair_done=False,
        paced_start_s=time.monotonic(),
        paced_data_bytes=0,
        max_rounds=1,
        max_chunks=1,
    )

    assert repaired == 0
    assert rounds == 0
    assert paced == 0
    assert completed is True


def test_drain_repair_requests_services_incomplete_status_ranges() -> None:
    sender = SpaceSyncSender(SenderConfig(enable_feedback=True))
    manifest = TransferManifest.from_bytes(raw=b"abcdef", file_name="sample.bin", chunk_size=2)
    status_frame = encode_status(
        TransferStatus(
            transfer_id=manifest.transfer_id,
            state=TransferState.INCOMPLETE,
            missing_ranges=[(1, 2)],
        )
    )
    sent_payloads: list[bytes] = []

    class FakeSocket:
        def __init__(self) -> None:
            self._reads = 0

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            if self._reads == 0:
                self._reads += 1
                return status_frame, ("127.0.0.1", 9000)
            raise BlockingIOError

        def sendto(self, payload: bytes, _destination: tuple[str, int]) -> int:
            sent_payloads.append(payload)
            return len(payload)

    repaired, rounds, paced, completed = sender._drain_repair_requests(
        sock=FakeSocket(),  # type: ignore[arg-type]
        manifest=manifest,
        chunks=[b"ab", b"cd", b"ef"],
        destination=("127.0.0.1", 9000),
        send_repair_done=False,
        paced_start_s=time.monotonic(),
        paced_data_bytes=0,
        max_rounds=1,
        max_chunks=2,
    )

    assert repaired == 1
    assert rounds == 1
    assert paced == 2
    assert completed is False
    assert sent_payloads


def test_maybe_send_beacon_respects_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    sender = SpaceSyncSender(SenderConfig(enable_feedback=True, beacon_interval_s=1.0))
    sent_count = 0

    class FakeSocket:
        def sendto(self, _payload: bytes, _destination: tuple[str, int]) -> int:
            nonlocal sent_count
            sent_count += 1
            return 1

    now_values = iter([10.0, 10.5, 11.1])
    monkeypatch.setattr(sender_module.time, "monotonic", lambda: next(now_values))
    last = 0.0
    last = sender._maybe_send_beacon(
        sock=FakeSocket(),  # type: ignore[arg-type]
        destination=("127.0.0.1", 9000),
        transfer_id=b"\x01" * 16,
        last_beacon_s=last,
    )
    last = sender._maybe_send_beacon(
        sock=FakeSocket(),  # type: ignore[arg-type]
        destination=("127.0.0.1", 9000),
        transfer_id=b"\x01" * 16,
        last_beacon_s=last,
    )
    last = sender._maybe_send_beacon(
        sock=FakeSocket(),  # type: ignore[arg-type]
        destination=("127.0.0.1", 9000),
        transfer_id=b"\x01" * 16,
        last_beacon_s=last,
    )
    assert last == pytest.approx(11.1)
    assert sent_count == 2


def test_send_file_open_loop_uses_blocking_socket(tmp_path: Path) -> None:
    source_path = tmp_path / "payload.bin"
    source_path.write_bytes(b"blocking-socket-check")
    sender = SpaceSyncSender(
        SenderConfig(
            enable_feedback=False,
            manifest_repeats=1,
            drop_every_nth_data=1,
        )
    )
    timeout_values: list[float | None] = []
    blocking_values: list[bool] = []

    class FakeSocket:
        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, value: float | None) -> None:
            timeout_values.append(value)

        def setblocking(self, flag: bool) -> None:
            blocking_values.append(flag)

        def sendto(self, _payload: bytes, _destination: tuple[str, int]) -> int:
            return 1

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: FakeSocket())
    try:
        sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()

    assert blocking_values and blocking_values[0] is True
    assert timeout_values and timeout_values[0] == pytest.approx(0.5)


def test_send_file_honors_stop_requested(tmp_path: Path) -> None:
    source_path = tmp_path / "payload-stop.bin"
    source_path.write_bytes(b"x" * 4096)
    sender = SpaceSyncSender(
        SenderConfig(
            enable_feedback=False,
            chunk_size=256,
            manifest_repeats=1,
        )
    )
    sendto_calls = 0

    class FakeSocket:
        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, _payload: bytes, _destination: tuple[str, int]) -> int:
            nonlocal sendto_calls
            sendto_calls += 1
            return 1

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: FakeSocket())
    try:
        result = sender.send_file(
            source_path,
            "127.0.0.1",
            9000,
            stop_requested=lambda: sendto_calls >= 3,
        )
    finally:
        monkeypatch.undo()

    assert result.completed is False
    assert sendto_calls >= 3


def test_send_file_open_loop_raises_on_send_timeout_without_stop_callback(tmp_path: Path) -> None:
    source_path = tmp_path / "payload-timeout.bin"
    source_path.write_bytes(b"x" * 1024)
    sender = SpaceSyncSender(
        SenderConfig(
            enable_feedback=False,
            chunk_size=256,
            manifest_repeats=1,
        )
    )

    class FakeSocket:
        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, _payload: bytes, _destination: tuple[str, int]) -> int:
            raise TimeoutError

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: FakeSocket())
    try:
        with pytest.raises(TimeoutError):
            sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()


def test_send_file_resends_manifest_and_fin_on_post_fin_timeout(tmp_path: Path) -> None:
    source_path = tmp_path / "payload-post-fin.bin"
    source_path.write_bytes(b"post-fin-timeout")
    sender = SpaceSyncSender(
        SenderConfig(
            enable_feedback=True,
            manifest_repeats=1,
            feedback_wait_s=0.01,
            max_feedback_idle_timeouts=2,
        )
    )
    sent_frame_types: list[FrameType] = []

    class FakeSocket:
        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, payload: bytes, _destination: tuple[str, int]) -> int:
            parsed = decode_frame(payload)
            assert parsed.frame_type is not None
            sent_frame_types.append(parsed.frame_type)
            return len(payload)

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            raise TimeoutError

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: FakeSocket())
    try:
        result = sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()

    assert result.completed is False
    # Initial send + two timeout retries.
    assert sent_frame_types.count(FrameType.FIN) == 3
    # Initial manifest + two timeout retries.
    assert sent_frame_types.count(FrameType.MANIFEST) == 3


def test_post_fin_duplicate_repair_request_not_acknowledged_without_data(tmp_path: Path) -> None:
    source_path = tmp_path / "payload-dup-repair.bin"
    source_path.write_bytes(b"x" * 2048)
    sender = SpaceSyncSender(
        SenderConfig(
            enable_feedback=True,
            manifest_repeats=1,
            chunk_size=1024,
            feedback_wait_s=0.01,
            max_feedback_idle_timeouts=1,
            repair_duplicate_suppression_s=5.0,
        )
    )
    repair_done_count = 0
    repair_data_chunks_sent: list[int] = []
    transfer_id: bytes | None = None
    fin_sent = False

    class FakeSocket:
        def __init__(self) -> None:
            self._responses_served = 0

        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, payload: bytes, _destination: tuple[str, int]) -> int:
            nonlocal fin_sent, repair_done_count, transfer_id
            parsed = decode_frame(payload)
            assert parsed.frame_type is not None
            if parsed.frame_type == FrameType.MANIFEST:
                transfer_id = decode_manifest(parsed.payload).transfer_id
            elif parsed.frame_type == FrameType.FIN:
                fin_sent = True
            elif parsed.frame_type == FrameType.DATA:
                chunk = decode_data_chunk(parsed.payload)
                repair_data_chunks_sent.append(chunk.chunk_index)
            elif parsed.frame_type == FrameType.REPAIR_DONE:
                _ = decode_repair_done(parsed.payload)
                repair_done_count += 1
            return len(payload)

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            if not fin_sent:
                raise BlockingIOError
            if self._responses_served < 2:
                assert transfer_id is not None
                self._responses_served += 1
                request = encode_repair_request(
                    RepairRequest(transfer_id=transfer_id, missing_ranges=[(0, 1)])
                )
                return request, ("127.0.0.1", 9000)
            raise TimeoutError

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: FakeSocket())
    try:
        _ = sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()

    # First request is serviced; duplicate should be suppressed without done ack.
    assert repair_done_count == 1
    assert repair_data_chunks_sent.count(0) >= 1
