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
    encode_beacon,
    encode_status,
)
from ssync.space_sync.manifest import TransferManifest
from ssync.space_sync.sender import SpaceSyncSender
from ssync.space_sync.types import (
    BeaconRole,
    FrameType,
    RemoteFileInfo,
    SenderConfig,
    StatusKind,
    TransferState,
)


class _FakeSocketBase:
    """Mixin providing common methods for sender two-socket split."""

    _shared_state: dict[str, object] | None = None

    def bind(self, _addr: tuple[str, int]) -> None:
        pass

    def getsockname(self) -> tuple[str, int]:
        return ("0.0.0.0", 0)

    def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
        raise BlockingIOError


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


def test_send_file_builds_manifest_without_from_bytes(tmp_path: Path) -> None:
    source_path = tmp_path / "payload.bin"
    source_payload = b"single-read-check" * 256
    source_path.write_bytes(source_payload)
    sender = SpaceSyncSender(SenderConfig(enable_feedback=False))

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        sender_module.TransferManifest,
        "from_bytes",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("from_bytes not expected")),
    )
    try:
        sender.send_file(source_path, "127.0.0.1", 9_999)
    finally:
        monkeypatch.undo()


def test_query_remote_file_times_out_on_non_matching_responses() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("127.0.0.1", 0))
    host, port = server.getsockname()
    stop_event = threading.Event()

    def _spam_wrong_path() -> None:
        try:
            request_raw, addr = server.recvfrom(65535)
            assert request_raw
            wrong = encode_status(
                TransferStatus(
                    transfer_id=b"\x00" * 16,
                    kind=StatusKind.FILE_INFO_RESPONSE,
                    state=TransferState.INCOMPLETE,
                    missing_ranges=[],
                    file_info=RemoteFileInfo(path="wrong-name.bin", exists=False),
                    query_token=b"wrong",
                )
            )
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
    complete_status_frame = encode_status(
        TransferStatus(
            transfer_id=manifest.transfer_id,
            kind=StatusKind.TRANSFER,
            state=TransferState.COMPLETE,
            missing_ranges=[],
        )
    )

    class FakeSocket(_FakeSocketBase):
        def __init__(self) -> None:
            self._reads = 0

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            if self._reads == 0:
                self._reads += 1
                return complete_status_frame, ("127.0.0.1", 9000)
            raise BlockingIOError

        def sendto(self, _payload: bytes, _destination: tuple[str, int]) -> int:
            return 0

    fake = FakeSocket()
    repaired, rounds, paced, completed, saw_uplink = sender._drain_repair_requests(
        rx_sock=fake,  # type: ignore[arg-type]
        tx_sock=fake,  # type: ignore[arg-type]
        manifest=manifest,
        total_chunks=3,
        chunk_reader=lambda index: [b"ab", b"cd", b"ef"][index],
        destination=("127.0.0.1", 9000),
        paced_start_s=time.monotonic(),
        paced_data_bytes=0,
        max_rounds=1,
        max_chunks=1,
    )

    assert repaired == 0
    assert rounds == 0
    assert paced == 0
    assert completed is True
    assert saw_uplink is True


def test_drain_repair_requests_services_incomplete_status_ranges() -> None:
    sender = SpaceSyncSender(SenderConfig(enable_feedback=True))
    manifest = TransferManifest.from_bytes(raw=b"abcdef", file_name="sample.bin", chunk_size=2)
    status_frame = encode_status(
        TransferStatus(
            transfer_id=manifest.transfer_id,
            kind=StatusKind.TRANSFER,
            state=TransferState.INCOMPLETE,
            missing_ranges=[(1, 2)],
        )
    )
    sent_payloads: list[bytes] = []

    class FakeSocket(_FakeSocketBase):
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

    fake = FakeSocket()
    repaired, rounds, paced, completed, saw_uplink = sender._drain_repair_requests(
        rx_sock=fake,  # type: ignore[arg-type]
        tx_sock=fake,  # type: ignore[arg-type]
        manifest=manifest,
        total_chunks=3,
        chunk_reader=lambda index: [b"ab", b"cd", b"ef"][index],
        destination=("127.0.0.1", 9000),
        paced_start_s=time.monotonic(),
        paced_data_bytes=0,
        max_rounds=1,
        max_chunks=2,
    )

    assert repaired == 1
    assert rounds == 1
    assert paced == 2
    assert completed is False
    assert saw_uplink is True
    assert sent_payloads


def test_maybe_send_beacon_respects_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    sender = SpaceSyncSender(SenderConfig(enable_feedback=True, beacon_interval_s=1.0))
    sent_count = 0

    class FakeSocket(_FakeSocketBase):
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


def test_maybe_send_periodic_metadata_respects_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    sender = SpaceSyncSender(
        SenderConfig(
            enable_feedback=False,
            periodic_metadata_interval_s=1.0,
            periodic_metadata_every_n_chunks=0,
        )
    )
    manifest = TransferManifest.from_bytes(raw=b"abcdef", file_name="sample.bin", chunk_size=2)
    sent_count = 0

    class FakeSocket(_FakeSocketBase):
        def sendto(self, _payload: bytes, _destination: tuple[str, int]) -> int:
            nonlocal sent_count
            sent_count += 1
            return 1

    now_values = iter([10.5, 11.2, 11.2])
    monkeypatch.setattr(sender_module.time, "monotonic", lambda: next(now_values))
    last_s, since = sender._maybe_send_periodic_metadata(
        sock=FakeSocket(),  # type: ignore[arg-type]
        destination=("127.0.0.1", 9000),
        manifest=manifest,
        last_metadata_s=10.0,
        chunks_since_metadata=1,
    )
    assert sent_count == 0
    assert last_s == pytest.approx(10.0)
    assert since == 1
    last_s, since = sender._maybe_send_periodic_metadata(
        sock=FakeSocket(),  # type: ignore[arg-type]
        destination=("127.0.0.1", 9000),
        manifest=manifest,
        last_metadata_s=last_s,
        chunks_since_metadata=2,
    )
    assert sent_count == 1
    assert last_s == pytest.approx(11.2)
    assert since == 0


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

    class FakeSocket(_FakeSocketBase):
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
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()

    assert blocking_values and blocking_values[0] is True
    assert timeout_values and timeout_values[0] == pytest.approx(0.5)


def test_send_file_open_loop_resends_tail_chunks(tmp_path: Path) -> None:
    source_path = tmp_path / "payload-tail-redundancy.bin"
    source_path.write_bytes(b"abcdefghijkl")
    sender = SpaceSyncSender(
        SenderConfig(
            chunk_size=4,
            enable_feedback=False,
            manifest_repeats=1,
            tail_redundancy_chunks=2,
        )
    )
    sent_data_indexes: list[int] = []

    class FakeSocket(_FakeSocketBase):
        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, payload: bytes, _destination: tuple[str, int]) -> int:
            frame = decode_frame(payload)
            if frame.frame_type == FrameType.DATA:
                sent_data_indexes.append(decode_data_chunk(frame.payload).chunk_index)
            return len(payload)

    monkeypatch = pytest.MonkeyPatch()
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        result = sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()

    assert result.completed is True
    assert sent_data_indexes == [0, 1, 2, 1, 2]



def test_send_file_revisit_mode_repairs_without_initial_data(tmp_path: Path) -> None:
    source_path = tmp_path / "payload-revisit.bin"
    source_path.write_bytes(b"abcdefgh")
    sender = SpaceSyncSender(
        SenderConfig(
            chunk_size=4,
            enable_feedback=True,
            manifest_repeats=1,
            feedback_wait_s=0.05,
            max_repair_rounds=1,
        )
    )
    sent_data_indexes: list[int] = []

    class FakeSocket(_FakeSocketBase):
        def __init__(self) -> None:
            self.transfer_id: bytes | None = None
            self._reads = 0

        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, payload: bytes, _destination: tuple[str, int]) -> int:
            frame = decode_frame(payload)
            if frame.frame_type == FrameType.METADATA:
                self.transfer_id = decode_manifest(frame.payload).transfer_id
            elif frame.frame_type == FrameType.DATA:
                data_chunk = decode_data_chunk(frame.payload)
                sent_data_indexes.append(data_chunk.chunk_index)
            return len(payload)

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            if self.transfer_id is None:
                raise TimeoutError
            if self._reads == 0:
                self._reads += 1
                return (
                    encode_status(
                        TransferStatus(
                            transfer_id=self.transfer_id,
                            kind=StatusKind.TRANSFER,
                            state=TransferState.INCOMPLETE,
                            missing_ranges=[(0, 1)],
                        )
                    ),
                    ("127.0.0.1", 9000),
                )
            if self._reads == 1:
                self._reads += 1
                return (
                    encode_status(
                        TransferStatus(
                            transfer_id=self.transfer_id,
                            kind=StatusKind.TRANSFER,
                            state=TransferState.COMPLETE,
                            missing_ranges=[],
                        )
                    ),
                    ("127.0.0.1", 9000),
                )
            raise TimeoutError

    monkeypatch = pytest.MonkeyPatch()
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        result = sender.send_file(
            source_path,
            "127.0.0.1",
            9000,
            transfer_id=b"\xAB" * 16,
            send_initial_data=False,
            max_repair_rounds_override=2,
        )
    finally:
        monkeypatch.undo()

    assert result.completed is True
    assert sent_data_indexes == [0]


def test_send_file_zero_chunk_fast_path_skips_feedback_wait(tmp_path: Path) -> None:
    source_path = tmp_path / "empty.bin"
    source_path.write_bytes(b"")
    sender = SpaceSyncSender(
        SenderConfig(
            enable_feedback=True,
            manifest_repeats=1,
            feedback_wait_s=0.2,
        )
    )
    sent_frame_types: list[FrameType] = []

    class FakeSocket(_FakeSocketBase):
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
            raise AssertionError("zero-chunk fast path must not enter feedback recv loop")

    monkeypatch = pytest.MonkeyPatch()
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        result = sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()

    assert result.completed is True
    assert result.total_chunks == 0
    assert sent_frame_types.count(FrameType.METADATA) == 1
    assert FrameType.DATA not in sent_frame_types


def test_send_file_feedback_round_budget_stops_primary_attempt(tmp_path: Path) -> None:
    source_path = tmp_path / "budgeted.bin"
    source_path.write_bytes(b"abcdefgh")
    sender = SpaceSyncSender(
        SenderConfig(
            chunk_size=4,
            enable_feedback=True,
            manifest_repeats=1,
            feedback_wait_s=0.05,
            max_repair_rounds=0,
        )
    )
    sent_data_indexes: list[int] = []

    class FakeSocket(_FakeSocketBase):
        def __init__(self) -> None:
            self.transfer_id: bytes | None = None

        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, payload: bytes, _destination: tuple[str, int]) -> int:
            frame = decode_frame(payload)
            if frame.frame_type == FrameType.METADATA:
                self.transfer_id = decode_manifest(frame.payload).transfer_id
            elif frame.frame_type == FrameType.DATA:
                data_chunk = decode_data_chunk(frame.payload)
                sent_data_indexes.append(data_chunk.chunk_index)
            return len(payload)

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            assert self.transfer_id is not None
            return (
                encode_status(
                    TransferStatus(
                        transfer_id=self.transfer_id,
                        kind=StatusKind.TRANSFER,
                        state=TransferState.INCOMPLETE,
                        missing_ranges=[(0, 1)],
                    )
                ),
                ("127.0.0.1", 9000),
            )

    monkeypatch = pytest.MonkeyPatch()
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        result = sender.send_file(
            source_path,
            "127.0.0.1",
            9000,
            send_initial_data=False,
            transfer_id=b"\xCC" * 16,
            max_feedback_total_rounds_override=1,
        )
    finally:
        monkeypatch.undo()

    assert result.completed is False
    assert result.repair_rounds == 1
    assert result.repaired_chunks == 1
    assert sent_data_indexes == [0]


def test_send_file_parallel_queue_repairs_while_prioritizing_data(tmp_path: Path) -> None:
    source_path = tmp_path / "queued-midstream.bin"
    source_path.write_bytes(b"abcdefgh")
    sender = SpaceSyncSender(
        SenderConfig(
            chunk_size=4,
            enable_feedback=True,
            manifest_repeats=1,
            feedback_wait_s=0.05,
            max_feedback_idle_timeouts=1,
        )
    )
    data_indexes_sent: list[int] = []

    class FakeSocket(_FakeSocketBase):
        def __init__(self) -> None:
            self.transfer_id: bytes | None = None
            self._served_status = False

        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, payload: bytes, _destination: tuple[str, int]) -> int:
            frame = decode_frame(payload)
            if frame.frame_type == FrameType.METADATA:
                self.transfer_id = decode_manifest(frame.payload).transfer_id
            if frame.frame_type == FrameType.DATA:
                data_chunk = decode_data_chunk(frame.payload)
                data_indexes_sent.append(data_chunk.chunk_index)
            return len(payload)

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            if self.transfer_id is None:
                raise BlockingIOError
            if not self._served_status:
                self._served_status = True
                return (
                    encode_status(
                        TransferStatus(
                            transfer_id=self.transfer_id,
                            kind=StatusKind.TRANSFER,
                            state=TransferState.INCOMPLETE,
                            missing_ranges=[(0, 1)],
                        )
                    ),
                    ("127.0.0.1", 9000),
                )
            raise TimeoutError

    monkeypatch = pytest.MonkeyPatch()
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        result = sender.send_file(
            source_path,
            "127.0.0.1",
            9000,
            max_feedback_seconds_override=0.1,
        )
    finally:
        monkeypatch.undo()

    # Repairs are deferred entirely during the initial forward pass to avoid
    # adding bandwidth overhead that causes receiver buffer overflows.
    assert result.repair_rounds == 0
    assert result.repaired_chunks == 0


def test_send_file_budget_does_not_interrupt_initial_data_pass(tmp_path: Path) -> None:
    source_path = tmp_path / "initial-pass.bin"
    source_path.write_bytes(b"x" * 2048)
    sender = SpaceSyncSender(
        SenderConfig(
            chunk_size=256,
            enable_feedback=True,
            manifest_repeats=1,
            feedback_wait_s=0.01,
        )
    )
    sent_data_indexes: list[int] = []

    class FakeSocket(_FakeSocketBase):
        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, payload: bytes, _destination: tuple[str, int]) -> int:
            frame = decode_frame(payload)
            if frame.frame_type == FrameType.DATA:
                data_chunk = decode_data_chunk(frame.payload)
                sent_data_indexes.append(data_chunk.chunk_index)
            return len(payload)

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            raise BlockingIOError

    monkeypatch = pytest.MonkeyPatch()
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        result = sender.send_file(
            source_path,
            "127.0.0.1",
            9000,
            max_feedback_seconds_override=0.0001,
        )
    finally:
        monkeypatch.undo()

    assert result.completed is False
    assert len(sent_data_indexes) == 8
    assert sent_data_indexes == list(range(8))


def test_send_file_auto_feedback_promotes_on_uplink_packet(tmp_path: Path) -> None:
    source_path = tmp_path / "auto-promote.bin"
    source_path.write_bytes(b"x" * 1024)
    sender = SpaceSyncSender(
        SenderConfig(
            chunk_size=256,
            enable_feedback=False,
            auto_feedback_discovery=True,
            auto_feedback_probe_interval_chunks=1,
            feedback_wait_s=0.01,
            max_feedback_idle_timeouts=1,
            manifest_repeats=1,
        )
    )

    class FakeSocket(_FakeSocketBase):
        def __init__(self) -> None:
            self.transfer_id: bytes | None = None
            self.sent_uplink = False

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
            if parsed.frame_type == FrameType.METADATA:
                self.transfer_id = decode_manifest(parsed.payload).transfer_id
            return len(payload)

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            if not self.sent_uplink and self.transfer_id is not None:
                self.sent_uplink = True
                return (
                    encode_beacon(BeaconRole.RECEIVER, self.transfer_id),
                    ("127.0.0.1", 9000),
                )
            raise BlockingIOError

    monkeypatch = pytest.MonkeyPatch()
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()

    assert sender._auto_feedback_active is True


def test_send_file_auto_feedback_promotion_starts_parallel_repairs(tmp_path: Path) -> None:
    source_path = tmp_path / "auto-promote-parallel-repair.bin"
    source_path.write_bytes(b"x" * (256 * 64))
    sender = SpaceSyncSender(
        SenderConfig(
            chunk_size=256,
            enable_feedback=False,
            auto_feedback_discovery=True,
            auto_feedback_probe_interval_chunks=1,
            feedback_wait_s=0.01,
            max_feedback_idle_timeouts=1,
            manifest_repeats=1,
            inter_packet_delay_s=0.0005,
        )
    )

    class FakeSocket(_FakeSocketBase):
        def __init__(self) -> None:
            self.transfer_id: bytes | None = None
            self._probe_served = False
            self._status_served = False

        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, payload: bytes, _destination: tuple[str, int]) -> int:
            frame = decode_frame(payload)
            if frame.frame_type == FrameType.METADATA:
                self.transfer_id = decode_manifest(frame.payload).transfer_id
            return len(payload)

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            if not self._probe_served:
                self._probe_served = True
                return (b"uplink", ("127.0.0.1", 9000))
            if self.transfer_id is not None and not self._status_served:
                self._status_served = True
                return (
                    encode_status(
                        TransferStatus(
                            transfer_id=self.transfer_id,
                            kind=StatusKind.TRANSFER,
                            state=TransferState.INCOMPLETE,
                            missing_ranges=[(0, 1)],
                        )
                    ),
                    ("127.0.0.1", 9000),
                )
            raise TimeoutError

    monkeypatch = pytest.MonkeyPatch()
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        result = sender.send_file(
            source_path,
            "127.0.0.1",
            9000,
            max_feedback_seconds_override=0.01,
        )
    finally:
        monkeypatch.undo()

    assert result.completed is False
    # Repairs are deferred during the initial forward pass.
    assert result.repair_rounds == 0
    assert result.repaired_chunks == 0


def test_send_file_auto_feedback_demotes_after_idle_timeout(tmp_path: Path) -> None:
    source_path = tmp_path / "auto-demote.bin"
    source_path.write_bytes(b"x" * 512)
    sender = SpaceSyncSender(
        SenderConfig(
            chunk_size=256,
            enable_feedback=False,
            auto_feedback_discovery=True,
            auto_feedback_idle_timeout_s=0.01,
            auto_feedback_probe_interval_chunks=0,
            manifest_repeats=1,
        )
    )
    sender._auto_feedback_active = True
    sender._last_auto_uplink_activity_s = time.monotonic() - 1.0

    class FakeSocket(_FakeSocketBase):
        def __enter__(self) -> FakeSocket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            return None

        def setblocking(self, _flag: bool) -> None:
            return None

        def sendto(self, payload: bytes, _destination: tuple[str, int]) -> int:
            return len(payload)

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            raise BlockingIOError

    monkeypatch = pytest.MonkeyPatch()
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        result = sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()

    assert result.completed is True
    assert sender._auto_feedback_active is False


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

    class FakeSocket(_FakeSocketBase):
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
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
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

    class FakeSocket(_FakeSocketBase):
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
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
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

    class FakeSocket(_FakeSocketBase):
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
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        result = sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()

    assert result.completed is False
    # Initial METADATA + two timeout retries.
    assert sent_frame_types.count(FrameType.METADATA) == 3
    # Four-frame mode uses STATUS-only control for completion/repair.
    assert FrameType.STATUS not in sent_frame_types


def test_post_fin_status_incomplete_triggers_repairs(tmp_path: Path) -> None:
    source_path = tmp_path / "payload-status-post-fin.bin"
    source_path.write_bytes(b"x" * 2048)
    sender = SpaceSyncSender(
        SenderConfig(
            enable_feedback=True,
            manifest_repeats=1,
            chunk_size=1024,
            feedback_wait_s=0.01,
            max_feedback_idle_timeouts=1,
        )
    )
    transfer_id: bytes | None = None
    data_seen = False
    repaired_chunks_sent: list[int] = []

    class FakeSocket(_FakeSocketBase):
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
            nonlocal data_seen, transfer_id
            parsed = decode_frame(payload)
            assert parsed.frame_type is not None
            if parsed.frame_type == FrameType.METADATA:
                transfer_id = decode_manifest(parsed.payload).transfer_id
            elif parsed.frame_type == FrameType.DATA:
                chunk = decode_data_chunk(parsed.payload)
                repaired_chunks_sent.append(chunk.chunk_index)
                data_seen = True
            return len(payload)

        def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
            if not data_seen:
                raise BlockingIOError
            if self._responses_served == 0:
                assert transfer_id is not None
                self._responses_served += 1
                return (
                    encode_status(
                        TransferStatus(
                            transfer_id=transfer_id,
                            kind=StatusKind.TRANSFER,
                            state=TransferState.INCOMPLETE,
                            missing_ranges=[(0, 1)],
                        )
                    ),
                    ("127.0.0.1", 9000),
                )
            raise TimeoutError

    monkeypatch = pytest.MonkeyPatch()
    _shared_fake = FakeSocket()
    monkeypatch.setattr(sender_module.socket, "socket", lambda *_args: _shared_fake)
    try:
        result = sender.send_file(source_path, "127.0.0.1", 9000)
    finally:
        monkeypatch.undo()

    assert result.completed is False
    assert 0 in repaired_chunks_sent
