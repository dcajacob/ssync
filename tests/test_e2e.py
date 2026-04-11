from __future__ import annotations

import json
import socket
import time
from pathlib import Path

import pytest

from ssync.space_sync import receiver as receiver_module
from ssync.space_sync.frames import (
    HEADER_STRUCT,
    decode_frame,
    decode_status,
    encode_beacon,
    encode_data_chunk,
    encode_manifest,
)
from ssync.space_sync.manifest import TransferManifest
from ssync.space_sync.receiver import SpaceSyncReceiver
from ssync.space_sync.sender import SpaceSyncSender
from ssync.space_sync.types import (
    BeaconRole,
    FrameType,
    ReceiverConfig,
    SenderConfig,
    TransferState,
)


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_file(path: Path, timeout_s: float = 3.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists():
            return True
        time.sleep(0.05)
    return False


def _wait_for_predicate(predicate: object, timeout_s: float = 3.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if callable(predicate) and predicate():
            return True
        time.sleep(0.05)
    return False


def _wait_for_ipc_events(
    sock: socket.socket,
    *,
    min_events: int,
    timeout_s: float = 2.0,
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline and len(events) < min_events:
        try:
            payload = sock.recv(65535)
        except (BlockingIOError, TimeoutError):
            time.sleep(0.02)
            continue
        if not payload:
            continue
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(decoded, dict):
            events.append(decoded)
    return events


def test_open_loop_local_transfer(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-open"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=False),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-open.bin"
        source_payload = b"abc123" * 500
        source_path.write_bytes(source_payload)

        sender = SpaceSyncSender(
            config=SenderConfig(chunk_size=256, enable_feedback=False),
        )
        result = sender.send_file(source_path, "127.0.0.1", receiver.bind_port)
        assert result.total_chunks > 0

        target_path = receiver_dir / source_path.name
        assert _wait_for_file(target_path)
        assert target_path.read_bytes() == source_payload
        part_path = receiver_dir / f".{result.transfer_id_hex}.part"
        assert not part_path.exists()
    finally:
        receiver.stop()


def test_receiver_emits_monitor_ipc_events(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-ipc-events"
    ipc_socket_path = tmp_path / "ssync-monitor-test.sock"
    monitor_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    monitor_sock.bind(str(ipc_socket_path))
    monitor_sock.setblocking(False)
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(
            output_dir=receiver_dir,
            enable_feedback=True,
            monitor_ipc_socket=ipc_socket_path,
        ),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-ipc.bin"
        source_path.write_bytes(b"ipc-events-" * 8_000)
        sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=256,
                enable_feedback=True,
                feedback_wait_s=0.2,
                max_repair_rounds=1,
                beacon_interval_s=0.01,
            )
        )
        sender.send_file(source_path, "127.0.0.1", receiver.bind_port)
        events = _wait_for_ipc_events(monitor_sock, min_events=2, timeout_s=3.0)
        assert events
        event_types = {str(item.get("type", "")) for item in events}
        assert "transfer_update" in event_types
        assert ("beacon_rx" in event_types) or ("beacon_tx" in event_types)
    finally:
        receiver.stop()
        monitor_sock.close()
        try:
            ipc_socket_path.unlink()
        except OSError:
            pass


def test_receiver_monitor_ipc_cleans_up_stale_socket_path(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-ipc-stale"
    ipc_socket_path = tmp_path / "ssync-monitor-stale.sock"
    stale_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    stale_sock.bind(str(ipc_socket_path))
    stale_sock.close()
    assert ipc_socket_path.exists()

    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(
            output_dir=receiver_dir,
            enable_feedback=True,
            monitor_ipc_socket=ipc_socket_path,
        ),
    )
    receiver._publish_monitor_event({"type": "transfer_update", "transfer_id_hex": "abc"})
    assert not ipc_socket_path.exists()


def test_feedback_zero_chunk_send_returns_immediately(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-empty-feedback"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver.start()
    try:
        source_path = tmp_path / "empty.bin"
        source_path.write_bytes(b"")
        sender = SpaceSyncSender(
            config=SenderConfig(
                enable_feedback=True,
                feedback_wait_s=0.2,
                manifest_repeats=1,
            )
        )
        start = time.monotonic()
        result = sender.send_file(source_path, "127.0.0.1", receiver.bind_port)
        elapsed = time.monotonic() - start
        assert result.completed is True
        assert result.total_chunks == 0
        assert elapsed < 0.5
        target_path = receiver_dir / source_path.name
        assert _wait_for_file(target_path)
        assert target_path.read_bytes() == b""
        part_path = receiver_dir / f".{result.transfer_id_hex}.part"
        assert not part_path.exists()
    finally:
        receiver.stop()


def test_receiver_can_keep_part_file_on_success(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-keep-part"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(
            output_dir=receiver_dir,
            enable_feedback=False,
            keep_part_files_on_complete=True,
        ),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-keep-part.bin"
        source_payload = b"keep-part" * 700
        source_path.write_bytes(source_payload)

        sender = SpaceSyncSender(
            config=SenderConfig(chunk_size=256, enable_feedback=False),
        )
        result = sender.send_file(source_path, "127.0.0.1", receiver.bind_port)
        assert result.total_chunks > 0

        target_path = receiver_dir / source_path.name
        assert _wait_for_file(target_path)
        assert target_path.read_bytes() == source_payload
        part_path = receiver_dir / f".{result.transfer_id_hex}.part"
        assert part_path.exists()
        assert part_path.read_bytes() == source_payload
    finally:
        receiver.stop()


def test_receiver_short_circuits_existing_complete_file(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-short-circuit"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    payload = b"already-there" * 512
    remote_name = "existing/data.bin"
    final_path = receiver_dir / remote_name
    final_path.parent.mkdir(parents=True, exist_ok=True)
    final_path.write_bytes(payload)
    manifest = TransferManifest.from_bytes(raw=payload, file_name=remote_name, chunk_size=256)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver_sock:
        receiver_sock.bind(("127.0.0.1", 0))
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender_sock:
            sender_sock.bind(("127.0.0.1", 0))
            sender_sock.settimeout(1.0)
            source_addr = sender_sock.getsockname()
            receiver._prepare_transfer(receiver_sock, manifest, source_addr)
            first_raw, _ = sender_sock.recvfrom(65535)

    first = decode_frame(first_raw)
    assert first.frame_type == FrameType.STATUS
    assert manifest.transfer_id not in receiver._transfers


def test_receiver_advertises_incomplete_state_on_repeated_manifest(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-state-advertisement"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    payload = b"state-ad" * 256
    manifest = TransferManifest.from_bytes(raw=payload, file_name="resume/data.bin", chunk_size=128)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver_sock:
        receiver_sock.bind(("127.0.0.1", 0))
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender_sock:
            sender_sock.bind(("127.0.0.1", 0))
            sender_sock.settimeout(1.0)
            source_addr = sender_sock.getsockname()
            receiver._prepare_transfer(receiver_sock, manifest, source_addr)
            with receiver._lock:
                transfer = receiver._transfers[manifest.transfer_id]
                transfer.tracker.add(0)
            receiver._prepare_transfer(receiver_sock, manifest, source_addr)
            response_raw, _ = sender_sock.recvfrom(65535)

    parsed = decode_frame(response_raw)
    assert parsed.frame_type == FrameType.STATUS
    status = decode_status(parsed.payload)
    assert status.transfer_id == manifest.transfer_id
    assert status.state == TransferState.INCOMPLETE
    assert status.missing_ranges


def test_receiver_replays_pre_metadata_buffered_chunks(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-pre-metadata"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    payload = b"buffer-before-metadata-" * 128
    manifest = TransferManifest.from_bytes(raw=payload, file_name="late/join.bin", chunk_size=64)
    first_chunk = payload[: manifest.chunk_size]
    remaining = [
        payload[offset : offset + manifest.chunk_size]
        for offset in range(manifest.chunk_size, len(payload), manifest.chunk_size)
    ]

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver_sock:
        receiver_sock.bind(("127.0.0.1", 0))
        source_addr = ("127.0.0.1", 22000)
        receiver._accept_data(
            receiver_sock,
            manifest.transfer_id,
            0,
            first_chunk,
            source_addr=source_addr,
        )
        receiver._prepare_transfer(receiver_sock, manifest, source_addr)
        with receiver._lock:
            transfer = receiver._transfers[manifest.transfer_id]
            assert transfer.tracker.received_count() == 1
        for index, chunk in enumerate(remaining, start=1):
            receiver._accept_data(receiver_sock, manifest.transfer_id, index, chunk)

    target = receiver_dir / "late" / "join.bin"
    assert target.exists()
    assert target.read_bytes() == payload


def test_receiver_reuses_partial_state_across_transfer_ids(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-resume-xfer-id"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=False),
    )
    payload = (b"resume-across-transfer-id-" * 32) + b"tail"
    manifest1 = TransferManifest.from_bytes(
        raw=payload,
        file_name="resume/chained.bin",
        chunk_size=64,
    )
    manifest2 = TransferManifest.from_bytes(
        raw=payload,
        file_name="resume/chained.bin",
        chunk_size=64,
    )
    assert manifest1.transfer_id != manifest2.transfer_id
    first_chunk = payload[: manifest1.chunk_size]
    remaining_chunks = [
        payload[offset : offset + manifest1.chunk_size]
        for offset in range(manifest1.chunk_size, len(payload), manifest1.chunk_size)
    ]

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver_sock:
        receiver_sock.bind(("127.0.0.1", 0))
        source_addr = ("127.0.0.1", 20000)
        receiver._prepare_transfer(receiver_sock, manifest1, source_addr)
        with receiver._lock:
            original_part = receiver._transfers[manifest1.transfer_id].part_path
        receiver._accept_data(receiver_sock, manifest1.transfer_id, 0, first_chunk)
        receiver._prepare_transfer(receiver_sock, manifest2, source_addr)
        with receiver._lock:
            assert manifest1.transfer_id not in receiver._transfers
            resumed = receiver._transfers[manifest2.transfer_id]
            assert resumed.part_path == original_part
            assert resumed.tracker.received_count() == 1
        for index, chunk in enumerate(remaining_chunks, start=1):
            receiver._accept_data(receiver_sock, manifest2.transfer_id, index, chunk)

    target = receiver_dir / "resume" / "chained.bin"
    assert target.exists()
    assert target.read_bytes() == payload
    completed = [
        item
        for item in receiver.completed_transfers
        if item.transfer_id_hex == manifest2.transfer_id.hex()
    ]
    assert len(completed) == 1
    assert completed[0].completed is True


def test_receiver_advertises_incomplete_until_hash_verified(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-state-complete-gating"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    payload = b"gate-complete" * 128
    manifest = TransferManifest.from_bytes(raw=payload, file_name="resume/full.bin", chunk_size=128)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver_sock:
        receiver_sock.bind(("127.0.0.1", 0))
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender_sock:
            sender_sock.bind(("127.0.0.1", 0))
            sender_sock.settimeout(1.0)
            source_addr = sender_sock.getsockname()
            receiver._prepare_transfer(receiver_sock, manifest, source_addr)
            with receiver._lock:
                transfer = receiver._transfers[manifest.transfer_id]
                for idx in range(transfer.manifest.total_chunks):
                    transfer.tracker.add(idx)
                assert transfer.tracker.missing_ranges() == []
                assert transfer.done is False
            receiver._prepare_transfer(receiver_sock, manifest, source_addr)
            response_raw, _ = sender_sock.recvfrom(65535)

    parsed = decode_frame(response_raw)
    assert parsed.frame_type == FrameType.STATUS
    status = decode_status(parsed.payload)
    assert status.transfer_id == manifest.transfer_id
    assert status.state == TransferState.INCOMPLETE
    assert status.missing_ranges == []


def test_matching_completed_file_uses_cached_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=tmp_path / "rx-cache", enable_feedback=True),
    )
    payload = b"cache-check" * 256
    final_path = tmp_path / "rx-cache" / "done.bin"
    final_path.parent.mkdir(parents=True, exist_ok=True)
    final_path.write_bytes(payload)
    manifest = TransferManifest.from_bytes(raw=payload, file_name="done.bin", chunk_size=128)

    assert receiver._is_matching_completed_file(final_path, manifest) is True

    def _fail_open(*args: object, **kwargs: object) -> object:
        raise AssertionError("expected hash cache hit without reading file")

    monkeypatch.setattr(Path, "open", _fail_open)
    assert receiver._is_matching_completed_file(final_path, manifest) is True


def test_receiver_beacon_updates_transfer_activity(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-beacon-activity"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    payload = b"activity" * 128
    manifest = TransferManifest.from_bytes(raw=payload, file_name="beacon/data.bin", chunk_size=128)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver_sock:
        receiver_sock.bind(("127.0.0.1", 0))
        source_addr = ("127.0.0.1", 34567)
        receiver._prepare_transfer(receiver_sock, manifest, source_addr)
        with receiver._lock:
            transfer = receiver._transfers[manifest.transfer_id]
            previous = transfer.last_activity_s
        time.sleep(0.01)
        receiver._handle_frame(
            receiver_sock,
            FrameType.BEACON,
            encode_beacon(BeaconRole.SENDER, manifest.transfer_id)[HEADER_STRUCT.size :],
            ("127.0.0.1", 45678),
        )
        with receiver._lock:
            updated = receiver._transfers[manifest.transfer_id]
            assert updated.last_activity_s > previous
            assert updated.source_addr == ("127.0.0.1", 45678)


def test_receiver_ignores_beacon_for_unknown_transfer(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-beacon-ignore"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    payload = b"known-transfer" * 128
    manifest = TransferManifest.from_bytes(
        raw=payload,
        file_name="beacon/known.bin",
        chunk_size=128,
    )
    unknown_transfer_id = b"\x9A" * 16

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver_sock:
        receiver_sock.bind(("127.0.0.1", 0))
        source_addr = ("127.0.0.1", 20001)
        receiver._prepare_transfer(receiver_sock, manifest, source_addr)
        with receiver._lock:
            before = receiver._transfers[manifest.transfer_id]
            before_activity = before.last_activity_s
            before_source = before.source_addr
        time.sleep(0.01)
        receiver._handle_frame(
            receiver_sock,
            FrameType.BEACON,
            encode_beacon(BeaconRole.SENDER, unknown_transfer_id)[HEADER_STRUCT.size :],
            ("127.0.0.1", 20002),
        )
        with receiver._lock:
            after = receiver._transfers[manifest.transfer_id]
            assert after.last_activity_s == before_activity
            assert after.source_addr == before_source


def test_receiver_maybe_send_beacon_respects_interval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(
            output_dir=tmp_path / "rx-beacon-rate",
            enable_feedback=True,
            beacon_interval_s=1.0,
        ),
    )
    transfer = TransferManifest.from_bytes(
        raw=b"beacon-rate" * 64,
        file_name="beacon/rate.bin",
        chunk_size=64,
    )
    transfer_state = receiver_module._TransferStateData(
        manifest=transfer,
        part_path=tmp_path / "noop.part",
        final_path=tmp_path / "noop.bin",
        tracker=receiver_module.ChunkTracker(total_chunks=transfer.total_chunks),
        source_addr=("127.0.0.1", 9100),
        last_activity_s=0.0,
    )
    sent_count = 0

    class FakeSocket:
        def sendto(self, _payload: bytes, _dest: tuple[str, int]) -> int:
            nonlocal sent_count
            sent_count += 1
            return 1

    now_values = iter([5.0, 5.5, 6.2])
    monkeypatch.setattr(receiver_module.time, "monotonic", lambda: next(now_values))
    receiver._maybe_send_beacon(FakeSocket(), transfer_state)  # type: ignore[arg-type]
    receiver._maybe_send_beacon(FakeSocket(), transfer_state)  # type: ignore[arg-type]
    receiver._maybe_send_beacon(FakeSocket(), transfer_state)  # type: ignore[arg-type]
    assert sent_count == 2


def test_matching_completed_file_negative_scenarios(tmp_path: Path) -> None:
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=tmp_path / "rx-matcher-negative", enable_feedback=True),
    )
    missing_path = tmp_path / "rx-matcher-negative" / "missing.bin"
    manifest = TransferManifest.from_bytes(raw=b"abc123", file_name="missing.bin", chunk_size=3)
    assert receiver._is_matching_completed_file(missing_path, manifest) is False

    mismatch_size_path = tmp_path / "rx-matcher-negative" / "size.bin"
    mismatch_size_path.parent.mkdir(parents=True, exist_ok=True)
    mismatch_size_path.write_bytes(b"abcd")
    assert receiver._is_matching_completed_file(mismatch_size_path, manifest) is False

    hash_mismatch_path = tmp_path / "rx-matcher-negative" / "hash.bin"
    hash_mismatch_path.write_bytes(b"abc123")
    wrong_hash_manifest = TransferManifest.from_bytes(
        raw=b"abc124",
        file_name="hash.bin",
        chunk_size=3,
    )
    assert receiver._is_matching_completed_file(hash_mismatch_path, wrong_hash_manifest) is False


def test_feedback_repair_transfer(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-repair"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-repair.bin"
        source_payload = b"0123456789abcdef" * 1000
        source_path.write_bytes(source_payload)

        sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=128,
                enable_feedback=True,
                drop_every_nth_data=4,
                max_repair_rounds=3,
                feedback_wait_s=3.0,
            )
        )
        result = sender.send_file(source_path, "127.0.0.1", receiver.bind_port)
        assert result.repaired_chunks > 0

        target_path = receiver_dir / source_path.name
        assert _wait_for_file(target_path)
        assert target_path.read_bytes() == source_payload
    finally:
        receiver.stop()


def test_transfer_to_remote_subpath(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-subpath"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=False),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-subpath.bin"
        source_payload = b"xyz987" * 400
        source_path.write_bytes(source_payload)

        sender = SpaceSyncSender(
            config=SenderConfig(chunk_size=256, enable_feedback=False),
        )
        result = sender.send_file(
            source_path,
            "127.0.0.1",
            receiver.bind_port,
            remote_name="nested/dir/final.bin",
        )
        assert result.total_chunks > 0

        target_path = receiver_dir / "nested" / "dir" / "final.bin"
        assert _wait_for_file(target_path)
        assert target_path.read_bytes() == source_payload
    finally:
        receiver.stop()


def test_feedback_mode_times_out_without_receiver(tmp_path: Path) -> None:
    source_path = tmp_path / "no-receiver.bin"
    source_path.write_bytes(b"abcdef" * 1024)

    sender = SpaceSyncSender(
        config=SenderConfig(
            chunk_size=256,
            enable_feedback=True,
            feedback_wait_s=0.1,
            max_feedback_idle_timeouts=1,
            max_repair_rounds=3,
        )
    )
    start = time.monotonic()
    result = sender.send_file(source_path, "127.0.0.1", _free_udp_port())
    elapsed = time.monotonic() - start

    assert result.completed is False
    assert result.repair_rounds == 0
    assert elapsed < 1.0


def test_remote_file_query_and_unchanged_detection(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-query"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-query.bin"
        source_payload = b"remote-query-" * 600
        source_path.write_bytes(source_payload)
        sender = SpaceSyncSender(
            config=SenderConfig(chunk_size=256, enable_feedback=True, feedback_wait_s=2.0),
        )
        send_result = sender.send_file(
            source_path,
            "127.0.0.1",
            receiver.bind_port,
            remote_name="nested/query.bin",
        )
        assert send_result.completed is True
        info = sender.query_remote_file(
            destination_host="127.0.0.1",
            destination_port=receiver.bind_port,
            remote_name="nested/query.bin",
            include_checksum=True,
        )
        assert info.exists is True
        assert info.size == len(source_payload)
        assert info.sha256 == SpaceSyncSender.local_file_checksum(source_path)
        assert info.mtime_ns == source_path.stat().st_mtime_ns
    finally:
        receiver.stop()


def test_receiver_recovers_incomplete_transfer_after_restart(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-restart"
    bind_port = _free_udp_port()
    source_path = tmp_path / "source-restart.bin"
    source_payload = b"restart-check-" * 300
    source_path.write_bytes(source_payload)

    manifest = TransferManifest.from_file(source_path, chunk_size=128, remote_name="resumed.bin")
    manifest.transfer_id = b"\x99" * 16
    chunks = [
        source_payload[offset : offset + manifest.chunk_size]
        for offset in range(0, len(source_payload), manifest.chunk_size)
    ]
    missing_chunk_index = 2

    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=bind_port,
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver.start()
    try:
        time.sleep(0.1)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            destination = ("127.0.0.1", bind_port)
            sock.sendto(encode_manifest(manifest), destination)
            for chunk_index, payload in enumerate(chunks):
                if chunk_index == missing_chunk_index:
                    continue
                sock.sendto(
                    encode_data_chunk(manifest.transfer_id, chunk_index, payload),
                    destination,
                )
        time.sleep(0.3)
    finally:
        receiver.stop()

    journal_path = receiver_dir / ".ssync-journal.json"
    assert journal_path.exists()
    journal_raw = json.loads(journal_path.read_text(encoding="utf-8"))
    assert "transfers" in journal_raw

    receiver2 = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=bind_port,
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver2.start()
    try:
        time.sleep(0.1)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            destination = ("127.0.0.1", bind_port)
            sock.sendto(
                encode_data_chunk(
                    manifest.transfer_id,
                    missing_chunk_index,
                    chunks[missing_chunk_index],
                ),
                destination,
            )
        target_path = receiver_dir / "resumed.bin"
        assert _wait_for_file(target_path, timeout_s=4.0)
        assert target_path.read_bytes() == source_payload
    finally:
        receiver2.stop()


def test_feedback_repair_converges_with_bounded_rounds(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-bounded-repair"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(
            output_dir=receiver_dir,
            enable_feedback=True,
            periodic_repair_request_s=0.25,
            periodic_repair_min_seen_chunks=64,
            max_repair_chunks_per_request=128,
            repair_request_cooldown_s=0.2,
            repair_request_inflight_timeout_s=0.8,
            socket_rcvbuf_bytes=32 * 1024 * 1024,
            journal_flush_interval_s=0.5,
        ),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-bounded-repair.bin"
        source_payload = b"post-fin-repair-" * 400_000
        source_path.write_bytes(source_payload)

        sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=1024,
                enable_feedback=True,
                drop_every_nth_data=17,
                feedback_wait_s=0.5,
                max_repair_rounds=0,
                max_feedback_idle_timeouts=120,
                max_data_rate_bps=16_000_000,
                midstream_repair_max_rounds_per_poll=1,
                midstream_repair_max_chunks_per_poll=128,
                repair_duplicate_suppression_s=0.2,
            )
        )
        result = sender.send_file(source_path, "127.0.0.1", receiver.bind_port)
        assert result.completed is True
        assert result.repaired_chunks > 0
        # Regression guard: prevent uncontrolled post-FIN repair storms.
        assert result.repair_rounds < 600

        target_path = receiver_dir / source_path.name
        assert _wait_for_file(target_path, timeout_s=8.0)
        assert target_path.read_bytes() == source_payload
    finally:
        receiver.stop()


def test_receiver_chains_post_fin_repairs_without_periodic_wait(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-chain-post-fin"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(
            output_dir=receiver_dir,
            enable_feedback=True,
            periodic_repair_request_s=60.0,
            periodic_repair_min_seen_chunks=10_000,
            max_repair_chunks_per_request=2,
            repair_request_cooldown_s=0.0,
            transfer_inactivity_timeout_s=2.0,
        ),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-chain-post-fin.bin"
        source_payload = b"post-fin-chain-" * 8_000
        source_path.write_bytes(source_payload)
        sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=512,
                enable_feedback=True,
                drop_every_nth_data=2,
                max_repair_rounds=0,
                max_feedback_idle_timeouts=30,
                feedback_wait_s=0.2,
                midstream_repair_max_rounds_per_poll=0,
                midstream_repair_max_chunks_per_poll=0,
            )
        )
        result = sender.send_file(source_path, "127.0.0.1", receiver.bind_port)
        assert result.completed is True
        assert result.repair_rounds > 1
        target_path = receiver_dir / source_path.name
        assert _wait_for_file(target_path, timeout_s=4.0)
        assert target_path.read_bytes() == source_payload
    finally:
        receiver.stop()


def test_receiver_keeps_stale_incomplete_transfer_for_resume(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-stale-incomplete-retained"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(
            output_dir=receiver_dir,
            enable_feedback=True,
            transfer_inactivity_timeout_s=0.5,
        ),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-stale-retain.bin"
        source_payload = b"retain-stale-" * 80_000
        source_path.write_bytes(source_payload)
        sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=256,
                enable_feedback=False,
            )
        )
        stop_deadline = time.monotonic() + 0.02
        result = sender.send_file(
            source_path,
            "127.0.0.1",
            receiver.bind_port,
            stop_requested=lambda: time.monotonic() >= stop_deadline,
        )
        assert result.completed is False
        journal_path = receiver_dir / ".ssync-journal.json"
        assert _wait_for_file(journal_path, timeout_s=2.0)
        time.sleep(0.8)
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
        transfers = journal.get("transfers", [])
        assert isinstance(transfers, list)
        assert any(
            isinstance(item, dict)
            and isinstance(item.get("manifest"), dict)
            and item["manifest"].get("file_name") == source_path.name
            for item in transfers
        )
    finally:
        receiver.stop()


def test_receiver_completed_transfers_has_single_entry_per_transfer(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-single-completed-entry"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-single-entry.bin"
        source_payload = b"single-entry-" * 5_000
        source_path.write_bytes(source_payload)
        sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=256,
                enable_feedback=True,
                drop_every_nth_data=5,
                max_repair_rounds=0,
                max_feedback_idle_timeouts=30,
                feedback_wait_s=0.2,
            )
        )
        result = sender.send_file(source_path, "127.0.0.1", receiver.bind_port)
        assert result.completed is True
        assert _wait_for_file(receiver_dir / source_path.name, timeout_s=4.0)
        assert _wait_for_predicate(
            lambda: any(
                item.transfer_id_hex == result.transfer_id_hex
                for item in receiver.completed_transfers
            ),
            timeout_s=2.0,
        )
        completed = [
            item
            for item in receiver.completed_transfers
            if item.transfer_id_hex == result.transfer_id_hex
        ]
        assert len(completed) == 1
        assert completed[0].completed is True
    finally:
        receiver.stop()


def test_feedback_revisit_completes_transfer_after_primary_incomplete(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-revisit"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(
            output_dir=receiver_dir,
            enable_feedback=True,
            max_repair_chunks_per_request=128,
            transfer_inactivity_timeout_s=20.0,
        ),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-revisit.bin"
        source_payload = (b"revisit-path-" * 64_000)[:524_288]
        source_path.write_bytes(source_payload)

        first_sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=256,
                enable_feedback=True,
                drop_every_nth_data=2,
                max_repair_rounds=1,
                max_feedback_idle_timeouts=2,
                feedback_wait_s=0.15,
            )
        )
        stop_deadline = time.monotonic() + 0.03
        first_result = first_sender.send_file(
            source_path,
            "127.0.0.1",
            receiver.bind_port,
            stop_requested=lambda: time.monotonic() >= stop_deadline,
        )
        assert first_result.completed is False

        second_sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=256,
                enable_feedback=True,
                drop_every_nth_data=0,
                max_repair_rounds=0,
                max_feedback_idle_timeouts=20,
                feedback_wait_s=0.15,
            )
        )
        revisit_result = second_sender.send_file(
            source_path,
            "127.0.0.1",
            receiver.bind_port,
            transfer_id=bytes.fromhex(first_result.transfer_id_hex),
            send_initial_data=False,
            max_repair_rounds_override=0,
        )
        assert revisit_result.completed is True
        assert revisit_result.transfer_id_hex == first_result.transfer_id_hex

        target_path = receiver_dir / source_path.name
        assert _wait_for_file(target_path, timeout_s=6.0)
        assert target_path.read_bytes() == source_payload
    finally:
        receiver.stop()


def test_receiver_adaptive_leading_hole_prioritizes_contiguous_prefix(tmp_path: Path) -> None:
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=0,
        config=ReceiverConfig(
            output_dir=tmp_path,
            enable_feedback=True,
            max_repair_chunks_per_request=256,
            adaptive_leading_hole_boost=True,
            leading_hole_start_threshold_chunks=512,
            leading_hole_min_span_chunks=2048,
            leading_hole_boost_multiplier=4,
            leading_hole_max_repair_chunks_per_request=4096,
        ),
    )
    manifest = TransferManifest(
        transfer_id=b"\xAA" * 16,
        file_name="large.bin",
        file_size=10000 * 1024,
        chunk_size=1024,
        total_chunks=10000,
        sha256=b"\x01" * 32,
        metadata={},
    )
    transfer = receiver_module._TransferStateData(
        manifest=manifest,
        part_path=tmp_path / ".part",
        final_path=tmp_path / "large.bin",
        tracker=receiver_module.ChunkTracker(total_chunks=manifest.total_chunks),
        source_addr=("127.0.0.1", 9000),
        highest_chunk_seen=6000,
    )
    missing_ranges = [(0, 5000), (7000, 7010)]
    limited = receiver._limit_missing_ranges_for_transfer(transfer, missing_ranges)
    assert limited == [(0, 1024)]


def test_receiver_inflight_progress_can_release_early(tmp_path: Path) -> None:
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=0,
        config=ReceiverConfig(
            output_dir=tmp_path,
            enable_feedback=True,
            periodic_repair_request_s=0.5,
        ),
    )
    manifest = TransferManifest(
        transfer_id=b"\xBB" * 16,
        file_name="progress.bin",
        file_size=4000 * 1024,
        chunk_size=1024,
        total_chunks=4000,
        sha256=b"\x02" * 32,
        metadata={},
    )
    tracker = receiver_module.ChunkTracker.from_received_ranges(4000, [(0, 1192)])
    transfer = receiver_module._TransferStateData(
        manifest=manifest,
        part_path=tmp_path / ".progress.part",
        final_path=tmp_path / "progress.bin",
        tracker=tracker,
        source_addr=("127.0.0.1", 9000),
        repair_request_in_flight=True,
        received_count_at_last_request=1000,
        requested_chunks_at_last_request=256,
        last_periodic_repair_request_s=10.0,
    )
    allowed = receiver._can_send_repair_request(transfer, now=10.6)
    assert allowed is True
    assert transfer.repair_request_in_flight is False
    assert transfer.last_periodic_repair_request_s == pytest.approx(0.0)


def test_hash_mismatch_removes_part_file(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-hash-mismatch"
    bind_port = _free_udp_port()
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=bind_port,
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-hash-mismatch.bin"
        payload = b"hash-mismatch-" * 2_000
        source_path.write_bytes(payload)
        manifest = TransferManifest.from_file(source_path, chunk_size=512)
        chunks = [
            payload[offset : offset + manifest.chunk_size]
            for offset in range(0, len(payload), manifest.chunk_size)
        ]
        corrupt_index = min(1, len(chunks) - 1)
        corrupted_chunk = bytearray(chunks[corrupt_index])
        corrupted_chunk[0] ^= 0x01
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            destination = ("127.0.0.1", bind_port)
            sock.sendto(encode_manifest(manifest), destination)
            for chunk_index, chunk in enumerate(chunks):
                if chunk_index == corrupt_index:
                    sock.sendto(
                        encode_data_chunk(
                            manifest.transfer_id,
                            chunk_index,
                            bytes(corrupted_chunk),
                        ),
                        destination,
                    )
                else:
                    sock.sendto(
                        encode_data_chunk(manifest.transfer_id, chunk_index, chunk),
                        destination,
                    )

        assert _wait_for_predicate(lambda: bool(receiver.completed_transfers), timeout_s=3.0)
        final_path = receiver_dir / source_path.name
        assert not final_path.exists()
        part_path = receiver_dir / f".{manifest.transfer_id.hex()}.part"
        assert not part_path.exists()
        statuses = [
            item
            for item in receiver.completed_transfers
            if item.transfer_id_hex == manifest.transfer_id.hex()
        ]
        assert len(statuses) == 1
        assert statuses[0].hash_mismatch is True
    finally:
        receiver.stop()

