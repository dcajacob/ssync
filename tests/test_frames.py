from ssync.space_sync.frames import (
    TransferStatus,
    decode_data_chunk,
    decode_frame,
    decode_manifest,
    decode_repair_request,
    decode_status,
    encode_data_chunk,
    encode_manifest,
    encode_repair_request,
    encode_status,
)
from ssync.space_sync.manifest import RepairRequest, TransferManifest
from ssync.space_sync.types import FrameType, TransferState


def test_manifest_frame_round_trip() -> None:
    manifest = TransferManifest(
        transfer_id=b"\x01" * 16,
        file_name="payload.bin",
        file_size=4096,
        chunk_size=1024,
        total_chunks=4,
        sha256=b"\x02" * 32,
        metadata={1: b"leo-pass", 2: b"note"},
    )
    raw = encode_manifest(manifest)
    parsed = decode_frame(raw)
    assert parsed.frame_type == FrameType.MANIFEST
    decoded = decode_manifest(parsed.payload)
    assert decoded == manifest


def test_data_frame_round_trip() -> None:
    raw = encode_data_chunk(transfer_id=b"\xAA" * 16, chunk_index=3, chunk_payload=b"hello")
    parsed = decode_frame(raw)
    decoded = decode_data_chunk(parsed.payload)
    assert decoded.transfer_id == b"\xAA" * 16
    assert decoded.chunk_index == 3
    assert decoded.payload == b"hello"


def test_status_and_repair_round_trip() -> None:
    status_raw = encode_status(
        TransferStatus(
            transfer_id=b"\xBB" * 16,
            state=TransferState.INCOMPLETE,
            missing_ranges=[(2, 5), (8, 9)],
        )
    )
    status_decoded = decode_status(decode_frame(status_raw).payload)
    assert status_decoded.transfer_id == b"\xBB" * 16
    assert status_decoded.missing_ranges == [(2, 5), (8, 9)]

    request_raw = encode_repair_request(
        RepairRequest(transfer_id=b"\xCC" * 16, missing_ranges=[(1, 2), (6, 7)])
    )
    request_decoded = decode_repair_request(decode_frame(request_raw).payload)
    assert request_decoded.transfer_id == b"\xCC" * 16
    assert request_decoded.missing_ranges == [(1, 2), (6, 7)]

