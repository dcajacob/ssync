from ssync.space_sync.frames import (
    HEADER_STRUCT,
    TransferStatus,
    decode_beacon,
    decode_data_chunk,
    decode_file_info_response,
    decode_frame,
    decode_manifest,
    decode_metadata,
    decode_status,
    encode_beacon,
    encode_data_chunk,
    encode_file_info_response,
    encode_manifest,
    encode_metadata,
    encode_status,
)
from ssync.space_sync.manifest import TransferManifest
from ssync.space_sync.types import BeaconRole, FrameType, RemoteFileInfo, StatusKind, TransferState


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
    assert parsed.frame_type == FrameType.METADATA
    decoded = decode_manifest(parsed.payload)
    assert decoded == manifest


def test_metadata_alias_round_trip() -> None:
    manifest = TransferManifest(
        transfer_id=b"\x11" * 16,
        file_name="payload.bin",
        file_size=1024,
        chunk_size=256,
        total_chunks=4,
        sha256=b"\x22" * 32,
        metadata={1: b"meta"},
    )
    raw = encode_metadata(manifest)
    parsed = decode_frame(raw)
    assert parsed.frame_type == FrameType.METADATA
    decoded = decode_metadata(parsed.payload)
    assert decoded == manifest


def test_data_frame_round_trip() -> None:
    raw = encode_data_chunk(transfer_id=b"\xAA" * 16, chunk_index=3, chunk_payload=b"hello")
    parsed = decode_frame(raw)
    decoded = decode_data_chunk(parsed.payload)
    assert decoded.transfer_id == b"\xAA" * 16
    assert decoded.chunk_index == 3
    assert decoded.payload == b"hello"


def test_decode_unknown_frame_type() -> None:
    payload = b"abc"
    raw = HEADER_STRUCT.pack(b"SS", 1, 99, 0, 0, len(payload)) + payload
    parsed = decode_frame(raw)
    assert parsed.frame_type is None
    assert parsed.frame_type_raw == 99


def test_manifest_validation_rejects_inconsistent_total_chunks() -> None:
    manifest = TransferManifest(
        transfer_id=b"\x01" * 16,
        file_name="payload.bin",
        file_size=4096,
        chunk_size=1024,
        total_chunks=5,
        sha256=b"\x02" * 32,
        metadata={},
    )
    try:
        encode_manifest(manifest)
    except ValueError as exc:
        assert "total_chunks" in str(exc)
    else:
        raise AssertionError("expected manifest validation failure")


def test_file_info_response_payload_round_trip() -> None:
    response_payload = encode_file_info_response(
        RemoteFileInfo(
            path="nested/file.bin",
            exists=True,
            size=123,
            mtime_ns=456,
            sha256=b"\xAA" * 32,
        )
    )
    response = decode_file_info_response(response_payload)
    assert response.path == "nested/file.bin"
    assert response.exists is True
    assert response.size == 123
    assert response.mtime_ns == 456
    assert response.sha256 == b"\xAA" * 32


def test_status_transfer_round_trip() -> None:
    status_raw = encode_status(
        TransferStatus(
            transfer_id=b"\xBB" * 16,
            kind=StatusKind.TRANSFER,
            state=TransferState.INCOMPLETE,
            missing_ranges=[(2, 5), (8, 9)],
        )
    )
    status_decoded = decode_status(decode_frame(status_raw).payload)
    assert status_decoded.transfer_id == b"\xBB" * 16
    assert status_decoded.missing_ranges == [(2, 5), (8, 9)]

def test_status_file_info_response_round_trip() -> None:
    status_raw = encode_status(
        TransferStatus(
            transfer_id=b"\xCC" * 16,
            kind=StatusKind.FILE_INFO_RESPONSE,
            state=TransferState.COMPLETE,
            missing_ranges=[],
            file_info=RemoteFileInfo(
                path="nested/file.bin",
                exists=True,
                size=11,
                mtime_ns=22,
                sha256=b"\xAB" * 32,
            ),
            query_token=b"tok-1",
        )
    )
    status_decoded = decode_status(decode_frame(status_raw).payload)
    assert status_decoded.kind == StatusKind.FILE_INFO_RESPONSE
    assert status_decoded.query_token == b"tok-1"
    assert status_decoded.file_info is not None
    assert status_decoded.file_info.path == "nested/file.bin"


def test_beacon_round_trip() -> None:
    raw = encode_beacon(BeaconRole.SENDER, b"\xEE" * 16)
    parsed = decode_frame(raw)
    assert parsed.frame_type == FrameType.BEACON
    role, transfer_id = decode_beacon(parsed.payload)
    assert role == BeaconRole.SENDER
    assert transfer_id == b"\xEE" * 16

