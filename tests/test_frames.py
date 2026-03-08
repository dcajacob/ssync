from ssync.space_sync.frames import (
    HEADER_STRUCT,
    TransferStatus,
    decode_data_chunk,
    decode_file_info_request,
    decode_file_info_response,
    decode_frame,
    decode_manifest,
    decode_repair_request,
    decode_status,
    decode_transfer_complete,
    encode_data_chunk,
    encode_file_info_request,
    encode_file_info_response,
    encode_manifest,
    encode_repair_request,
    encode_status,
    encode_transfer_complete,
)
from ssync.space_sync.manifest import RepairRequest, TransferManifest
from ssync.space_sync.types import FrameType, RemoteFileInfo, TransferState


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


def test_file_info_request_and_response_round_trip() -> None:
    request_raw = encode_file_info_request("nested/file.bin", include_checksum=True)
    parsed_request = decode_frame(request_raw)
    assert parsed_request.frame_type == FrameType.FILE_INFO_REQUEST
    remote_path, include_checksum = decode_file_info_request(parsed_request.payload)
    assert remote_path == "nested/file.bin"
    assert include_checksum is True

    response_raw = encode_file_info_response(
        RemoteFileInfo(
            path="nested/file.bin",
            exists=True,
            size=123,
            mtime_ns=456,
            sha256=b"\xAA" * 32,
        )
    )
    parsed_response = decode_frame(response_raw)
    assert parsed_response.frame_type == FrameType.FILE_INFO_RESPONSE
    response = decode_file_info_response(parsed_response.payload)
    assert response.path == "nested/file.bin"
    assert response.exists is True
    assert response.size == 123
    assert response.mtime_ns == 456
    assert response.sha256 == b"\xAA" * 32


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


def test_transfer_complete_round_trip() -> None:
    raw = encode_transfer_complete(b"\xDD" * 16)
    parsed = decode_frame(raw)
    assert parsed.frame_type == FrameType.TRANSFER_COMPLETE
    assert decode_transfer_complete(parsed.payload) == b"\xDD" * 16

