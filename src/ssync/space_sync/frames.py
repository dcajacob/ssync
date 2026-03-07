from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import cast

from .manifest import RepairRequest, TransferManifest
from .ranges import decode_ranges, encode_ranges
from .types import (
    MAX_FILE_NAME_BYTES,
    MAX_RANGES_PER_FRAME,
    MAX_TLV_TOTAL_SIZE,
    MAX_TLV_VALUE_SIZE,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    SHA256_SIZE,
    TRANSFER_ID_SIZE,
    FrameType,
    RemoteFileInfo,
    TransferState,
)

HEADER_STRUCT = struct.Struct("!2sBBBBI")
MANIFEST_FIXED_STRUCT = struct.Struct(f"!{TRANSFER_ID_SIZE}sQII{SHA256_SIZE}sH")
DATA_FIXED_STRUCT = struct.Struct(f"!{TRANSFER_ID_SIZE}sIH")
FIN_STRUCT = struct.Struct(f"!{TRANSFER_ID_SIZE}s")
STATUS_FIXED_STRUCT = struct.Struct(f"!{TRANSFER_ID_SIZE}sBH")
TLV_HEADER_STRUCT = struct.Struct("!BH")
FILE_INFO_REQUEST_FIXED_STRUCT = struct.Struct("!BH")
FILE_INFO_RESPONSE_FIXED_STRUCT = struct.Struct(f"!BBQQ{SHA256_SIZE}sH")
KNOWN_FRAME_TYPE_VALUES = {item.value for item in FrameType}


@dataclass(slots=True)
class ParsedFrame:
    frame_type: FrameType | None
    frame_type_raw: int
    flags: int
    payload: bytes


@dataclass(slots=True)
class DataChunk:
    transfer_id: bytes
    chunk_index: int
    payload: bytes


@dataclass(slots=True)
class TransferStatus:
    transfer_id: bytes
    state: TransferState
    missing_ranges: list[tuple[int, int]]


def encode_frame(frame_type: FrameType, payload: bytes, flags: int = 0) -> bytes:
    header = HEADER_STRUCT.pack(
        PROTOCOL_MAGIC,
        PROTOCOL_VERSION,
        int(frame_type),
        flags,
        0,
        len(payload),
    )
    return header + payload


def decode_frame(raw: bytes) -> ParsedFrame:
    if len(raw) < HEADER_STRUCT.size:
        raise ValueError("Frame too short")
    magic, version, frame_type, flags, _reserved, payload_size = HEADER_STRUCT.unpack_from(raw, 0)
    if magic != PROTOCOL_MAGIC:
        raise ValueError("Invalid protocol magic")
    if version != PROTOCOL_VERSION:
        raise ValueError(f"Unsupported protocol version: {version}")
    payload = raw[HEADER_STRUCT.size:]
    if payload_size != len(payload):
        raise ValueError("Invalid payload size")
    parsed_type: FrameType | None = None
    if frame_type in KNOWN_FRAME_TYPE_VALUES:
        parsed_type = FrameType(frame_type)
    return ParsedFrame(
        frame_type=parsed_type,
        frame_type_raw=frame_type,
        flags=flags,
        payload=payload,
    )


def encode_tlvs(metadata: dict[int, bytes]) -> bytes:
    out = bytearray()
    for key, value in metadata.items():
        if len(value) > MAX_TLV_VALUE_SIZE:
            raise ValueError("TLV value too large")
        out.extend(TLV_HEADER_STRUCT.pack(key, len(value)))
        out.extend(value)
    if len(out) > MAX_TLV_TOTAL_SIZE:
        raise ValueError("TLV payload too large")
    return bytes(out)


def decode_tlvs(payload: bytes) -> dict[int, bytes]:
    cursor = 0
    tlvs: dict[int, bytes] = {}
    while cursor < len(payload):
        if len(payload) - cursor < TLV_HEADER_STRUCT.size:
            raise ValueError("Corrupt TLV payload")
        key, value_size = TLV_HEADER_STRUCT.unpack_from(payload, cursor)
        cursor += TLV_HEADER_STRUCT.size
        if len(payload) - cursor < value_size:
            raise ValueError("Corrupt TLV value length")
        tlvs[key] = payload[cursor : cursor + value_size]
        cursor += value_size
    return tlvs


def encode_manifest(manifest: TransferManifest) -> bytes:
    if manifest.chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    if manifest.file_size:
        expected_total_chunks = (
            manifest.file_size + manifest.chunk_size - 1
        ) // manifest.chunk_size
    else:
        expected_total_chunks = 0
    if manifest.total_chunks != expected_total_chunks:
        raise ValueError("manifest total_chunks does not match file_size/chunk_size")
    file_name_bytes = manifest.file_name.encode("utf-8")
    if not file_name_bytes:
        raise ValueError("file_name must not be empty")
    if len(file_name_bytes) > MAX_FILE_NAME_BYTES:
        raise ValueError("file_name too long")
    tlv_bytes = encode_tlvs(manifest.metadata)
    fixed = MANIFEST_FIXED_STRUCT.pack(
        manifest.transfer_id,
        manifest.file_size,
        manifest.chunk_size,
        manifest.total_chunks,
        manifest.sha256,
        len(file_name_bytes),
    )
    tail = file_name_bytes + struct.pack("!H", len(tlv_bytes)) + tlv_bytes
    return encode_frame(FrameType.MANIFEST, fixed + tail)


def decode_manifest(payload: bytes) -> TransferManifest:
    if len(payload) < MANIFEST_FIXED_STRUCT.size + 2:
        raise ValueError("Manifest payload too short")
    (
        transfer_id,
        file_size,
        chunk_size,
        total_chunks,
        sha256,
        file_name_size,
    ) = MANIFEST_FIXED_STRUCT.unpack_from(payload, 0)
    cursor = MANIFEST_FIXED_STRUCT.size
    if len(payload) - cursor < file_name_size + 2:
        raise ValueError("Manifest missing file name or metadata size")
    file_name = payload[cursor : cursor + file_name_size].decode("utf-8")
    if not file_name:
        raise ValueError("Manifest file name must not be empty")
    if file_name_size > MAX_FILE_NAME_BYTES:
        raise ValueError("Manifest file name too large")
    cursor += file_name_size
    tlv_size = struct.unpack_from("!H", payload, cursor)[0]
    cursor += 2
    if len(payload) - cursor != tlv_size:
        raise ValueError("Manifest metadata size mismatch")
    metadata = decode_tlvs(payload[cursor:])
    if chunk_size <= 0:
        raise ValueError("Manifest chunk_size must be positive")
    expected_total_chunks = ((file_size + chunk_size - 1) // chunk_size) if file_size else 0
    if total_chunks != expected_total_chunks:
        raise ValueError("Manifest total_chunks mismatch")
    return TransferManifest(
        transfer_id=transfer_id,
        file_name=file_name,
        file_size=file_size,
        chunk_size=chunk_size,
        total_chunks=total_chunks,
        sha256=sha256,
        metadata=metadata,
    )


def encode_data_chunk(transfer_id: bytes, chunk_index: int, chunk_payload: bytes) -> bytes:
    if len(chunk_payload) > 65535:
        raise ValueError("Chunk payload too large")
    fixed = DATA_FIXED_STRUCT.pack(transfer_id, chunk_index, len(chunk_payload))
    return encode_frame(FrameType.DATA, fixed + chunk_payload)


def decode_data_chunk(payload: bytes) -> DataChunk:
    if len(payload) < DATA_FIXED_STRUCT.size:
        raise ValueError("Data payload too short")
    transfer_id, chunk_index, payload_size = DATA_FIXED_STRUCT.unpack_from(payload, 0)
    chunk_payload = payload[DATA_FIXED_STRUCT.size :]
    if payload_size != len(chunk_payload):
        raise ValueError("Chunk payload size mismatch")
    return DataChunk(
        transfer_id=transfer_id,
        chunk_index=chunk_index,
        payload=chunk_payload,
    )


def encode_fin(transfer_id: bytes) -> bytes:
    return encode_frame(FrameType.FIN, FIN_STRUCT.pack(transfer_id))


def decode_fin(payload: bytes) -> bytes:
    if len(payload) != FIN_STRUCT.size:
        raise ValueError("FIN payload size mismatch")
    return cast(bytes, FIN_STRUCT.unpack(payload)[0])


def encode_status(status: TransferStatus) -> bytes:
    if len(status.missing_ranges) > MAX_RANGES_PER_FRAME:
        raise ValueError("Too many ranges in STATUS")
    ranges_payload = encode_ranges(status.missing_ranges)
    fixed = STATUS_FIXED_STRUCT.pack(
        status.transfer_id,
        int(status.state),
        len(ranges_payload) // 8,
    )
    return encode_frame(FrameType.STATUS, fixed + ranges_payload)


def decode_status(payload: bytes) -> TransferStatus:
    if len(payload) < STATUS_FIXED_STRUCT.size:
        raise ValueError("Status payload too short")
    transfer_id, raw_state, range_count = STATUS_FIXED_STRUCT.unpack_from(payload, 0)
    if range_count > MAX_RANGES_PER_FRAME:
        raise ValueError("STATUS range count too large")
    ranges_payload = payload[STATUS_FIXED_STRUCT.size :]
    if len(ranges_payload) != range_count * 8:
        raise ValueError("Status range count mismatch")
    return TransferStatus(
        transfer_id=transfer_id,
        state=TransferState(raw_state),
        missing_ranges=decode_ranges(ranges_payload),
    )


def encode_repair_request(request: RepairRequest) -> bytes:
    if len(request.missing_ranges) > MAX_RANGES_PER_FRAME:
        raise ValueError("Too many ranges in REPAIR_REQUEST")
    ranges_payload = encode_ranges(request.missing_ranges)
    payload = (
        FIN_STRUCT.pack(request.transfer_id)
        + struct.pack("!H", len(ranges_payload) // 8)
        + ranges_payload
    )
    return encode_frame(FrameType.REPAIR_REQUEST, payload)


def decode_repair_request(payload: bytes) -> RepairRequest:
    if len(payload) < FIN_STRUCT.size + 2:
        raise ValueError("Repair request payload too short")
    transfer_id = FIN_STRUCT.unpack_from(payload, 0)[0]
    range_count = struct.unpack_from("!H", payload, FIN_STRUCT.size)[0]
    if range_count > MAX_RANGES_PER_FRAME:
        raise ValueError("Repair request range count too large")
    ranges_payload = payload[FIN_STRUCT.size + 2 :]
    if len(ranges_payload) != range_count * 8:
        raise ValueError("Repair request range count mismatch")
    return RepairRequest(
        transfer_id=transfer_id,
        missing_ranges=decode_ranges(ranges_payload),
    )


def encode_repair_done(transfer_id: bytes) -> bytes:
    return encode_frame(FrameType.REPAIR_DONE, FIN_STRUCT.pack(transfer_id))


def decode_repair_done(payload: bytes) -> bytes:
    return decode_fin(payload)


def encode_file_info_request(path: str, include_checksum: bool) -> bytes:
    path_bytes = path.encode("utf-8")
    if not path_bytes:
        raise ValueError("Remote path must not be empty")
    if len(path_bytes) > MAX_FILE_NAME_BYTES:
        raise ValueError("Remote path too long")
    payload = (
        FILE_INFO_REQUEST_FIXED_STRUCT.pack(1 if include_checksum else 0, len(path_bytes))
        + path_bytes
    )
    return encode_frame(FrameType.FILE_INFO_REQUEST, payload)


def decode_file_info_request(payload: bytes) -> tuple[str, bool]:
    if len(payload) < FILE_INFO_REQUEST_FIXED_STRUCT.size:
        raise ValueError("File info request payload too short")
    checksum_requested, path_size = FILE_INFO_REQUEST_FIXED_STRUCT.unpack_from(payload, 0)
    path_bytes = payload[FILE_INFO_REQUEST_FIXED_STRUCT.size :]
    if len(path_bytes) != path_size:
        raise ValueError("File info request path size mismatch")
    path = path_bytes.decode("utf-8")
    if not path:
        raise ValueError("File info request path must not be empty")
    return path, bool(checksum_requested)


def encode_file_info_response(file_info: RemoteFileInfo) -> bytes:
    path_bytes = file_info.path.encode("utf-8")
    if not path_bytes:
        raise ValueError("File info response path must not be empty")
    if len(path_bytes) > MAX_FILE_NAME_BYTES:
        raise ValueError("File info response path too long")
    sha256 = file_info.sha256 if file_info.sha256 is not None else b"\x00" * SHA256_SIZE
    if len(sha256) != SHA256_SIZE:
        raise ValueError("File info response sha256 must be 32 bytes")
    payload = (
        FILE_INFO_RESPONSE_FIXED_STRUCT.pack(
            1 if file_info.exists else 0,
            1 if file_info.sha256 is not None else 0,
            file_info.size,
            file_info.mtime_ns,
            sha256,
            len(path_bytes),
        )
        + path_bytes
    )
    return encode_frame(FrameType.FILE_INFO_RESPONSE, payload)


def decode_file_info_response(payload: bytes) -> RemoteFileInfo:
    if len(payload) < FILE_INFO_RESPONSE_FIXED_STRUCT.size:
        raise ValueError("File info response payload too short")
    (
        exists_raw,
        has_sha_raw,
        size,
        mtime_ns,
        sha256,
        path_size,
    ) = FILE_INFO_RESPONSE_FIXED_STRUCT.unpack_from(payload, 0)
    path_bytes = payload[FILE_INFO_RESPONSE_FIXED_STRUCT.size :]
    if len(path_bytes) != path_size:
        raise ValueError("File info response path size mismatch")
    path = path_bytes.decode("utf-8")
    if not path:
        raise ValueError("File info response path must not be empty")
    return RemoteFileInfo(
        path=path,
        exists=bool(exists_raw),
        size=size,
        mtime_ns=mtime_ns,
        sha256=sha256 if has_sha_raw else None,
    )

