from __future__ import annotations

import struct
from dataclasses import dataclass

from .manifest import RepairRequest, TransferManifest
from .ranges import decode_ranges, encode_ranges
from .types import (
    FrameType,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    SHA256_SIZE,
    TRANSFER_ID_SIZE,
    TransferState,
)


HEADER_STRUCT = struct.Struct("!2sBBBBI")
MANIFEST_FIXED_STRUCT = struct.Struct(f"!{TRANSFER_ID_SIZE}sQII{SHA256_SIZE}sH")
DATA_FIXED_STRUCT = struct.Struct(f"!{TRANSFER_ID_SIZE}sIH")
FIN_STRUCT = struct.Struct(f"!{TRANSFER_ID_SIZE}s")
STATUS_FIXED_STRUCT = struct.Struct(f"!{TRANSFER_ID_SIZE}sBH")
TLV_HEADER_STRUCT = struct.Struct("!BH")


@dataclass(slots=True)
class ParsedFrame:
    frame_type: FrameType
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
    return ParsedFrame(frame_type=FrameType(frame_type), flags=flags, payload=payload)


def encode_tlvs(metadata: dict[int, bytes]) -> bytes:
    out = bytearray()
    for key, value in metadata.items():
        out.extend(TLV_HEADER_STRUCT.pack(key, len(value)))
        out.extend(value)
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
    file_name_bytes = manifest.file_name.encode("utf-8")
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
    cursor += file_name_size
    tlv_size = struct.unpack_from("!H", payload, cursor)[0]
    cursor += 2
    if len(payload) - cursor != tlv_size:
        raise ValueError("Manifest metadata size mismatch")
    metadata = decode_tlvs(payload[cursor:])
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
    return FIN_STRUCT.unpack(payload)[0]


def encode_status(status: TransferStatus) -> bytes:
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
    ranges_payload = payload[STATUS_FIXED_STRUCT.size :]
    if len(ranges_payload) != range_count * 8:
        raise ValueError("Status range count mismatch")
    return TransferStatus(
        transfer_id=transfer_id,
        state=TransferState(raw_state),
        missing_ranges=decode_ranges(ranges_payload),
    )


def encode_repair_request(request: RepairRequest) -> bytes:
    ranges_payload = encode_ranges(request.missing_ranges)
    payload = FIN_STRUCT.pack(request.transfer_id) + struct.pack("!H", len(ranges_payload) // 8) + ranges_payload
    return encode_frame(FrameType.REPAIR_REQUEST, payload)


def decode_repair_request(payload: bytes) -> RepairRequest:
    if len(payload) < FIN_STRUCT.size + 2:
        raise ValueError("Repair request payload too short")
    transfer_id = FIN_STRUCT.unpack_from(payload, 0)[0]
    range_count = struct.unpack_from("!H", payload, FIN_STRUCT.size)[0]
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

