# Space Sync Design Note (Initial Prototype)

## Intent

Space Sync (`ssync`) is a UDP-based transport prototype for mission-specific links with
strong asymmetry and intermittent connectivity. The first capability target is reliable
file delivery with two operating styles:

- open-loop delivery where no return link is assumed
- sparse-feedback repair when a low-rate or delayed return path is available

The prototype is intentionally simple and extensible, not production complete.

## Core Architecture

The transport uses compact binary frames over UDP with a fixed common header and
frame-specific payloads.

### Frame Types

- `MANIFEST`: transfer identity and file metadata (size, chunk size, hash, file name, TLVs)
- `DATA`: indexed file chunk
- `FIN`: end of sender pass
- `STATUS`: receiver summary (`INCOMPLETE`, `COMPLETE`, `HASH_MISMATCH`) and missing ranges
- `REPAIR_REQUEST`: receiver request for missing chunk ranges
- `REPAIR_DONE`: sender signal that requested repair pass has ended

### Transfer Model

1. Sender emits `MANIFEST` one or more times for robustness.
2. Sender emits sequential `DATA` chunks (fixed chunk size).
3. Sender emits `FIN`.
4. Receiver either finalizes (if complete) or requests sparse repair ranges.
5. Sender repairs only requested chunks and emits `REPAIR_DONE`.
6. Receiver validates full-file SHA-256 and reports final status.

## Why This Fits Asymmetric LEO Links

- No mandatory ACK stream: forward delivery can proceed without uplink.
- Feedback is sparse and range-based: suitable for constrained return capacity.
- Deferred repair is explicit: missing chunks can be repaired in a later contact.
- Chunk-indexed transfer state enables partial progress and deterministic requests.

## Extensibility Hooks

- `MANIFEST` supports TLV metadata for mission/application-specific fields.
- New frame types can be added without replacing base framing.
- Current chunk-based logic can be extended with optional parity/FEC blocks.
- File mode lays groundwork for stream mode in future revisions.

## Integrity and Reliability Choices

- Reliability primitive: chunk-indexed retransmission.
- Integrity primitive: whole-file SHA-256 verification.
- Current status model avoids per-packet ACK chatter.
- Duplicate and out-of-order data chunks are tolerated.
- Feedback mode sender behavior is timer-bounded (`feedback_wait_s`,
  `max_feedback_idle_timeouts`, `max_repair_rounds`) with explicit incomplete
  terminal outcome when no terminal status is received.

## Assumptions

- Links are private/dedicated and fairness with public Internet traffic is out of scope.
- Prototype runs over IPv4 UDP in local tests.
- Receiver transfer state is persisted as a local journal in output directories so
  incomplete transfers can resume after receiver restart.
- One active sender destination per transfer in the current implementation.

## Intentionally Deferred

- Congestion control and adaptive rate control
- FEC/parity transport
- Cryptographic authentication and encryption
- Durable sender-side transfer ledgers and cross-contact scheduling
- Prioritization, deletion/housekeeping controls
- Stream transport semantics

## Next Steps

1. Add durable transfer journals for pass-to-pass recovery.
2. Add explicit contact/session identifiers and policy-based scheduling.
3. Add optional FEC blocks and hybrid retransmit+FEC strategies.
4. Add authenticated control frames and key management hooks.
5. Add bidirectional telemetry (delivery quality, RTT, repair efficiency).

