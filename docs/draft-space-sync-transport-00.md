# Network Working Group                                           D. User
## Internet-Draft                                                 Space Sync
Intended status: Experimental                                 March 2026
Expires: September 2026

# Space Sync (SSYNC): UDP File Transport for Asymmetric Intermittent Links

## Abstract

This document specifies Space Sync (SSYNC), an experimental UDP-based file
transport protocol for mission-specific links with intermittent connectivity
and strongly asymmetric forward/return capacity. SSYNC supports both open-loop
delivery and sparse-feedback repair using missing chunk ranges. The protocol
uses compact binary framing, chunk-indexed transfer state, and whole-file
SHA-256 verification.

## Status of This Memo

This Internet-Draft is submitted in full conformance with the provisions of
BCP 78 and BCP 79.

Internet-Drafts are working documents of the Internet Engineering Task Force
(IETF). Internet-Drafts are draft documents valid for a maximum of six months
and may be updated, replaced, or obsoleted at any time.

## 1.  Introduction

SSYNC targets environments where:

- forward link capacity may be high but return link may be absent or low-rate;
- contacts may be brief and intermittent;
- repair may need to occur in later contacts;
- operation is constrained to private and managed networks.

The initial protocol objective is reliable file delivery. Stream transport and
advanced control functions are out of scope for this version.

## 2.  Conventions and Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" are to
be interpreted as described in BCP 14 [RFC2119] [RFC8174].

Terms:

- **Transfer ID**: 16-byte value uniquely identifying one transfer.
- **Chunk**: fixed-size file segment addressed by `Chunk Index`.
- **Missing Range**: half-open interval `[start, end)` of missing chunk indexes.
- **Open-loop mode**: sender does not require any return-link feedback.
- **Feedback mode**: receiver sends sparse status/repair requests.

## 3.  Protocol Model

### 3.1.  Frame Families

`MANIFEST`, `DATA`, `FIN`, `STATUS`, `REPAIR_REQUEST`, `REPAIR_DONE`,
`FILE_INFO_REQUEST`, `FILE_INFO_RESPONSE`, `TRANSFER_COMPLETE`, and `BEACON`.

### 3.2.  High-Level Transfer Sequence

1. Sender transmits `MANIFEST` one or more times.
2. Sender transmits `DATA` chunks.
3. Sender transmits `FIN`.
4. Receiver either finalizes (complete) or sends `REPAIR_REQUEST`.
5. Sender retransmits requested ranges and transmits `REPAIR_DONE`.
6. Receiver verifies whole-file hash and emits final `STATUS` in feedback mode.

## 4.  UDP Encapsulation

- One SSYNC frame is carried in one UDP datagram.
- Implementations SHOULD select chunk size and pacing to avoid IP fragmentation.
- Default implementation port is 9000 (deployment-specific, not assigned by IANA).

## 5.  Common Frame Header

All frames begin with:

```
0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------+---------------+---------------+---------------+
|     Magic     |   Version     |  Frame Type   |     Flags     |
+---------------+---------------+---------------+---------------+
|   Reserved    |                  Payload Length                |
+---------------+---------------+---------------+---------------+
```

- `Magic` (16 bits): ASCII `"SS"` (`0x53 0x53`).
- `Version` (8 bits): this document defines version `1`.
- `Frame Type` (8 bits): registry value from Section 15.
- `Flags` (8 bits): all zero in this version.
- `Reserved` (8 bits): sender sets zero; receiver ignores.
- `Payload Length` (32 bits): payload octet count.

Receiver behavior:

- Invalid `Magic` or `Version` => MUST discard frame.
- Payload length mismatch => MUST discard frame.
- Unknown `Frame Type` => MUST ignore frame and continue processing.

## 6.  Versioning and Compatibility

- A receiver implementing version `1` MUST discard frames with unsupported
  `Version`.
- Unknown frame types at supported version MUST be ignored.
- Unknown TLV types MUST be ignored.
- Extensions that change core transfer semantics MUST define a new version.

## 7.  Payload Definitions

All numeric fields are network byte order.

### 7.1.  `MANIFEST`

Fields:

- `Transfer ID` (16 bytes)
- `File Size` (uint64)
- `Chunk Size` (uint32)
- `Total Chunks` (uint32)
- `SHA-256` (32 bytes)
- `File Name Length` (uint16)
- `File Name` (UTF-8, `1..4096` bytes)
- `TLV Length` (uint16)
- `TLVs` (`0..65535` bytes)

Validation:

- `Chunk Size` MUST be > 0.
- `Total Chunks` MUST equal `ceil(File Size / Chunk Size)` when `File Size > 0`,
  and MUST be `0` when `File Size = 0`.
- Empty file name is invalid.

Repeated `MANIFEST` handling:

- If repeated `MANIFEST` for a transfer has identical `(File Size, Chunk Size,
  SHA-256, File Name)`, receiver SHOULD refresh source address and continue.
- If repeated `MANIFEST` differs in those fields, receiver MUST treat it as a
  collision and ignore the conflicting manifest.

### 7.2.  TLV Encoding

Each TLV element:

- `Type` (uint8)
- `Length` (uint16)
- `Value` (`Length` bytes)

Unknown TLV types are ignored.

### 7.3.  `DATA`

- `Transfer ID` (16 bytes)
- `Chunk Index` (uint32)
- `Chunk Payload Length` (uint16, `0..65535`)
- `Chunk Payload`

Receiver MUST discard `DATA` when:

- transfer is unknown;
- `Chunk Index >= Total Chunks`;
- byte placement exceeds declared `File Size`.

### 7.4.  `FIN`

- `Transfer ID` (16 bytes)

### 7.5.  `STATUS`

- `Transfer ID` (16 bytes)
- `State` (uint8): `0=INCOMPLETE`, `1=COMPLETE`, `2=HASH_MISMATCH`
- `Range Count` (uint16)
- Missing ranges (`Range Count * 8` bytes)

### 7.6.  `REPAIR_REQUEST`

- `Transfer ID` (16 bytes)
- `Range Count` (uint16)
- Missing ranges (`Range Count * 8` bytes)

### 7.7.  `REPAIR_DONE`

- `Transfer ID` (16 bytes)

### 7.8.  `FILE_INFO_REQUEST`

- `Include Checksum` (uint8, 0/1)
- `Path Length` (uint16)
- `Path` (UTF-8, non-empty)

### 7.9.  `FILE_INFO_RESPONSE`

- `Exists` (uint8, 0/1)
- `Has SHA-256` (uint8, 0/1)
- `Size` (uint64)
- `Mtime (ns)` (uint64)
- `SHA-256` (32 bytes, zeroed when `Has SHA-256 = 0`)
- `Path Length` (uint16)
- `Path` (UTF-8, non-empty)

### 7.10.  `TRANSFER_COMPLETE`

- `Transfer ID` (16 bytes)

`TRANSFER_COMPLETE` is a compatibility completion hint. Implementations SHOULD
use `STATUS(COMPLETE)` as the authoritative receiver completion signal.

### 7.11.  `BEACON`

- `Role` (uint8): `1=SENDER`, `2=RECEIVER`
- `Transfer ID` (16 bytes)

`BEACON` is an optional availability keepalive and does not directly advance
transfer completion state.

## 8.  Missing Range Encoding

Each range entry:

- `Start` (uint32)
- `End` (uint32)

Rules:

- `Start < End` MUST hold.
- Range is half-open `[Start, End)`.
- Sender and receiver SHOULD normalize to merged sorted ranges.
- Implementations MUST reject `Range Count` above local safety limits.

## 9.  Sender State Machine and Timers

### 9.1.  Open-Loop Mode

Sender behavior:

1. Transmit `MANIFEST` at least once; repetition is RECOMMENDED.
2. Transmit all `DATA` chunks.
3. Transmit `FIN`.
4. Transfer result is complete if no intentional local test-drop was configured.

### 9.2.  Feedback Mode

After `FIN`, sender waits for receiver feedback:

- `feedback_wait_s` timer per receive wait (default 2.0s in reference implementation).
- `max_feedback_idle_timeouts` consecutive wait expirations before termination (default 2).
- `max_repair_rounds` maximum repair request loops (default 32).

If no relevant transfer progress is observed for one feedback wait window, sender
SHOULD re-send `MANIFEST` and `FIN` to recover from control-frame loss.

Terminal conditions:

- On `STATUS(COMPLETE)`: sender marks transfer complete and stops.
- On `STATUS(HASH_MISMATCH)`: sender marks transfer failed and stops.
- On `TRANSFER_COMPLETE` with matching transfer ID: sender MAY stop early as a
  compatibility fast-path.
- On `REPAIR_REQUEST`: sender retransmits requested chunks, sends `REPAIR_DONE`,
  increments repair-round count.
- On timer exhaustion without terminal status: sender marks transfer incomplete.

## 10.  Receiver State Machine

On `MANIFEST`, receiver allocates transfer staging and tracking state.

On `DATA`, receiver writes payload and marks chunk received.

On `FIN`:

- If complete, receiver verifies SHA-256 and finalizes.
- If incomplete and feedback enabled, receiver sends `REPAIR_REQUEST`.
- If incomplete and feedback disabled, receiver records incomplete state.

On `REPAIR_DONE`, receiver reevaluates completeness and hash.

Repeated `MANIFEST` optimization:

- Receiver MAY advertise resumable state via `STATUS(INCOMPLETE)` when a repeated
  manifest arrives for an active transfer.
- Receiver MAY short-circuit already-complete files by sending `STATUS(COMPLETE)`.

Feedback mode status:

- Receiver SHOULD emit `STATUS` on terminal transitions (`COMPLETE`,
  `INCOMPLETE`, `HASH_MISMATCH`).

## 11.  Deferred Repair and Restart Recovery

Receiver implementations SHOULD persist transfer journals containing at least:

- manifest attributes;
- source endpoint;
- staging-file location;
- received chunk coverage (for example received ranges).

On restart, receiver SHOULD restore incomplete transfers from journal and allow
completion from subsequent `DATA` and `REPAIR_DONE` frames.

## 12.  Transfer ID Uniqueness and Replay Handling

Transfer IDs SHOULD be generated with at least 128 bits of entropy.

Receiver collision policy:

- same Transfer ID + same manifest signature => continue existing transfer;
- same Transfer ID + different manifest signature => ignore conflicting transfer.

Replay handling is deployment-specific in v1; operators SHOULD bound retention
windows for completed transfer IDs where feasible.

## 13.  Path and Storage Safety

If `File Name` is mapped to filesystem paths:

- absolute paths MUST be rejected;
- parent traversal (`..`) MUST be rejected;
- writes MUST remain inside configured receiver root directory.

## 14.  Security Considerations

SSYNC provides file integrity verification but no built-in authentication or
confidentiality. Unprotected deployments are vulnerable to spoofing, replay,
tampering, and traffic observation.

Deployments MUST operate SSYNC on trusted/private networks or encapsulate SSYNC
in an authenticated secure channel (for example IPsec, DTLS, or VPN).

Deployments SHOULD apply source filtering, endpoint ACLs, and audit logging.
Future revisions SHOULD define authenticated control and anti-replay semantics.

## 15.  IANA Considerations

This document requests creation of a **Space Sync Parameters** registry group:

### 15.1.  Frame Type Registry

Policy: Specification Required.

Initial values:

- 1 `MANIFEST`
- 2 `DATA`
- 3 `FIN`
- 4 `STATUS`
- 5 `REPAIR_REQUEST`
- 6 `REPAIR_DONE`
- 7 `FILE_INFO_REQUEST`
- 8 `FILE_INFO_RESPONSE`
- 9 `TRANSFER_COMPLETE`
- 10 `BEACON`
- 240-255 Private Use

### 15.2.  STATUS State Registry

Policy: Specification Required.

Initial values:

- 0 `INCOMPLETE`
- 1 `COMPLETE`
- 2 `HASH_MISMATCH`
- 240-255 Private Use

### 15.3.  MANIFEST TLV Type Registry

Policy: Expert Review.

Initial values:

- 1 `MISSION_TAG`
- 2 `USER_NOTE`
- 3 `SOURCE_MTIME_NS`
- 240-255 Private Use

### 15.4.  Header Flags Registry

Policy: Specification Required.

Initial values:

- 0x00 all bits unassigned in version 1

## 16.  References

### 16.1.  Normative References

[RFC2119]  Bradner, S., "Key words for use in RFCs to Indicate Requirement
           Levels", BCP 14, RFC 2119, DOI 10.17487/RFC2119, March 1997.

[RFC8174]  Leiba, B., "Ambiguity of Uppercase vs Lowercase in RFC 2119 Key
           Words", BCP 14, RFC 8174, DOI 10.17487/RFC8174, May 2017.

### 16.2.  Informative References

[RFC0768]  Postel, J., "User Datagram Protocol", STD 6, RFC 768,
           DOI 10.17487/RFC0768, August 1980.

