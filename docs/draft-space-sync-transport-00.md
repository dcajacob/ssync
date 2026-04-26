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

`METADATA`, `DATA`, `STATUS`, and `BEACON`.

### 3.2.  High-Level Transfer Sequence

1. Sender transmits `METADATA` one or more times.
2. Sender transmits `DATA` chunks.
3. Receiver emits `STATUS(INCOMPLETE)` with missing ranges when repair is needed.
4. Sender retransmits missing ranges as `DATA`.
5. Receiver verifies whole-file hash and emits final `STATUS` in feedback mode.

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

### 7.1.  `METADATA`

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

### 7.5.  `STATUS`

- `Transfer ID` (16 bytes)
- `Kind` (uint8): `0=TRANSFER`, `1=FILE_INFO_RESPONSE`
- `State` (uint8): `0=INCOMPLETE`, `1=COMPLETE`, `2=HASH_MISMATCH`, plus private/extension values
- `Range Count` (uint16)
- For `TRANSFER` kind: Missing ranges (`Range Count * 8` bytes)
- For `FILE_INFO_RESPONSE` kind: optional query token and embedded file-info payload

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

1. Transmit `METADATA` at least once; repetition is RECOMMENDED.
2. Transmit all `DATA` chunks.
3. Transfer result is complete if no intentional local test-drop was configured.

### 9.2.  Feedback Mode

After initial data transmission, sender waits for receiver feedback:

- `feedback_wait_s` timer per receive wait (default 5.0s in reference implementation).
- `max_feedback_idle_timeouts` consecutive wait expirations before termination (default 2).
- `max_repair_rounds` maximum repair request loops (default 32).
- `periodic_metadata_interval_s` cadence for re-sending metadata during transfer
  (default 10.0s, `0` disables).

If no relevant transfer progress is observed for one feedback wait window, sender
SHOULD re-send `METADATA` to recover from control-context loss.

Terminal conditions:

- On `STATUS(COMPLETE)`: sender marks transfer complete and stops.
- On `STATUS(HASH_MISMATCH)`: sender marks transfer failed and stops.
- On `STATUS(INCOMPLETE)` with missing ranges: sender retransmits requested chunks
  as `DATA` and increments repair-round count.
- On timer exhaustion without terminal status: sender marks transfer incomplete.

## 10.  Receiver State Machine

On `METADATA`, receiver allocates transfer staging and tracking state.
Receiver MAY buffer bounded pre-manifest data keyed by transfer ID and replay those
chunks when metadata arrives.

On `DATA`, receiver writes payload and marks chunk received.

Repeated `METADATA` optimization:

- Receiver MAY advertise resumable state via `STATUS(INCOMPLETE)` when a repeated
  metadata arrives for an active transfer.
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
completion from subsequent `DATA` and `STATUS` feedback cycles.

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

- 1 `METADATA` (legacy alias: `MANIFEST`)
- 2 `DATA`
- 4 `STATUS`
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

