# Network Working Group                                           D. User
## Internet-Draft                                                 Space Sync
Intended status: Experimental                                 March 2026
Expires: September 2026

# Space Sync (SSYNC): A UDP-Based File Transport for Asymmetric Intermittent Links

## Abstract

This document specifies Space Sync (SSYNC), an experimental UDP-based
file transport protocol for mission-specific links with strong asymmetry and
intermittent connectivity, such as Low Earth Orbit (LEO) downlink operations.
SSYNC supports both open-loop delivery (no return path required) and
feedback-assisted repair (sparse missing-range signaling).

The protocol uses compact binary framing, chunk-indexed file transfer, and
whole-file integrity verification using SHA-256. The design prioritizes
operational robustness and extensibility over Internet fairness and
general-purpose transport behavior.

## Status of This Memo

This Internet-Draft is submitted in full conformance with the provisions of
BCP 78 and BCP 79.

Internet-Drafts are working documents of the Internet Engineering Task Force
(IETF). Note that other groups may also distribute working documents as
Internet-Drafts.

Internet-Drafts are draft documents valid for a maximum of six months and may
be updated, replaced, or obsoleted by other documents at any time. It is
inappropriate to use Internet-Drafts as reference material or to cite them
other than as "work in progress."

## 1.  Introduction

LEO and other mission-specific links often exhibit:

- high-rate forward links and constrained or absent return links;
- intermittent contacts, including deferred opportunities for repair;
- non-Internet deployment goals where fairness is not primary.

SSYNC provides a focused transport primitive for reliable file delivery under
these constraints. It avoids mandatory per-packet acknowledgments, enables
range-based repair when feedback exists, and keeps wire structures compact for
implementation in constrained systems.

## 2.  Conventions and Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this
document are to be interpreted as described in BCP 14 [RFC2119] [RFC8174] when,
and only when, they appear in all capitals, as shown here.

Terms used in this document:

- **Sender**: SSYNC endpoint transmitting a file transfer.
- **Receiver**: SSYNC endpoint accepting and reassembling a file transfer.
- **Transfer ID**: 16-byte random identifier for one file transfer.
- **Chunk**: Fixed-size payload unit addressed by a zero-based chunk index.
- **Open-loop mode**: Delivery mode with no expectation of return feedback.
- **Feedback mode**: Delivery mode where sparse status and repair requests are used.
- **Missing range**: Half-open interval `[start, end)` of missing chunk indexes.

## 3.  Protocol Overview

For each file transfer:

1. Sender transmits one or more `MANIFEST` frames.
2. Sender transmits `DATA` frames for chunk indexes in the transfer.
3. Sender transmits `FIN` to mark end of current pass.
4. Receiver:
   - finalizes immediately when complete, or
   - requests missing chunk ranges (`REPAIR_REQUEST`) when feedback is enabled.
5. Sender retransmits requested chunks and transmits `REPAIR_DONE`.
6. Receiver verifies whole-file SHA-256 and emits terminal `STATUS` in feedback mode.

Open-loop operation is complete after step 3. Deferred repair is achieved by
re-running transfer(s) over later contacts and requesting only missing ranges.

## 4.  Transport and Encapsulation

SSYNC frames are carried in UDP datagrams. One SSYNC frame occupies exactly one
UDP payload.

- UDP port selection is deployment-specific (default implementation port is 9000).
- Implementations SHOULD choose chunk size and pacing to avoid path MTU fragmentation.
- This version specifies IPv4/IPv6 agnostic framing (network byte order).

## 5.  Common Frame Header

Every frame starts with a fixed 10-byte header:

```
0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+---------------+---------------+---------------+---------------+
|     Magic     |   Version     |  Frame Type   |     Flags     |
+---------------+---------------+---------------+---------------+
|   Reserved    |                  Payload Length                |
+---------------+---------------+---------------+---------------+
```

Field definitions:

- `Magic` (16 bits): ASCII `"SS"` (`0x53 0x53`), MUST match exactly.
- `Version` (8 bits): protocol version, this document specifies `1`.
- `Frame Type` (8 bits): one of Section 6 values.
- `Flags` (8 bits): frame flags, MUST be sent as `0` in this version.
- `Reserved` (8 bits): MUST be sent as `0`, MUST be ignored on receipt.
- `Payload Length` (32 bits): octet length of frame payload.

Receivers MUST validate `Magic`, `Version`, and payload length consistency.
Frames failing validation MUST be discarded.

## 6.  Frame Types

This version defines:

- `1` = `MANIFEST`
- `2` = `DATA`
- `3` = `FIN`
- `4` = `STATUS`
- `5` = `REPAIR_REQUEST`
- `6` = `REPAIR_DONE`

Unknown frame types MUST be ignored.

## 7.  Transfer Metadata and Payload Formats

All integer fields use network byte order (big-endian).

### 7.1.  `MANIFEST` Payload

`MANIFEST` announces file transfer parameters:

- `Transfer ID` (16 bytes)
- `File Size` (uint64)
- `Chunk Size` (uint32)
- `Total Chunks` (uint32)
- `SHA-256` (32 bytes)
- `File Name Length` (uint16)
- `File Name` (UTF-8, variable)
- `TLV Length` (uint16)
- `TLVs` (variable)

`Total Chunks` SHOULD equal `ceil(File Size / Chunk Size)` for non-empty files,
and `0` for empty files.

#### 7.1.1.  TLV Format

Each TLV element:

- `Type` (uint8)
- `Length` (uint16)
- `Value` (`Length` octets)

Unknown TLV types MUST be ignored.

### 7.2.  `DATA` Payload

- `Transfer ID` (16 bytes)
- `Chunk Index` (uint32)
- `Chunk Payload Length` (uint16)
- `Chunk Payload` (variable)

Receivers MUST discard chunks where:

- `Chunk Index >= Total Chunks`; or
- payload placement exceeds declared file size.

### 7.3.  `FIN` Payload

- `Transfer ID` (16 bytes)

`FIN` marks sender completion of a transmission pass.

### 7.4.  `STATUS` Payload

- `Transfer ID` (16 bytes)
- `State` (uint8): `0=INCOMPLETE`, `1=COMPLETE`, `2=HASH_MISMATCH`
- `Range Count` (uint16)
- `Missing Ranges` (`Range Count * 8` octets)

### 7.5.  `REPAIR_REQUEST` Payload

- `Transfer ID` (16 bytes)
- `Range Count` (uint16)
- `Missing Ranges` (`Range Count * 8` octets)

### 7.6.  `REPAIR_DONE` Payload

- `Transfer ID` (16 bytes)

## 8.  Missing Range Encoding

A missing range entry is encoded as:

- `Start` (uint32)
- `End` (uint32)

Ranges are half-open `[Start, End)`.

Validity rules:

- `Start < End` MUST hold for every range.
- Implementations SHOULD merge overlapping or adjacent ranges before sending.
- Receivers SHOULD normalize and merge ranges on receipt.

## 9.  Sender Behavior

### 9.1.  Open-Loop Mode

In open-loop mode:

1. Sender MUST transmit `MANIFEST` at least once; repeated transmission is RECOMMENDED.
2. Sender transmits `DATA` for each chunk index.
3. Sender transmits `FIN`.
4. Sender MAY terminate transfer state immediately after `FIN`.

### 9.2.  Feedback-Assisted Mode

In feedback mode, after `FIN`:

- Sender SHOULD wait for `STATUS` and/or `REPAIR_REQUEST` for a bounded interval.
- On `REPAIR_REQUEST`, sender retransmits exactly requested chunks (if available).
- Sender then transmits `REPAIR_DONE`.
- Sender MAY repeat repair rounds up to a deployment-defined limit.

`STATUS(INCOMPLETE)` is informational. `REPAIR_REQUEST` drives retransmission.

## 10.  Receiver Behavior

Receiver behavior:

1. On `MANIFEST`, allocate transfer state and a staging object sized to `File Size`.
2. On each valid `DATA`, place payload at `Chunk Index * Chunk Size`.
3. On `FIN`:
   - if complete, verify SHA-256 and finalize transfer;
   - if incomplete and feedback enabled, transmit `REPAIR_REQUEST`.
4. On `REPAIR_DONE`, evaluate completion and integrity again.
5. In feedback mode, receiver SHOULD send terminal `STATUS`.

Receiver MUST NOT mark transfer complete unless SHA-256 matches manifest hash.

## 11.  Path and File Name Handling

`File Name` MAY contain relative path components for destination placement.
Implementations that materialize to filesystem paths:

- MUST reject absolute paths;
- MUST reject parent traversal segments (`..`);
- SHOULD normalize and create required parent directories within configured root.

## 12.  Reliability and Integrity Properties

This version provides:

- chunk-level loss recovery via range-based retransmission;
- file-level integrity via SHA-256.

This version does **not** provide:

- cryptographic peer authentication;
- payload confidentiality;
- transport-layer congestion control.

## 13.  Security Considerations

Without additional protections, SSYNC is vulnerable to spoofing, tampering,
replay, and metadata exposure on untrusted networks.

Deployments SHOULD:

- restrict SSYNC to private, controlled network domains; and/or
- encapsulate SSYNC in a secure channel (for example IPsec, DTLS, or VPN); and
- apply source filtering and endpoint authentication at network boundaries.

Future versions SHOULD define authenticated control frames and anti-replay
mechanisms.

## 14.  IANA Considerations

This document has no IANA actions.

Future versions MAY request:

- a UDP port assignment for SSYNC;
- frame type registries;
- TLV type registries.

## 15.  Extensibility and Future Work

The protocol is designed to evolve with backward-compatible framing extensions.
Candidate extensions include:

- parity/FEC block signaling and hybrid repair;
- persistent pass-to-pass transfer ledgers;
- stream-oriented transport mode;
- authenticated control and key-management integration;
- mission policy fields in manifest TLVs.

## 16.  References

### 16.1.  Normative References

[RFC2119]  Bradner, S., "Key words for use in RFCs to Indicate Requirement
           Levels", BCP 14, RFC 2119, DOI 10.17487/RFC2119, March 1997.

[RFC8174]  Leiba, B., "Ambiguity of Uppercase vs Lowercase in RFC 2119 Key
           Words", BCP 14, RFC 8174, DOI 10.17487/RFC8174, May 2017.

### 16.2.  Informative References

[RFC768]   Postel, J., "User Datagram Protocol", STD 6, RFC 768,
           DOI 10.17487/RFC0768, August 1980.

