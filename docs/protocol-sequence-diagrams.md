# SSYNC Protocol Sequence Diagrams

This document captures the primary SSYNC protocol exchanges as Mermaid sequence diagrams.
It complements the wire-format and behavior details in the transport draft.

## 1) Open-Loop Transfer (No Feedback)

In open-loop mode, the sender does not rely on a return path. It sends repeated
`METADATA`, then streams all `DATA` chunks and exits. `manifest_repeats` controls
metadata repetition.

```mermaid
sequenceDiagram
    autonumber
    participant Sender
    participant Receiver

    Sender->>Receiver: METADATA (repeat 1)
    Sender->>Receiver: METADATA (repeat 2)
    Sender->>Receiver: METADATA (repeat N = manifest_repeats)
    Sender->>Receiver: DATA chunk 0
    Sender->>Receiver: DATA chunk 1
    Sender->>Receiver: DATA chunk 2..N-1
    Note over Sender: Transfer loop ends after full data pass
```

## 2) Feedback Mode (Happy Path)

In feedback mode, receiver-driven repair is sparse and range-based. The sender
periodically re-emits `METADATA` (`periodic_metadata_interval_s`) while sending data.
The receiver completes after whole-file SHA-256 verification and emits `STATUS(COMPLETE)`.

```mermaid
sequenceDiagram
    autonumber
    participant Sender
    participant Receiver

    Sender->>Receiver: METADATA (x manifest_repeats)
    Sender->>Receiver: DATA chunks initial pass (0..N-1)
    Receiver-->>Sender: STATUS(INCOMPLETE, missing_ranges=[2,5))
    Sender->>Receiver: DATA repair chunk 2
    Sender->>Receiver: DATA repair chunk 3
    Sender->>Receiver: DATA repair chunk 4
    Sender->>Receiver: METADATA (periodic_metadata_interval_s)
    Receiver-->>Sender: STATUS(INCOMPLETE, missing_ranges=[11,12))
    Sender->>Receiver: DATA repair chunk 11
    Receiver->>Receiver: Verify SHA-256 over final file
    Receiver-->>Sender: STATUS(COMPLETE)
    Note over Sender: Sender marks transfer complete and exits
```

## 3) Feedback Mode: Repair Rounds and Timeout Exit

This flow shows timeout-driven metadata refresh and bounded repair loops.
`feedback_wait_s` controls per-wait timeout; sender stops after
`max_feedback_idle_timeouts` consecutive idle waits or `max_repair_rounds`.

```mermaid
sequenceDiagram
    autonumber
    participant Sender
    participant Receiver

    Sender->>Receiver: METADATA + initial DATA pass
    loop Feedback loop (up to max_repair_rounds)
        Sender->>Sender: Wait for STATUS (feedback_wait_s)
        alt STATUS received
            Receiver-->>Sender: STATUS(INCOMPLETE, missing ranges)
            Sender->>Receiver: DATA repairs for requested ranges
        else Timeout (no STATUS)
            Sender->>Receiver: METADATA retransmit (control-context recovery)
            Sender->>Sender: idle_timeouts += 1
        end
    end

    alt idle_timeouts > max_feedback_idle_timeouts
        Note over Sender: Exit transfer as incomplete
    else STATUS(COMPLETE) received earlier
        Note over Sender: Exit transfer as complete
    end
```

## 4) Auto Feedback Discovery and Fallback

The top-level sync workflow can begin open-loop, promote to feedback when uplink
activity is observed, and fall back to open-loop on prolonged uplink idle
(`auto_feedback_idle_timeout_s`).

```mermaid
sequenceDiagram
    autonumber
    participant Sender
    participant Receiver

    Note over Sender: Start in open-loop (auto feedback enabled)
    Sender->>Receiver: METADATA + DATA (forward-only)
    Receiver-->>Sender: BEACON or STATUS observed on uplink
    Note over Sender: Promote to feedback-active mode
    Receiver-->>Sender: STATUS(INCOMPLETE, missing ranges)
    Sender->>Receiver: DATA repairs
    Note over Sender: Continue with feedback while uplink active
    Note over Sender: If no uplink for auto_feedback_idle_timeout_s
    Note over Sender: Fall back to open-loop behavior
```

## 5) Pre-Metadata Buffering

When `DATA` arrives before `METADATA`, receiver can buffer bounded chunks keyed
by transfer ID (`pre_metadata_max_pending_bytes`, per-transfer limits, TTL).
After metadata arrives, buffered chunks are replayed.

```mermaid
sequenceDiagram
    autonumber
    participant Sender
    participant Receiver

    Sender->>Receiver: DATA chunk 40 (unknown transfer_id)
    Receiver->>Receiver: Buffer pending pre-metadata chunk
    Sender->>Receiver: DATA chunk 41 (unknown transfer_id)
    Receiver->>Receiver: Buffer pending pre-metadata chunk
    Sender->>Receiver: METADATA (same transfer_id)
    Receiver->>Receiver: Create/restore transfer state
    Receiver->>Receiver: Replay buffered chunks into tracker/file
    Receiver-->>Sender: STATUS(INCOMPLETE, reduced missing ranges)
```

## 6) Short-Circuit When File Already Complete

If receiver already has a matching completed file for the manifest signature
(file size, chunk size, SHA-256, file name), it can immediately return
`STATUS(COMPLETE)` and avoid data transfer.

```mermaid
sequenceDiagram
    autonumber
    participant Sender
    participant Receiver

    Sender->>Receiver: METADATA
    Receiver->>Receiver: Detect existing complete file (hash match)
    Receiver-->>Sender: STATUS(COMPLETE)
    Note over Sender: Exit early without sending DATA
```

## 7) File Info Query for Skip-Unchanged

For sync pre-check (`--skip-unchanged`, optionally `--checksum`), sender can
query remote file information using `METADATA` TLVs and receive
`STATUS(FILE_INFO_RESPONSE)`.

```mermaid
sequenceDiagram
    autonumber
    participant Sender
    participant Receiver

    Sender->>Receiver: METADATA with FILE_INFO_QUERY TLVs
    Note over Sender,Receiver: TLVs include query path/token/checksum request
    Receiver->>Receiver: Resolve path under receiver root
    Receiver-->>Sender: STATUS(FILE_INFO_RESPONSE, token, file_info)
    alt Remote file unchanged
        Note over Sender: Skip transfer for this file
    else Remote file missing or differs
        Sender->>Receiver: Start normal METADATA + DATA transfer
    end
```

## 8) Deferred Repair Across Contacts (Journal Resume)

Receiver persists incomplete transfer state in `.ssync-journal.json`. On a later
contact, sender may use a new transfer ID for the same file; receiver correlates
by manifest signature and resumes coverage.

```mermaid
sequenceDiagram
    autonumber
    participant SenderContact1 as Sender(Contact1)
    participant Receiver
    participant SenderContact2 as Sender(Contact2)

    SenderContact1->>Receiver: METADATA(TID_A) + partial DATA
    Receiver-->>SenderContact1: STATUS(INCOMPLETE, missing ranges)
    Note over SenderContact1,Receiver: Contact ends before completion
    Receiver->>Receiver: Persist progress in .ssync-journal.json

    SenderContact2->>Receiver: METADATA(TID_B, same manifest signature)
    Receiver->>Receiver: Correlate signature and resume existing partial state
    Receiver-->>SenderContact2: STATUS(INCOMPLETE, remaining missing ranges)
    SenderContact2->>Receiver: DATA repairs for remaining ranges
    Receiver-->>SenderContact2: STATUS(COMPLETE) after hash verify
```

## 9) Beacon Keepalive

`BEACON` frames are optional keepalives (`beacon_interval_s`). They are used for
availability and source-address freshness and do not directly complete transfers.

```mermaid
sequenceDiagram
    autonumber
    participant Sender
    participant Receiver

    loop Every beacon_interval_s
        Sender->>Receiver: BEACON(role=SENDER, transfer_id)
        Receiver->>Sender: BEACON(role=RECEIVER, transfer_id)
    end
    Note over Sender,Receiver: Keepalives aid liveness/source refresh only
```

## Related References

- [`docs/draft-space-sync-transport-00.md`](docs/draft-space-sync-transport-00.md)
- [`src/ssync/space_sync/frames.py`](src/ssync/space_sync/frames.py)
- [`src/ssync/space_sync/types.py`](src/ssync/space_sync/types.py)
- [`src/ssync/space_sync/sender.py`](src/ssync/space_sync/sender.py)
- [`src/ssync/space_sync/receiver.py`](src/ssync/space_sync/receiver.py)
