from __future__ import annotations


def result_outcome_value(result: object) -> str:
    outcome = getattr(result, "outcome", None)
    value = getattr(outcome, "value", None)
    if isinstance(value, str):
        return value
    if isinstance(outcome, str):
        return outcome
    return "receiver_complete" if bool(getattr(result, "completed", False)) else "incomplete"


def result_transmission_complete(result: object) -> bool:
    value = getattr(result, "transmission_complete", None)
    if isinstance(value, bool):
        return value
    return bool(getattr(result, "completed", False))


def result_delivery_confirmed(result: object) -> bool:
    value = getattr(result, "delivery_confirmed", None)
    if isinstance(value, bool):
        return value
    return result_outcome_value(result) == "receiver_complete"
