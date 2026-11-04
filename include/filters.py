"""
Message filters for ``MessageQueueTrigger(apply_function=...)``, referenced by dotted path.

The trigger calls the filter with each Kafka message. A truthy return value fires the trigger and
becomes the asset event's payload. ``None`` skips the message.
"""

from __future__ import annotations

import json


def is_paid_order(message):
    """demo1 and demo2-before: return the order if it is paid, else None."""
    order = json.loads(message.value())
    return order if order.get("type") == "paid" else None


def is_refund(message):
    """demo2-before: return the order if it is refunded, else None."""
    order = json.loads(message.value())
    return order if order.get("type") == "refunded" else None
