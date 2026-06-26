"""Data models exchanged between the API layer, workflow, and activities.

These are plain dataclasses so Temporal's default data converter can serialize
them to/from JSON in workflow history. The webhook request models mirror the
exact snake_case payload shapes sent by the Commerce App and Payment Processor.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class OrderStatus(str, enum.Enum):
    """Typed constants for every business status an order can occupy.

    Inheriting from ``str`` means every member IS a string, so:
      - Temporal's JSON converter serialises them as plain strings.
      - Comparisons against plain string literals still evaluate True
        (``OrderStatus.FULFILLED == "FULFILLED"``).
      - A typo is a ``AttributeError`` at import time, not a silent
        wrong-status write that only surfaces in a dashboard query.
    """

    INITIALIZING = "INITIALIZING"
    PENDING_CORRECTION = "PENDING_CORRECTION"
    CORRECTION_TIMEOUT = "CORRECTION_TIMEOUT"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    AWAITING_RISK_ASSESSMENT = "AWAITING_RISK_ASSESSMENT"
    RISK_REJECTED = "RISK_REJECTED"
    RISK_ASSESSMENT_FAILED = "RISK_ASSESSMENT_FAILED"
    AWAITING_PAYMENT = "AWAITING_PAYMENT"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"
    VALIDATING_PAYMENT = "VALIDATING_PAYMENT"
    PAYMENT_INVALID = "PAYMENT_INVALID"
    PAYMENT_EXPIRED = "PAYMENT_EXPIRED"
    PAYMENT_VALIDATION_FAILED = "PAYMENT_VALIDATION_FAILED"
    ENRICHING = "ENRICHING"
    FULFILLED = "FULFILLED"
    FULFILLMENT_FAILED = "FULFILLMENT_FAILED"


@dataclass
class OrderItem:
    item_id: str
    quantity: int
    sku_id: str = ""
    brand_code: str = ""


@dataclass
class OrderInput:
    customer_id: str
    order_id: str
    items: list[OrderItem] = field(default_factory=list)


@dataclass
class PaymentInput:
    rrn: str
    amount_cents: int


@dataclass
class EnrichedOrder:
    customer_id: str
    order_id: str
    rrn: str
    items: list[OrderItem] = field(default_factory=list)



ERR_INVALID_RRN = "INVALID_RRN"
ERR_INVALID_ORDER = "INVALID_ORDER"
ERR_ORDER_CANCELLED = "ORDER_CANCELLED"
ERR_DUPLICATE_PAYMENT = "DUPLICATE_PAYMENT"
ERR_PAYMENT_NOT_ACCEPTED = "PAYMENT_NOT_ACCEPTED"


@dataclass
class DashboardUpdate:
    """Typed input for update_customer_dashboard.

    Using a dataclass instead of raw (order_id, status) primitives makes it
    trivial to add fields (e.g. previous_status, timestamp) without a breaking
    signature change across all callers.
    """

    order_id: str
    status: str


@dataclass
class PaymentValidationInput:
    """Typed input for validate_payment_rrn.

    Extensible without breaking callers; mirrors the convention used by all
    other activity inputs in this codebase.
    """

    rrn: str


@dataclass
class EnrichmentInput:
    """Typed input for enrich_with_pim (OMS-012).

    Replaces the raw two-argument positional signature (OrderInput, rrn: str)
    with a single dataclass, consistent with every other activity in this module.
    Extensible without breaking callers: adding fields (e.g. pim_region) is a
    backward-compatible change.
    """

    order: OrderInput
    rrn: str


@dataclass
class WorkflowState:
    """All mutable workflow fields in one place.

    Encapsulating state in a single dataclass:
      - makes the full state returnable from a @workflow.query without listing
        each field individually
      - eliminates manual field resets scattered across the workflow body
      - makes adding new state fields a backward-compatible change
    """

    current_order: OrderInput | None = None
    captured_payment: PaymentInput | None = None
    is_cancelled: bool = False
    correction_received: bool = False
    # str (not OrderStatus) so Temporal's JSON converter round-trips it without
    # needing to know about the enum type.  Writes always use OrderStatus members,
    # which are str subclasses and therefore serialise identically to plain strings.
    current_status: str = OrderStatus.INITIALIZING
