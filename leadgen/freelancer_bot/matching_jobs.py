"""Explicit durable job contracts for Opportunity matching destinations.

The historical ``matching_delivery`` protocol remains unchanged for the
Telegram product.  AI Sales Radar uses a distinct ``matching_inbox`` protocol,
so a delivery-capable worker can never claim a desktop materialization job.
"""

from __future__ import annotations

from uuid import UUID

from .persistence.jobs import DurableJobRepository


MATCHING_DELIVERY_JOB_TYPE = "opportunity.matching_delivery.v1"
MATCHING_DELIVERY_MAX_ATTEMPTS = 5
MATCHING_INBOX_JOB_TYPE = "opportunity.matching_inbox.v1"
MATCHING_INBOX_MAX_ATTEMPTS = 5
MATCHING_JOB_KEY_PREFIX = "opportunity:"


def matching_job_idempotency_key(opportunity_id: UUID) -> str:
    return f"{MATCHING_JOB_KEY_PREFIX}{opportunity_id}"


def opportunity_id_from_matching_job_key(value: str) -> UUID:
    if not value.startswith(MATCHING_JOB_KEY_PREFIX):
        raise ValueError("matching job idempotency key has an unsupported prefix")
    try:
        return UUID(value.removeprefix(MATCHING_JOB_KEY_PREFIX))
    except ValueError:
        raise ValueError(
            "matching job idempotency key has an invalid Opportunity id"
        ) from None


async def enqueue_matching_delivery(
    connection,
    *,
    jobs: DurableJobRepository,
    opportunity_id: UUID,
) -> UUID:
    """Enqueue matching once in the Opportunity write transaction."""

    return await jobs.enqueue(
        connection,
        job_type=MATCHING_DELIVERY_JOB_TYPE,
        idempotency_key=matching_job_idempotency_key(opportunity_id),
        max_attempts=MATCHING_DELIVERY_MAX_ATTEMPTS,
        correlation_id=opportunity_id,
    )


async def enqueue_matching_inbox(
    connection,
    *,
    jobs: DurableJobRepository,
    opportunity_id: UUID,
) -> UUID:
    """Enqueue the desktop Inbox consumer without any delivery capability."""

    return await jobs.enqueue(
        connection,
        job_type=MATCHING_INBOX_JOB_TYPE,
        idempotency_key=matching_job_idempotency_key(opportunity_id),
        max_attempts=MATCHING_INBOX_MAX_ATTEMPTS,
        correlation_id=opportunity_id,
    )


__all__ = [
    "MATCHING_DELIVERY_JOB_TYPE",
    "MATCHING_DELIVERY_MAX_ATTEMPTS",
    "MATCHING_JOB_KEY_PREFIX",
    "MATCHING_INBOX_JOB_TYPE",
    "MATCHING_INBOX_MAX_ATTEMPTS",
    "enqueue_matching_delivery",
    "enqueue_matching_inbox",
    "matching_job_idempotency_key",
    "opportunity_id_from_matching_job_key",
]
