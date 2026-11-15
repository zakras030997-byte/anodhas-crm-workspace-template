"""Compact authenticated Telegram callback payloads."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum
import hashlib
import hmac
import struct
from uuid import UUID


CALLBACK_PREFIX = b"asr1:"
CALLBACK_VERSION = 1
CALLBACK_TAG_BYTES = 12
CALLBACK_DOMAIN = b"ai-sales-radar/operator-callback/v1"
_BODY = struct.Struct("!BB16sII")
_PAYLOAD_BYTES = _BODY.size + CALLBACK_TAG_BYTES


class CallbackDecodeError(ValueError):
    pass


class CallbackAction(IntEnum):
    SHORTLIST = 1
    SKIP = 2
    NOT_FIT = 3
    DRAFT_PROMPT = 4
    DRAFT_CONFIRM = 5
    SOURCE_DISCOVERY_START = 6
    SOURCE_DISCOVERY_RESULTS = 7
    NAVIGATE = 8


SYSTEM_CALLBACK_ID = UUID(int=0)


@dataclass(frozen=True)
class DecodedCallback:
    action: CallbackAction
    lead_id: UUID
    expected_revision: int
    expires_at: int


class SignedCallbackCodec:
    """Sign callbacks with a key derived from the bot token.

    Callback data contains no raw lead text, Telegram identifiers or secrets.
    """

    def __init__(self, bot_token: str, *, ttl_seconds: int = 900) -> None:
        selected = bot_token.strip().encode("utf-8")
        if not selected:
            raise ValueError("bot token must not be blank")
        if not 60 <= ttl_seconds <= 3_600:
            raise ValueError("callback ttl must be between 60 and 3600 seconds")
        self._key = hmac.new(selected, CALLBACK_DOMAIN, hashlib.sha256).digest()
        self._ttl_seconds = ttl_seconds

    def encode(
        self,
        action: CallbackAction,
        lead_id: UUID,
        expected_revision: int,
        *,
        now: datetime | None = None,
    ) -> bytes:
        selected_action = CallbackAction(action)
        if not 1 <= expected_revision <= 0xFFFFFFFF:
            raise ValueError("expected revision is out of range")
        issued_at = _utc_timestamp(now)
        expires_at = issued_at + self._ttl_seconds
        if expires_at > 0xFFFFFFFF:
            raise ValueError("callback expiry is out of range")
        body = _BODY.pack(
            CALLBACK_VERSION,
            int(selected_action),
            lead_id.bytes,
            expected_revision,
            expires_at,
        )
        tag = hmac.new(self._key, body, hashlib.sha256).digest()[:CALLBACK_TAG_BYTES]
        encoded = base64.urlsafe_b64encode(body + tag).rstrip(b"=")
        result = CALLBACK_PREFIX + encoded
        if len(result) > 64:
            raise ValueError("signed callback exceeds Telegram limit")
        return result

    def decode(
        self,
        value: bytes,
        *,
        now: datetime | None = None,
    ) -> DecodedCallback:
        if not isinstance(value, bytes) or not value.startswith(CALLBACK_PREFIX):
            raise CallbackDecodeError("invalid callback")
        if len(value) > 64:
            raise CallbackDecodeError("invalid callback")
        encoded = value[len(CALLBACK_PREFIX) :]
        if not encoded:
            raise CallbackDecodeError("invalid callback")
        try:
            padding = b"=" * (-len(encoded) % 4)
            payload = base64.b64decode(
                encoded + padding,
                altchars=b"-_",
                validate=True,
            )
        except (ValueError, TypeError):
            raise CallbackDecodeError("invalid callback") from None
        if len(payload) != _PAYLOAD_BYTES:
            raise CallbackDecodeError("invalid callback")
        canonical = base64.urlsafe_b64encode(payload).rstrip(b"=")
        if not hmac.compare_digest(encoded, canonical):
            raise CallbackDecodeError("invalid callback")
        body = payload[:-CALLBACK_TAG_BYTES]
        supplied_tag = payload[-CALLBACK_TAG_BYTES:]
        expected_tag = hmac.new(
            self._key,
            body,
            hashlib.sha256,
        ).digest()[:CALLBACK_TAG_BYTES]
        if not hmac.compare_digest(supplied_tag, expected_tag):
            raise CallbackDecodeError("invalid callback")
        version, action_code, lead_bytes, revision, expires_at = _BODY.unpack(body)
        if version != CALLBACK_VERSION or revision < 1:
            raise CallbackDecodeError("invalid callback")
        try:
            action = CallbackAction(action_code)
        except ValueError:
            raise CallbackDecodeError("invalid callback") from None
        if expires_at < _utc_timestamp(now):
            raise CallbackDecodeError("expired callback")
        return DecodedCallback(
            action=action,
            lead_id=UUID(bytes=lead_bytes),
            expected_revision=revision,
            expires_at=expires_at,
        )


def callback_request_id(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc_timestamp(value: datetime | None) -> int:
    selected = value or datetime.now(timezone.utc)
    if selected.tzinfo is None or selected.utcoffset() is None:
        raise ValueError("callback time must include a timezone")
    timestamp = int(selected.astimezone(timezone.utc).timestamp())
    if timestamp < 0:
        raise ValueError("callback time is out of range")
    return timestamp
