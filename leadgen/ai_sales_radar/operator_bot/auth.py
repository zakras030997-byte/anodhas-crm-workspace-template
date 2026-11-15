"""Fail-closed owner authorization for every Telegram update."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OwnerGate:
    owner_user_id: int
    owner_chat_id: int

    def __post_init__(self) -> None:
        if self.owner_user_id <= 0 or self.owner_chat_id <= 0:
            raise ValueError("owner identifiers must be positive")
        if self.owner_user_id != self.owner_chat_id:
            raise ValueError("operator bot requires a private owner chat")

    def allows(
        self,
        *,
        is_private: bool,
        sender_id: int | None,
        chat_id: int | None,
    ) -> bool:
        return (
            is_private is True
            and type(sender_id) is int
            and type(chat_id) is int
            and sender_id == self.owner_user_id
            and chat_id == self.owner_chat_id
        )
