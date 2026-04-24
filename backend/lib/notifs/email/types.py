from dataclasses import dataclass, field
from typing import Optional, Sequence

from pydantic import BaseModel


@dataclass(frozen=True)
class EmailAddress:
    """Represents an email address, optionally with a display name."""

    email: str
    name: Optional[str] = None

    def as_rfc822(self) -> str:
        if self.name and self.name.strip():
            return f"{self.name} <{self.email}>"
        return self.email


@dataclass(frozen=True)
class EmailMessage:
    """
    Provider-agnostic message shape.
    - At least one of html or text must be provided.
    """

    subject: str
    from_: EmailAddress
    to_: Sequence[EmailAddress]
    html: str
    idempotency_key: str
    reply_to: Optional[EmailAddress] = None
    tags: dict[str, str] = field(default_factory=dict[str, str])


class EmailSendResult(BaseModel):
    message_id: Optional[str]
    idempotency_key: Optional[str]
