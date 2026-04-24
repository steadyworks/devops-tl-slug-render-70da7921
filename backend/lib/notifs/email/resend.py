import asyncio

import resend
from resend.exceptions import ResendError

from backend.db.data_models import ShareProvider
from backend.lib.utils.rate_limiter import AsyncRateLimiter
from backend.lib.utils.retryable import retryable_with_backoff

from .base import AbstractEmailProvider
from .types import EmailMessage, EmailSendResult


class ResendEmailProvider(AbstractEmailProvider):
    def __init__(self) -> None:
        # 2 requests per 1 second
        self._limiter = AsyncRateLimiter(rate=2, per=1.0)

    @classmethod
    def get_share_provider(cls) -> ShareProvider:
        return ShareProvider.RESEND

    def _build_params(
        self, msg: EmailMessage
    ) -> tuple[resend.Emails.SendParams, resend.Emails.SendOptions]:
        send_params: resend.Emails.SendParams = {
            "from": msg.from_.as_rfc822(),
            "to": [addr.email for addr in msg.to_],
            "subject": msg.subject,
            "html": msg.html,
        }
        if msg.reply_to:
            send_params["reply_to"] = msg.reply_to.email

        send_options: resend.Emails.SendOptions = {
            "idempotency_key": msg.idempotency_key,
        }
        return (send_params, send_options)

    async def _send_once(self, msg: EmailMessage) -> EmailSendResult:
        params, options = self._build_params(msg)

        async with self._limiter:
            resp: resend.Emails.SendResponse = await asyncio.to_thread(
                resend.Emails.send, params, options
            )

        return EmailSendResult(
            message_id=resp["id"],
            idempotency_key=msg.idempotency_key,
        )

    async def send(self, msg: EmailMessage) -> EmailSendResult:
        """
        Public entrypoint. Retries on ResendError with backoff,
        and enforces global 2QPS limit.
        """
        return await retryable_with_backoff(
            lambda: self._send_once(msg),
            retryable=(ResendError, OSError, TimeoutError, ConnectionError),
            max_attempts=3,
            base_delay=0.5,
        )
