from abc import ABC, abstractmethod

from backend.db.data_models import ShareChannelType, ShareProvider
from backend.lib.notifs.protocol import NotificationProviderProtocol

from .types import EmailMessage, EmailSendResult


class AbstractEmailProvider(NotificationProviderProtocol, ABC):
    @classmethod
    def get_share_channel_type(cls) -> ShareChannelType:
        return ShareChannelType.EMAIL

    @classmethod
    @abstractmethod
    def get_share_provider(cls) -> ShareProvider: ...

    @abstractmethod
    async def send(self, msg: EmailMessage) -> EmailSendResult: ...
