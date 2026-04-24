from typing import Protocol

from backend.db.data_models import ShareChannelType, ShareProvider


class NotificationProviderProtocol(Protocol):
    @classmethod
    def get_share_channel_type(cls) -> ShareChannelType: ...

    @classmethod
    def get_share_provider(cls) -> ShareProvider: ...
