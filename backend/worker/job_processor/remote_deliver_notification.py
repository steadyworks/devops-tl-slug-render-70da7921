from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal import (
    DALNotificationDeliveryAttempts,
    DALPhotobooks,
    DALShareChannels,
    DALShares,
    DAONotificationDeliveryAttemptsCreate,
    DAOPhotobooks,
    DAOShareChannels,
    DAOShareChannelsUpdate,
    DAOShares,
    safe_commit,
)
from backend.db.data_models import (
    NotificationDeliveryEvent,
    ShareChannelStatus,
    ShareChannelType,
)
from backend.lib.notifs.email.types import EmailAddress, EmailMessage
from backend.lib.utils.common import none_throws
from backend.worker.process.types import RemoteIOBoundWorkerProcessResources

from .remote import RemoteJobProcessor
from .types import DeliverNotificationInputPayload, DeliverNotificationOutputPayload


class RemoteDeliverNotificationJobProcessor(
    RemoteJobProcessor[
        DeliverNotificationInputPayload,
        DeliverNotificationOutputPayload,
        RemoteIOBoundWorkerProcessResources,
    ]
):
    async def process(
        self, input_payload: DeliverNotificationInputPayload
    ) -> DeliverNotificationOutputPayload:
        async with self.db_session_factory.new_session() as db_session:
            share_channel = none_throws(
                await DALShareChannels.get_by_id(
                    db_session, input_payload.share_channel_id
                ),
                f"Invalid share channel: {input_payload.share_channel_id}",
            )
            share = none_throws(
                await DALShares.get_by_id(db_session, share_channel.photobook_share_id),
                f"Invalid share: {share_channel.photobook_share_id}",
            )
            photobook = none_throws(
                await DALPhotobooks.get_by_id(db_session, share_channel.photobook_id),
                f"Invalid photobook: {share_channel.photobook_id}",
            )

            if share_channel.channel_type == ShareChannelType.EMAIL:
                await self._process_email_notif(
                    db_session, input_payload, share_channel, share, photobook
                )
            elif share_channel.channel_type == ShareChannelType.SMS:
                pass  # To implement
            elif share_channel.channel_type == ShareChannelType.APNS:
                pass  # To implement
            else:
                raise RuntimeError(
                    f"Unrecognized share channel type: {share_channel.channel_type}"
                )

            return DeliverNotificationOutputPayload(job_id=self.job_id)

    async def _process_email_notif(
        self,
        db_session: AsyncSession,
        input_payload: DeliverNotificationInputPayload,
        share_channel: DAOShareChannels,
        share: DAOShares,
        photobook: DAOPhotobooks,
    ) -> None:
        async with safe_commit(
            db_session, "notification delivery attempt start log", raise_on_fail=False
        ):
            await DALNotificationDeliveryAttempts.create(
                db_session,
                DAONotificationDeliveryAttemptsCreate(
                    share_channel_id=share_channel.id,
                    notification_type=input_payload.notification_type.value,
                    channel_type=share_channel.channel_type,
                    provider=self.worker_process_resources.email_provider_client.get_share_provider(),
                    event=NotificationDeliveryEvent.PROCESSING,
                ),
            )
            await DALShareChannels.update_by_id(
                db_session,
                share_channel.id,
                DAOShareChannelsUpdate(status=ShareChannelStatus.SENDING),
            )

        try:
            # TODO: move this inside provider logic
            send_result = await self.worker_process_resources.email_provider_client.send(
                EmailMessage(
                    subject=f"{photobook.title} from {share.sender_display_name or ''}",
                    from_=EmailAddress(
                        email="hello@snapgifts.app", name=share.sender_display_name
                    ),
                    to_=[
                        EmailAddress(
                            email=share_channel.destination,
                            name=share.recipient_display_name,
                        )
                    ],
                    html=f"<p>You received a gift from {share.sender_display_name}! Check it out at https://snapgifts.app/share/{share.share_slug}</p>",
                    idempotency_key=f"{input_payload.notification_type.value}_{share_channel.id}",
                )
            )
            async with safe_commit(
                db_session,
                "notification delivery attempt succeeded log",
                raise_on_fail=True,
            ):
                await DALShareChannels.update_by_id(
                    db_session,
                    share_channel.id,
                    DAOShareChannelsUpdate(
                        status=ShareChannelStatus.SENT,
                        last_provider_message_id=send_result.message_id,
                    ),
                )
                await DALNotificationDeliveryAttempts.create(
                    db_session,
                    DAONotificationDeliveryAttemptsCreate(
                        share_channel_id=share_channel.id,
                        notification_type=input_payload.notification_type.value,
                        channel_type=share_channel.channel_type,
                        provider=self.worker_process_resources.email_provider_client.get_share_provider(),
                        event=NotificationDeliveryEvent.SENT,
                        payload={"result": send_result.model_dump(mode="json")},
                    ),
                )

        except Exception as e:
            async with safe_commit(
                db_session,
                "notification delivery attempt failed log",
                raise_on_fail=False,
            ):
                await DALShareChannels.update_by_id(
                    db_session,
                    share_channel.id,
                    DAOShareChannelsUpdate(
                        status=ShareChannelStatus.FAILED,
                        last_error=str(e),
                    ),
                )
                await DALNotificationDeliveryAttempts.create(
                    db_session,
                    DAONotificationDeliveryAttemptsCreate(
                        share_channel_id=share_channel.id,
                        notification_type=input_payload.notification_type.value,
                        channel_type=share_channel.channel_type,
                        provider=self.worker_process_resources.email_provider_client.get_share_provider(),
                        event=NotificationDeliveryEvent.FAILED,
                        payload={"error": str(e)},
                    ),
                )
