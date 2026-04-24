# backend/route_handler/share.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from fastapi import Request
from pydantic import BaseModel

from backend.db.dal import (
    DALShareChannels,
    DALShares,
    FilterOp,
    safe_transaction,
)
from backend.db.dal.schemas import (
    DAOSharesCreate,
    DAOSharesUpdate,
)
from backend.db.data_models import (
    DAOShareChannels,
    DAOShares,
    ShareAccessPolicy,
    ShareChannelStatus,
    ShareChannelType,
    ShareKind,
)
from backend.db.externals import (
    ShareChannelsOverviewResponse,
    SharesOverviewResponse,
)
from backend.lib.notifs.types import NotificationType
from backend.lib.utils.common import utcnow
from backend.lib.utils.share_destination_normalizer import normalize_destination
from backend.lib.utils.slug import uuid_to_base62
from backend.route_handler.base import RouteHandler, enforce_response_model
from backend.worker.job_processor.types import (
    DeliverNotificationInputPayload,
    JobType,
)

# --------------------------------------------------------------------
# Request / Response models
# --------------------------------------------------------------------


class ShareChannelIn(BaseModel):
    channel_type: ShareChannelType
    destination: str


class ShareIn(BaseModel):
    recipient_display_name: Optional[str] = None
    recipient_user_id: Optional[UUID] = None
    access_policy: ShareAccessPolicy = ShareAccessPolicy.ANYONE_WITH_LINK
    notes: Optional[str] = None
    channels: list[ShareChannelIn] = []


class ShareCreateRequest(BaseModel):
    shares: list[ShareIn]
    sender_display_name: str
    schedule_send_at: Optional[datetime] = None  # if None or past => send now


class ShareWithChannelsResponse(BaseModel):
    share: SharesOverviewResponse
    channels: list[ShareChannelsOverviewResponse]


class ShareCreateResponse(BaseModel):
    shares: list[ShareWithChannelsResponse]
    schedule_send_at: Optional[datetime]
    send_immediately: bool


class PhotobookRequireAuthResponse(BaseModel):
    photobook_id: UUID
    public_share: SharesOverviewResponse | None
    updated_to_require_auth: list[UUID]
    revoked: list[UUID]


class SharePublicEnsureRequest(BaseModel):
    """
    Ensure a public share exists; optionally update access policy or notes.
    Only allowed access policies for public are: LINK_OPEN, REVOKED.
    """

    access_policy: Optional[ShareAccessPolicy] = (
        None  # must be LINK_OPEN or REVOKED if provided
    )
    notes: Optional[str] = None


# You can return just the share overview (simplest):
SharePublicEnsureResponse = SharesOverviewResponse

# --------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------


def _is_send_now(ts: Optional[datetime]) -> bool:
    if ts is None:
        return True
    # Treat naive timestamps as UTC for safety; adjust if you prefer client TZ enforcement
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts <= utcnow()


# --------------------------------------------------------------------
# Handler
# --------------------------------------------------------------------


class ShareAPIHandler(RouteHandler):
    def register_routes(self) -> None:
        self.route(
            "/api/share/{photobook_id}/initialize-share",
            "share_photobook_initialize",
            methods=["POST"],
        )

        self.route(
            "/api/share/{photobook_id}/public",
            "share_photobook_request_public_link",
            methods=["POST"],
        )

        self.route(
            "/api/share/{photobook_id}/require-auth",
            "share_photobook_require_auth",
            methods=["POST"],
        )

    @enforce_response_model
    async def share_photobook_initialize(
        self,
        photobook_id: UUID,
        payload: ShareCreateRequest,
        request: Request,
    ) -> ShareCreateResponse:
        """
        Create recipient shares + channels under a per-photobook advisory xact lock.
        - Channels use ON CONFLICT DO NOTHING against (photobook_id, channel_type, destination).
        - If a requested destination already exists under a different share, we simply
          return that existing channel (no cleanup of the newly created share).
        - schedule_send_at applies to all channels: None/past => PENDING (send now), future => SCHEDULED.
        """
        async with self.app.new_db_session() as db_session:
            ctx = await self.get_request_context(request)

            async with safe_transaction(db_session, context="share create"):
                # Ownership check
                await self.get_photobook_assert_owned_by(
                    db_session, photobook_id, ctx.user_id
                )

                schedule_ts = payload.schedule_send_at
                send_now = _is_send_now(schedule_ts)
                default_status = (
                    ShareChannelStatus.PENDING
                    if send_now
                    else ShareChannelStatus.SCHEDULED
                )
                default_sched = None if send_now else schedule_ts
                created: list[tuple[DAOShares, list[DAOShareChannels]]] = []

                # 2) create shares + upsert channels
                for item in payload.shares:
                    # 2a) create a recipient share (simple, no extra logic)
                    if item.recipient_user_id is not None:
                        share_row = await DALShares.get_or_create_recipient_for_user(
                            db_session,
                            photobook_id=photobook_id,
                            created_by_user_id=ctx.user_id,
                            sender_display_name=payload.sender_display_name,
                            recipient_user_id=item.recipient_user_id,
                            recipient_display_name=item.recipient_display_name,
                            access_policy=item.access_policy,
                            notes=item.notes,
                        )
                    else:
                        # no stable user id → allow multiple recipient shares; keep your normal create()
                        share_id = uuid4()
                        slug = uuid_to_base62(share_id)
                        share_row = await DALShares.create(
                            db_session,
                            DAOSharesCreate(
                                id=share_id,
                                photobook_id=photobook_id,
                                kind=ShareKind.RECIPIENT,
                                created_by_user_id=ctx.user_id,
                                sender_display_name=payload.sender_display_name,
                                recipient_display_name=item.recipient_display_name,
                                recipient_user_id=None,
                                share_slug=slug,
                                access_policy=item.access_policy,
                                notes=item.notes,
                            ),
                        )

                    # 2b) bulk upsert channels for this share
                    requested_pairs: list[tuple[ShareChannelType, str]] = [
                        (c.channel_type, c.destination) for c in item.channels
                    ]

                    channels = await DALShareChannels.upsert_bulk_for_share_return_all(
                        db_session,
                        photobook_share_id=share_row.id,
                        photobook_id=photobook_id,
                        channels=requested_pairs,
                        default_status=default_status,
                        default_scheduled_for=default_sched,
                        normalize_destination=normalize_destination,
                    )

                    created.append((share_row, channels))

                await DALShares.prune_anonymous_empty_recipient_shares(
                    session=db_session, photobook_id=photobook_id
                )

            # 3) (Optional) enqueue jobs when send_now == True (left to your worker pipeline)
            # TODO: turn into parallelized
            if send_now:
                for _, share_channels in created:
                    for ch in share_channels:
                        await self.app.remote_job_manager_io_bound.enqueue(
                            JobType.REMOTE_DELIVER_NOTIFICATION,
                            job_payload=DeliverNotificationInputPayload(
                                user_id=ctx.user_id,
                                originating_photobook_id=photobook_id,
                                notification_type=NotificationType.PHOTOBOOK_SHARING_WITH_YOU_INITIAL,
                                share_channel_id=ch.id,
                            ),
                            max_retries=1,
                            db_session=db_session,
                        )

            # 4) Build response
            resp: list[ShareWithChannelsResponse] = []
            for s, chs in created:
                resp.append(
                    ShareWithChannelsResponse(
                        share=SharesOverviewResponse.from_dao(s),
                        channels=[
                            ShareChannelsOverviewResponse.from_dao(c) for c in chs
                        ],
                    )
                )

            return ShareCreateResponse(
                shares=resp,
                schedule_send_at=schedule_ts,
                send_immediately=send_now,
            )

    @enforce_response_model
    async def share_photobook_request_public_link(
        self,
        photobook_id: UUID,
        request: Request,
    ) -> SharesOverviewResponse:
        """
        Ensure there is exactly one public share (kind=PUBLIC) for the photobook.
        - Creates it if missing with access_policy=LINK_OPEN
        - If it exists but is not LINK_OPEN (e.g., REVOKED), flip it back to LINK_OPEN
        - Returns the public share overview
        """
        async with self.app.new_db_session() as db_session:
            ctx = await self.get_request_context(request)

            # 2) Ensure or create inside a single transaction
            async with safe_transaction(
                db_session, context="ensure public share", raise_on_fail=True
            ):
                await self.get_photobook_assert_owned_by(
                    db_session, photobook_id, ctx.user_id
                )

                existing = await DALShares.list_all(
                    db_session,
                    filters={
                        "photobook_id": (FilterOp.EQ, photobook_id),
                        "kind": (FilterOp.EQ, ShareKind.PUBLIC),
                    },
                    limit=1,
                )

                if not existing:
                    # Create new public share with LINK_OPEN
                    share_id = uuid4()
                    slug = uuid_to_base62(share_id)
                    row = await DALShares.create(
                        db_session,
                        DAOSharesCreate(
                            id=share_id,
                            photobook_id=photobook_id,
                            created_by_user_id=ctx.user_id,
                            kind=ShareKind.PUBLIC,
                            recipient_display_name=None,
                            recipient_user_id=None,
                            share_slug=slug,
                            access_policy=ShareAccessPolicy.ANYONE_WITH_LINK,
                            notes=None,
                        ),
                    )
                else:
                    row = existing[0]
                    # Force LINK_OPEN if it was toggled (e.g., REVOKED)
                    if row.access_policy != ShareAccessPolicy.ANYONE_WITH_LINK:
                        row = await DALShares.update_by_id(
                            db_session,
                            row.id,
                            DAOSharesUpdate(
                                access_policy=ShareAccessPolicy.ANYONE_WITH_LINK
                            ),
                        )

            return SharesOverviewResponse.from_dao(row)

    @enforce_response_model
    async def share_photobook_require_auth(
        self,
        photobook_id: UUID,
        request: Request,
    ) -> PhotobookRequireAuthResponse:
        """
        Switch all sharing for this photobook to RECIPIENT_MUST_AUTH:
          - Public share (kind=PUBLIC) -> REVOKED (kept, not deleted)
          - Recipient share with recipient_user_id -> RECIPIENT_MUST_AUTH
          - Recipient share with NO recipient_user_id -> REVOKED (cannot enforce auth)
        Returns a summary and the (revoked) public share if present.
        """
        async with self.app.new_db_session() as db_session:
            ctx = await self.get_request_context(request)

            async with safe_transaction(
                db_session, context="photobook require-auth flip"
            ):
                # 1) Ownership check
                await self.get_photobook_assert_owned_by(
                    db_session, photobook_id, ctx.user_id
                )

                updated_to_auth_ids: list[UUID] = []
                revoked_ids: list[UUID] = []
                public_share_id: UUID | None = None

                # 2) Single transaction: gather + update
                # Fetch all shares for this photobook
                shares = await DALShares.list_all(
                    db_session,
                    filters={
                        "photobook_id": (FilterOp.EQ, photobook_id),
                    },
                )

                # Partition
                recipient_all: list[DAOShares] = []
                public_share: DAOShares | None = None

                for s in shares:
                    if s.kind == ShareKind.PUBLIC:
                        public_share = s
                    elif s.kind == ShareKind.RECIPIENT:
                        recipient_all.append(s)

                updates: dict[UUID, DAOSharesUpdate] = {}

                # a) all recipient shares -> RECIPIENT_MUST_AUTH
                for s in recipient_all:
                    if s.access_policy != ShareAccessPolicy.RECIPIENT_MUST_AUTH:
                        updates[s.id] = DAOSharesUpdate(
                            access_policy=ShareAccessPolicy.RECIPIENT_MUST_AUTH
                        )
                        updated_to_auth_ids.append(s.id)

                # b) public share -> REVOKED
                if (
                    public_share is not None
                    and public_share.access_policy != ShareAccessPolicy.REVOKED
                ):
                    updates[public_share.id] = DAOSharesUpdate(
                        access_policy=ShareAccessPolicy.REVOKED
                    )
                    revoked_ids.append(public_share.id)
                    public_share_id = public_share.id
                elif public_share is not None:
                    public_share_id = public_share.id

                # Execute
                if updates:
                    await DALShares.update_many_by_ids(db_session, updates)

            # 3) Refetch the (now revoked) public share if any
            public_share_resp: SharesOverviewResponse | None = None
            if public_share_id is not None:
                refreshed = await DALShares.get_by_id(db_session, public_share_id)
                if refreshed is not None:
                    public_share_resp = SharesOverviewResponse.from_dao(refreshed)

            return PhotobookRequireAuthResponse(
                photobook_id=photobook_id,
                public_share=public_share_resp,
                updated_to_require_auth=updated_to_auth_ids,
                revoked=revoked_ids,
            )
