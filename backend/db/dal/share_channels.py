from datetime import datetime, timezone
from typing import Any, Callable, Optional, Sequence
from uuid import UUID

from sqlalchemy import select, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.data_models import (
    DAOShareChannels,
    ShareChannelStatus,
    ShareChannelType,
)

from . import AsyncPostgreSQLDAL, DAOShareChannelsCreate, DAOShareChannelsUpdate


class DALShareChannels(
    AsyncPostgreSQLDAL[DAOShareChannels, DAOShareChannelsCreate, DAOShareChannelsUpdate]
):
    model = DAOShareChannels

    @classmethod
    async def upsert_bulk_for_share_return_all(
        cls,
        session: AsyncSession,
        *,
        photobook_share_id: UUID,
        photobook_id: UUID,
        channels: Sequence[tuple[ShareChannelType, str]],  # [(type, destination)]
        default_status: ShareChannelStatus,
        default_scheduled_for: Optional[datetime],
        normalize_destination: Optional[Callable[[ShareChannelType, str], str]] = None,
    ) -> list[DAOShareChannels]:
        """
        Bulk UPSERT (DO NOTHING) share_channels for a given share, returning ALL rows
        (both newly inserted and pre-existing that conflicted).

        - Destination is normalized if a normalizer is provided.
        - Existing rows are *not* modified (status/scheduled_for remain as-is).
        """
        if not channels:
            return []

        # Normalize scheduled_for to UTC (if provided)
        if default_scheduled_for is None:
            sched_for_utc: Optional[datetime] = None
        else:
            sched_for_utc = (
                default_scheduled_for
                if default_scheduled_for.tzinfo
                else default_scheduled_for.replace(tzinfo=timezone.utc)
            )

        # Normalize & dedup requested pairs
        seen: set[tuple[ShareChannelType, str]] = set()
        normalized_pairs: list[tuple[ShareChannelType, str]] = []
        values: list[dict[str, Any]] = []

        for ch_type, dest in channels:
            # Defensive normalization (never throw)
            try:
                dest_norm = (
                    normalize_destination(ch_type, dest)
                    if normalize_destination
                    else (dest or "").strip()
                )
            except Exception:
                dest_norm = (dest or "").strip()

            key = (ch_type, dest_norm)
            if key in seen:
                continue
            seen.add(key)
            normalized_pairs.append(key)

            values.append(
                dict(
                    photobook_share_id=photobook_share_id,
                    photobook_id=photobook_id,
                    channel_type=ch_type,
                    destination=dest_norm,
                    status=default_status,
                    scheduled_for=sched_for_utc,
                )
            )

        if not values:
            return []

        # --- keep your original insert pattern ---
        photobook_id_col = getattr(cls.model, "photobook_id")
        channel_type_col = getattr(cls.model, "channel_type")
        destination_col = getattr(cls.model, "destination")

        stmt = (
            pg_insert(cls.model)
            .values(values)
            .on_conflict_do_nothing(
                index_elements=[photobook_id_col, channel_type_col, destination_col]
            )
            .returning(cls.model)
        )
        res = await session.execute(stmt)
        inserted: list[DAOShareChannels] = list(res.scalars().all())

        # (Optional) attach to session for identity-map consistency
        for obj in inserted:
            session.add(obj)

        # If everything inserted, we're done
        if len(inserted) == len(values):
            return inserted

        # Otherwise, re-select all requested pairs so we return a complete list
        photobook_id_col = getattr(cls.model, "photobook_id")
        sel = (
            select(DAOShareChannels)
            .where(photobook_id_col == photobook_id)
            .where(tuple_(channel_type_col, destination_col).in_(normalized_pairs))
        )
        res2 = await session.execute(sel)
        selected: list[DAOShareChannels] = list(res2.scalars().all())

        # Deduplicate in case RETURNING rows overlap with SELECT rows
        seen_ids: set[UUID] = set()
        out: list[DAOShareChannels] = []
        for row in inserted + selected:
            if row.id in seen_ids:
                continue
            seen_ids.add(row.id)
            out.append(row)
        return out
