from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import and_, delete, exists, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal.schemas import DAOSharesCreate, DAOSharesUpdate
from backend.db.data_models import (
    DAOShares,
    ShareAccessPolicy,
    ShareKind,
)
from backend.lib.utils.slug import uuid_to_base62

from . import AsyncPostgreSQLDAL


class DALShares(AsyncPostgreSQLDAL[DAOShares, DAOSharesCreate, DAOSharesUpdate]):
    model = DAOShares

    @classmethod
    async def get_or_create_recipient_for_user(
        cls,
        session: AsyncSession,
        *,
        photobook_id: UUID,
        created_by_user_id: UUID,
        sender_display_name: str,
        recipient_user_id: UUID,
        recipient_display_name: Optional[str],
        access_policy: ShareAccessPolicy,
        notes: Optional[str],
    ) -> DAOShares:
        """
        Idempotently ensure exactly one RECIPIENT share exists for (photobook_id, recipient_user_id).
        Uses INSERT ... ON CONFLICT DO NOTHING (matching partial unique index) + RETURNING.
        If not inserted, selects and returns the existing row.
        """
        # Prebuild values for the INSERT
        new_id = uuid4()
        slug = uuid_to_base62(new_id)

        t = cls.model
        photobook_id_col = getattr(t, "photobook_id")
        recipient_user_id_col = getattr(t, "recipient_user_id")
        kind_col = getattr(t, "kind")

        stmt = (
            pg_insert(t)
            .values(
                id=new_id,
                photobook_id=photobook_id,
                created_by_user_id=created_by_user_id,
                sender_display_name=sender_display_name,
                kind=ShareKind.RECIPIENT,
                recipient_display_name=recipient_display_name,
                recipient_user_id=recipient_user_id,
                share_slug=slug,
                access_policy=access_policy,
                notes=notes,
            )
            # match the partial unique index: (photobook_id, recipient_user_id) WHERE kind='recipient' AND recipient_user_id IS NOT NULL
            .on_conflict_do_nothing(
                index_elements=[photobook_id_col, recipient_user_id_col],
                index_where=and_(
                    kind_col == ShareKind.RECIPIENT, recipient_user_id_col.isnot(None)
                ),
            )
            .returning(t)
        )

        res = await session.execute(stmt)
        inserted = res.scalar_one_or_none()
        if inserted is not None:
            # attached to identity map for you
            return inserted

        # Not inserted → already exists. Select and return it.
        sel = (
            select(t)
            .where(
                photobook_id_col == photobook_id,
                recipient_user_id_col == recipient_user_id,
                kind_col == ShareKind.RECIPIENT,
            )
            .limit(1)
        )
        res2 = await session.execute(sel)
        existing = res2.scalar_one_or_none()
        if existing is None:
            # Extremely unlikely (index guarantees existence), but be defensive.
            raise RuntimeError(
                "Unique conflict implied an existing recipient share, but none was found."
            )
        return existing

    @classmethod
    async def prune_anonymous_empty_recipient_shares(
        cls,
        session: AsyncSession,
        photobook_id: Optional[UUID] = None,
    ) -> list[UUID]:
        """
        Delete RECIPIENT shares that have:
          - recipient_user_id IS NULL (anonymous)
          - zero rows in share_channels
        Scope by one (or both) of:
          - photobook_id
          - share_ids (specific candidates)
        If neither scope is provided, this is a no-op (defensive).

        Returns the list of pruned share IDs.
        """
        s = cls.model  # DAOShares table
        from backend.db.data_models import (
            DAOShareChannels,  # local import to avoid cycles
        )

        stmt = (
            delete(s)
            .where(
                and_(
                    getattr(s, "kind") == ShareKind.RECIPIENT,
                    getattr(s, "recipient_user_id").is_(None),
                    ~exists(
                        select(1).where(
                            getattr(DAOShareChannels, "photobook_share_id")
                            == getattr(s, "id")
                        )
                    ),
                    getattr(s, "photobook_id") == photobook_id,
                )
            )
            .returning(getattr(s, "id"))
        )
        res = await session.execute(stmt)
        return [row[0] for row in res.fetchall()]
