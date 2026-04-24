-- migrate:up

-- === Enums (sharing v2) ===
CREATE TYPE public.share_access_policy AS ENUM ('anyone_with_link', 'recipient_must_auth', 'revoked');
CREATE TYPE public.share_kind          AS ENUM ('public', 'recipient');
CREATE TYPE public.share_channel_type  AS ENUM ('email', 'sms', 'apns');
CREATE TYPE public.share_provider      AS ENUM ('resend', 'twilio', 'apns');
CREATE TYPE public.share_channel_status AS ENUM ('pending', 'scheduled', 'sending', 'sent', 'failed');
CREATE TYPE public.notification_delivery_event AS ENUM ('processing', 'sent', 'failed');

COMMENT ON TYPE public.share_access_policy   IS 'Access model for a share link.';
COMMENT ON TYPE public.share_kind            IS 'Share audience: public link (one per photobook) vs per-recipient share.';
COMMENT ON TYPE public.share_channel_type    IS 'Notification channel type.';
COMMENT ON TYPE public.share_channel_status  IS 'Per-channel delivery status (provider webhooks are source of truth).';
COMMENT ON TYPE public.share_provider        IS 'External provider for notifications.';
COMMENT ON TYPE public.notification_delivery_event  IS 'Append-only timeline event from send/webhooks.';

-- === Shares (v2) ===
CREATE TABLE public.shares (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  photobook_id        uuid NOT NULL REFERENCES public.photobooks(id) ON DELETE CASCADE,
  created_by_user_id  uuid REFERENCES public.users(id) ON DELETE SET NULL,

  kind                public.share_kind NOT NULL DEFAULT 'recipient',

  -- Recipient inlined. For public links, both NULL.
  sender_display_name    text,
  recipient_display_name text,
  recipient_user_id      uuid REFERENCES public.users(id) ON DELETE SET NULL,

  -- Link identity & control
  share_slug           text NOT NULL UNIQUE,
  access_policy        public.share_access_policy NOT NULL DEFAULT 'anyone_with_link',
  notes                text,

  -- Cached/derived aggregate status
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),

  -- Invariants by kind:
  CONSTRAINT chk_shares_kind_fields
    CHECK (
      (kind = 'public'    AND recipient_user_id IS NULL AND recipient_display_name IS NULL)
      OR
      (kind = 'recipient' AND (recipient_user_id IS NOT NULL OR recipient_display_name IS NOT NULL))
    ),

  -- Access policy constraints:
  -- - public shares: only 'anyone_with_link' or 'revoked'
  -- - recipient shares: any policy (including 'recipient_must_auth' even without recipient_user_id; OTP allowed)
  CONSTRAINT chk_shares_access_policy_by_kind
    CHECK (
      (kind = 'public'   AND access_policy IN ('anyone_with_link', 'revoked'))
      OR
      (kind = 'recipient')
    )
);

COMMENT ON TABLE public.shares IS 'Per-recipient (or public) share for a photobook. Public: one per photobook, no channels.';

-- Helpful indexes
CREATE INDEX idx_shares_photobook_id    ON public.shares (photobook_id);
CREATE INDEX idx_shares_kind            ON public.shares (kind);
CREATE INDEX idx_shares_access_policy   ON public.shares (access_policy);

-- Enforce at most one public share per photobook
CREATE UNIQUE INDEX uq_single_public_share_per_photobook
  ON public.shares (photobook_id)
  WHERE kind = 'public';

-- === Per-avenue channels (email/sms/apns) ===
-- NOTE: public shares have NO channels.
CREATE TABLE public.share_channels (
  id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  photobook_share_id      uuid NOT NULL REFERENCES public.shares(id) ON DELETE CASCADE,

  -- Denormalized for cross-share uniqueness
  photobook_id            uuid NOT NULL REFERENCES public.photobooks(id) ON DELETE CASCADE,

  channel_type            public.share_channel_type NOT NULL,
  destination             text NOT NULL,              -- normalized: email lowercased; phone E.164; APNs token as-is
  status                  public.share_channel_status NOT NULL DEFAULT 'pending',
  last_error              text,
  last_provider_message_id text,
  scheduled_for           timestamptz,

  created_at              timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now(),

  -- Per-share uniqueness (kept)
  CONSTRAINT uq_share_channels_unique_destination
    UNIQUE (photobook_share_id, channel_type, destination)
);

COMMENT ON TABLE public.share_channels IS 'One row per delivery avenue under a RECIPIENT share. Public shares have no channels.';

CREATE INDEX idx_share_channels_share_id        ON public.share_channels (photobook_share_id);
CREATE INDEX idx_share_channels_photobook_id    ON public.share_channels (photobook_id);
CREATE INDEX idx_share_channels_status          ON public.share_channels (status);
CREATE INDEX idx_share_channels_scheduled_for   ON public.share_channels (scheduled_for);

-- Cross-share, photobook-scoped uniqueness to prevent dup deliveries across separate share objects
CREATE UNIQUE INDEX uq_share_channel_dest_per_photobook
  ON public.share_channels (photobook_id, channel_type, destination);

CREATE UNIQUE INDEX IF NOT EXISTS uq_recipient_share_by_user
  ON public.shares (photobook_id, recipient_user_id)
  WHERE kind = 'recipient' AND recipient_user_id IS NOT NULL;


-- === Immutable delivery/log timeline (append-only) ===
CREATE TABLE public.notification_delivery_attempts (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  share_channel_id  uuid NOT NULL REFERENCES public.share_channels(id) ON DELETE CASCADE,
  notification_type text,
  channel_type      public.share_channel_type NOT NULL,
  provider          public.share_provider,
  event             public.notification_delivery_event NOT NULL,
  payload           jsonb,
  created_at        timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.notification_delivery_attempts IS 'Append-only audit of send/webhook events for a share channel.';

CREATE INDEX idx_notification_delivery_attempts_channel_created
  ON public.notification_delivery_attempts (share_channel_id, created_at);



-- migrate:down

-- Drop in reverse order of dependencies
DROP INDEX IF EXISTS idx_notification_delivery_attempts_channel_created;
DROP TABLE IF EXISTS public.notification_delivery_attempts;

DROP INDEX IF EXISTS uq_share_channel_dest_per_photobook;
DROP INDEX IF EXISTS idx_share_channels_scheduled_for;
DROP INDEX IF EXISTS idx_share_channels_status;
DROP INDEX IF EXISTS idx_share_channels_photobook_id;
DROP INDEX IF EXISTS idx_share_channels_share_id;
DROP TABLE IF EXISTS public.share_channels;

DROP INDEX IF EXISTS uq_single_public_share_per_photobook;
DROP INDEX IF EXISTS idx_shares_access_policy;
DROP INDEX IF EXISTS idx_shares_kind;
DROP INDEX IF EXISTS idx_shares_photobook_id;
DROP TABLE IF EXISTS public.shares;

DROP TYPE IF EXISTS public.notification_delivery_event;
DROP TYPE IF EXISTS public.share_channel_status;
DROP TYPE IF EXISTS public.share_provider;
DROP TYPE IF EXISTS public.share_channel_type;
DROP TYPE IF EXISTS public.share_kind;
DROP TYPE IF EXISTS public.share_access_policy;
