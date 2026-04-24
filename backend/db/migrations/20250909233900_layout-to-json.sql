-- migrate:up

CREATE TYPE public.page_layout AS ENUM (
  'default',
  'polaroid',
  'masonry',
  'surrounding',
  'two_diagonal'
);

ALTER TABLE public.pages
    ALTER COLUMN layout DROP DEFAULT,
    ALTER COLUMN layout TYPE public.page_layout
    USING layout::public.page_layout;

-- migrate:down
-- Revert layout back to text
ALTER TABLE public.pages
    ALTER COLUMN layout TYPE text
    USING layout::text;

-- Drop enum type if desired
DROP TYPE IF EXISTS public.page_layout;