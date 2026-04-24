-- migrate:up
ALTER TABLE public.pages
ADD COLUMN page_lightweight_title text;


-- migrate:down
ALTER TABLE public.pages
DROP COLUMN page_lightweight_title;
