-- migrate:up
-- Move from owners/owner_identities → users (with guest + orphan purges).
-- Dry run safe: final DO block raises to rollback everything.

-- 1) Add user_id columns (nullable for backfill)
alter table public.photobooks add column if not exists user_id uuid;
alter table public.assets     add column if not exists user_id uuid;

-- 2) Backfill from owner_identities(kind = 'user') → user_id
with mapping as (
  select oi.owner_id, oi.identity as user_id
  from public.owner_identities oi
  where oi.kind = 'user'
)
update public.photobooks p
set user_id = m.user_id
from mapping m
where p.owner_id = m.owner_id and p.user_id is distinct from m.user_id;

with mapping as (
  select oi.owner_id, oi.identity as user_id
  from public.owner_identities oi
  where oi.kind = 'user'
)
update public.assets a
set user_id = m.user_id
from mapping m
where a.owner_id = m.owner_id and a.user_id is distinct from m.user_id;

-- 3) Purge GUEST-owned rows (materialize sets; delete dependents in correct order)
create temporary table tmp_guest_assets on commit drop as
  select a.id
  from public.assets a
  where a.user_id is null
    and not exists (
      select 1 from public.owner_identities oi
      where oi.owner_id = a.owner_id and oi.kind = 'user'
    );

create temporary table tmp_guest_photobooks on commit drop as
  select p.id
  from public.photobooks p
  where p.user_id is null
    and not exists (
      select 1 from public.owner_identities oi
      where oi.owner_id = p.owner_id and oi.kind = 'user'
    );

-- 3a) pages_assets_rel edges → guest assets
delete from public.pages_assets_rel par
using tmp_guest_assets ga
where par.asset_id = ga.id;

-- 3b) jobs → guest photobooks
delete from public.jobs j
using tmp_guest_photobooks gp
where j.photobook_id = gp.id;

-- 3c) pages → guest photobooks (pages_assets_rel.page_id cascades)
delete from public.pages pg
using tmp_guest_photobooks gp
where pg.photobook_id = gp.id;

-- 3d) guest assets
delete from public.assets a
using tmp_guest_assets ga
where a.id = ga.id;

-- 3e) guest photobooks
delete from public.photobooks p
using tmp_guest_photobooks gp
where p.id = gp.id;

-- 4) Purge ORPHANS: rows whose user_id is set but user record doesn’t exist
create temporary table tmp_orphan_photobooks on commit drop as
  select p.id
  from public.photobooks p
  left join public.users u on u.id = p.user_id
  where p.user_id is not null and u.id is null;

create temporary table tmp_orphan_assets on commit drop as
  select a.id
  from public.assets a
  left join public.users u on u.id = a.user_id
  where a.user_id is not null and u.id is null;

-- 4a) pages_assets_rel edges → orphan assets
delete from public.pages_assets_rel par
using tmp_orphan_assets oa
where par.asset_id = oa.id;

-- 4b) jobs → orphan photobooks
delete from public.jobs j
using tmp_orphan_photobooks op
where j.photobook_id = op.id;

-- 4c) pages → orphan photobooks
delete from public.pages pg
using tmp_orphan_photobooks op
where pg.photobook_id = op.id;

-- 4d) orphan assets
delete from public.assets a
using tmp_orphan_assets oa
where a.id = oa.id;

-- 4e) orphan photobooks
delete from public.photobooks p
using tmp_orphan_photobooks op
where p.id = op.id;

-- 5) Guard: ensure no NULLs remain, and all user_ids exist
do $$
declare
  c1 int; c2 int; c3 int; c4 int;
begin
  select count(*) into c1 from public.photobooks where user_id is null;
  select count(*) into c2 from public.assets     where user_id is null;
  select count(*) into c3
    from public.photobooks p left join public.users u on u.id = p.user_id
    where p.user_id is not null and u.id is null;
  select count(*) into c4
    from public.assets a left join public.users u on u.id = a.user_id
    where a.user_id is not null and u.id is null;

  if c1 > 0 or c2 > 0 or c3 > 0 or c4 > 0 then
    raise exception 'Abort: invalid user links. nulls(p=%,a=%) or orphans(p=%,a=%).', c1, c2, c3, c4;
  end if;
end $$;

-- 6) Enforce NOT NULL + add FKs to public.users(id)
alter table public.photobooks alter column user_id set not null;
alter table public.assets     alter column user_id set not null;

alter table public.photobooks
  add constraint photobooks_user_fk
  foreign key (user_id) references public.users(id) on delete cascade;

alter table public.assets
  add constraint assets_user_fk
  foreign key (user_id) references public.users(id) on delete cascade;

-- 7) Indexes on new columns
create index if not exists idx_photobooks_user_id on public.photobooks (user_id);
create index if not exists idx_assets_user_id     on public.assets     (user_id);

-- 8) Drop old FKs/indexes/columns referencing owners
do $$
begin
  if exists (select 1 from pg_constraint where conname = 'photobooks_owner_fk') then
    alter table public.photobooks drop constraint photobooks_owner_fk;
  end if;
  if exists (select 1 from pg_constraint where conname = 'assets_owner_fk') then
    alter table public.assets drop constraint assets_owner_fk;
  end if;
  if exists (select 1 from pg_constraint where conname = 'jobs_owner_fk') then
    alter table public.jobs drop constraint jobs_owner_fk;
  end if;
exception when others then null;
end $$;

drop index if exists public.idx_photobooks_owner_id;
drop index if exists public.idx_assets_owner_id;
drop index if exists public.idx_jobs_owner_id;

alter table public.photobooks drop column if exists owner_id;
alter table public.assets     drop column if exists owner_id;
alter table public.jobs       drop column if exists owner_id;

-- 9) Drop the owner tables (and their constraints/indexes)
drop table if exists public.owner_identities;
drop table if exists public.owners;


-- migrate:down
-- Recreate owners/owner_identities and owner_id columns, rebuild mapping from user_id.
-- Dry run safe: final DO block raises to rollback everything.

-- 1) Recreate owners / owner_identities
create table if not exists public.owners (
  id uuid default gen_random_uuid() primary key,
  created_at timestamptz not null default now()
);

create type if not exists public.identity_kind as enum ('guest','user');

create table if not exists public.owner_identities (
  id uuid default gen_random_uuid() primary key,
  owner_id uuid not null references public.owners(id) on delete cascade,
  kind public.identity_kind not null,
  identity uuid not null,
  created_at timestamptz not null default now(),
  unique (kind, identity),
  unique (owner_id, kind)
);

create index if not exists owner_identities_owner_idx on public.owner_identities(owner_id);
create index if not exists owner_identities_identity_lookup on public.owner_identities(kind, identity);

-- 2) Re-add owner_id columns
alter table public.photobooks add column if not exists owner_id uuid;
alter table public.assets     add column if not exists owner_id uuid;
alter table public.jobs       add column if not exists owner_id uuid;

-- 3) Deterministic 1-1 map: each user_id gets an owner_id
create temporary table tmp_user_owner_map on commit drop as
select u.id as user_id, gen_random_uuid() as owner_id
from public.users u;

insert into public.owners (id)
select owner_id from tmp_user_owner_map;

insert into public.owner_identities (owner_id, kind, identity)
select owner_id, 'user'::public.identity_kind, user_id
from tmp_user_owner_map;

-- 4) Backfill owner_id from the map
update public.photobooks p
set owner_id = m.owner_id
from tmp_user_owner_map m
where p.user_id = m.user_id and p.owner_id is distinct from m.owner_id;

update public.assets a
set owner_id = m.owner_id
from tmp_user_owner_map m
where a.user_id = m.user_id and a.owner_id is distinct from m.owner_id;

update public.jobs j
set owner_id = m.owner_id
from tmp_user_owner_map m
where j.user_id = m.user_id and j.owner_id is distinct from m.owner_id;

-- 5) Restore owner FKs / indexes
alter table public.photobooks
  add constraint photobooks_owner_fk
  foreign key (owner_id) references public.owners(id);

alter table public.assets
  add constraint assets_owner_fk
  foreign key (owner_id) references public.owners(id);

alter table public.jobs
  add constraint jobs_owner_fk
  foreign key (owner_id) references public.owners(id);

create index if not exists idx_photobooks_owner_id on public.photobooks(owner_id);
create index if not exists idx_assets_owner_id     on public.assets(owner_id);
create index if not exists idx_jobs_owner_id       on public.jobs(owner_id);

-- 6) Relax user FKs / NOT NULL (keep user_id columns but make nullable, drop FKs)
do $$
begin
  if exists (select 1 from pg_constraint where conname = 'photobooks_user_fk') then
    alter table public.photobooks drop constraint photobooks_user_fk;
  end if;
  if exists (select 1 from pg_constraint where conname = 'assets_user_fk') then
    alter table public.assets drop constraint assets_user_fk;
  end if;
exception when others then null;
end $$;

alter table public.photobooks alter column user_id drop not null;
alter table public.assets     alter column user_id drop not null;

drop index if exists public.idx_photobooks_user_id;
drop index if exists public.idx_assets_user_id;
