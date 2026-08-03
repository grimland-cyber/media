-- Intel Media Pulse — persistence layer for Render's ephemeral filesystem.
-- Run this once in the Supabase SQL editor (Project → SQL Editor → New query).

create table if not exists kv_store (
  key text primary key,
  value jsonb not null,
  updated_at timestamptz not null default now()
);

-- Keep updated_at fresh on every upsert.
create or replace function kv_store_set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists kv_store_touch on kv_store;
create trigger kv_store_touch
  before update on kv_store
  for each row execute function kv_store_set_updated_at();
