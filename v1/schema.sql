-- jobs table (POC)
create table if not exists jobs (
  id              bigserial primary key,
  run_at          timestamptz not null,
  job_type        text not null,
  payload         jsonb not null default '{}'::jsonb,

  status          text not null default 'pending', -- pending|running|succeeded|failed

  locked_at       timestamptz,
  locked_by       text,

  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

create index if not exists idx_jobs_due
  on jobs (status, run_at);

-- optional: keep updated_at fresh
create or replace function set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_jobs_updated_at on jobs;
create trigger trg_jobs_updated_at
before update on jobs
for each row execute function set_updated_at();