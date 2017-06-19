---
--- logging data model
---

create table dbversion
(
  version     int primary key,
  release     timestamptz,
  description text
);

insert into dbversion(version, release, description)
      values(1, now(), 'Work In Progress');


create type log_level as enum ('debug', 'info', 'warning', 'error', 'critical');

create table log
(
  id          bigserial primary key,
  ts          timestamptz not null default now(),
  level       log_level not null default 'info',  -- importance
  message     text not null,  -- human readable message
  data        jsonb, -- extra data; when NOT NULL, must contain a key "type"
                     -- denoting the kind of message within src_module
  src_module  text,  -- fully-qualified source module, e.g., "swh.loader.git"
  src_host    text,  -- FQDN source hostname, e.g., "worker03.softwareheritage.org"
  src_pid     int    -- originating PID, relative to src_host
);

create index on log (ts);
create index on log (src_module);
create index on log (src_host);
