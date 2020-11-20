-- schema version table which won't get truncated
create table if not exists dbversion (
  version     int primary key,
  release     timestamptz,
  description text
);

-- origin table
create table if not exists origin (
  id       bigserial not null,
  url      text not null,
  hash     text not null
);
