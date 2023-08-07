-- origin table
create table if not exists origin (
  id       bigserial not null,
  url      text not null,
  hash     text not null
);
