-- origin2 table
create table if not exists origin2 (
  id       bigserial not null,
  url      text not null,
  hash     text not null
);
