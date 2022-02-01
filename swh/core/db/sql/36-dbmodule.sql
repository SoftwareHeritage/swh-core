-- common metadata/context structures
--
-- we use a 3x- prefix for this to make it executed after db schema initialisation
-- sql scripts, which are normally 30- prefixed, so that it remains compatible
-- with packages that have not yet migrated to swh.core 1.2

-- swh module this db is storing data for
create table if not exists dbmodule (
  dbmodule    text,
  single_row  char(1) primary key default 'x',
  check       (single_row = 'x')
);
comment on table dbmodule is 'Database module storage';
comment on column dbmodule.dbmodule is 'Database (swh) module currently deployed';
comment on column dbmodule.single_row is 'Bogus column to force the table to have a single row';
