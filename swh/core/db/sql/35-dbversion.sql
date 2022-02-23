-- common metadata/context structures
--
-- we use a 35- prefix for this to make it executed after db schema initialisation
-- sql scripts, which are normally 30- prefixed, so that it remains compatible
-- with packages that have not yet migrated to swh.core 1.2

-- schema versions
create table if not exists dbversion
(
  version     int primary key,
  release     timestamptz,
  description text
);

comment on table dbversion is 'Details of current db version';
comment on column dbversion.version is 'SQL schema version';
comment on column dbversion.release is 'Version deployment timestamp';
comment on column dbversion.description is 'Release description';
