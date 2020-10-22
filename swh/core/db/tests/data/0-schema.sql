-- schema version table which won't get truncated
create table dbversion (
  version     int primary key,
  release     timestamptz,
  description text
);

-- a people table which won't get truncated
create table people (
  fullname text not null
);

-- a fun table which will get truncated for each test
create table fun (
    time timestamptz not null
);

-- one sequence to check for reset as well
create sequence serial;
