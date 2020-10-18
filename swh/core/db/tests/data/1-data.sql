-- insert some values in dbversion
insert into dbversion(version, release, description) values (1, '2016-02-22 15:56:28.358587+00', 'Work In Progress');
insert into dbversion(version, release, description) values (2, '2016-02-24 18:05:54.887217+00', 'Work In Progress');
insert into dbversion(version, release, description) values (3, '2016-10-21 14:10:18.629763+00', 'Work In Progress');
insert into dbversion(version, release, description) values (4, '2017-08-08 19:01:11.723113+00', 'Work In Progress');
insert into dbversion(version, release, description) values (7, '2018-03-30 12:58:39.256679+00', 'Work In Progress');

insert into fun(time) values ('2020-10-19 09:00:00.666999+00');
insert into fun(time) values ('2020-10-18 09:00:00.666999+00');
insert into fun(time) values ('2020-10-17 09:00:00.666999+00');

select nextval('serial');

insert into people(fullname) values ('dudess');
insert into people(fullname) values ('dude');
