-- this script should never be executed by an upgrade procedure (because
-- version 1 is set by 'swh db init')

insert into origin2(url, hash)
values ('this should never be executed', hash_sha1(''));
