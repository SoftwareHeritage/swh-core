--

insert into origin(url, hash)
values ('version006', hash_sha1('version006'));

insert into dbversion(version, release, description)
values (6, 'NOW()', 'Updated version from upgrade script');
