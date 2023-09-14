--

-- This file is added but not executed by the migration cli. It's present to reflect the
-- current status on some of our modules declaring some on-the-side sql migration script
-- that need to be deal with manually
insert into origin(url, hash)
values ('version005-bis', hash_sha1('version005-bis'));
