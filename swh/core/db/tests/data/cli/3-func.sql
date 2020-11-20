create or replace function hash_sha1(text)
    returns text
    language sql strict immutable
as $$
    select encode(public.digest($1, 'sha1'), 'hex')
$$;
