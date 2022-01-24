
/*
    Some remote commands are registered in the preinstall or upgrade scripts.

    Two remote commands are registered in the idempotent scripts which get run
    at the end of a fresh install and after every version upgrade. Thus, it's
    difficult to know where in the sequence these two will show up.

    This update will ensure a consistent ordering of our remote commands and
    place any potential user defined remote commands at the end of ours in
    original order.
*/
WITH x(key, seq) AS
(
    VALUES
    ('create_prom_reader'                        ,  1),
    ('create_prom_writer'                        ,  2),
    ('create_prom_modifier'                      ,  3),
    ('create_prom_admin'                         ,  4),
    ('create_prom_maintenance'                   ,  5),
    ('grant_prom_reader_prom_writer'             ,  6),
    ('create_schemas'                            ,  7),
    ('tracing_types'                             ,  8),
    ('_prom_catalog.do_decompress_chunks_after' ,  9),
    ('_prom_catalog.compress_old_chunks'        , 10)
)
UPDATE _prom_catalog.remote_commands u SET seq = z.seq
FROM
(
    -- our remote commands from above
    SELECT key, seq
    FROM x
    UNION
    -- any other remote commands get listed afterwards
    SELECT key, (SELECT max(seq) FROM x) + row_number() OVER (ORDER BY seq)
    FROM _prom_catalog.remote_commands k
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM x
        WHERE x.key = k.key
    )
    ORDER BY seq
) z
WHERE u.key = z.key
;