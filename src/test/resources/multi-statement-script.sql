--liquibase formatted sql

--changeset test:multi-1 rollbackSplitStatements:false
CREATE TABLE ms_events (id UInt8, label String) ENGINE MergeTree ORDER BY id;
-- a comment; with a semicolon
INSERT INTO ms_events VALUES (1, 'semi;colon');
INSERT INTO ms_events VALUES (2, 'escaped \' quote');
CREATE MATERIALIZED VIEW ms_events_mv
ENGINE MergeTree ORDER BY id AS
SELECT
    id,
    coalesce(label, '') AS label, -- trailing comment
    CAST('A', 'Enum8(\'A\' = 0, \'B\' = 1)') AS kind
FROM ms_events;
--rollback DROP TABLE ms_events_mv;
--rollback DROP TABLE ms_events;
