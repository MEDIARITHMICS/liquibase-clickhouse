--liquibase formatted sql

--changeset test:1
CREATE TABLE test (
    ID UInt8,
    DESC String
) ENGINE MergeTree() ORDER BY ID;
--rollback DROP table test

--changeset test:2
INSERT INTO test VALUES (1, 'foo');
INSERT INTO test VALUES (2, 'bar');
INSERT INTO test VALUES (3, 'quz');
--rollback ALTER TABLE test DELETE WHERE ID IN (1,2,3)