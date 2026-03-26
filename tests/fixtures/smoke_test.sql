-- Smoke test for CREATE INDEX and COUNT(*)

CREATE TABLE test_tbl (id INT, name VARCHAR, score INT);
INSERT INTO test_tbl VALUES (1, 'Alice', 90);
INSERT INTO test_tbl VALUES (2, 'Bob', 85);
INSERT INTO test_tbl VALUES (3, 'Charlie', 90);

CREATE INDEX idx_score ON test_tbl (score);

SELECT COUNT(*) FROM test_tbl;
SELECT COUNT(*) FROM test_tbl WHERE score = 90;
SELECT * FROM test_tbl WHERE score = 90;

DROP TABLE test_tbl;
