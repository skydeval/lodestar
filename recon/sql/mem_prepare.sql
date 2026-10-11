-- Prepare batches of randomly chosen DIDs for the memory-limit test.
-- BERNOULLI sampling, so the DIDs are spread over the whole table.
\timing on
DROP TABLE IF EXISTS mem_batches;
CREATE TABLE mem_batches AS
SELECT ((row_number() OVER (ORDER BY random())) - 1) / 1000 AS batch, did
FROM c_accounts TABLESAMPLE BERNOULLI (0.05);
CREATE INDEX ON mem_batches (batch);
CREATE TABLE IF NOT EXISTS mem_results (
    label text NOT NULL,
    batch integer NOT NULL,
    at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    found integer NOT NULL,
    ms    double precision NOT NULL
);
SELECT count(*) AS dids, max(batch) + 1 AS batches FROM mem_batches;
