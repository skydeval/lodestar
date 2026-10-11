-- Time 20 batches of 1,000 primary-key lookups, each batch different DIDs.
-- psql variables: `label` names the run, `first` is the first batch number.
-- Use batches a previous run has not touched, or the result only measures
-- the cache.
\pset border 1
\if :{?label}
\else
\set label run
\endif
\if :{?first}
\else
\set first 0
\endif
SELECT set_config('recon.label', :'label', false), set_config('recon.first', :'first', false);
DO $$
DECLARE
    i integer;
    f integer := current_setting('recon.first')::integer;
    t0 timestamptz;
    n integer;
BEGIN
    FOR i IN f..f + 19 LOOP
        t0 := clock_timestamp();
        SELECT count(*) INTO n FROM mem_batches b JOIN c_accounts a USING (did) WHERE b.batch = i;
        INSERT INTO mem_results (label, batch, found, ms)
        VALUES (current_setting('recon.label'), i, n, extract(epoch FROM clock_timestamp() - t0) * 1000);
        COMMIT;
    END LOOP;
END $$;
SELECT label, count(*) AS batches, min(batch) AS first_batch,
       round(avg(ms)::numeric, 1) AS avg_ms,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY ms))::numeric, 1) AS p50_ms,
       round(min(ms)::numeric, 1) AS min_ms, round(max(ms)::numeric, 1) AS max_ms,
       round((avg(ms) FILTER (WHERE batch - (SELECT min(batch) FROM mem_results m WHERE m.label = r.label) < 5))::numeric, 1) AS first_5_avg,
       round((avg(ms) FILTER (WHERE batch - (SELECT min(batch) FROM mem_results m WHERE m.label = r.label) >= 15))::numeric, 1) AS last_5_avg
FROM mem_results r GROUP BY label ORDER BY min(at);
