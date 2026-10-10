-- The vacuum step of footprint_build, without parallel workers: the recon
-- container's shared memory is too small for a parallel vacuum of the
-- largest tables.
\timing on
VACUUM (ANALYZE, PARALLEL 0) identity_history;
VACUUM (ANALYZE, PARALLEL 0) c_accounts;
VACUUM (ANALYZE, PARALLEL 0) c_history;
VACUUM (ANALYZE, PARALLEL 0) v_handle_index;
VACUUM (ANALYZE, PARALLEL 0) v_handle_index_c;
VACUUM (ANALYZE, PARALLEL 0) v_history_changed;
