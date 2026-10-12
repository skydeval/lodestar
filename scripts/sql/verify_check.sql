-- What verification has done, and what is waiting.
\pset border 1

SELECT CASE handle_status WHEN 0 THEN 'verified' WHEN 1 THEN 'mismatch' WHEN 2 THEN 'unresolved'
            WHEN 3 THEN 'unreachable' WHEN 4 THEN 'pending' ELSE 'none' END AS handle_status,
       count(*), count(handle_checked_at) AS with_check_time
FROM accounts GROUP BY 1 ORDER BY 2 DESC;

SELECT kind, reason, count(*) FROM verify_due GROUP BY 1, 2 ORDER BY 1, 2;

SELECT count(*) AS failing, count(nothing_at) AS awaiting_confirmation FROM check_failures;

-- The handles most often waiting in the queue.
SELECT a.handle_claim, count(*) FROM verify_due v JOIN accounts a USING (did)
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;

-- Each of these should be 0.
SELECT
  (SELECT count(*) FROM (SELECT handle_claim FROM accounts WHERE handle_status = 0 GROUP BY 1 HAVING count(*) > 1) x) AS handles_with_two_holders,
  (SELECT count(*) FROM accounts WHERE handle_status IN (0, 1, 2, 3) AND handle_checked_at IS NULL) AS checked_status_without_time,
  (SELECT count(*) FROM accounts WHERE handle_status = 0 AND status <> 0) AS verified_but_not_active;
