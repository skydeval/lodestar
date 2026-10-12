-- Claims under bsky.social with an extra label, which can never verify:
-- how many, and whether they sit together in storage order (no requests).
-- A lap reads in storage order, so a page where most of the provider's
-- claims are of this kind is a run of failing first attempts.
\pset border 1

WITH per_page AS (
    SELECT (ctid::text::point)[0]::bigint AS page,
           count(*) AS claims,
           count(*) FILTER (WHERE handle_claim LIKE '%.%.bsky.social') AS extra
    FROM accounts
    WHERE handle_claim LIKE '%.bsky.social'
    GROUP BY 1
)
SELECT count(*) AS pages_with_claims,
       sum(claims) AS claims,
       sum(extra) AS extra_label_claims,
       count(*) FILTER (WHERE extra > 0) AS pages_with_any,
       count(*) FILTER (WHERE claims >= 20 AND extra > 0.6 * claims) AS pages_mostly_extra,
       max(extra) AS most_on_one_page,
       sum(extra) FILTER (WHERE claims >= 20 AND extra > 0.6 * claims) AS extra_on_those_pages
FROM per_page;

-- Longest stretches of consecutive pages that are mostly of this kind.
WITH per_page AS (
    SELECT (ctid::text::point)[0]::bigint AS page,
           count(*) AS claims,
           count(*) FILTER (WHERE handle_claim LIKE '%.%.bsky.social') AS extra
    FROM accounts
    WHERE handle_claim LIKE '%.bsky.social'
    GROUP BY 1
), bad AS (
    SELECT page, extra, page - row_number() OVER (ORDER BY page) AS grp
    FROM per_page
    WHERE claims >= 20 AND extra > 0.6 * claims
)
SELECT min(page) AS first_page, count(*) AS pages, sum(extra) AS extra_label_claims
FROM bad GROUP BY grp ORDER BY sum(extra) DESC LIMIT 10;
