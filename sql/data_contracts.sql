-- Each query should return zero rows. Register the corresponding output table
-- as clean_edges, adjacency, candidate_scores, recommendations before executing.
SELECT left, right, COUNT(*) AS duplicates
FROM clean_edges
GROUP BY left, right
HAVING COUNT(*) > 1 OR left < 0 OR left >= right;

SELECT c.left, c.right
FROM candidate_scores c
INNER JOIN clean_edges e ON c.left = e.left AND c.right = e.right;

SELECT user, method, rank, COUNT(*) AS duplicates
FROM recommendations
GROUP BY user, method, rank
HAVING COUNT(*) > 1 OR rank < 1;

SELECT user, method, candidate, COUNT(*) AS duplicates
FROM recommendations
GROUP BY user, method, candidate
HAVING COUNT(*) > 1 OR user = candidate;
