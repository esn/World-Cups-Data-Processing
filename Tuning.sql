--INDEXING

--first we execute this query to see the complete query plan and total run time
EXPLAIN ANALYZE
SELECT DISTINCT Clubs.Name 
FROM Clubs, Players 
WHERE Clubs.Ncid = Players.Ncid 
AND JNum <= 3

-- let's look at the query plan:
-- 1: HashAggregate  (cost=227.26..238.30 rows=1104 width=13) (actual time=11.865..12.160 rows=555 loops=1)
-- 2:   ->  Hash Join  (cost=42.58..224.50 rows=1104 width=13) (actual time=1.642..9.948 rows=1104 loops=1)
-- 3:         Hash Cond: (players.ncid = clubs.ncid)
-- 4:         ->  Seq Scan on players  (cost=0.00..166.74 rows=1104 width=4) (actual time=0.019..6.205 rows=1104 loops=1)
-- 5:               Filter: (jnum <= 3)
-- 6:               Rows Removed by Filter: 7115
-- 7:         ->  Hash  (cost=24.48..24.48 rows=1448 width=17) (actual time=1.741..1.741 rows=1448 loops=1)
-- 8:               Buckets: 1024  Batches: 1  Memory Usage: 72kB
-- 9:               ->  Seq Scan on clubs  (cost=0.00..24.48 rows=1448 width=17) (actual time=0.019..0.755 rows=1448 loops=1)
-- 10: Total runtime: 8.184 ms

-- what we see is the complete query plan executed on this simple query from our database
-- in this query plan there are Hashjoins performed, because of the WHERE clause in the query, thes Hashjoin joins the tables based on Ncid
-- from Clubs and Players tables. Also the main thing executing query without indexing is that the SQL has to scan whole table in order
-- to return the results (Filter)

-- next up is executing this query with index and to see the difference

--creating the index 
CREATE INDEX jNum 
ON Players (JNum);

--execute the query again to see the results change with index
EXPLAIN ANALYZE
SELECT DISTINCT Clubs.Name 
FROM Clubs, Players 
WHERE Clubs.Ncid = Players.Ncid 
AND JNum <= 3

-- let's look at the query plan:
-- 1: HashAggregate  (cost=163.16..174.20 rows=1104 width=13) (actual time=7.127..7.327 rows=555 loops=1)
-- 2:   ->  Hash Join  (cost=67.42..160.40 rows=1104 width=13) (actual time=1.875..5.898 rows=1104 loops=1)
-- 3:         Hash Cond: (players.ncid = clubs.ncid)
-- 4:         ->  Bitmap Heap Scan on players  (cost=24.84..102.64 rows=1104 width=4) (actual time=0.462..2.970 rows=1104 loops=1)
-- 5:               Recheck Cond: (jnum <= 3)
-- 6:               ->  Bitmap Index Scan on jnum  (cost=0.00..24.57 rows=1104 width=0) (actual time=0.425..0.425 rows=1104 loops=1)
-- 7:                     Index Cond: (jnum <= 3)
-- 8:         ->  Hash  (cost=24.48..24.48 rows=1448 width=17) (actual time=1.341..1.341 rows=1448 loops=1)
-- 9:               Buckets: 1024  Batches: 1  Memory Usage: 72kB
-- 10:               ->  Seq Scan on clubs  (cost=0.00..24.48 rows=1448 width=17) (actual time=0.011..0.564 rows=1448 loops=1)
-- 11: Total runtime: 7.485 ms

-- in this instance of executing the query we see that with index already created there is no point in scanning the whole table
-- but instead just jumping to data we want and getting that

-- this next command in terminal gives us the overview of all indexes and
-- everything in that table
-- \d table_name;


--MATERIZALIZED VIEWS
--As in the first part of this exercise, we firstly execute the query with EXPLAIN ANALYZE keywords
--Query10:
EXPLAIN ANALYZE
-- Group G of Tournament 2014 (name, tid)
WITH 
GroupG14 AS
(SELECT C.Name, T.Tid, TM.PtsWin 
  FROM Groups G, Teams T, Countries C, Tournaments TM
  WHERE G.Gid = T.Gid AND T.Cid = C.Cid AND TM.Tyear=G.TYear
  AND TM.TYear = 2014 AND G.Name='Group G'),
-- Normalized matches (home team aligned)
NormMatchG14 AS
(SELECT G.Name, M.HomeScore, M.VisitScore, G.PtsWin FROM GroupG14 G, Matches M
  WHERE M.HomeTid = G.Tid AND M.MatchType LIKE 'Group%'
UNION ALL
SELECT G.Name, M.VisitScore, M.HomeScore, G.PtsWin FROM GroupG14 G, Matches M
  WHERE M.VisitTid = G.Tid AND M.MatchType LIKE 'Group%'),
-- Indicators for win/loss
Norm2MatchG14 as 
(SELECT Name, HomeScore, VisitScore, PtsWin,
    CASE WHEN HomeScore>VisitScore THEN 1 ELSE 0 END AS Win,
    CASE WHEN HomeScore=VisitScore THEN 1 ELSE 0 END AS Draw,
    CASE WHEN HomeScore<VisitScore THEN 1 ELSE 0 END AS Loss 
  FROM NormMatchG14 M)

-- Final group table
-- (Country, matches, wins, draws, losses, goal difference, points)
SELECT Name, count(*) AS Matches, sum(Win) AS Wins, sum(Draw) AS Draws, sum(Loss) AS Losses, 
    sum(HomeScore-VisitScore) AS GoalDiff, sum(PtsWin*Win)+sum(Draw) AS Points
  FROM Norm2MatchG14 
  GROUP BY Name
  ORDER BY (sum(PtsWin*Win)+sum(Draw), sum(HomeScore-VisitScore)) DESC

-- let's look at the query plan:
-- Sort  (cost=57.64..57.68 rows=14 width=534) (actual time=1.448..1.448 rows=4 loops=1)"
--   Sort Key: (ROW((sum((norm2matchg14.ptswin * norm2matchg14.win)) + sum(norm2matchg14.draw)), sum((norm2matchg14.homescore - norm2matchg14.visitscore))))"
--   Sort Method: quicksort  Memory: 25kB"
--   CTE groupg14"
--     ->  Nested Loop  (cost=2.81..20.02 rows=4 width=15) (actual time=0.235..0.270 rows=4 loops=1)"
--           ->  Index Scan using tournaments_pkey on tournaments tm  (cost=0.15..8.17 rows=1 width=4) (actual time=0.010..0.011 rows=1 loops=1)"
--                 Index Cond: (tyear = 2014)"
--           ->  Nested Loop  (cost=2.66..11.81 rows=4 width=15) (actual time=0.221..0.252 rows=4 loops=1)"
--                 ->  Hash Join  (cost=2.51..11.00 rows=4 width=10) (actual time=0.211..0.230 rows=4 loops=1)"
--                       Hash Cond: (t.gid = g.gid)"
--                       ->  Seq Scan on teams t  (cost=0.00..6.96 rows=396 width=12) (actual time=0.008..0.092 rows=396 loops=1)"
--                       ->  Hash  (cost=2.50..2.50 rows=1 width=6) (actual time=0.041..0.041 rows=1 loops=1)"
--                             Buckets: 1024  Batches: 1  Memory Usage: 1kB"
--                             ->  Seq Scan on groups g  (cost=0.00..2.50 rows=1 width=6) (actual time=0.032..0.036 rows=1 loops=1)"
--                                   Filter: ((tyear = 2014) AND ((name)::text = 'Group G'::text))"
--                                   Rows Removed by Filter: 99"
--                 ->  Index Scan using countries_pkey on countries c  (cost=0.14..0.19 rows=1 width=13) (actual time=0.003..0.003 rows=1 loops=4)"
--                       Index Cond: (cid = t.cid)"
--   CTE normmatchg14"
--     ->  Append  (cost=0.13..35.95 rows=14 width=522) (actual time=0.882..1.356 rows=12 loops=1)"
--           ->  Hash Join  (cost=0.13..17.91 rows=7 width=522) (actual time=0.882..0.899 rows=6 loops=1)"
--                 Hash Cond: (m.hometid = g_1.tid)"
--                 ->  Seq Scan on matches m  (cost=0.00..15.51 rows=585 width=8) (actual time=0.014..0.476 rows=585 loops=1)"
--                       Filter: ((matchtype)::text ~~ 'Group%'::text)"
--                       Rows Removed by Filter: 176"
--                 ->  Hash  (cost=0.08..0.08 rows=4 width=522) (actual time=0.281..0.281 rows=4 loops=1)"
--                       Buckets: 1024  Batches: 1  Memory Usage: 1kB"
--                       ->  CTE Scan on groupg14 g_1  (cost=0.00..0.08 rows=4 width=522) (actual time=0.239..0.278 rows=4 loops=1)"
--           ->  Hash Join  (cost=0.13..17.91 rows=7 width=522) (actual time=0.435..0.454 rows=6 loops=1)"
--                 Hash Cond: (m_1.visittid = g_2.tid)"
--                 ->  Seq Scan on matches m_1  (cost=0.00..15.51 rows=585 width=8) (actual time=0.009..0.344 rows=585 loops=1)"
--                       Filter: ((matchtype)::text ~~ 'Group%'::text)"
--                       Rows Removed by Filter: 176"
--                 ->  Hash  (cost=0.08..0.08 rows=4 width=522) (actual time=0.005..0.005 rows=4 loops=1)"
--                       Buckets: 1024  Batches: 1  Memory Usage: 1kB"
--                       ->  CTE Scan on groupg14 g_2  (cost=0.00..0.08 rows=4 width=522) (actual time=0.002..0.002 rows=4 loops=1)"
--   CTE norm2matchg14"
--     ->  CTE Scan on normmatchg14 m_2  (cost=0.00..0.39 rows=14 width=522) (actual time=0.888..1.380 rows=12 loops=1)"
--   ->  HashAggregate  (cost=0.81..1.02 rows=14 width=534) (actual time=1.429..1.433 rows=4 loops=1)"
--         ->  CTE Scan on norm2matchg14  (cost=0.00..0.28 rows=14 width=534) (actual time=0.891..1.394 rows=12 loops=1)"
-- Total runtime: 1.623 ms"


-- now we create materialized views for the same query
CREATE MATERIALIZED VIEW AnyGroup AS
SELECT C.Name, T.Tid, TM.PtsWin,G.Name AS Group
  FROM Groups G, Teams T, Countries C, Tournaments TM
  WHERE G.Gid = T.Gid AND T.Cid = C.Cid AND TM.Tyear=G.TYear
  AND TM.TYear = 2014
-- we made a materialized view and now it is time to use it in the query to get all results
-- to get the results

EXPLAIN ANALYZE
WITH
AnyGroup14 AS
(SELECT Name, Tid, PtsWin
FROM AnyGroup A
-- since we need the same results, we must have a WHERE clause near our Materialized View
WHERE A.Group = 'Group G'),
-- Normalized matches (home team aligned)
NormMatchG14 AS
(SELECT G.Name, M.HomeScore, M.VisitScore, G.PtsWin FROM AnyGroup14 G, Matches M
  WHERE M.HomeTid = G.Tid AND M.MatchType LIKE 'Group%'
UNION ALL
SELECT G.Name, M.VisitScore, M.HomeScore, G.PtsWin FROM AnyGroup14 G, Matches M
  WHERE M.VisitTid = G.Tid AND M.MatchType LIKE 'Group%'),
-- Indicators for win/loss
Norm2MatchG14 as 
(SELECT Name, HomeScore, VisitScore, PtsWin,
    CASE WHEN HomeScore>VisitScore THEN 1 ELSE 0 END AS Win,
    CASE WHEN HomeScore=VisitScore THEN 1 ELSE 0 END AS Draw,
    CASE WHEN HomeScore<VisitScore THEN 1 ELSE 0 END AS Loss 
  FROM NormMatchG14 M)

-- Final group table
-- (Country, matches, wins, draws, losses, goal difference, points)
SELECT Name, count(*) AS Matches, sum(Win) AS Wins, sum(Draw) AS Draws, sum(Loss) AS Losses, 
    sum(HomeScore-VisitScore) AS GoalDiff, sum(PtsWin*Win)+sum(Draw) AS Points
  FROM Norm2MatchG14 
  GROUP BY Name
  ORDER BY (sum(PtsWin*Win)+sum(Draw), sum(HomeScore-VisitScore)) DESC

-- let's take a look at the query plan:
-- Sort  (cost=47.37..47.38 rows=4 width=534) (actual time=0.349..0.349 rows=4 loops=1)"
--   Sort Key: (ROW((sum((norm2matchg14.ptswin * norm2matchg14.win)) + sum(norm2matchg14.draw)), sum((norm2matchg14.homescore - norm2matchg14.visitscore))))"
--   Sort Method: quicksort  Memory: 25kB"
--   CTE anygroup14"
--     ->  Seq Scan on anygroup a  (cost=0.00..11.38 rows=1 width=522) (actual time=0.003..0.008 rows=4 loops=1)"
--           Filter: (("group")::text = 'Group G'::text)"
--           Rows Removed by Filter: 28"
--   CTE normmatchg14"
--     ->  Append  (cost=0.03..35.56 rows=4 width=522) (actual time=0.159..0.316 rows=12 loops=1)"
--           ->  Hash Join  (cost=0.03..17.76 rows=2 width=522) (actual time=0.159..0.166 rows=6 loops=1)"
--                 Hash Cond: (m.hometid = g.tid)"
--                 ->  Seq Scan on matches m  (cost=0.00..15.51 rows=585 width=8) (actual time=0.005..0.110 rows=585 loops=1)"
--                       Filter: ((matchtype)::text ~~ 'Group%'::text)"
--                       Rows Removed by Filter: 176"
--                 ->  Hash  (cost=0.02..0.02 rows=1 width=522) (actual time=0.010..0.010 rows=4 loops=1)"
--                       Buckets: 1024  Batches: 1  Memory Usage: 1kB"
--                       ->  CTE Scan on anygroup14 g  (cost=0.00..0.02 rows=1 width=522) (actual time=0.004..0.010 rows=4 loops=1)"
--           ->  Hash Join  (cost=0.03..17.76 rows=2 width=522) (actual time=0.143..0.149 rows=6 loops=1)"
--                 Hash Cond: (m_1.visittid = g_1.tid)"
--                 ->  Seq Scan on matches m_1  (cost=0.00..15.51 rows=585 width=8) (actual time=0.003..0.108 rows=585 loops=1)"
--                       Filter: ((matchtype)::text ~~ 'Group%'::text)"
--                       Rows Removed by Filter: 176"
--                 ->  Hash  (cost=0.02..0.02 rows=1 width=522) (actual time=0.003..0.003 rows=4 loops=1)"
--                       Buckets: 1024  Batches: 1  Memory Usage: 1kB"
--                       ->  CTE Scan on anygroup14 g_1  (cost=0.00..0.02 rows=1 width=522) (actual time=0.001..0.003 rows=4 loops=1)"
--   CTE norm2matchg14"
--     ->  CTE Scan on normmatchg14 m_2  (cost=0.00..0.11 rows=4 width=522) (actual time=0.161..0.323 rows=12 loops=1)"
--   ->  HashAggregate  (cost=0.23..0.29 rows=4 width=534) (actual time=0.341..0.342 rows=4 loops=1)"
--         ->  CTE Scan on norm2matchg14  (cost=0.00..0.08 rows=4 width=534) (actual time=0.161..0.324 rows=12 loops=1)"
-- Total runtime: 0.405 ms"


-- much faster and way lower number of steps to execute the query, this is because materialized views are already holding 
-- data that we need and we just use it

-- we use materialized views to save data on disk in order to use it later on
-- this process of creating materialized views is called materialization
-- this process is used to increase application performance