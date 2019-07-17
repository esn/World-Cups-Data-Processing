--first part of 3.4
create table R(a INT,b INT);
create table S(a INT,b INT);

--second part
BEGIN;
INSERT INTO R (a,b) VALUES (1,2),(2,3);

INSERT INTO S (a,b) VALUES (1,2),(2,3),(3,4);
COMMIT;

-- creating deadlock (third part)
-- I was working in linux termina, so I am going to explain how to make
-- a deadlock on linux terminal (with 2 windows)
--TERMINAL 1:
SELECT * FROM S;
--TERMINAL 1:
BEGIN;
--TERMINAL 1:
UPDATE R SET b=b*5 where a=1;
--TERMINAL 2:
BEGIN;
--TERMINAL 2:
UPDATE R SET b=b*6 where a=2;
--TERMINAL 2:
UPDATE R SET b=b*4 where a=1;
--TERMINAL 1:
UPDATE R SET b=b*4 where a=2;
--right after that command the error occurs and this is the output
/*
ERROR:  deadlock detected
DETAIL:  Process 5684 waits for ShareLock on transaction 859; blocked by process 4797.
Process 4797 waits for ShareLock on transaction 858; blocked by process 5684.
HINT:  See server log for query details.
*/
-- that is a deadlock error that we were looking for
-- this deadlock happens, because there are two transactions going at
-- the same time and there is a conflict, which leads to deadlock