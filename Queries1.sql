--Query 1

select distinct tyear from Groups G, Teams T,Countries C
Where G.Gid = T.Gid AND T.Cid = C.Cid AND C.name = 'Austria' AND (G.Tyear >= 1954 AND G.Tyear <= 2014)

--Query 3

select P.Name, MIN(G.GTime) AS FastestGoal
FROM Players P, Goals G

--Query 2

select M.team1 
from MatchesTemp M
WHERE M.score1 = 0 AND Year = 2014

select M.team2 
from MatchesTemp M
WHERE M.score2 = 0 AND Year = 2014

--Query 4

select M.Country
FROM MatchesTemp M
WHERE NOT 

--from temp table
select G.player
FROM GoalsTemp G
HAVING MIN(G.Minute) > 5
GROUP BY G.Player

--Query 5
Select distinct COUNT(Clubs.Name)
FROM Players,Clubs,Teams,Countries
WHERE Countries.Name = 'Germany' AND Countries.Cid = Teams.Cid AND Teams.Tid = Players.Tid AND Clubs.Ncid = Players.Ncid


--Query 6
SELECT COUNT(Players.Name)
FROM Players
JOIN Clubs
ON Players.Ncid = Clubs.Ncid
WHERE Clubs.Name = 'Sturm Graz'

--from temporary tables
Select S.Name
From SquadsTemp S
WHERE S.Club_Name = 'Sturm Graz'

--Query 7

SELECT Players.Name
FROM Players, Matches, Goals
WHERE COUNT(

--from temporary tables
Select G.Player
From GoalsTemp G
WHERE G.Year = 2014
GROUP BY G.Player



