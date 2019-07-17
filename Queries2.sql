--Q11
SELECT DISTINCT Clubs.Name,COUNT(Players)
FROM Clubs,Countries,Players,Teams,Groups
WHERE Countries.Name = 'Germany' AND Clubs.Ncid = Players.Ncid
AND Teams.Tid = Players.Tid AND Teams.Cid = Countries.Cid AND Groups.Gid = Teams.Gid AND Groups.Tyear = 2014
GROUP BY Clubs.Name
ORDER BY COUNT(Players) DESC

--Q12
SELECT DISTINCT Hosts.TYear,Countries.Name,Max(Matches.MatchDate) - Min(Matches.MatchDate) AS LENGTH
FROM Countries,Hosts,Matches
WHERE Countries.Cid = Hosts.Cid AND Hosts.TYear = EXTRACT(YEAR FROM Matches.MatchDate)
GROUP BY Hosts.Tyear,Countries.Name
ORDER BY LENGTH,TYear ASC