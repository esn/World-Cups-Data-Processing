def executeQ11Dataset(): Unit = {

import org.apache.spark.{SparkConf, SparkContext}

val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("Q11").getOrCreate;

val countries = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("countries.csv")
countries.createOrReplaceTempView("countries")
val groups = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("groups.csv")
groups.createOrReplaceTempView("groups")
val teams = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("teams.csv")
teams.createOrReplaceTempView("teams")
val clubs = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("clubs.csv")
clubs.createOrReplaceTempView("clubs")
val players = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("players.csv")
players.createOrReplaceTempView("players")

val Q11 = spark.sql("SELECT DISTINCT Clubs.Name,COUNT(Players.Name) AS Number FROM Clubs,Countries,Players,Teams,Groups WHERE Countries.Name = 'Germany' AND Clubs.Ncid = Players.Ncid AND Teams.Tid = Players.Tid AND Teams.Cid = Countries.Cid AND Groups.Gid = Teams.Gid AND Groups.Tyear = 2014 GROUP BY Clubs.Name ORDER BY Number DESC").write.json("out11.json")
}




def executeQ12Dataset(): Unit = {

import org.apache.spark.{SparkConf, SparkContext}

val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("Q12").getOrCreate;

val tournaments = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("tournaments.csv")
tournaments.createOrReplaceTempView("tournaments")
val countries = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("countries.csv")
countries.createOrReplaceTempView("countries")
val hosts = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("hosts.csv")
hosts.createOrReplaceTempView("hosts")
val teams = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("teams.csv")
teams.createOrReplaceTempView("teams")
val matches = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("matches.csv")
matches.createOrReplaceTempView("matches")

val Q12 = spark.sql("SELECT Tournaments.TYear,Countries.Name,datediff(Max(Matches.MatchDate), Min(Matches.MatchDate)) AS LENGTH FROM Tournaments,Countries,Hosts,Teams,Matches WHERE Tournaments.TYear = Hosts.TYear AND Countries.Cid = Hosts.Cid AND (Teams.Tid = Matches.HomeTid OR Teams.Tid = Matches.VisitTid) AND CAST(year(Matches.MatchDate) AS String)  LIKE (Tournaments.TYear || '%') GROUP BY Tournaments.TYear,Countries.Name ORDER BY LENGTH,Tournaments.TYear ASC").write.json("out12.json")
}
