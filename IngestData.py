import psycopg2
import copy
import sys

def pg_load_table(table_name, dbname, host, port, user, pwd):
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=pwd)
        print("Connection Open")
        cur = conn.cursor()

        #drop all current tables in the database

        drop_goals_temp_sql = \
        """
            DROP TABLE IF EXISTS GoalsTemp CASCADE 
        """
        cur.execute(drop_goals_temp_sql)
        drop_match_temp_sql = \
        """
            DROP TABLE IF EXISTS MatchesTemp CASCADE
        """
        cur.execute(drop_match_temp_sql)
        drop_squad_temp_sql = \
        """
            DROP TABLE IF EXISTS SquadsTemp CASCADE
        """
        cur.execute(drop_squad_temp_sql)
        print("All temp tables dropped")

        #create temp tables
        create_squads_sql = \
        """
            CREATE TABLE SquadsTemp(    
            Year SMALLINT NOT NULL,
            Host_Country VARCHAR(256) NOT NULL,
            Country VARCHAR(256) NOT NULL,
            Jersey_Number SMALLINT NOT NULL,
            Position CHAR(2) NOT NULL,
            Name VARCHAR(256) NOT NULL,
            Club_Name VARCHAR(256),
            Club_Country VARCHAR(256)
            );
        """
        cur.execute(create_squads_sql)
        create_match_sql = \
        """
            CREATE TABLE MatchesTemp(    
            Year SMALLINT NOT NULL,
            Host_Country VARCHAR(256) NOT NULL,
            Match_ID SMALLINT NOT NULL,
            Type VARCHAR(64) NOT NULL,
            Date DATE NOT NULL,
            Location VARCHAR(256) NOT NULL,
            Team1 VARCHAR(64) NOT NULL,
            Team2 VARCHAR(64) NOT NULL,
            Score1 SMALLINT NOT NULL,
            Score2 SMALLINT NOT NULL
            );
        """
        cur.execute(create_match_sql)
        create_goals_sql = \
        """
            CREATE TABLE GoalsTemp(    
            Year SMALLINT NOT NULL,
            Host_Country VARCHAR(256) NOT NULL,
            Match_ID SMALLINT NOT NULL,
            Team VARCHAR(256) NOT NULL,
            Player VARCHAR(256) NOT NULL,
            Minute SMALLINT NOT NULL
            );
        """
        cur.execute(create_goals_sql)
        k=0
        # Truncate the table first
        for i in table_name:
            f = open(file_names[k], "r", encoding="utf8")
            cur.execute("Truncate {} Cascade;".format(i))
            #print("Truncated {}".format(i))
            copy_sql = """
                       COPY {} FROM stdin WITH CSV HEADER
                       DELIMITER as ','
                       """.format(i)
            # Load table from the file with header
            # cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(table_name), f)
            cur.copy_expert(sql=copy_sql, file=f)
            k += 1


        #insert data from temporary tables to 10 tables

        drop_all_tables = \
            """
            DROP TABLE IF EXISTS Tournaments, Countries, Stadiums, Hosts, Groups, Teams, Clubs, Players, Matches, Goals
            """

        cur.execute(drop_all_tables)

        drop_tournaments_temp = \
            """
            DROP TABLE IF EXISTS TournamentsTemp
            """

        cur.execute(drop_tournaments_temp)

        create_tournaments_temp = \
            """
            CREATE TABLE TournamentsTemp(
            TYear SMALLINT PRIMARY KEY,
            PtsWin SMALLINT  
            );
            """
        cur.execute(create_tournaments_temp)

        insert_tournaments_temp = \
            """
            INSERT INTO TournamentsTemp(TYear)
            SELECT year FROM SquadsTemp
            Group by year
            order by year ASC
            """
        cur.execute(insert_tournaments_temp)

        update_first = \
            """
            UPDATE TournamentsTemp
            SET PtsWin = 2
            WHERE TYear < 1994
            """

        cur.execute(update_first)

        update_second = \
            """
           UPDATE TournamentsTemp
            SET PtsWin = 3
            WHERE TYear >= 1994
            """
        
        cur.execute(update_second)


        create_tournaments = \
            """
            CREATE TABLE Tournaments(
            TYear SMALLINT PRIMARY KEY,
            PtsWin SMALLINT NOT NULL   
            );
            """
        cur.execute(create_tournaments)

        insert_tournaments = \
        """
        --populate table Tournaments from TournamentsTemp for both columns
            INSERT INTO Tournaments(TYear,PtsWin)
            SELECT TYear,PtsWin FROM TournamentsTemp
        """
        cur.execute(insert_tournaments)

        drop_countries_temp = \
            """
            DROP TABLE IF EXISTS CountriesTemp CASCADE
            """

        cur.execute(drop_countries_temp)

        create_countries_temp = \
            """
            CREATE TABLE CountriesTemp(
            Cid SERIAL PRIMARY KEY,
            Name VARCHAR(256) NOT NULL UNIQUE
            );
            """
        cur.execute(create_countries_temp)

        insert_countries_temp_country = \
            """
            INSERT INTO CountriesTemp(Name)
            SELECT country FROM SquadsTemp
            Group by country
            order by country
            ON CONFLICT DO NOTHING
            """
        cur.execute(insert_countries_temp_country)


        insert_countries_temp_host_country = \
            """
            INSERT INTO CountriesTemp(Name)
            SELECT host_country FROM SquadsTemp
            Group by host_country
            order by host_country
            ON CONFLICT DO NOTHING
            """

        cur.execute(insert_countries_temp_host_country)

        create_countries = \
            """
            CREATE TABLE Countries(
            Cid SERIAL PRIMARY KEY,
            Name VARCHAR(256) NOT NULL UNIQUE
            );
            """
        
        cur.execute(create_countries)

        insert_country = \
            """
            INSERT INTO Countries (Name)
            SELECT Name FROM CountriesTemp
            ON CONFLICT DO NOTHING
            """
        cur.execute(insert_country)

        create_stadiums = \
            """
            CREATE TABLE Stadiums(
            Sid SERIAL PRIMARY KEY,
            Name VARCHAR(256) NOT NULL UNIQUE, 
            Cid INT REFERENCES Countries NOT NULL
            );
            """

        cur.execute(create_stadiums)

        insert_stadiums = \
        """
            INSERT INTO Stadiums(name,cid)
            Select distinct M.location,c.Cid
            FROM MatchesTemp M, Countries C
            WHERE M.host_country = c.Name
            ON CONFLICT DO NOTHING
        """
        cur.execute(insert_stadiums)


        create_hosts = \
            """
            CREATE TABLE Hosts(
            TYear SMALLINT REFERENCES Tournaments NOT NULL,
            Cid INT REFERENCES Countries NOT NULL,
            PRIMARY KEY(TYear, CID)
            );
            """

        cur.execute(create_hosts)
        
        insert_hosts = \
        """
            Insert into Hosts
            Select Distinct Year,c.Cid
            FROM SquadsTemp T,Countries C
            WHERE T.host_country = c.Name
            ON CONFLICT DO NOTHING
        """
        cur.execute(insert_hosts)

        create_groups = \
            """
            CREATE TABLE Groups(
            Gid SERIAL PRIMARY KEY,
            Name VARCHAR(64) NOT NULL,
            TYear SMALLINT REFERENCES Tournaments NOT NULL,
            UNIQUE(Name,TYear)
            );
            """

        cur.execute(create_groups)

        insert_groups = \
        """
            INSERT INTO Groups(Name,TYear)
            Select distinct M.type,M.Year
            from MatchesTemp M
            WHERE M.type LIKE 'Group%'
            ON CONFLICT DO NOTHING
        """
        cur.execute(insert_groups)

        create_teams = \
            """
            CREATE TABLE Teams(
            Tid SERIAL PRIMARY KEY,
            Cid INT REFERENCES Countries NOT NULL,
            Gid INT REFERENCES Groups NOT NULL,
            UNIQUE(Cid,Gid)
            );
            """

        cur.execute(create_teams)

        insert_teams = \
            """
            INSERT INTO Teams(Cid,Gid)
            SELECT distinct C.Cid,G.Gid
            FROM Countries C, Groups G, MatchesTemp M
            WHERE G.TYear = M.year AND G.NAME = m.type AND (C.Name = M.Team1 OR C.Name = M.Team2)
            ON CONFLICT DO NOTHING
            """
        cur.execute(insert_teams)    

        create_clubs = \
            """
            CREATE TABLE Clubs(
            Ncid SERIAL PRIMARY KEY,
            Name VARCHAR(256) NOT NULL,
            Cid INT REFERENCES Countries NOT NULL,
            UNIQUE (Name,Cid)
            );
            """

        cur.execute(create_clubs)

        insert_clubs = \
            """
            INSERT INTO Clubs(Name,Cid)
            SELECT distinct T.Club_Name, C.Cid
            FROM SquadsTemp T, Countries C
            WHERE T.Club_Country = C.Name 
            ORDER BY T.Club_Name
            ON CONFLICT DO NOTHING
            """

        cur.execute(insert_clubs)

        create_players = \
            """
            CREATE TABLE Players(
            Pid SERIAL PRIMARY KEY,
            Name VARCHAR(256) NOT NULL,
            Pos CHAR(2) NOT NULL CHECK(Pos IN('GK','DF','MF','FW')),
            JNum SMALLINT NOT NULL,
            Ncid INT REFERENCES Clubs,  
            Tid INT REFERENCES Teams NOT NULL,
            UNIQUE(Tid, JNum)
            );
            """

        cur.execute(create_players)

        insert_players = \
            """
            INSERT INTO Players(Name,Pos,JNum,Ncid,Tid)
            SELECT distinct S.Name,S.Position,S.Jersey_Number,C.Ncid,T.Tid
            FROM SquadsTemp S,Clubs C, Teams T, Countries
            WHERE S.Club_Name = C.Name AND Countries.Cid = T.Cid AND Countries.Name = S.country AND S.Club_Country = Countries.Name AND S.country = 'Brazil'
            ON CONFLICT DO NOTHING
            """

        cur.execute(insert_players)


        conn.commit()
		
		
		
        #print("Loaded data into {}".format(table_name))
        cur.close()
        print("Connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)

# Execution Example
# python IngestData.py ./1954_2010_Squads.csv ./1954_2010_Matches.csv ./1954_2010_Goals.csv 127.0.0.1:5432/db11817339
# python IngestData.py ./2014_Squads.csv ./2014_Matches.csv ./2014_Goals.csv 127.0.0.1:5432/db11817339
file_names = [sys.argv[1],sys.argv[2],sys.argv[3]]
for i in file_names:
    if (i[0] == '.' and i[1] == '/'):
        i = i[2:]
        print(i)
table_name = ['SquadsTemp', 'MatchesTemp', 'GoalsTemp']
host_port_dbname = sys.argv[4]
host,port_dbname = host_port_dbname.split(':')
port,dbname = port_dbname.split('/')
user = 'postgres'
pwd = 'LakiPro1999'
pg_load_table(table_name, dbname, host, port, user, pwd)