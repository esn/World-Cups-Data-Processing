import psycopg2
import copy
import sys

from collections import defaultdict


#for testing purposes I made parsing of files if you want to test on tables
def get_data(table_name):
    file_names = ['tournaments.csv', 'countries.csv', 'stadiums.csv', 'hosts.csv', 'groups.csv', 'teams.csv', 'clubs.csv', 'players.csv', 'matches.csv', 'goals.csv']
    for i in file_names:
        if (i[0] == '.' and i[1] == '/'):
            i = i[2:]
    #table names = ['Tournaments', 'Countries', 'Stadiums', 'Hosts', 'Groups', 'Teams', 'Clubs', 'Players', 'Matches', 'Goals']
    host_port_dbname = sys.argv[1]
    host,port_dbname = host_port_dbname.split(':')
    port,dbname = port_dbname.split('/')
    user = 'postgres'
    pwd = 'admin'
    conn = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=pwd)
    #print("Connection Open")
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + table_name)
        #print("The number of rows: %d" %cur.rowcount)
        counter = 0
        row = 1
        list = []
        while True:
            row = cur.fetchone()
            if row is None:
                break
            row = (counter,) + row
            list.append(row)
            counter+=1

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return list

#for testing purposes
def get_column_names(table_name):
    host_port_dbname = sys.argv[1]
    host,port_dbname = host_port_dbname.split(':')
    port,dbname = port_dbname.split('/')
    user = 'postgres'
    pwd = 'admin'
    conn = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=pwd)
    #print("Connection Open")
    try:
        cur = conn.cursor()
        cur.execute("Select * FROM " + table_name)
        colnames = [desc[0] for desc in cur.description]

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return colnames

#for testing purposes
table_name = 'Tournaments'
A = get_data(table_name)
namesA = get_column_names(table_name)
table_name = 'Countries'
B = get_data(table_name)
namesB = get_column_names(table_name)
C = [(0, 1954, 2),(0, 1954, 3),(1, 1888, 7)]
D = [(0, 1956, 5),(0, 1954, 4),(0, 1956, 8)]

#this is the complete volcano model
class VIterator:
    def __init__(self):
        return None


class TableScan(VIterator):
    def __init__(self, table):
        self.scanedTable = table
        self.opened = False
        self.progress = 0

    def open(self):
        self.opened = True
        self.progress = 0

    def next(self):
        if(not self.opened):
            print("ERROR: NOT OPEN")
            return None
        self.progress += 1
        if(len(self.scanedTable) == self.progress-1):
            return None
        return self.scanedTable[self.progress-1]

    def close(self):
        self.opened = False


class HashJoin(VIterator):
    def __init__(self, left, right):
        self.opened = False
        self.l = left
        self.r = right
        self.progress = 0
        self.l.open()
        self.hashmap = defaultdict(list)
        while True:
            b = self.l.next()
            if b is None:
                break
            self.hashmap[b[0]].append(b)
        self.l.close()

        self.r.open()
        

    
    def open(self):
        self.opened = True
        self.r.open()
        self.progress = 0
        self.a = self.r.next()

    def next(self):
        if(not self.opened):
            print("ERROR: NOT OPEN")
            return None


        self.progress += 1
        if self.progress > len(self.hashmap[self.a[0]]):
            self.progress = 1
            self.a = self.r.next()
            if self.a is None:
                return None
        return self.hashmap[self.a[0]][self.progress - 1] + self.a[1:]
    
        
    def close(self):
        self.opened = False
        self.r.close()

class NestedLoopJoin(VIterator):
    def __init__(self, left : VIterator, right : VIterator):
        self.opened = False
        self.l = left
        self.r = right
        self.s = None
        self.t = None
        

    def open(self):
        self.opened = True
        self.l.open()
        self.r.open()
        self.s = self.l.next()

    def next(self):
        if(not self.opened):
            print("ERROR: NOT OPEN")
            return None

        if self.s is None: # if l is empty table
            return None
        while True:
            self.t = self.r.next()
            if self.t is None:
                self.s = self.l.next()
                if self.s is None:
                    return None
                self.r.close()
                self.r.open()
                self.t = self.r.next()
                if self.t is None: # if r is empty table
                    return None

            if self.s[0] == self.t[0]:
                return self.s + self.t[1:]
        return 
        
    def close(self):
        self.opened = False
        self.l.close()
        self.r.close()

# usage:
# python3 11817339_3.3.py 127.0.0.1:5432/postgres


#for testing purposes (it handles multisets)
new = TableScan(C)
new.open()
t = new.next()
while t:
    print(t)
    t = new.next()
new.close()

print('\n')

new1 = TableScan(D)
new1.open()
t = new1.next()
while t:
    print(t)
    t = new1.next()
new1.close()

print('\n')

new2 = NestedLoopJoin(new,new1)
new2.open()
t = new2.next()
while t:
    print(t)
    t = new2.next()
new2.close()
print('\n')


new3 = HashJoin(new,new1)
new3.open()
t = new3.next()
while t:
    print(t)
    t = new3.next()
new3.close()

