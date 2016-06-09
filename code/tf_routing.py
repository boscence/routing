# -*- coding: utf-8 -*-
"""
Created on Mon May  2 17:49:29 2016

@author: ab
"""


import psycopg2
import pandas as pd
from sqlalchemy import create_engine, MetaData


conn = psycopg2.connect(database="ie_datathon", user="boscence", password="password", host="localhost", port="5432")
cursor = conn.cursor()

df = pd.read_sql_query("select * from tf",con=conn)
df.groupby(['cod_sscc']).size()


###########################
###########################
### Select the Day
###########################
###########################

pd.read_sql_query("create table day2 as select b.uid as id, st_x(st_centroid(b.geom)) as x, st_y(st_centroid(b.geom)) as y, w.base_waste, w.tourist_waste_lite,w.base_waste + w.tourist_waste_lite as total_waste, b.source from alicante_bins_ways as b left join waste_sscc_day as w on st_within(b.geom,w.geom)where  w.date_dt = '2014-08-02' and w.base_waste + w.tourist_waste_lite > 2000 order by uid",con = conn)
pd.read_sql_query("select * from day2",con = conn)

###########################
###########################
### Making the route
###########################
###########################

### For Day1 Only Full Bins
cursor = conn.cursor()

r1 = pd.read_sql_query("SELECT seq, id1, id2, COST AS cost, d.source FROM pgr_tsp('select * from day1 order by id', 123) as r left join day1 as d on r.id2 = d.id",con=conn)

startp = []
endp = []
order = zip(startp,endp)

for i in r1['source']:
    startp.append(i)
    
for i in r1[1:]['source']:
    endp.append(i)

for i,j in zip(startp, endp):
    query = "SELECT seq, id1 AS node, id2 AS edge, cost,the_geom FROM pgr_astar('SELECT gid AS id,source::integer,target::integer,length::double precision AS cost,x1, y1, x2, y2 FROM ways',%a ,%a, true, false) as di JOIN ways pt ON di.id2 = pt.gid" % (i, j)
    mini_route =  pd.read_sql_query(query,con=conn)
    for k,v in mini_route.iterrows():
        data = (v[0],v[1],v[2],v[3],v[4])    
        query =  "INSERT INTO opt_route (seq, node, edge,cost,the_geom) VALUES (%s,%s,%s,%s,%s)"
        cursor.execute(query,(v[0],v[1],v[2],v[3],v[4]))
    conn.commit()
    
##################################
##################################   
### For Day2 the bins we missed
r2 = pd.read_sql_query("SELECT seq, id1, id2, COST AS cost, d.source FROM pgr_tsp('select * from day2 order by id', 2) as r left join day2 as d on r.id2 = d.id",con=conn)

startp = []
endp = []
order = zip(startp,endp)

for i in r2['source']:
    startp.append(i)
    
for i in r2[1:]['source']:
    endp.append(i)

for i,j in zip(startp, endp):
    query = "SELECT seq, id1 AS node, id2 AS edge, cost,the_geom FROM pgr_dijkstra('SELECT gid AS id,source::integer,target::integer,length::double precision AS cost,x1, y1, x2, y2 FROM ways',%a ,%a, true, false) as di JOIN ways pt ON di.id2 = pt.gid" % (i, j)
    mini_route =  pd.read_sql_query(query,con=conn)
    for k,v in mini_route.iterrows():
        data = (v[0],v[1],v[2],v[3],v[4])    
        query =  "INSERT INTO opt_route2 (seq, node, edge,cost,the_geom) VALUES (%s,%s,%s,%s,%s)"
        cursor.execute(query,(v[0],v[1],v[2],v[3],v[4]))
    conn.commit()
    
    
##################################
##################################   
### All bins
all_bins = pd.read_sql_query("SELECT seq, id1, id2, COST AS cost, d.source FROM pgr_tsp('select * from all_bins order by id', 2) as r left join all_bins as d on r.id2 = d.id",con=conn)

startp = []
endp = []
order = zip(startp,endp)

for i in all_bins['source']:
    startp.append(i)
    
for i in all_bins[1:]['source']:
    endp.append(i)

for i,j in zip(startp, endp):
    query = "SELECT seq, id1 AS node, id2 AS edge, cost,the_geom FROM pgr_dijkstra('SELECT gid AS id,source::integer,target::integer,length::double precision AS cost,x1, y1, x2, y2 FROM ways',%a ,%a, true, false) as di JOIN ways pt ON di.id2 = pt.gid" % (i, j)
    mini_route =  pd.read_sql_query(query,con=conn)
    for k,v in mini_route.iterrows():
        data = (v[0],v[1],v[2],v[3],v[4])    
        query =  "INSERT INTO opt_route_all (seq, node, edge,cost,the_geom) VALUES (%s,%s,%s,%s,%s)"
        cursor.execute(query,(v[0],v[1],v[2],v[3],v[4]))
    conn.commit()

### 5% Fewer bins
all_bins_5p = pd.read_sql_query("SELECT seq, id1, id2, COST AS cost, d.source FROM pgr_tsp('select * from all_bins_5p order by id', 2) as r left join all_bins_5p as d on r.id2 = d.id",con=conn)

startp = []
endp = []
order = zip(startp,endp)

for i in all_bins_5p['source']:
    startp.append(i)
    
for i in all_bins_5p[1:]['source']:
    endp.append(i)

for i,j in zip(startp, endp):
    query = "SELECT seq, id1 AS node, id2 AS edge, cost,the_geom FROM pgr_dijkstra('SELECT gid AS id,source::integer,target::integer,length::double precision AS cost,x1, y1, x2, y2 FROM ways',%a ,%a, true, false) as di JOIN ways pt ON di.id2 = pt.gid" % (i, j)
    mini_route =  pd.read_sql_query(query,con=conn)
    for k,v in mini_route.iterrows():
        data = (v[0],v[1],v[2],v[3],v[4])    
        query =  "INSERT INTO opt_route_all_5p (seq, node, edge,cost,the_geom) VALUES (%s,%s,%s,%s,%s)"
        cursor.execute(query,(v[0],v[1],v[2],v[3],v[4]))
    conn.commit()


### 10% Fewer bins
all_bins_10p = pd.read_sql_query("SELECT seq, id1, id2, COST AS cost, d.source FROM pgr_tsp('select * from all_bins_10p order by id', 2) as r left join all_bins_10p as d on r.id2 = d.id",con=conn)

startp = []
endp = []
order = zip(startp,endp)

for i in all_bins_10p['source']:
    startp.append(i)
    
for i in all_bins_10p[1:]['source']:
    endp.append(i)

for i,j in zip(startp, endp):
    query = "SELECT seq, id1 AS node, id2 AS edge, cost,the_geom FROM pgr_dijkstra('SELECT gid AS id,source::integer,target::integer,length::double precision AS cost,x1, y1, x2, y2 FROM ways',%a ,%a, true, false) as di JOIN ways pt ON di.id2 = pt.gid" % (i, j)
    mini_route =  pd.read_sql_query(query,con=conn)
    for k,v in mini_route.iterrows():
        data = (v[0],v[1],v[2],v[3],v[4])    
        query =  "INSERT INTO opt_route_all_10p (seq, node, edge,cost,the_geom) VALUES (%s,%s,%s,%s,%s)"
        cursor.execute(query,(v[0],v[1],v[2],v[3],v[4]))
    conn.commit()

10/66
