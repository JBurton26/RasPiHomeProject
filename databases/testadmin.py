import sqlite3
import random
conn = sqlite3.connect('sensors.db')
c=conn.cursor()
c.execute("SELECT id FROM readings ORDER BY id DESC LIMIT 1")
index = c.fetchall()
#print(index[0][0]
i = index[0][0]
c.execute("SELECT id FROM nodes ORDER BY id DESC LIMIT 1")
node = c.fetchall()
nodecount = node[0][0]
for x in range(10):
	i = i+1
	nodeid = random.randint(1,nodecount)
	temp = random.randint(0,30)
	hum = random.randint(5,100)
	c.execute("""INSERT INTO readings VALUES((?),(?),datetime('now'),(?),(?))""",(i,nodeid,temp,hum))
conn.commit()
conn.close()
