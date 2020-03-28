import sqlite3
conn=sqlite3.connect('res/readings.db')
c=conn.cursor()
node1 = "node3"
c.execute("""SELECT readings.read_seq FROM readings WHERE readings.node_name = (?) ORDER BY readings.read_seq DESC LIMIT 1""", (node1,))
xid = c.fetchall()
print(len(xid))

