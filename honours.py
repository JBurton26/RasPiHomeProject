import paho.mqtt.client as mqtt
import json
import sqlite3


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe([("test",0),("lastread",0)])
# The callback for when a PUBLISH message is received.
def on_message(client, userdata, message):
    #print(message.payload.decode('utf-8'))
    print(message.topic)
    if message.topic == "test":
        readings_json = json.loads(message.payload.decode('utf-8'))
        print(readings_json.get("name"))
        conn=sqlite3.connect('res/readings.db')
        c=conn.cursor()
        c.execute("SELECT id FROM readings ORDER BY id DESC LIMIT 1")
        index = c.fetchall()
        ireading = index[0][0]
        ireading = ireading+1
        print("Reading Received: ",readings_json)
        c.execute("""INSERT INTO readings VALUES ((?), (?), (?), (?), (?), (?))""",(ireading, readings_json.get("id"), readings_json.get("name"), readings_json.get("timestamp"), readings_json.get("type"), readings_json.get("value")))
        conn.commit()
        conn.close()
    if message.topic == "lastread":
#        print("YYYYY")
        #readings_last = json.loads(str(message.payload))
        print(message.payload.decode('utf-8'))
        msggg = json.loads(message.payload)
        print(msggg.get("name"))
        conn=sqlite3.connect('res/readings.db')
        c=conn.cursor()
        c.execute("""SELECT readings.read_seq FROM readings WHERE readings.node_name = (?) 
ORDER BY readings.read_seq DESC LIMIT 1""", (msggg['name'],))
        xid = c.fetchall()
        print(xid)
        if(len(xid) == 0):
            lastread = -1
            print("LASTREAD IS:",lastread)
        else:
            lastread = xid[0][0]
            print("lastread is: ",lastread)
        jdict = {"reading_id":lastread}
        jmsg = json.dumps(jdict)
        print(jmsg)
        mqttc.publish(topic=msggg.get("name"),payload=jmsg)
        print("Message Sent:",msggg.get("name"))
        

mqttc=mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.username_pw_set("jakepi","jbpi1234")
mqttc.connect("localhost",1883,60)
mqttc.loop_start()

while True:
    mqttc.loop()
