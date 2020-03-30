############
# Jake Burton
# 40278490
# Final Honours Project
# Sink Main Script
# 
# Uses the mosquitto broker, along with Eclipse Paho's MQTT library
############
import paho.mqtt.client as mqtt
import json
import sqlite3
import time
############
# Unsure Purpose of this method, don't want to remove unless its necessary
############
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
###########
# The callback for when the client receives a CONNACK response from the server.
###########
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe([("test",0),("lastread",0)])
###########
# The callback for when a PUBLISH message is received.
# Takes the message in a JSON format and separates out the indivvidual elements of the 
# message.
# If the topic of the message is "lastread" then the method returns the sequence identifier for the
# last reading that the sink received from the given node. Message format for this is 
# '{"name": "nodex"}' meaning that the database will look for readings from this node.
# If the message received has the topic "test", the sink will add the contents of the message to a database.
# Method does nothing if message topic isn't lastread or test
###########
def on_message(client, userdata, message):
    if message.topic == "test":
        readings_json = json.loads(message.payload.decode('utf-8'))
        conn=sqlite3.connect('res/readings.db')
        c=conn.cursor()
        c.execute("SELECT id FROM readings ORDER BY id DESC LIMIT 1")
        index = c.fetchall()
        ireading = index[0][0]
        ireading = ireading+1
        t = (readings_json['timestamp'][0],readings_json['timestamp'][1],readings_json['timestamp'][2],readings_json['timestamp'][3],readings_json['timestamp'][4],readings_json['timestamp'][5],readings_json['timestamp'][6],readings_json['timestamp'][7],0)
        y = (time.asctime(time.localtime(time.mktime(t))))
        c.execute("""INSERT INTO readings VALUES ((?),(?),(?),(?),(?),(?))""", [ireading,readings_json['id'],readings_json['name'],y,readings_json['type'],readings_json['value']])
        conn.commit()
        conn.close()
    if message.topic == "lastread":
        msggg = json.loads(message.payload)
        conn=sqlite3.connect('res/readings.db')
        c=conn.cursor()
        c.execute("""SELECT readings.read_seq FROM readings WHERE readings.node_name = (?) 
ORDER BY readings.read_seq DESC LIMIT 1""", (msggg['name'],))
        xid = c.fetchall()
        print(xid)
        if(len(xid) == 0):
            lastread = -1
        else:
            lastread = xid[0][0]
        jdict = {"reading_id":lastread}
        jmsg = json.dumps(jdict)
        mqttc.publish(topic=msggg.get("name"),payload=jmsg)
###########
# Sets mqtt client credentials and connects to the broker on localhost.
##########
mqttc=mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.username_pw_set("jakepi","jbpi1234")
mqttc.connect("localhost",1883,60)
mqttc.loop_start()
###########
# Loops the main method until interupted by the message handler.
##########
while True:
    mqttc.loop()
