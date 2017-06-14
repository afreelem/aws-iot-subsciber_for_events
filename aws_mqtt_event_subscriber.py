# ####Subscribes to Data Published to AWS Cloud

import paho.mqtt.client as mqtt
import os
import socket
import ssl
import json
import db_mysql_connection
from datetime import datetime
mysqldb = db_mysql_connection.dbsql()
path1="/home/deependra/Desktop/event_connection"
path2 = "/home/deependra/Desktop/event_subscribed"

def on_connect(client, userdata, flags, rc):
    print("Connection returned result: " + str(rc) )
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("events" , 1 )



def csv_writer(data, path):
    """
    Write data to a CSV file path
    """
    with open(path, "a") as csv_file:
        csv_file.write(data)
        csv_file.write("\n")




def on_message(client, userdata, msg):
    a=(msg.payload).decode("utf-8")
    Parsed_data = json.loads(a)
    global row

    if Parsed_data['eventType']=='connected' or Parsed_data['eventType']=='Connected':
        row=("'{0}','{1}','{2}','{3}','{4}','{5}' ".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),Parsed_data['clientId'], Parsed_data['timestamp'], Parsed_data['eventType'], Parsed_data['sessionIdentifier'],Parsed_data['principalIdentifier']))
        csv_writer(row,path1)
        query = "INSERT INTO connected_user(dtime,client_id,timestamp,event_type,session_id,cert_id) VALUES ({0});".format(row)
        mysqldb.query_in(sql=query)


    elif Parsed_data['eventType']=='disconnected' or Parsed_data['eventType']=='Disconnected':
        row=("'{0}','{1}','{2}','{3}','{4}','{5}' ".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),Parsed_data['clientId'], Parsed_data['timestamp'], Parsed_data['eventType'], Parsed_data['sessionIdentifier'],Parsed_data['principalIdentifier']))
        csv_writer(row, path1)
        query="Delete from connected_user where session_id = '{0}' ".format(Parsed_data['sessionIdentifier'])
        mysqldb.query_in(sql=query)
        query="Delete from subscribed_user where session_id='{0}'".format(Parsed_data['sessionIdentifier'])
        mysqldb.query_in(sql=query)


    elif Parsed_data['eventType']=='subscribed' or Parsed_data['eventType']=='Subscribed':

        row=("'{0}','{1}','{2}','{3}','{4}','{5}'".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),Parsed_data['clientId'], Parsed_data['timestamp'],".".join(Parsed_data['topics']), Parsed_data['sessionIdentifier'],Parsed_data['principalIdentifier']))
        csv_writer(row, path2)
        query = "INSERT INTO subscribed_user(dtime,client_id,timestamp,topics,session_id,cert_id) VALUES ({0});".format(row)
        mysqldb.query_in(sql=query)


    elif Parsed_data['eventType']=='unsubscribed' or Parsed_data['eventType']=='Unsubscribed':
        row=("'{0}','{1}','{2}','{3}','{4}','{5}'".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),Parsed_data['clientId'], Parsed_data['timestamp'],".".join(Parsed_data['topics']), Parsed_data['sessionIdentifier'],Parsed_data['principalIdentifier']))
        csv_writer(row, path2)
        query="Delete from subscribed_user where session_id='{0}'and topics='{1}'".format(Parsed_data['sessionIdentifier'],".".join(Parsed_data['topics']))
        mysqldb.query_in(sql=query)



    print(row)


mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_message = on_message



####### Aws Configuration Details ##############

awshost = ""
awsport = 8883
clientId = "myLaptop"
thingName = "myThingName"
caPath = "/home/deependra/aws/deep_aws/root-CA.crt"
certPath = "/home/deependra/aws/deep_aws/deependra.cert.pem"
keyPath = "/home/deependra/aws/deep_aws/deependra.private.key"

#################################################

mqttc.tls_set(caPath, certfile=certPath, keyfile=keyPath, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

mqttc.connect(awshost, awsport, keepalive=60)

mqttc.loop_forever()

mysqldb.close()
