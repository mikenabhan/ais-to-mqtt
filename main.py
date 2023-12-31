import asyncio
import websockets
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timezone

api_key = ''

# MQTT configuration
mqtt_broker_address = ""
mqtt_base_topic = "ais_data"
mqtt_username = ""
mqtt_password = ""

async def connect_ais_stream():
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {"APIKey": api_key, "BoundingBoxes": [[[-11, -178], [30, 74]]], "FiltersShipMMSI": ["",""], "FilterMessageTypes": ["PositionReport"]}

        subscribe_message_json = json.dumps(subscribe_message)
        await websocket.send(subscribe_message_json)

        # Connect to MQTT broker with authentication
        mqtt_client = mqtt.Client()
        mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
        mqtt_client.connect(mqtt_broker_address)
        mqtt_client.loop_start()

        async for message_json in websocket:
            message = json.loads(message_json)
            ais_message = message['Message']['PositionReport']
            ship_id = ais_message['UserID']

            # Create AIS message string
            ais_info = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "shipId": ais_message['UserID'],
                "latitude": ais_message['Latitude'],
                "longitude": ais_message['Longitude'],
                "trueHeading": ais_message['TrueHeading'],
                "speedOverGround": ais_message['Sog'],
                 "courseOverGround": ais_message['Cog'],
                  "navigationalStatus": ais_message['NavigationalStatus']
            }

            # Publish AIS message to MQTT broker
            print(ais_info)
            mqtt_topic = f"{mqtt_base_topic}/{ship_id}"
            mqtt_client.publish(mqtt_topic, payload=json.dumps(ais_info), qos=1)

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())
