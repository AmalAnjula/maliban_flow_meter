"""
MQTT Test Publisher  –  pip install paho-mqtt
Simulates infeed / outfeed state changes on topic: ols/tx/status

Broker options (change BROKER below):
  "broker.hivemq.com"      – free public broker, no setup needed
  "test.mosquitto.org"     – free public broker
  "localhost"              – only if you have Mosquitto running locally
"""

import json, time
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion   # paho-mqtt v2+

BROKER = "broker.hivemq.com"   # ← free public broker, no install needed
PORT   = 1883
TOPIC  = "ols/tx/status"

client = mqtt.Client(CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT)
client.loop_start()

SEQUENCE = [
    {"infeed": False, "outfeed": False, "weight": 0.0,   "label": "IDLE"},
    {"infeed": True,  "outfeed": False, "weight": 25.5,  "label": "INFEED  ← oil coming in"},
    {"infeed": True,  "outfeed": False, "weight": 51.0,  "label": "INFEED  ← oil coming in"},
    {"infeed": True,  "outfeed": False, "weight": 99.4,  "label": "INFEED  ← oil coming in"},
    {"infeed": False, "outfeed": False, "weight": 99.4,  "label": "IDLE"},
    {"infeed": False, "outfeed": True,  "weight": 74.2,  "label": "OUTFEED → oil going out"},
    {"infeed": False, "outfeed": True,  "weight": 48.0,  "label": "OUTFEED → oil going out"},
    {"infeed": False, "outfeed": False, "weight": 48.0,  "label": "IDLE"},
]

print(f"Publishing to  {BROKER}:{PORT}  →  {TOPIC}\n")

for step in SEQUENCE:
    payload = {
        "ts":      int(time.time()),
        "weight":  step["weight"],
        "infeed":  step["infeed"],
        "outfeed": step["outfeed"],
    }
    client.publish(TOPIC, json.dumps(payload))
    print(f"  [{step['label']:30s}]  {json.dumps(payload)}")
    time.sleep(3)

print("\nDone.")
client.loop_stop()
client.disconnect()