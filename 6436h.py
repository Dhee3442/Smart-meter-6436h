import minimalmodbus
import struct
import json
import time
from datetime import datetime
import paho.mqtt.client as mqtt
import os

# --------------------- Modbus & MQTT Setup ---------------------

PORT = '/dev/ttyUSB0'
BAUDRATE = 9600
METER_IDS = [1]

MQTT_BROKER = "mqtt.sworks.co.in"
MQTT_PORT = 1883
MQTT_TOPIC = "smart-meter/kochhar"
MQTT_USERNAME = "sworks"
MQTT_PASSWORD = "S@works@1231"

client = mqtt.Client()
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_start()

PF_CORRECTION = {
    1: 0.85,
    4: 0.84,
    6: 0.50,
    8: 0.50,
    9: 0.50,
    12: 0.50,
    14: 0.50,
    16: 0.50,
    18: 0.50
}

RETRY_COUNT = 3
READ_DELAY = 0.3
WATCHDOG_FILE = "/tmp/last_publish_time"

# --------------------- Modbus Read Functions ---------------------

def create_meter(meter_id):
    m = minimalmodbus.Instrument(PORT, meter_id)
    m.serial.baudrate = BAUDRATE
    m.serial.parity = minimalmodbus.serial.PARITY_EVEN
    m.serial.bytesize = 8
    m.serial.stopbits = 1
    m.serial.timeout = 1
    m.mode = minimalmodbus.MODE_RTU
    m.clear_buffers_before_each_transaction = True
    return m

def read_float_cdab(instrument, register):
    for attempt in range(RETRY_COUNT):
        try:
            regs = instrument.read_registers(register, 2, functioncode=3)
            print(f"[ID {instrument.address}] Register {register} raw: {regs}")
            if len(regs) == 2:
                b1 = (regs[0] >> 8) & 0xFF
                b2 = regs[0] & 0xFF
                b3 = (regs[1] >> 8) & 0xFF
                b4 = regs[1] & 0xFF
                raw_bytes = bytes([b1, b2, b3, b4])
                return round(struct.unpack('>f', raw_bytes)[0], 3)
        except Exception as e:
            print(f"[ID {instrument.address}] Error reading register {register} (try {attempt+1}): {e}")
        time.sleep(READ_DELAY)
    return None

def read_energy_kwh(instrument):
    for attempt in range(RETRY_COUNT):
        try:
            regs = instrument.read_registers(2699, 2, functioncode=3)
            print(f"[ID {instrument.address}] kWh registers: {regs}")
            if len(regs) == 2:
                return round(struct.unpack('>f', struct.pack('>HH', regs[0], regs[1]))[0], 3)
        except Exception as e:
            print(f"[ID {instrument.address}] Error reading energy (try {attempt+1}): {e}")
        time.sleep(READ_DELAY)
    return None

# --------------------- Main Loop ---------------------

if __name__ == "__main__":
    while True:
        print(f"\n========== Reading at {datetime.now().isoformat()} ==========")
        for meter_id in METER_IDS:
            meter = create_meter(meter_id)

            current = read_float_cdab(meter, 3009)
            time.sleep(READ_DELAY)

            voltage = read_float_cdab(meter, 3027)
            time.sleep(READ_DELAY)

            pf_raw = read_float_cdab(meter, 3083)
            time.sleep(READ_DELAY)

            pf = round(pf_raw * PF_CORRECTION.get(meter_id, 1), 3) if pf_raw is not None else None

            kwh = read_energy_kwh(meter)

            payload = {
                "timestamp": datetime.now().isoformat(),
                "meter_id": meter_id,
                "Avg Current": current,
                "Avg Voltage": voltage,
                "Avg PF": pf,
                "kWh": kwh
            }

            print(json.dumps(payload, indent=2))

            try:
                client.publish(MQTT_TOPIC, json.dumps(payload), qos=1)
                print(f"✅ Meter {meter_id} data published to topic: {MQTT_TOPIC}")

                # ✅ Update watchdog file on success
                with open(WATCHDOG_FILE, "w") as f:
                    f.write(datetime.now().isoformat())

            except Exception as e:
                print(f"❌ MQTT publish error for meter {meter_id}: {e}")

        time.sleep(300)
