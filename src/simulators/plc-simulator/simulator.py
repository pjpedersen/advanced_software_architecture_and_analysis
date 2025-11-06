#!/usr/bin/env python3
"""
PLC Simulator - Simulates industrial PLC controllers
Publishes sensor data via MQTT for injection molding and assembly stations
"""

import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Dict
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PLCSimulator:
    """Simulates a PLC controller with various sensors"""

    def __init__(self, station_id: str, mqtt_broker: str, mqtt_port: int):
        self.station_id = station_id
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.running = False

        # Sensor baseline values
        self.temperature = 220.0  # °C for injection molding
        self.pressure = 150.0  # bar
        self.cycle_count = 0
        self.error_rate = 0.02  # 2% chance of anomaly

        # Initialize MQTT client
        self.client = mqtt.Client(client_id=f"plc_{station_id}")
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

        logger.info(f"PLC Simulator initialized for station {station_id}")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to MQTT broker at {self.mqtt_broker}:{self.mqtt_port}")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            logger.warning(f"Unexpected disconnection from MQTT broker")

    def generate_sensor_data(self) -> Dict:
        """Generate simulated sensor data"""
        # Add some realistic variation
        temp_variation = random.gauss(0, 2)  # ±2°C variation
        pressure_variation = random.gauss(0, 5)  # ±5 bar variation

        # Occasional anomalies
        if random.random() < self.error_rate:
            temp_variation += random.choice([-20, 20])  # Large spike
            logger.warning(f"Simulating temperature anomaly at station {self.station_id}")

        data = {
            "station_id": self.station_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "sensors": {
                "mold_temperature_celsius": round(self.temperature + temp_variation, 1),
                "injection_pressure_bar": round(self.pressure + pressure_variation, 1),
                "cycle_time_seconds": round(random.uniform(25, 35), 1),
                "cooling_time_seconds": round(random.uniform(15, 20), 1),
                "vibration_mm_s": round(random.uniform(0.5, 2.0), 2),
                "power_consumption_kw": round(random.uniform(15, 25), 1)
            },
            "status": {
                "operational": True,
                "cycle_count": self.cycle_count,
                "last_maintenance": "2025-01-01T00:00:00Z",
                "error_code": None
            },
            "current_batch": {
                "batch_id": f"B{random.randint(1000, 9999)}",
                "color": random.choice(["BLACK", "GRAY", "WHITE"]),
                "units_produced": self.cycle_count % 100
            }
        }

        self.cycle_count += 1
        return data

    def publish_sensor_data(self, data: Dict):
        """Publish sensor data to MQTT"""
        topic = f"plc/{self.station_id}/sensors"
        payload = json.dumps(data)

        try:
            result = self.client.publish(topic, payload, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.debug(f"Published sensor data to {topic}")
            else:
                logger.error(f"Failed to publish to {topic}, rc={result.rc}")
        except Exception as e:
            logger.error(f"Error publishing sensor data: {e}")

    def run(self, interval: float = 5.0):
        """Run the PLC simulator"""
        self.running = True

        try:
            # Connect to MQTT broker
            self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
            self.client.loop_start()

            logger.info(f"PLC Simulator {self.station_id} started (publishing every {interval}s)")

            while self.running:
                # Generate and publish sensor data
                sensor_data = self.generate_sensor_data()
                self.publish_sensor_data(sensor_data)

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.stop()

    def stop(self):
        """Stop the simulator"""
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()
        logger.info(f"PLC Simulator {self.station_id} stopped")


def main():
    # Configuration
    station_id = os.getenv('STATION_ID', 'P1')
    mqtt_broker = os.getenv('MQTT_BROKER', 'localhost')
    mqtt_port = int(os.getenv('MQTT_PORT', '1883'))
    interval = float(os.getenv('PUBLISH_INTERVAL', '5.0'))

    # Create and run simulator
    simulator = PLCSimulator(station_id, mqtt_broker, mqtt_port)
    simulator.run(interval)


if __name__ == '__main__':
    main()
