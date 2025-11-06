#!/usr/bin/env python3
"""
Validation Station Simulator - Simulates automated validation test station
Runs various tests on IoT devices and reports results
"""

import json
import logging
import os
import random
import time
from datetime import datetime
from typing import List, Dict
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ValidationStationSimulator:
    """Simulates an automated validation test station"""

    def __init__(self, station_id: str, mqtt_broker: str, mqtt_port: int):
        self.station_id = station_id
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.running = False

        # Test definitions
        self.test_suite = [
            {
                "name": "WiFi Connectivity",
                "duration": 5,
                "pass_rate": 0.98,
                "value_range": (-60, -30),  # dBm
                "unit": "dBm"
            },
            {
                "name": "Temperature Sensor Accuracy",
                "duration": 3,
                "pass_rate": 0.95,
                "value_range": (0.0, 1.0),  # °C error
                "unit": "°C"
            },
            {
                "name": "Humidity Sensor Accuracy",
                "duration": 3,
                "pass_rate": 0.95,
                "value_range": (0.0, 2.0),  # % error
                "unit": "%"
            },
            {
                "name": "Pressure Sensor Accuracy",
                "duration": 3,
                "pass_rate": 0.96,
                "value_range": (0.0, 5.0),  # hPa error
                "unit": "hPa"
            },
            {
                "name": "Display Brightness",
                "duration": 2,
                "pass_rate": 0.97,
                "value_range": (300, 500),  # nits
                "unit": "nits"
            },
            {
                "name": "Power Consumption",
                "duration": 4,
                "pass_rate": 0.99,
                "value_range": (2.5, 4.0),  # Watts
                "unit": "W"
            },
            {
                "name": "Button Response Time",
                "duration": 2,
                "pass_rate": 0.99,
                "value_range": (50, 150),  # ms
                "unit": "ms"
            },
            {
                "name": "GPS Accuracy",
                "duration": 6,
                "pass_rate": 0.93,
                "value_range": (0, 10),  # meters
                "unit": "m"
            }
        ]

        self.tests_completed = 0

        # Initialize MQTT client
        self.client = mqtt.Client(client_id=f"validation_{station_id}")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        logger.info(f"Validation Station Simulator initialized for {station_id}")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to MQTT broker")
            client.subscribe(f"validation/{self.station_id}/request")
            logger.info(f"Subscribed to validation/{self.station_id}/request")
        else:
            logger.error(f"Failed to connect, return code {rc}")

    def on_message(self, client, userdata, msg):
        """Handle incoming validation requests"""
        try:
            request = json.loads(msg.payload.decode())
            logger.info(f"Received validation request for device: {request.get('unit_id')}")
            self.run_validation_tests(request)
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run_validation_tests(self, request: dict):
        """Run validation test suite"""
        unit_id = request.get('unit_id')
        device_id = request.get('device_id')

        logger.info(f"Starting validation tests for device {unit_id}")

        test_results = []
        total_duration = sum(test['duration'] for test in self.test_suite)
        elapsed = 0

        for test_config in self.test_suite:
            # Simulate test execution
            time.sleep(test_config['duration'])
            elapsed += test_config['duration']

            # Determine if test passes
            passed = random.random() < test_config['pass_rate']

            # Generate test value
            min_val, max_val = test_config['value_range']
            if passed:
                value = random.uniform(min_val, max_val)
            else:
                # Generate out-of-range value for failure
                value = random.uniform(max_val, max_val * 1.5)

            test_result = {
                "test_name": test_config['name'],
                "passed": passed,
                "value": round(value, 2),
                "unit": test_config['unit'],
                "lower_limit": min_val,
                "upper_limit": max_val,
                "duration_seconds": test_config['duration']
            }

            test_results.append(test_result)

            # Publish progress
            progress = int((elapsed / total_duration) * 100)
            self.publish_progress(unit_id, progress)

        # Calculate overall status
        passed_count = sum(1 for t in test_results if t['passed'])
        overall_status = "PASS" if passed_count == len(test_results) else "FAIL"

        # Publish completion
        self.publish_completion(device_id, unit_id, test_results, overall_status)
        self.tests_completed += 1

    def publish_progress(self, unit_id: str, progress: int):
        """Publish validation progress"""
        data = {
            "station_id": self.station_id,
            "unit_id": unit_id,
            "progress_percent": progress,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        topic = f"validation/{self.station_id}/progress"
        self.client.publish(topic, json.dumps(data), qos=1)

    def publish_completion(self, device_id: str, unit_id: str, test_results: List[Dict], status: str):
        """Publish validation completion"""
        data = {
            "metadata": {
                "event_id": f"val_{int(time.time())}",
                "event_type": "validation.test.complete",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source_service": f"validation-station-{self.station_id}"
            },
            "device_id": device_id,
            "unit_id": unit_id,
            "station_id": self.station_id,
            "tests": test_results,
            "summary": {
                "total_tests": len(test_results),
                "passed_tests": sum(1 for t in test_results if t['passed']),
                "failed_tests": sum(1 for t in test_results if not t['passed']),
                "status": status
            }
        }

        topic = f"validation/{self.station_id}/complete"
        self.client.publish(topic, json.dumps(data), qos=1)
        logger.info(f"Validation completed for device {unit_id} with status: {status}")

    def publish_status(self):
        """Publish station status"""
        data = {
            "station_id": self.station_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": {
                "operational": True,
                "tests_completed": self.tests_completed,
                "current_device": None,
                "ready": True
            }
        }

        topic = f"validation/{self.station_id}/status"
        self.client.publish(topic, json.dumps(data), qos=1)

    def run(self):
        """Run the validation station simulator"""
        self.running = True

        try:
            self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
            self.client.loop_start()

            logger.info(f"Validation Station Simulator {self.station_id} started")

            while self.running:
                # Publish status every 10 seconds
                self.publish_status()
                time.sleep(10)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.stop()

    def stop(self):
        """Stop the simulator"""
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()
        logger.info(f"Validation Station Simulator {self.station_id} stopped")


def main():
    station_id = os.getenv('STATION_ID', 'VS1')
    mqtt_broker = os.getenv('MQTT_BROKER', 'localhost')
    mqtt_port = int(os.getenv('MQTT_PORT', '1883'))

    simulator = ValidationStationSimulator(station_id, mqtt_broker, mqtt_port)
    simulator.run()


if __name__ == '__main__':
    main()
