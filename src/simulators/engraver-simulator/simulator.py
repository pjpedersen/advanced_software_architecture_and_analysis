#!/usr/bin/env python3
"""
Engraver Simulator - Simulates laser engraving station
Subscribes to engraving requests and simulates engraving operations
"""

import json
import logging
import os
import random
import time
from datetime import datetime
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EngraverSimulator:
    """Simulates a laser engraving station"""

    def __init__(self, engraver_id: str, mqtt_broker: str, mqtt_port: int):
        self.engraver_id = engraver_id
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.running = False

        # Engraver state
        self.laser_power = 0.0  # Watts
        self.engraving_speed = 0.0  # mm/s
        self.jobs_completed = 0
        self.current_job = None

        # Initialize MQTT client
        self.client = mqtt.Client(client_id=f"engraver_{engraver_id}")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        logger.info(f"Engraver Simulator initialized for {engraver_id}")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to MQTT broker")
            # Subscribe to engraving request topic
            client.subscribe(f"engraver/{self.engraver_id}/request")
            logger.info(f"Subscribed to engraver/{self.engraver_id}/request")
        else:
            logger.error(f"Failed to connect, return code {rc}")

    def on_message(self, client, userdata, msg):
        """Handle incoming engraving requests"""
        try:
            request = json.loads(msg.payload.decode())
            logger.info(f"Received engraving request: {request.get('job_id')}")
            self.process_engraving_job(request)
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def process_engraving_job(self, request: dict):
        """Simulate engraving process"""
        job_id = request.get('job_id')
        unit_id = request.get('unit_id')
        engraving_data = request.get('engraving_data', {})

        logger.info(f"Starting engraving job {job_id} for device {unit_id}")

        # Simulate engraving parameters
        self.laser_power = 40.0  # Watts
        self.engraving_speed = 300.0  # mm/s

        # Calculate engraving time based on text length
        text = engraving_data.get('text', '')
        engraving_time = len(text) * 0.5 + random.uniform(2, 5)

        # Publish progress updates
        for progress in [25, 50, 75]:
            time.sleep(engraving_time / 4)
            self.publish_progress(job_id, unit_id, progress)

        # Final completion
        time.sleep(engraving_time / 4)

        # 5% chance of failure
        success = random.random() > 0.05

        self.publish_completion(job_id, unit_id, "SUCCESS" if success else "FAILED")
        self.jobs_completed += 1

        self.laser_power = 0.0
        self.engraving_speed = 0.0

    def publish_progress(self, job_id: str, unit_id: str, progress: int):
        """Publish engraving progress"""
        data = {
            "engraver_id": self.engraver_id,
            "job_id": job_id,
            "unit_id": unit_id,
            "progress_percent": progress,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        topic = f"engraver/{self.engraver_id}/progress"
        self.client.publish(topic, json.dumps(data), qos=1)
        logger.info(f"Engraving progress: {progress}%")

    def publish_completion(self, job_id: str, unit_id: str, status: str):
        """Publish engraving completion"""
        data = {
            "engraver_id": self.engraver_id,
            "job_id": job_id,
            "unit_id": unit_id,
            "status": status,
            "completed_at": datetime.utcnow().isoformat() + "Z",
            "jobs_completed": self.jobs_completed
        }

        topic = f"engraver/{self.engraver_id}/complete"
        self.client.publish(topic, json.dumps(data), qos=1)
        logger.info(f"Engraving job {job_id} completed with status: {status}")

    def publish_status(self):
        """Publish engraver status"""
        data = {
            "engraver_id": self.engraver_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "status": {
                "operational": True,
                "laser_power_watts": self.laser_power,
                "engraving_speed_mm_s": self.engraving_speed,
                "jobs_completed": self.jobs_completed,
                "temperature_celsius": round(random.uniform(20, 30), 1),
                "working": self.laser_power > 0
            }
        }

        topic = f"engraver/{self.engraver_id}/status"
        self.client.publish(topic, json.dumps(data), qos=1)

    def run(self):
        """Run the engraver simulator"""
        self.running = True

        try:
            self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
            self.client.loop_start()

            logger.info(f"Engraver Simulator {self.engraver_id} started")

            while self.running:
                # Publish status every 5 seconds
                self.publish_status()
                time.sleep(5)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.stop()

    def stop(self):
        """Stop the simulator"""
        self.running = False
        self.client.loop_stop()
        self.client.disconnect()
        logger.info(f"Engraver Simulator {self.engraver_id} stopped")


def main():
    engraver_id = os.getenv('ENGRAVER_ID', 'E1')
    mqtt_broker = os.getenv('MQTT_BROKER', 'localhost')
    mqtt_port = int(os.getenv('MQTT_PORT', '1883'))

    simulator = EngraverSimulator(engraver_id, mqtt_broker, mqtt_port)
    simulator.run()


if __name__ == '__main__':
    main()
