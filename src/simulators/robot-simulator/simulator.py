#!/usr/bin/env python3
"""
Robot Arm Simulator - Simulates industrial robot arm for material handling
Publishes position, speed, and load data via MQTT
"""

import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Dict, List
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RobotArmSimulator:
    """Simulates a 6-axis industrial robot arm"""

    def __init__(self, robot_id: str, mqtt_broker: str, mqtt_port: int):
        self.robot_id = robot_id
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.running = False

        # Robot state
        self.position = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]  # 6-axis position in degrees
        self.target_position = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        self.current_task = "IDLE"
        self.pick_count = 0

        # Predefined positions
        self.home_position = [0.0, -45.0, 90.0, 0.0, 45.0, 0.0]
        self.pick_position = [30.0, -60.0, 120.0, 0.0, 60.0, 0.0]
        self.place_position = [-30.0, -60.0, 120.0, 0.0, 60.0, 180.0]

        # Initialize MQTT client
        self.client = mqtt.Client(client_id=f"robot_{robot_id}")
        self.client.on_connect = lambda c, u, f, rc: logger.info(f"Robot {robot_id} connected to MQTT")

        logger.info(f"Robot Arm Simulator initialized for robot {robot_id}")

    def move_to_position(self, target: List[float], speed: float = 10.0):
        """Simulate smooth movement to target position"""
        for i in range(6):
            delta = target[i] - self.position[i]
            step = min(speed, abs(delta)) * (1 if delta > 0 else -1)
            self.position[i] += step

    def generate_robot_data(self) -> Dict:
        """Generate robot telemetry data"""
        # Simulate pick-and-place operation
        if self.current_task == "IDLE":
            self.target_position = self.pick_position
            self.current_task = "MOVING_TO_PICK"
        elif self.current_task == "MOVING_TO_PICK":
            self.move_to_position(self.target_position)
            if self.is_at_target():
                self.current_task = "PICKING"
        elif self.current_task == "PICKING":
            time.sleep(0.5)  # Gripper actuation
            self.target_position = self.place_position
            self.current_task = "MOVING_TO_PLACE"
        elif self.current_task == "MOVING_TO_PLACE":
            self.move_to_position(self.target_position)
            if self.is_at_target():
                self.current_task = "PLACING"
        elif self.current_task == "PLACING":
            time.sleep(0.5)  # Release gripper
            self.pick_count += 1
            self.target_position = self.home_position
            self.current_task = "RETURNING_HOME"
        elif self.current_task == "RETURNING_HOME":
            self.move_to_position(self.target_position)
            if self.is_at_target():
                self.current_task = "IDLE"

        data = {
            "robot_id": self.robot_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "position": {
                "joint1_deg": round(self.position[0], 2),
                "joint2_deg": round(self.position[1], 2),
                "joint3_deg": round(self.position[2], 2),
                "joint4_deg": round(self.position[3], 2),
                "joint5_deg": round(self.position[4], 2),
                "joint6_deg": round(self.position[5], 2)
            },
            "status": {
                "operational": True,
                "current_task": self.current_task,
                "pick_count": self.pick_count,
                "gripper_open": self.current_task in ["IDLE", "RETURNING_HOME", "MOVING_TO_PICK"],
                "load_kg": round(random.uniform(0.5, 2.0), 2) if "MOVING_TO_PLACE" in self.current_task else 0.0
            },
            "performance": {
                "speed_percent": round(random.uniform(80, 100), 1),
                "accuracy_mm": round(random.uniform(0.01, 0.05), 3),
                "cycle_time_seconds": round(random.uniform(4, 6), 1)
            }
        }

        return data

    def is_at_target(self) -> bool:
        """Check if robot is at target position"""
        return all(abs(self.position[i] - self.target_position[i]) < 1.0 for i in range(6))

    def publish_robot_data(self, data: Dict):
        """Publish robot data to MQTT"""
        topic = f"robot/{self.robot_id}/telemetry"
        payload = json.dumps(data)

        try:
            self.client.publish(topic, payload, qos=1)
            logger.debug(f"Published robot data: {self.current_task}")
        except Exception as e:
            logger.error(f"Error publishing robot data: {e}")

    def run(self, interval: float = 1.0):
        """Run the robot simulator"""
        self.running = True

        try:
            self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
            self.client.loop_start()

            logger.info(f"Robot Simulator {self.robot_id} started")

            while self.running:
                robot_data = self.generate_robot_data()
                self.publish_robot_data(robot_data)
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
        logger.info(f"Robot Simulator {self.robot_id} stopped")


def main():
    robot_id = os.getenv('ROBOT_ID', 'R1')
    mqtt_broker = os.getenv('MQTT_BROKER', 'localhost')
    mqtt_port = int(os.getenv('MQTT_PORT', '1883'))
    interval = float(os.getenv('PUBLISH_INTERVAL', '1.0'))

    simulator = RobotArmSimulator(robot_id, mqtt_broker, mqtt_port)
    simulator.run(interval)


if __name__ == '__main__':
    main()
