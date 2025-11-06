#!/usr/bin/env python3
"""
Weather Aggregator Service (UC-6)
Fetches weather data from external APIs and publishes to Kafka
"""

import asyncio
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from confluent_kafka import Producer
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Simple circuit breaker for external API calls"""

    def __init__(self, failure_threshold=3, timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker entering HALF_OPEN state")
            else:
                raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                logger.info("Circuit breaker CLOSED")
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.error(f"Circuit breaker OPEN after {self.failure_count} failures")

            raise e


class WeatherAggregator:
    """Weather data aggregation service"""

    def __init__(self, kafka_bootstrap_servers: str):
        self.producer = Producer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'client.id': 'weather-aggregator'
        })
        self.circuit_breaker = CircuitBreaker()
        self.cache = {}
        self.cache_ttl = 900  # 15 minutes

    def fetch_weather_data(self, location_id: str) -> Optional[Dict]:
        """Fetch weather data from external API (mocked)"""

        # Mock weather API response
        # In production, this would call OpenWeatherMap, WeatherAPI, etc.
        data = {
            "location_id": location_id,
            "latitude": 55.4038 + random.uniform(-0.1, 0.1),
            "longitude": 10.4024 + random.uniform(-0.1, 0.1),
            "observation_time": datetime.utcnow().isoformat() + "Z",
            "temperature_celsius": 15.0 + random.uniform(-5, 10),
            "humidity_percent": 60.0 + random.uniform(-20, 20),
            "pressure_hpa": 1013.0 + random.uniform(-10, 10),
            "wind_speed_mps": 5.0 + random.uniform(0, 10),
            "wind_direction_degrees": random.uniform(0, 360),
            "conditions": random.choice(["Clear", "Cloudy", "Rainy", "Partly Cloudy"])
        }

        # Simulate API latency
        time.sleep(random.uniform(0.1, 0.5))

        return data

    def fetch_with_circuit_breaker(self, location_id: str) -> Optional[Dict]:
        """Fetch weather data with circuit breaker"""
        try:
            return self.circuit_breaker.call(self.fetch_weather_data, location_id)
        except Exception as e:
            logger.error(f"Failed to fetch weather data: {e}")

            # Try to return cached data
            if location_id in self.cache:
                cache_entry = self.cache[location_id]
                if time.time() - cache_entry['timestamp'] < 3600:  # 1 hour cache
                    logger.info(f"Returning cached data for {location_id}")
                    return cache_entry['data']

            return None

    def validate_data_quality(self, data: Dict) -> Dict:
        """Validate data quality and flag anomalies"""

        quality = {
            "is_fresh": True,
            "age_seconds": 0,
            "is_complete": True,
            "missing_fields": [],
            "units_validated": True,
            "anomalies": []
        }

        # Check required fields
        required_fields = ["temperature_celsius", "humidity_percent", "pressure_hpa"]
        for field in required_fields:
            if field not in data or data[field] is None:
                quality["is_complete"] = False
                quality["missing_fields"].append(field)

        # Check for anomalies
        if data.get("temperature_celsius", 0) < -50 or data.get("temperature_celsius", 0) > 60:
            quality["anomalies"].append({
                "field_name": "temperature_celsius",
                "description": "Temperature out of expected range",
                "confidence_score": 0.9
            })

        if data.get("humidity_percent", 0) < 0 or data.get("humidity_percent", 0) > 100:
            quality["anomalies"].append({
                "field_name": "humidity_percent",
                "description": "Humidity out of valid range",
                "confidence_score": 1.0
            })

        return quality

    def publish_weather_event(self, observations: List[Dict]):
        """Publish weather data update event to Kafka"""

        event = {
            "metadata": {
                "event_id": f"weather-{int(time.time())}",
                "event_type": "weather.data.updated",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "source_service": "weather-aggregator",
                "schema_version": 1
            },
            "source_provider": "MockWeatherAPI",
            "observations": observations,
            "quality": {
                "total_observations": len(observations),
                "valid_observations": sum(1 for obs in observations if obs.get("quality", {}).get("is_complete", False))
            },
            "ingested_at": datetime.utcnow().isoformat() + "Z"
        }

        # Publish to Kafka
        self.producer.produce(
            'weather.data.updated',
            key=event["source_provider"],
            value=json.dumps(event),
            callback=lambda err, msg: logger.error(f"Delivery failed: {err}") if err else None
        )
        self.producer.flush()

        logger.info(f"Published weather event with {len(observations)} observations")

    async def run(self):
        """Main aggregation loop"""

        locations = ["ODN-FACTORY-01", "ODN-FACTORY-02", "ODN-FACTORY-03"]

        logger.info("Weather Aggregator Service started")

        while True:
            observations = []

            for location_id in locations:
                data = self.fetch_with_circuit_breaker(location_id)

                if data:
                    # Validate quality
                    quality = self.validate_data_quality(data)
                    data["quality"] = quality

                    # Cache the data
                    self.cache[location_id] = {
                        "data": data,
                        "timestamp": time.time()
                    }

                    observations.append(data)

            # Publish aggregated data
            if observations:
                self.publish_weather_event(observations)

            # Wait before next fetch (15 minutes)
            await asyncio.sleep(900)


def main():
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    aggregator = WeatherAggregator(kafka_servers)

    try:
        asyncio.run(aggregator.run())
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    main()
