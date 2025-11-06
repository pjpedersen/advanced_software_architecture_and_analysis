#!/usr/bin/env python3
"""
Data Quality Service - Validates incoming sensor and external data quality
Monitors for anomalies, missing data, and data integrity issues
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from confluent_kafka import Consumer, Producer, KafkaError
import statistics

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class DataQualityIssue:
    """Represents a data quality issue"""
    issue_id: str
    source: str
    field_name: str
    issue_type: str  # MISSING, ANOMALY, OUT_OF_RANGE, DUPLICATE
    severity: str    # LOW, MEDIUM, HIGH, CRITICAL
    description: str
    detected_at: str
    value: Optional[Any] = None


@dataclass
class DataQualityReport:
    """Data quality report"""
    report_id: str
    source: str
    timestamp: str
    total_records: int
    valid_records: int
    invalid_records: int
    completeness_score: float  # 0-100
    accuracy_score: float      # 0-100
    timeliness_score: float    # 0-100
    overall_score: float       # 0-100
    issues: List[Dict]


class DataQualityValidator:
    """Validates data quality for various data sources"""

    def __init__(self):
        self.history = {}  # Store recent values for anomaly detection
        self.max_history = 100

    def validate_weather_data(self, data: Dict) -> List[DataQualityIssue]:
        """Validate weather data quality"""
        issues = []

        # Check completeness
        required_fields = ['temperature_celsius', 'humidity_percent', 'pressure_hpa']
        for field in required_fields:
            if field not in data or data[field] is None:
                issues.append(DataQualityIssue(
                    issue_id=f"missing_{field}_{int(time.time())}",
                    source="weather_data",
                    field_name=field,
                    issue_type="MISSING",
                    severity="HIGH",
                    description=f"Required field {field} is missing",
                    detected_at=datetime.utcnow().isoformat()
                ))

        # Check range validity
        if 'temperature_celsius' in data:
            temp = data['temperature_celsius']
            if not isinstance(temp, (int, float)) or temp < -50 or temp > 60:
                issues.append(DataQualityIssue(
                    issue_id=f"range_temperature_{int(time.time())}",
                    source="weather_data",
                    field_name="temperature_celsius",
                    issue_type="OUT_OF_RANGE",
                    severity="MEDIUM",
                    description=f"Temperature {temp}°C is outside valid range [-50, 60]",
                    detected_at=datetime.utcnow().isoformat(),
                    value=temp
                ))

        if 'humidity_percent' in data:
            humidity = data['humidity_percent']
            if not isinstance(humidity, (int, float)) or humidity < 0 or humidity > 100:
                issues.append(DataQualityIssue(
                    issue_id=f"range_humidity_{int(time.time())}",
                    source="weather_data",
                    field_name="humidity_percent",
                    issue_type="OUT_OF_RANGE",
                    severity="MEDIUM",
                    description=f"Humidity {humidity}% is outside valid range [0, 100]",
                    detected_at=datetime.utcnow().isoformat(),
                    value=humidity
                ))

        # Check timeliness
        if 'timestamp' in data:
            data_time = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            age = (datetime.utcnow().replace(tzinfo=data_time.tzinfo) - data_time).total_seconds()
            if age > 600:  # More than 10 minutes old
                issues.append(DataQualityIssue(
                    issue_id=f"stale_data_{int(time.time())}",
                    source="weather_data",
                    field_name="timestamp",
                    issue_type="ANOMALY",
                    severity="LOW",
                    description=f"Data is {age/60:.1f} minutes old",
                    detected_at=datetime.utcnow().isoformat()
                ))

        # Anomaly detection using simple statistical methods
        if 'temperature_celsius' in data:
            temp = data['temperature_celsius']
            if 'temperature' not in self.history:
                self.history['temperature'] = []

            self.history['temperature'].append(temp)
            if len(self.history['temperature']) > self.max_history:
                self.history['temperature'].pop(0)

            if len(self.history['temperature']) >= 10:
                mean = statistics.mean(self.history['temperature'])
                stdev = statistics.stdev(self.history['temperature'])

                # Check if value is more than 3 standard deviations from mean
                if abs(temp - mean) > 3 * stdev:
                    issues.append(DataQualityIssue(
                        issue_id=f"anomaly_temperature_{int(time.time())}",
                        source="weather_data",
                        field_name="temperature_celsius",
                        issue_type="ANOMALY",
                        severity="MEDIUM",
                        description=f"Temperature {temp}°C is {abs(temp - mean)/stdev:.1f}σ from mean {mean:.1f}°C",
                        detected_at=datetime.utcnow().isoformat(),
                        value=temp
                    ))

        return issues

    def validate_sensor_data(self, data: Dict) -> List[DataQualityIssue]:
        """Validate PLC/sensor data quality"""
        issues = []

        # Check for missing sensor ID
        if 'sensor_id' not in data:
            issues.append(DataQualityIssue(
                issue_id=f"missing_sensor_id_{int(time.time())}",
                source="sensor_data",
                field_name="sensor_id",
                issue_type="MISSING",
                severity="CRITICAL",
                description="Sensor ID is missing",
                detected_at=datetime.utcnow().isoformat()
            ))

        # Check for data staleness
        if 'timestamp' in data:
            data_time = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            age = (datetime.utcnow().replace(tzinfo=data_time.tzinfo) - data_time).total_seconds()
            if age > 30:  # More than 30 seconds old
                issues.append(DataQualityIssue(
                    issue_id=f"stale_sensor_{int(time.time())}",
                    source="sensor_data",
                    field_name="timestamp",
                    issue_type="ANOMALY",
                    severity="HIGH",
                    description=f"Sensor data is {age:.1f} seconds old",
                    detected_at=datetime.utcnow().isoformat()
                ))

        return issues

    def generate_quality_report(self, source: str, data_samples: List[Dict], issues: List[DataQualityIssue]) -> DataQualityReport:
        """Generate a comprehensive data quality report"""
        total_records = len(data_samples)
        invalid_records = len([i for i in issues if i.severity in ['HIGH', 'CRITICAL']])
        valid_records = total_records - invalid_records

        # Calculate completeness score
        completeness_score = 100.0
        if total_records > 0:
            missing_count = len([i for i in issues if i.issue_type == 'MISSING'])
            completeness_score = max(0, 100 - (missing_count / total_records * 100))

        # Calculate accuracy score
        accuracy_score = 100.0
        if total_records > 0:
            error_count = len([i for i in issues if i.issue_type in ['OUT_OF_RANGE', 'ANOMALY']])
            accuracy_score = max(0, 100 - (error_count / total_records * 100))

        # Calculate timeliness score
        timeliness_score = 100.0
        stale_count = len([i for i in issues if 'stale' in i.description.lower()])
        if total_records > 0 and stale_count > 0:
            timeliness_score = max(0, 100 - (stale_count / total_records * 100))

        # Overall score (weighted average)
        overall_score = (completeness_score * 0.4 + accuracy_score * 0.4 + timeliness_score * 0.2)

        return DataQualityReport(
            report_id=f"dq_report_{int(time.time())}",
            source=source,
            timestamp=datetime.utcnow().isoformat(),
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            completeness_score=round(completeness_score, 2),
            accuracy_score=round(accuracy_score, 2),
            timeliness_score=round(timeliness_score, 2),
            overall_score=round(overall_score, 2),
            issues=[asdict(i) for i in issues]
        )


class DataQualityService:
    """Main data quality service"""

    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.group_id = os.getenv('KAFKA_GROUP_ID', 'data-quality-service')
        self.validator = DataQualityValidator()
        self.running = False

        # Initialize Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })

        # Initialize Kafka producer
        self.producer = Producer({
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'acks': 'all',
            'retries': 3
        })

        # Subscribe to topics
        self.consumer.subscribe([
            'weather.data.updated',
            'production.sensor.data',
            'quality.test.metrics'
        ])

        logger.info(f"Data Quality Service initialized with Kafka: {self.kafka_bootstrap_servers}")

    def process_weather_data(self, data: Dict):
        """Process and validate weather data"""
        observations = data.get('observations', [])
        issues = []

        for obs in observations:
            obs_issues = self.validator.validate_weather_data(obs)
            issues.extend(obs_issues)

        # Generate quality report
        report = self.validator.generate_quality_report('weather_data', observations, issues)

        # Publish quality report
        self.publish_quality_report(report)

        # Publish critical issues
        for issue in issues:
            if issue.severity in ['HIGH', 'CRITICAL']:
                self.publish_quality_alert(issue)

    def process_sensor_data(self, data: Dict):
        """Process and validate sensor data"""
        issues = self.validator.validate_sensor_data(data)

        if issues:
            report = self.validator.generate_quality_report('sensor_data', [data], issues)
            self.publish_quality_report(report)

            for issue in issues:
                if issue.severity in ['HIGH', 'CRITICAL']:
                    self.publish_quality_alert(issue)

    def publish_quality_report(self, report: DataQualityReport):
        """Publish data quality report to Kafka"""
        try:
            message = json.dumps(asdict(report))
            self.producer.produce(
                topic='data.quality.report',
                key=report.report_id,
                value=message
            )
            self.producer.flush()
            logger.info(f"Published quality report: {report.report_id} (score: {report.overall_score})")
        except Exception as e:
            logger.error(f"Failed to publish quality report: {e}")

    def publish_quality_alert(self, issue: DataQualityIssue):
        """Publish data quality alert to Kafka"""
        try:
            alert = {
                'metadata': {
                    'event_id': issue.issue_id,
                    'event_type': 'data.quality.alert',
                    'timestamp': issue.detected_at,
                    'source_service': 'data-quality-service'
                },
                'issue': asdict(issue)
            }

            self.producer.produce(
                topic='data.quality.alert',
                key=issue.issue_id,
                value=json.dumps(alert)
            )
            self.producer.flush()
            logger.warning(f"Published quality alert: {issue.severity} - {issue.description}")
        except Exception as e:
            logger.error(f"Failed to publish quality alert: {e}")

    def run(self):
        """Main service loop"""
        self.running = True
        logger.info("Data Quality Service started")

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue

                try:
                    # Parse message
                    data = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()

                    logger.info(f"Processing message from topic: {topic}")

                    # Route to appropriate handler
                    if topic == 'weather.data.updated':
                        self.process_weather_data(data)
                    elif topic == 'production.sensor.data':
                        self.process_sensor_data(data)
                    elif topic == 'quality.test.metrics':
                        self.process_sensor_data(data)

                    # Commit offset after successful processing
                    self.consumer.commit(msg)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.stop()

    def stop(self):
        """Stop the service"""
        self.running = False
        self.consumer.close()
        self.producer.flush()
        logger.info("Data Quality Service stopped")


def main():
    service = DataQualityService()
    service.run()


if __name__ == '__main__':
    main()
