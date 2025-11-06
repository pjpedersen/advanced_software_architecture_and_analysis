import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');

// Test configuration
export const options = {
  stages: [
    { duration: '1m', target: 10 },   // Ramp up to 10 users
    { duration: '3m', target: 50 },   // Ramp up to 50 users
    { duration: '5m', target: 100 },  // Stay at 100 users
    { duration: '2m', target: 200 },  // Spike to 200 users
    { duration: '3m', target: 100 },  // Scale back to 100
    { duration: '2m', target: 0 },    // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'],  // 95% of requests under 2s (NFR-B)
    http_req_failed: ['rate<0.01'],     // Less than 1% errors
    errors: ['rate<0.05'],               // Less than 5% business errors
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

// Generate realistic production plan
function generateProductionPlan() {
  const colors = ['BLACK', 'GRAY', 'WHITE', 'BLUE'];
  const now = new Date();
  const startTime = new Date(now.getTime() + Math.random() * 3600000); // Start within next hour
  const endTime = new Date(startTime.getTime() + 2 * 3600000); // 2 hours duration

  return {
    batches: [
      {
        batch_id: `B${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        product_sku: 'WIC-001',
        quantity: Math.floor(Math.random() * 200) + 50,
        color: colors[Math.floor(Math.random() * colors.length)],
        start_time: startTime.toISOString(),
        end_time: endTime.toISOString(),
        priority: Math.floor(Math.random() * 3) + 1,
      },
    ],
  };
}

// Test scenario: Create production plan
export default function () {
  const plan = generateProductionPlan();

  // Create production plan
  const createRes = http.post(
    `${BASE_URL}/api/v1/plans`,
    JSON.stringify(plan),
    {
      headers: { 'Content-Type': 'application/json' },
      tags: { name: 'CreatePlan' },
    }
  );

  const createCheck = check(createRes, {
    'plan created successfully': (r) => r.status === 201,
    'response time < 2s': (r) => r.timings.duration < 2000,
    'has plan_id': (r) => JSON.parse(r.body).plan_id !== undefined,
  });

  errorRate.add(!createCheck);

  if (createCheck && createRes.status === 201) {
    const planId = JSON.parse(createRes.body).plan_id;

    // Wait a bit
    sleep(1);

    // Get production plan
    const getRes = http.get(`${BASE_URL}/api/v1/plans/${planId}`, {
      tags: { name: 'GetPlan' },
    });

    const getCheck = check(getRes, {
      'plan retrieved successfully': (r) => r.status === 200,
      'response time < 1s': (r) => r.timings.duration < 1000,
    });

    errorRate.add(!getCheck);
  }

  sleep(Math.random() * 3 + 1); // Random sleep 1-4 seconds
}

// Scenario: Concurrent device registration
export function deviceRegistrationScenario() {
  const device = {
    pcb_id: `PCB-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    serial_number: `SN-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    production_batch: `B${Math.floor(Math.random() * 1000)}`,
  };

  const res = http.post(
    'http://localhost:8081/api/v1/devices',
    JSON.stringify(device),
    {
      headers: { 'Content-Type': 'application/json' },
      tags: { name: 'RegisterDevice' },
    }
  );

  check(res, {
    'device registered': (r) => r.status === 201,
    'response time < 1s': (r) => r.timings.duration < 1000,
  });

  sleep(1);
}

// Spike test for production planning
export function spikeTest() {
  const plans = [];

  // Create multiple plans rapidly
  for (let i = 0; i < 10; i++) {
    const plan = generateProductionPlan();
    const res = http.post(
      `${BASE_URL}/api/v1/plans`,
      JSON.stringify(plan),
      {
        headers: { 'Content-Type': 'application/json' },
        tags: { name: 'SpikePlan' },
      }
    );

    plans.push(res);
  }

  // Check all responses
  plans.forEach((res) => {
    check(res, {
      'spike test passed': (r) => r.status === 201 || r.status === 429, // Allow rate limiting
    });
  });

  sleep(5);
}

// Stress test configuration
export const stressOptions = {
  stages: [
    { duration: '2m', target: 500 },   // Ramp up to 500 users
    { duration: '5m', target: 500 },   // Stay at 500
    { duration: '2m', target: 1000 },  // Spike to 1000
    { duration: '3m', target: 500 },   // Scale back
    { duration: '2m', target: 0 },     // Ramp down
  ],
};

// Soak test configuration (long-running stability test)
export const soakOptions = {
  stages: [
    { duration: '5m', target: 100 },   // Ramp up
    { duration: '4h', target: 100 },   // Stay at 100 for 4 hours
    { duration: '5m', target: 0 },     // Ramp down
  ],
};
