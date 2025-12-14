import grpc from "k6/net/grpc";
import { check, sleep } from "k6";
import { Rate, Trend, Counter } from "k6/metrics";

const successRate = new Rate("success_rate");
const redirectRate = new Rate("redirect_rate");
const setLatency = new Trend("set_latency");
const getLatency = new Trend("get_latency");
const deleteLatency = new Trend("delete_latency");
const totalOperations = new Counter("total_operations");

export const options = {
  stages: [
    { duration: "10s", target: 10 }, // Warm up
    { duration: "10s", target: 50 }, // Warm up
    { duration: "10s", target: 50 }, // steady
    { duration: "10s", target: 50 }, // steady
    { duration: "15s", target: 20 }, // ramp down
    { duration: "10s", target: 0 }, // ramp down
  ],
  thresholds: {
    success_rate: ["rate>0.95"],
    "grpc_req_duration{expected_response:true}": ["p(95)<100"],
  },
};

const servers = ["localhost:6001", "localhost:6002", "localhost:6003"];

const connectionPool = {}; // Connection pool
let currentLeader = null; // Track known leader
let leaderCheckTime = 0; // Last time verified leader

// ✅ Global client shared by all VUs (k6 handles concurrency)
servers.forEach((addr) => {
  const client = new grpc.Client();
  client.load(["../proto"], "kv.proto");
  connectionPool[addr] = client;
});

export function setup() {
  console.log("🚀 Starting load test with optimized connection handling...");
  return { startTime: Date.now() };
}

// Get or create connection, prefer leader for writes
function getConnection(preferLeader = false) {
  const now = Date.now();

  // For writes, always try known leader first
  if (preferLeader && currentLeader && now - leaderCheckTime < 5000) {
    const client = connectionPool[currentLeader];
    if (client) {
      return { client: client, server: currentLeader };
    }
  }

  // Otherwise, pick random server
  const server = servers[Math.floor(Math.random() * servers.length)];
  const client = connectionPool[server];
  client.connect(server, {
    plaintext: true,
    timeout: "5s",
  });

  if (!connectionPool[server]) {
    try {
      client.connect(server, {
        plaintext: true,
        timeout: "5s",
      });
      connectionPool[server] = client;
    } catch (error) {
      console.error(`Failed to connect to ${server}: ${error}`);
      return { client: null, server: null };
    }
  }
  return { client: client, server: server };
}

// Update leader info when we learn about it
function updateLeader(leaderAddress) {
  if (leaderAddress && leaderAddress !== currentLeader) {
    currentLeader = leaderAddress;
    leaderCheckTime = Date.now();

    // Ensure we have connection to leader
    if (!connectionPool[leaderAddress]) {
      try {
        client.connect(leaderAddress, {
          plaintext: true,
          timeout: "5s",
        });
        connectionPool[leaderAddress] = client;
      } catch (error) {
        console.error(`Failed to connect to leader ${leaderAddress}: ${error}`);
      }
    }
  }
}

export default function () {
  try {
    const rand = Math.random();

    if (rand < 0.7) {
      // 70% writes (SET)
      doSet();
    } else if (rand < 0.9) {
      // 20% reads (GET)
      doGet();
    } else {
      // 10% deletes
      doDelete();
    }

    totalOperations.add(1);
  } catch (e) {
    console.error(`Operation error: ${e}`);
    successRate.add(0);
  } finally {
    // ✅ Don't close connection - let k6 manage it
    // This is the key difference from original script
  }

  // ✅ Reduced sleep for higher throughput
  sleep(0.01); // 100 ops/sec per VU max
}

function doSet() {
  const key = `key${Math.floor(Math.random() * 1000)}`;
  const value = `value_${Date.now()}_${randomString(16)}`;

  // Try leader first for writes
  let { client, server } = getConnection(true);

  if (!client) {
    successRate.add(0);
    return;
  }

  const startTime = new Date();

  try {
    const response = client.invoke("kv.KV/Set", {
      key: key,
      value: value,
    });

    const duration = new Date() - startTime;
    setLatency.add(duration);

    if (!response || response.status !== grpc.StatusOK) {
      successRate.add(0);
      return;
    }

    // Handle redirect
    if (
      response.message &&
      !response.message.isLeader &&
      response.message.leaderAddress
    ) {
      redirectRate.add(1);

      // Update our knowledge of leader
      updateLeader(response.message.leaderAddress);

      // Retry on leader
      const leaderAddress = response.message.leaderAddress;
      const leaderClient = connectionPool[leaderAddress];
      leaderClient.connect(leaderAddress, {
        plaintext: true,
        timeout: "5s",
      });

      if (!leaderClient) {
        successRate.add(0);
        return;
      }

      const retryResponse = leaderClient.invoke("kv.KV/Set", {
        key: key,
        value: value,
      });

      const retrySuccess =
        retryResponse &&
        retryResponse.status === grpc.StatusOK &&
        retryResponse.message &&
        retryResponse.message.success;

      successRate.add(retrySuccess ? 1 : 0);
    } else {
      // Direct success
      redirectRate.add(0);

      // Update leader cache (this server is leader)
      if (response.message && response.message.isLeader) {
        updateLeader(server);
      }

      const success = response.message && response.message.success;
      successRate.add(success ? 1 : 0);
    }
  } catch (e) {
    successRate.add(0);
    console.error(`SET error: ${e}`);
  }
}

function doGet() {
  // Reads can go to any server
  let { client, _ } = getConnection(false);
  if (!client) {
    successRate.add(0);
    return;
  }

  const key = `key${Math.floor(Math.random() * 1000)}`;
  const startTime = new Date();

  try {
    const response = client.invoke("kv.KV/Get", {
      key: key,
      linearizable: false, // Stale reads for performance
    });

    const duration = new Date() - startTime;
    getLatency.add(duration);

    const success = response && response.status === grpc.StatusOK;
    successRate.add(success ? 1 : 0);
  } catch (e) {
    successRate.add(0);
    console.error(`GET error: ${e}`);
  }
}

function doDelete() {
  // Try leader first for writes
  let { client, server } = getConnection(true);
  if (!client) {
    successRate.add(0);
    return;
  }

  const key = `key${Math.floor(Math.random() * 1000)}`;
  const startTime = new Date();

  try {
    const response = client.invoke("kv.KV/Delete", {
      key: key,
    });

    const duration = new Date() - startTime;
    deleteLatency.add(duration);

    if (!response || response.status !== grpc.StatusOK) {
      successRate.add(0);
      return;
    }

    // Handle redirect
    if (
      response.message &&
      !response.message.isLeader &&
      response.message.leaderAddress
    ) {
      redirectRate.add(1);

      updateLeader(response.message.leaderAddress);

      const leaderAddress = response.message.leaderAddress;
      const leaderClient = connectionPool[leaderAddress];
      leaderClient.connect(leaderAddress, {
        plaintext: true,
        timeout: "5s",
      });

      if (!leaderClient) {
        successRate.add(0);
        return;
      }

      const retryResponse = leaderClient.invoke("kv.KV/Delete", {
        key: key,
      });

      const retrySuccess =
        retryResponse &&
        retryResponse.status === grpc.StatusOK &&
        retryResponse.message &&
        retryResponse.message.success;

      successRate.add(retrySuccess ? 1 : 0);
    } else {
      redirectRate.add(0);

      if (response.message && response.message.isLeader) {
        updateLeader(server);
      }

      const success = response.message && response.message.success;
      successRate.add(success ? 1 : 0);
    }
  } catch (e) {
    successRate.add(0);
    console.error(`DELETE error: ${e}`);
  }
}

export function teardown(data) {
  const duration = (Date.now() - data.startTime) / 1000;
  console.log(`🧹 Test completed in ${duration.toFixed(1)}s`);

  // Close all pooled connections
  for (const server in connectionPool) {
    try {
      connectionPool[server].close();
      console.log(`✅ Closed connection to ${server}`);
    } catch (e) {
      console.error(`❌ Error closing ${server}: ${e}`);
    }
  }
}

function randomString(length) {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}
