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
    { duration: "15s", target: 20 }, // Ramp up
    { duration: "30s", target: 50 }, // steady
    { duration: "15s", target: 0 }, // Ramp down
    // { duration: "10s", target: 0 }, // Cool down
  ],
  thresholds: {
    success_rate: ["rate>0.95"],
  },
};

const servers = ["localhost:6001", "localhost:6002", "localhost:6003"];

// ✅ Global client shared by all VUs (k6 handles concurrency)
const client = new grpc.Client();
client.load(["../proto"], "kv.proto");

export function setup() {
  console.log("🚀 Starting load test with optimized connection handling...");
  return { startTime: Date.now() };
}

export default function (data) {
  // ✅ Pick random server for this iteration
  const server = servers[Math.floor(Math.random() * servers.length)];

  // ✅ Connect for this iteration (k6 reuses connections internally)
  try {
    client.connect(server, {
      plaintext: true,
      timeout: "10s",
    });
  } catch (e) {
    console.error(`Connection failed to ${server}: ${e}`);
    successRate.add(0);
    return;
  }

  try {
    const rand = Math.random();

    if (rand < 0.7) {
      // 70% writes (SET)
      doSet(server);
    } else if (rand < 0.9) {
      // 20% reads (GET)
      doGet();
    } else {
      // 10% deletes
      doDelete(server);
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

function doSet(currentServer) {
  const key = `key${Math.floor(Math.random() * 1000)}`;
  const value = `value_${Date.now()}_${randomString(16)}`;
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

      const leaderAddress = response.message.leaderAddress;

      try {
        // ✅ Reconnect to leader for retry
        client.close();
        client.connect(leaderAddress, {
          plaintext: true,
          timeout: "10s",
        });

        const retryResponse = client.invoke("kv.KV/Set", {
          key: key,
          value: value,
        });

        if (
          retryResponse &&
          retryResponse.status === grpc.StatusOK &&
          retryResponse.message &&
          retryResponse.message.success
        ) {
          successRate.add(1);
        } else {
          successRate.add(0);
        }
      } catch (e) {
        successRate.add(0);
        console.error(`Redirect to leader failed: ${e}`);
      }
    } else {
      // Direct success
      redirectRate.add(0);
      const success = response.message && response.message.success;
      successRate.add(success ? 1 : 0);
    }
  } catch (e) {
    successRate.add(0);
    console.error(`SET error: ${e}`);
  }
}

function doGet() {
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

function doDelete(currentServer) {
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

      const leaderAddress = response.message.leaderAddress;

      try {
        // ✅ Reconnect to leader for retry
        client.close();
        client.connect(leaderAddress, {
          plaintext: true,
          timeout: "10s",
        });

        const retryResponse = client.invoke("kv.KV/Delete", {
          key: key,
        });

        if (
          retryResponse &&
          retryResponse.status === grpc.StatusOK &&
          retryResponse.message &&
          retryResponse.message.success
        ) {
          successRate.add(1);
        } else {
          successRate.add(0);
        }
      } catch (e) {
        successRate.add(0);
        console.error(`Redirect failed: ${e}`);
      }
    } else {
      redirectRate.add(0);
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

  try {
    client.close();
  } catch (e) {
    // Ignore
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
