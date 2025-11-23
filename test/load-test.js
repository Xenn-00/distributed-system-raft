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
    { duration: "30s", target: 200 }, // Warm up
    { duration: "1m", target: 200 }, // Ramp up
    { duration: "30s", target: 500 }, // Steady
    { duration: "1m", target: 500 }, // Steady
    { duration: "30s", target: 0 }, // Ramp down
  ],
  thresholds: {
    success_rate: ["rate>0.90"], // 90% success (more lenient for testing)
  },
};

const client = new grpc.Client();
client.load(["../proto"], "kv.proto");

const servers = ["localhost:6001", "localhost:6002", "localhost:6003"];

export default function () {
  const server = servers[Math.floor(Math.random() * servers.length)];

  try {
    client.connect(server, {
      plaintext: true,
      timeout: "5s",
    });

    const rand = Math.random();
    if (rand < 0.9) {
      doSet();
    } else if (rand < 0.1) {
      doGet();
    } else {
      doDelete();
    }

    totalOperations.add(1);
  } catch (e) {
    console.error(`Connection error: ${e}`);
    successRate.add(0);
  } finally {
    client.close();
  }

  sleep(0.1);
}

function doSet() {
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

    // ✅ FIX: Check redirect FIRST, don't count yet!
    if (
      response.message &&
      !response.message.isLeader &&
      response.message.leaderAddress
    ) {
      redirectRate.add(1);

      try {
        client.close();
        client.connect(response.message.leaderAddress, {
          plaintext: true,
          timeout: "5s",
        });

        const retryResponse = client.invoke("kv.KV/Set", {
          key: key,
          value: value,
        });

        // ✅ Count redirect result ONCE
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
      // ✅ No redirect, count direct result ONCE
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
      linearizable: false,
    });

    const duration = new Date() - startTime;
    getLatency.add(duration);

    const success = response && response.status === grpc.StatusOK;
    successRate.add(success ? 1 : 0);
  } catch (e) {
    successRate.add(0);
  }
}

function doDelete() {
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

    if (
      response.message &&
      !response.message.isLeader &&
      response.message.leaderAddress
    ) {
      redirectRate.add(1);

      try {
        client.close();
        client.connect(response.message.leaderAddress, {
          plaintext: true,
          timeout: "5s",
        });

        const retryResponse = client.invoke("kv.KV/Delete", {
          key: key,
        });

        // ✅ Count redirect result ONCE
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
      // ✅ No redirect, count direct result ONCE
      redirectRate.add(0);
      const success = response.message && response.message.success;
      successRate.add(success ? 1 : 0);
    }
  } catch (error) {
    successRate.add(0);
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
