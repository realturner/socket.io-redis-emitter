// @ts-ignore
import { Assertion, stringify as i } from "expect.js";

// add support for Set/Map
const contain = Assertion.prototype.contain;
Assertion.prototype.contain = function (...args) {
  if (typeof this.obj === "object") {
    args.forEach((obj) => {
      this.assert(
        this.obj.has(obj),
        function () {
          return "expected " + i(this.obj) + " to contain " + i(obj);
        },
        function () {
          return "expected " + i(this.obj) + " to not contain " + i(obj);
        }
      );
    });
    return this;
  }
  return contain.apply(this, args);
};

export const createClient = (() => {
  if (process.env.REDIS_CLUSTER !== undefined) {
    const rootNodes = process.env.REDIS_CLUSTER.split(",").map((url) => ({
      url,
    }));
    return async () => {
      const client = require("redis").createCluster({
        rootNodes,
      });
      client.on("error", () => null);
      await client.connect();
      return client;
    };
  } else {
    return async () => {
      const client = require("redis").createClient();
      await client.connect();
      return client;
    };
  }
})();
