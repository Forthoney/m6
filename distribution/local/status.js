const id = require("../util/id");
const { createRPC, toAsync } = require("../util/wire");
const { serialize } = require("../util/serialization");
const { fork } = require("node:child_process");
const process = require("node:process");
const path = require("node:path");

global.moreStatus = {
  sid: id.getSID(global.nodeConfig),
  nid: id.getNID(global.nodeConfig),
  counts: 0,
};

const status = {
  get: (configuration, callback = () => {}) => {
    if (configuration in global.nodeConfig) {
      callback(null, global.nodeConfig[configuration]);
    } else if (configuration in moreStatus) {
      callback(null, moreStatus[configuration]);
    } else if (configuration === "heapTotal") {
      callback(null, process.memoryUsage().heapTotal);
    } else if (configuration === "heapUsed") {
      callback(null, process.memoryUsage().heapUsed);
    } else {
      callback(new Error(`Key ${configuration} not found`));
    }
  },

  stop: (callback = () => {}) => {
    setTimeout(() => {
      callback(null, global.nodeConfig);
      process.exit(0);
    }, 1000);
  },

  spawn: (config, callback = () => {}) => {
    const callbackRPC = createRPC(toAsync(callback));
    let originalOnStart = "";
    if ("onStart" in config) {
      originalOnStart = `
      const originalOnStart = ${config.onStart.toString()};
      originalOnStart();
      `;
    }
    const funcStr = `
    ${originalOnStart}
    const callbackRPC = ${callbackRPC.toString()};
    callbackRPC(null, global.nodeConfig, () => {});
    `;
    config.onStart = new Function(funcStr);

    const args = ["--config", serialize(config)];
    const distPath = path.join(__dirname, "../../distribution.js");
    fork(distPath, args, { stdio: "inherit" });
  },
};

module.exports = status;
