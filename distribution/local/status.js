const id = require("../util/id");
const { createRPC, toAsync } = require("../util/wire");
const { serialize } = require("../util/serialization");
const { fork } = require("node:child_process");
const process = require("node:process");
const path = require("node:path");
const { promisify } = require("node:util");

global.moreStatus = {
  sid: id.getSID(global.nodeConfig),
  nid: id.getNID(global.nodeConfig),
  counts: 0,
};

function get(configuration, callback) {
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
}

function stop(callback) {
  setTimeout(() => {
    callback(null, global.nodeConfig);
    process.exit(0);
  }, 1000);
}

function spawn(config, callback) {
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

  const args = ["--config", JSON.stringify(config)];
  const distPath = path.join(__dirname, "../../distribution.js");
  const child = fork(distPath, args, { stdio: "inherit" });
  child.on("error", (e) =>
    callback(new Error(`Failed to fork child: ${e.message}`)),
  );
  child.on("message", (_) => callback(null, config));
}

module.exports = { get, stop, spawn, spawnPromise: promisify(spawn) };
