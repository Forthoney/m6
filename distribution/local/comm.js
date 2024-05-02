// @ts-check
/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../types").NodeInfo} NodeInfo */

const http = require("node:http");
const { promisify } = require("node:util");
const serialization = require("../util/serialization");

/**
 * @typedef {Object} LocalRemote
 * @property {string} service
 * @property {string} method
 * @property {NodeInfo} node
 */

/**
 * @param {Array} message
 * @param {LocalRemote} remote
 * @param {Callback} callback
 * @return {void}
 */
function send(message, remote, callback = () => {}) {
  if (
    remote.node.ip === global.nodeConfig.ip &&
    remote.node.port === global.nodeConfig.port
  ) {
    const local = global.distribution.local;
    if (remote.service in local && remote.method in local[remote.service]) {
      return local[remote.service][remote.method](...message, callback);
    }
  }

  const options = {
    hostname: remote.node.ip,
    port: remote.node.port,
    path: `/${remote.service}/${remote.method}`,
    method: "PUT",
    headers: {
      "content-type": "application/json",
    },
  };
  const req = http.request(options, (res) => {
    let body = "";
    res.on("data", (chunk) => {
      body += chunk;
    });
    res.on("end", () => {
      const [err, content] = serialization.deserialize(body);
      callback(err, content);
    });
  });

  const serialized = serialization.serialize(message);

  req.on("error", (e) => {
    const err = `${e.message}: sending ${message} to ${remote.node.ip}:${remote.node.port}`;
    console.log(err);
    callback(Error(err));
  });

  req.write(serialized);
  req.end();
}

module.exports = { send, sendPromise: promisify(send) };
