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

function sendInner(options, serialized, message, remote, callback) {
  const req = http.request(options, (res) => {
    let body = "";
    res.on("data", (chunk) => {
      body += chunk;
    });
    res.on("end", () => {
      const [err, content] = serialization.deserialize(body);
      return callback(err, content);
    });
  });

  req.on("error", (e) => {
    const err = `${e.message}: sending ${message} to ${remote.node.ip}:${remote.node.port}`;
    console.log(err);
    return callback(Error(err));
  });

  req.write(serialized);
  req.end();
}

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
  const serialized = serialization.serialize(message);

  let retryCount = 0;
  const sendInnerRec = (e, content) => {
    if (e) {
      if (retryCount++ < 5) {
        return sendInner(options, serialized, message, remote, sendInnerRec);
      } else {
        return callback(e);
      }
    }
    return callback(null, content);
  };
  sendInner(options, serialized, message, remote, sendInnerRec);
}

module.exports = { send, sendPromise: promisify(send) };
