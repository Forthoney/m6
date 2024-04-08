// @ts-check

const http = require("http");
const serialization = require("../util/serialization");
const types = require("../types");

/**
 * @typedef {Object} LocalRemote
 * @property {string} service
 * @property {string} method
 * @property {types.NodeInfo} node
 */

/**
 * @param {Array} message
 * @param {LocalRemote} remote
 * @param {types.Callback} callback
 */
function send(message, remote, callback = (_e, _) => {}) {
  const msg = serialization.serialize(message);
  const options = {
    hostname: remote.node.ip,
    port: remote.node.port,
    path: `/${remote.service}/${remote.method}`,
    method: "PUT",
    headers: {
      "content-type": "application/json",
      "content-length": msg.length,
    },
  };
  const req = http.request(options, (res) => {
    let body = "";
    res.on("data", (chunk) => {
      body += chunk;
    });
    res.on("end", () => {
      callback(...serialization.deserialize(body));
    });
  });

  req.on("error", (e) => {
    callback(new Error(`Error on Request: ${e.message}`), null);
  });

  req.write(msg);
  req.end();
}

module.exports = { send };
