// @ts-check
/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../types").NodeInfo} NodeInfo */

const http = require('http');
const serialization = require('../util/serialization');

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
  const msg = serialization.serialize(message);
  const options = {
    hostname: remote.node.ip,
    port: remote.node.port,
    path: `/${remote.service}/${remote.method}`,
    method: 'PUT',
    headers: {
      'content-type': 'application/json',
      'content-length': msg.length,
    },
  };
  const req = http.request(options, (res) => {
    let body = '';
    res.on('data', (chunk) => {
      body += chunk;
    });
    res.on('end', () => {
      const [err, content] = serialization.deserialize(body);
      callback(err, content);
    });
  });

  req.on('error', (e) => {
    callback(new Error(`Error on Request: ${e.message}`), null);
  });

  req.write(msg);
  req.end();
}

module.exports = {send};
