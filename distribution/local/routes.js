// @ts-check
const types = require("../types");

/** @type {Map.<string, object>} */
const services = new Map();

/**
 * @param {object} service
 * @param {types.Callback} callback
 */
function get(service, callback = () => {}) {
  if (services.has(service)) {
    return callback(null, services.get(service));
  } else if (global.rpcLocal.has(service)) {
    return callback(null, { call: global.rpcLocal.get(service) });
  } else {
    return callback(Error(`Route ${service} not found in services`), null);
  }
}

/**
 * @param {object} service
 * @param {string} route
 * @param {types.Callback} callback
 */
function put(service, route, callback = () => {}) {
  services.set(route, service);
  return callback(null, services.get(route));
}

const routes = { get, put };

services.set("status", require("./status"));
services.set("groups", require("./groups"));
services.set("comm", require("./comm"));
services.set("gossip", require("./gossip"));
services.set("mem", require("./mem"));
services.set("store", require("./store"));
services.set("mr", require("./mr"));
services.set("routes", routes);

module.exports = routes;
