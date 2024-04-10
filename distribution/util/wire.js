// @ts-check

const {getID} = require('./id');

global.rpcLocal = new Map();

/**
 * Adds func as endpoint and returns serialized rpc function that calls func
 * @param {Function} fn
 * @param {string} [fnID=getID(fn)]
 * @return {Function}
 */
function createRPC(fn, fnID = getID(fn)) {
  // Write some code...
  const nodeInfo = global.nodeConfig;
  global.rpcLocal.set(fnID, fn);
  const stubString = `
  const callback = args.pop() || function () {}
  const remote = {
    node: {
      ip: "${nodeInfo.ip}",
      port: ${nodeInfo.port},
    },
    service: "${fnID}",
    method: "call"
  };
  distribution.local.comm.send(args, remote, callback);
  `;
  return new Function('...args', stubString);
}

/**
 * The toAsync function converts a synchronous function that returns a value
 * to one that takes a callback as its last argument and returns the value
 * to the callback.
 * @param {Function} func
 * @return {Function}
 */
function toAsync(func) {
  return function(...args) {
    const callback = args.pop() || function() {};
    try {
      const result = func(...args);
      callback(null, result);
    } catch (error) {
      callback(error);
    }
  };
}

module.exports = {
  createRPC: createRPC,
  toAsync: toAsync,
};
