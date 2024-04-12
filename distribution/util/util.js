// @ts-check
/** @typedef {import("../types").Callback} Callback */

const serialization = require("./serialization");
const id = require("./id");
const wire = require("./wire");
const { wrap } = require("yargs");

/**
 * Returns a function with an internal counter. The returned function will
 * execute the callback after being called n times. If it encounters an
 * error before then, it will short circuit.
 * If n is 0, it will return a thunk and immediately execute the callback
 * @param {number} n
 * @param {Callback} callback
 * @return {Callback}
 */
function waitAll(n, callback) {
  if (n === 0) {
    callback(null, null);
    return () => {};
  }

  let counter = 0;
  let failed = false;
  const results = [];
  function barrier(e, v) {
    if (failed) return;
    if (e) {
      failed = true;
      return callback(e);
    }
    results.push(v);
    if (++counter === n) {
      callback(null, results);
    }
  }

  return barrier;
}

function groupPromisify(func) {
  return (/** @type {any} */ ...args) => {
    return new Promise((resolve, reject) => {
      func(...args, (e, v) => {
        if (Object.keys(e).length !== 0) {
          reject(e);
        } else {
          resolve(v);
        }
      });
    });
  };
}

module.exports = {
  serialize: serialization.serialize,
  deserialize: serialization.deserialize,
  id,
  wire,
  waitAll,
  groupPromisify,
};
