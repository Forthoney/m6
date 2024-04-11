// @ts-check
/** @typedef {import("../types").Callback} Callback */

const serialization = require('./serialization');
const id = require('./id');
const wire = require('./wire');

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

module.exports = {
  serialize: serialization.serialize,
  deserialize: serialization.deserialize,
  id,
  wire,
  waitAll,
};
