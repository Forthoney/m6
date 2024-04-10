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
  function barrier(e, _) {
    if (failed) return;
    if (e) {
      failed = true;
      return callback(e);
    }

    if (++counter === n) {
      callback(null, null);
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
