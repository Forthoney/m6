const serialization = require("./serialization");
const id = require("./id");
const wire = require("./wire");

/**
 * @param {number} n
 * @param {types.Callback} callback
 */
function waitAll(n, callback) {
  if (n === 0) {
    callback();
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
      callback();
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
