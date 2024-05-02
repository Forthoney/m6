// @ts-check
/** @typedef {import("../types").Callback} Callback */

const url = require("node:url");

const serialization = require("./serialization");
const id = require("./id");
const wire = require("./wire");

function groupPromisify(func) {
  return (/** @type {any} */ ...args) => {
    return new Promise((resolve, reject) => {
      func(...args, (e, v) => {
        Object.keys(e).length !== 0 ? reject(e) : resolve(v);
      });
    });
  };
}

module.exports = {
  serialize: serialization.serialize,
  deserialize: serialization.deserialize,
  id,
  wire,
  groupPromisify,
};
