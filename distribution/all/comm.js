// @ts-check
/** @typedef {import("../types").Callback} Callback */

const assert = require("node:assert");
const local = require("../local/local");
const { getSID } = require("../util/id");
const { promisify } = require("node:util");
const { writeFileSync } = require("node:fs");

/**
 * @typedef {Object} AllRemote
 * @property {string} service
 * @property {string} method
 */

/**
 * @param {object} config
 * @return {object}
 */
function comm(config) {
  const context = {
    gid: config.gid || "all",
  };

  /**
   * @param {Array} message
   * @param {object} remote
   * @param {Callback} callback
   */
  function send(message, remote, callback = () => {}) {
    local.groups.getPromise(context.gid).then((group) => {
      assert(group);
      const entries = Object.entries(group);
      const promises = entries.map(([sid, node]) => {
        Object.assign(remote, { node: node });
        return local.comm
          .sendPromise(message, remote)
          .then((value) => ({ sid, value }))
          .catch((error) => Promise.reject({ sid, error }));
      });

      Promise.allSettled(promises).then((results) => {
        const allError = {};
        const allSuccess = {};

        results.forEach((result) => {
          if (result.status === "fulfilled") {
            allSuccess[result.value.sid] = result.value.value;
          } else if (result.status === "rejected") {
            allError[result.reason.sid] = result.reason.error;
          } else {
            console.error(results);
          }
        });
        callback(allError, allSuccess);
      });
    });
  }

  return { send, sendPromise: promisify(send) };
}

module.exports = comm;
