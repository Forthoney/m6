// @ts-check
const local = require("../local/local");
const types = require("../types");
const { getSID } = require("../util/id");

/**
 * @typedef {Object} AllRemote
 * @property {string} service
 * @property {string} method
 */

/**
 * @param {object} config
 */
function comm(config) {
  const context = {
    gid: config.gid || "all",
  };
  const mySid = getSID(global.nodeConfig);

  /**
   * @param {Array} message
   * @param {object} remote
   * @param {types.Callback} callback
   */
  function send(message, remote, callback = (_e, _) => {}) {
    local.groups.get(context.gid, (e, group) => {
      if (e) {
        const err = {};
        err[mySid] = e;
        return callback(e, null);
      }

      const errors = {};
      const results = {};
      let count = 0;
      const entries = Object.entries(group);

      entries.forEach(([sid, node]) => {
        Object.assign(remote, { node: node });
        local.comm.send(message, remote, (e, v) => {
          if (e) {
            errors[sid] = e;
          } else {
            results[sid] = v;
          }
          if (++count === entries.length) {
            callback(errors, results);
          }
        });
      });
    });
  }

  return { send };
}

module.exports = comm;
