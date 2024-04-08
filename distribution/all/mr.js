// @ts-check

const local = require("../local/local");
const { toAsync, createRPC } = require("../util/wire");
const { getID } = require("../util/id");
const comm = require("./comm");
const types = require("../types");

/**
 * @param {object} config
 */
function mr(config) {
  const context = {};
  context.gid = config.gid || "all";

  /**
   * Setup an notification endpoint. When workers are done mapping, they will
   * ping this endpoint. Once all workers are finished, the endpoint will trigger
   * the reduce phase
   * @param {string} jobID
   * @param {number} numNotify
   * @param {types.Reducer} reducer
   * @param {types.Callback} callback
   */
  function setupNotifyEndpoint(jobID, numNotify, reducer, callback) {
    let completed = 0;
    const notify = () => {
      if (++completed == numNotify) {
        comm(config).send(
          [jobID, reducer],
          {
            service: "map",
            method: "reduce",
          },
          callback,
        );
      }
    };
    createRPC(toAsync(notify), `mr-${jobID}`);
    return;
  }

  /**
   * @param {object} setting
   * @param {types.Callback} callback
   */
  function exec(setting, callback = (_e, _) => {}) {
    if (setting.map == null || setting.reduce == null) {
      return callback(Error("Did not supply mapper or reducer"), null);
    }
    local.groups.get(context.gid, (e, group) => {
      if (e) return callback(e, {});

      const jobID = setting.id || getID(setting);
      const nodes = Object.values(group);
      setupNotifyEndpoint(jobID, nodes.length, setting.reduce, callback);
      comm(config).send(
        [context.gid, global.nodeConfig, jobID, setting.mapper],
        {
          service: "mr",
          method: "map",
        },
        (e, _) => {
          if (Object.values(e).length !== 0) return callback(e, {});
        },
      );
    });
  }

  return { exec };
}

module.exports = mr;
