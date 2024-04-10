// @ts-check

const assert = require("node:assert");
const local = require("../local/local");
const { toAsync, createRPC } = require("../util/wire");
const id = require("../util/id");
const comm = require("./comm");
const store = require("./store");
const types = require("../types");

/**
 * @param {object} config
 */
function mr(config) {
  const context = {
    gid: config.gid || "all",
  };

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
            service: "mr",
            method: "reduce",
          },
          (e, results) => {
            if (Object.values(e).length !== 0) {
              return callback(e);
            }

            const mergedResults = Object.values(results)
              .flat()
              .filter((res) => Object.keys(res).length > 0);

            // Cleanup data
            if (false) {
              store(config).delGroup(jobID, (e, _) => {
                if (Object.values(e).length !== 0) {
                  return callback(e);
                }
                return callback(e, mergedResults);
              });
            } else {
              return callback(e, mergedResults);
            }
          },
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

      const jobID = setting.id || id.getID(setting);
      assert(group);
      const nodes = Object.values(group);
      setupNotifyEndpoint(jobID, nodes.length, setting.reduce, callback);
      comm(config).send(
        [context.gid, global.nodeConfig, jobID, setting.map],
        {
          service: "mr",
          method: "map",
        },
        (e, _) => {
          // if (Object.values(e).length !== 0) {
          //   console.log("+++++++++++++++++++++++++++++++++++++++++", e);
          //   return callback(e, {});
          // }
        },
      );
    });
  }

  return { exec };
}

module.exports = mr;
