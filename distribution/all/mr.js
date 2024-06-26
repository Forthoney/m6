// @ts-check
/** @typedef {import("../types").Reducer} Reducer*/
/** @typedef {import("../types").Callback} Callback*/
/** @typedef {import("../types").MapReduceJobMetadata} MRJobMetadata */

const assert = require("node:assert");
const local = require("../local/local");
const util = require("../util/util");
const { toAsync, createRPC } = util.wire;
const id = util.id;

/**
 * @param {object} config
 * @return {object}
 */
function mr(config) {
  const context = {
    gid: config.gid || "all",
  };

  const distService = global.distribution[context.gid];

  /**
   * Setup an notification endpoint. When workers are done mapping, they will
   * ping this endpoint. Once all workers are finished, the endpoint will
   * trigger the reduce phase
   * @param {MRJobMetadata} jobData
   * @param {number} numNotify
   * @param {object} setting
   * @param {Callback} callback
   * @return {void}
   */
  function setupNotifyEndpoint(jobData, numNotify, setting, callback) {
    const { reduce } = setting;
    const { jobID } = jobData;
    let completed = 0;
    const errors = [];
    const notify = (err) => {
      console.log("COMPLETED", completed + 1);
      if (err) {
        errors.push(err);
      }

      if (++completed == numNotify) {
        if (errors.length > 0) {
          return callback(errors);
        }

        console.log("notified");
        if (!reduce) {
          return callback(errors, "Finished");
        }

        distService.comm
          .sendPromise([jobData, reduce], {
            service: "mr",
            method: "reduce",
          })
          .then((results) => {
            const promises = Object.values(results)
              .filter((key) => key !== null)
              .map((key) =>
                distService.store.getSubgroupPromise(key, `reduce-${jobID}`),
              );
            return Promise.all(promises);
          })
          .then((vals) => {
            callback({}, Object.assign({}, ...vals.flat()));
          })
          .catch((e) => callback(e));
      }
    };
    createRPC(toAsync(notify), `notify-${jobData.jobID}`);
    return;
  }

  /**
   * @param {object} setting
   * @param {Callback} callback
   * @return {void}
   */
  function exec(setting, callback = () => {}) {
    local.groups
      .getPromise(context.gid)
      .then((group) => {
        const jobID = setting.id || id.getID(setting);
        assert(group);
        const nodes = Object.values(group);
        const jobData = {
          gid: context.gid,
          supervisor: global.nodeConfig,
          jobID: jobID,
        };
        setupNotifyEndpoint(jobData, nodes.length, setting, callback);

        distService.comm.send([jobData, setting], {
          service: "mr",
          method: "map",
        });
      })
      .catch((e) => callback(e, {}));
  }

  return { exec };
}

module.exports = mr;
