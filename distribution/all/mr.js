// @ts-check
/** @typedef {import("../types").Reducer} Reducer*/
/** @typedef {import("../types").Callback} Callback*/
/** @typedef {import("../types").MapReduceJobMetadata} MRJobMetadata */

const assert = require("node:assert");
const local = require("../local/local");
const { toAsync, createRPC } = require("../util/wire");
const id = require("../util/id");

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
   * @param {Reducer} reducer
   * @param {Callback} callback
   * @return {void}
   */
  function setupNotifyEndpoint(jobData, numNotify, reducer, callback) {
    let completed = 0;
    const errors = [];
    const notify = (err) => {
      if (err) {
        errors.push(err);
      }

      if (++completed == numNotify) {
        if (errors.length > 0) {
          return callback(err);
        }

        let mergeResults = null;
        distService.comm
          .sendPromise([jobData, reducer], {
            service: "mr",
            method: "reduce",
          })
          .then((results) => {
            const promises = Object.values(results).map((storeID) =>
              distService.store.getPromise(storeID),
            );
            return Promise.all(promises);
          })
          .then((vals) => {
            mergeResults = vals.flat().filter((v) => Object.keys(v).length > 0);
            distService.store.delGroupPromise(jobData.jobID);
          })
          .then(() => callback({}, mergeResults))
          .catch((e) => callback(e));
      }
    };
    createRPC(toAsync(notify), `mr-${jobData.jobID}`);
    return;
  }

  /**
   * @param {object} setting
   * @param {Callback} callback
   * @return {void}
   */
  function exec(setting, callback = () => {}) {
    if (setting.map == null || setting.reduce == null) {
      return callback(Error("Did not supply mapper or reducer"), null);
    }
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
        setupNotifyEndpoint(jobData, nodes.length, setting.reduce, callback);

        distService.comm.send([jobData, setting.map], {
          service: "mr",
          method: setting.async ? "mapPromise" : "map",
        });
      })
      .catch((e) => callback(e, {}));
  }

  return { exec };
}

module.exports = mr;
