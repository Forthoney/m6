// @ts-check
/** @typedef {import("../types").Reducer} Reducer*/
/** @typedef {import("../types").Callback} Callback*/
/** @typedef {import("../types").MapReduceJobMetadata} MRJobMetadata */

const assert = require("node:assert");
const local = require("../local/local");
const util = require("../util/util");
const { toAsync, createRPC } = require("../util/wire");
const id = require("../util/id");
const comm = require("./comm");
const store = require("./store");

/**
 * @param {object} config
 * @return {object}
 */
function mr(config) {
  const context = {
    gid: config.gid || "all",
  };

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
        comm(config).send(
          [jobData, reducer],
          {
            service: "mr",
            method: "reduce",
          },
          resultCompiler(jobData, callback),
        );
      }
    };
    createRPC(toAsync(notify), `mr-${jobData.jobID}`);
    return;
  }

  function resultCompiler(jobData, callback) {
    return (e, results) => {
      if (Object.values(e).length !== 0) {
        return callback(e);
      }
      const keys = Object.values(results);
      const mergeBarrier = util.waitAll(keys.length, (e, vals) => {
        if (e) return callback(e);

        const mergedResults = vals
          .flat()
          .filter((v) => Object.keys(v).length > 0);
        console.log(mergedResults);

        // Cleanup data
        store(config).delGroup(jobData.jobID, (e, _) => {
          if (Object.values(e).length !== 0) {
            return callback(e);
          }
          return callback(e, mergedResults);
        });
      });

      keys.forEach((storeID) => {
        store(config).get(storeID, mergeBarrier);
      });
    };
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
    local.groups.get(context.gid, (e, group) => {
      if (e) return callback(e, {});

      const jobID = setting.id || id.getID(setting);
      assert(group);
      const nodes = Object.values(group);
      const jobData = {
        gid: context.gid,
        supervisor: global.nodeConfig,
        jobID: jobID,
      };
      setupNotifyEndpoint(jobData, nodes.length, setting.reduce, callback);

      comm(config).send([jobData, setting.map], {
        service: "mr",
        method: "map",
      });
    });
  }

  return { exec };
}

module.exports = mr;
