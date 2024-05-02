// @ts-check
/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../types").NodeInfo} NodeInfo*/
/** @typedef {import("../types").Mapper} Mapper */
/** @typedef {import("../types").Reducer} Reducer */
/** @typedef {import("../types").MapReduceJobMetadata} MRJobMetadata */

const assert = require("node:assert");
const store = require("./store");
const comm = require("./comm");
const groups = require("./groups");
const util = require("../util/util");
const id = util.id;

const mySid = id.getSID(global.nodeConfig);

/**
 * @param {MRJobMetadata} jobData
 * @param {object} setting
 * @param {Callback} callback
 */
function map(jobData, setting, callback = () => {}) {
  const { keys, map: mapper, storeLocally } = setting;
  const { jobID, gid, supervisor } = jobData;
  groups
    .getPromise(gid)
    .then((peers) => {
      const peerNIDNodeMap = new Map(
        Object.values(peers).map((node) => [id.getNID(node), node]),
      );

      const peerNIDs = Array.from(peerNIDNodeMap.keys());
      const computeAndSave = keys.map((key) =>
        store
          .getPromise({ gid, key })
          .then((data) => mapper(key, data))
          .then((res) => {
            const uniqueKey = id.getID(res) + id.getSID(global.nodeConfig);
            const fullKey = { gid: `${gid}/map-${jobID}`, key: uniqueKey };
            if (storeLocally) {
              return store.putPromise(res, fullKey);
            } else {
              const destinationNode = peerNIDNodeMap.get(
                id.consistentHash(id.getID(key), peerNIDs),
              );
              assert(destinationNode);
              return comm.sendPromise(
                [res, { gid: `${gid}/map-${jobID}`, key: uniqueKey }],
                {
                  node: destinationNode,
                  service: "store",
                  method: "put",
                },
              );
            }
          }),
      );
      return Promise.allSettled(computeAndSave);
    })
    .then(() => callback(null, jobID))
    .catch((e) => callback(e))
    .finally(() => {
      const notifyRemote = {
        node: supervisor,
        service: `notify-${jobID}`,
        method: "call",
      };
      return comm.sendPromise([null], notifyRemote);
    });
}

function reduceOnMapResults(mapResults, reducer) {
  const organizedMapResults = mapResults.reduce((acc, obj) => {
    const [key, val] = Object.entries(obj)[0];
    acc[key] = key in acc ? acc[key].concat(val) : val;
    return acc;
  }, {});

  const resultList = Object.entries(organizedMapResults).map(([key, val]) =>
    reducer(key, val),
  );
  return Object.assign({}, ...resultList);
}

/**
 * @param {MRJobMetadata} jobData
 * @param {Reducer} reducer
 * @param {Callback} callback
 */
function reduce(jobData, reducer, callback = () => {}) {
  const { gid, jobID } = jobData;
  return Promise.all([
    store.getAllPromise(`${gid}/map-${jobID}`),
    groups
      .getPromise(gid)
      .then(
        (peers) =>
          new Map(Object.values(peers).map((node) => [id.getNID(node), node])),
      ),
  ])
    .then(([mapResults, peerNIDNodeMap]) => {
      assert(mapResults);
      if (mapResults.length !== 0) {
        const key = jobID + mySid;

        const peerNIDs = Array.from(peerNIDNodeMap.keys());
        const destinationNID = id.consistentHash(id.getID(key), peerNIDs);
        const destinationNode = peerNIDNodeMap.get(destinationNID);
        assert(destinationNode);
        const remote = {
          node: destinationNode,
          service: "store",
          method: "put",
        };

        const reduceResult = reduceOnMapResults(mapResults, reducer);

        return new Promise((resolve, reject) => {
          comm.send(
            [reduceResult, { gid: `${gid}/reduce-${jobID}`, key }],
            remote,
            (e, _) => (e ? reject(e) : resolve(key)),
          );
        });
      } else {
        return null;
      }
    })
    .then((key) => callback(null, key))
    .catch((e) => callback(e));
}

module.exports = { map, reduce };
