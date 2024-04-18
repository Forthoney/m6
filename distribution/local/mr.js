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

function computeMap(gid, mapper) {
  return store
    .getPromise({ gid, key: null })
    .then((keys) =>
      Promise.all(
        keys.map((key) =>
          store.getPromise({ gid, key }).then((data) => mapper(key, data)),
        ),
      ),
    )
    .then((results) => {
      const mapperRes = new Map();

      results.flat().forEach((res) => {
        Object.entries(res).forEach(([key, val]) => {
          if (mapperRes.has(key)) {
            mapperRes.get(key).push(val);
          } else {
            mapperRes.set(key, [val]);
          }
        });
      });
      return mapperRes;
    });
}

function calcPeerNIDNodeMap(gid) {
  return groups
    .getPromise(gid)
    .then(
      (peers) =>
        new Map(Object.values(peers).map((node) => [id.getNID(node), node])),
    );
}

/**
 * @param {MRJobMetadata} jobData
 * @param {Mapper} mapper
 * @param {Callback} callback
 */
function map(jobData, mapper, callback = () => {}) {
  const { jobID, gid, supervisor } = jobData;

  Promise.all([computeMap(gid, mapper), calcPeerNIDNodeMap(gid)])
    .then(([mapperRes, peerNIDNodeMap]) => {
      const entries = Array.from(mapperRes.entries());
      const peerNIDs = Array.from(peerNIDNodeMap.keys());
      const storePromises = entries.map(([key, val]) => {
        const entry = { [key]: val };
        const destinationNode = peerNIDNodeMap.get(
          id.consistentHash(id.getID(key), peerNIDs),
        );
        assert(destinationNode);

        const uniqueKey = id.getID(entry) + id.getSID(global.nodeConfig);
        return comm.sendPromise(
          [entry, { gid: `${gid}/map-${jobID}`, key: uniqueKey }],
          {
            node: destinationNode,
            service: "store",
            method: "put",
          },
        );
      });
      return Promise.all(storePromises);
    })
    .then((_) => {
      const notifyRemote = {
        node: supervisor,
        service: `notify-${jobID}`,
        method: "call",
      };
      return comm.sendPromise([null], notifyRemote);
    })
    .then((_) => callback(null, jobID))
    .catch((e) => callback(e));
}

function reduceOnMapResults(mapResults, reducer) {
  const organizedMapResults = mapResults.reduce((acc, obj) => {
    const [key, val] = Object.entries(obj)[0];
    acc[key] = key in acc ? acc[key].concat(val) : val;
    return acc;
  }, {});

  return Object.entries(organizedMapResults).map(([key, val]) =>
    reducer(key, val),
  );
}

/**
 * @param {MRJobMetadata} jobData
 * @param {Reducer} reducer
 * @param {Callback} callback
 */
function reduce(jobData, reducer, callback = () => {}) {
  const { gid, jobID } = jobData;
  Promise.all([
    store.getAllPromise(`${gid}/map-${jobID}`),
    calcPeerNIDNodeMap(gid),
  ])
    .then(([mapResults, peerNIDNodeMap]) => {
      assert(mapResults);
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
    })
    .then((key) => callback(null, key))
    .catch((e) => callback(e));
}

module.exports = { map, reduce };
