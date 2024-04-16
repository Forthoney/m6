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
const { warn } = require("node:console");
const id = util.id;

/**
 * @param {string} gid
 * @param {string} hashKey
 * @param {string} storeKey
 * @param {any} item
 * @param {Map<string, NodeInfo>} neighborNIDNodeMap
 * @param {Callback} callback
 */
function storeOnRemote(
  gid,
  hashKey,
  storeKey,
  item,
  neighborNIDNodeMap,
  callback,
) {
  const neighborNIDs = Array.from(neighborNIDNodeMap.keys());
  const destinationNID = id.consistentHash(id.getID(hashKey), neighborNIDs);
  const destinationNode = neighborNIDNodeMap.get(destinationNID);
  assert(destinationNode);
  comm.send(
    [item, { gid, key: storeKey }],
    {
      node: destinationNode,
      service: "store",
      method: "put",
    },
    (e, _) => (e ? callback(e) : callback(null, storeKey)),
  );
}

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

/**
 * @param {MRJobMetadata} jobData
 * @param {Mapper} mapper
 * @param {Callback} callback
 */
function map(jobData, mapper, callback = () => {}) {
  const { jobID, gid, supervisor } = jobData;

  Promise.all([computeMap(gid, mapper), groups.getPromise(gid)])
    .then(([mapperRes, neighbors]) => {
      assert(neighbors);
      const entries = Array.from(mapperRes.entries());
      const peerNIDNodeMap = new Map(
        Object.values(neighbors).map((node) => [id.getNID(node), node]),
      );
      const peerNIDs = Array.from(peerNIDNodeMap.keys());
      const storePromises = entries.map(([key, val]) => {
        const entry = { [key]: val };
        const destinationNode = peerNIDNodeMap.get(
          id.consistentHash(id.getID(key), peerNIDs),
        );
        assert(destinationNode);

        const uniqueKey = id.getID(entry) + id.getSID(global.nodeConfig);
        const remote = {
          node: destinationNode,
          service: "store",
          method: "put",
        };
        return comm.sendPromise(
          [entry, { gid: jobID, key: uniqueKey }],
          remote,
        );
      });
      return Promise.all(storePromises);
    })
    .then((_) => {
      const notifyRemote = {
        node: supervisor,
        service: `mr-${jobID}`,
        method: "call",
      };
      return comm.sendPromise([null], notifyRemote);
    })
    .then((_) => {
      callback(null, jobID);
    })
    .catch((e) => callback(e));
}

/**
 * @param {MRJobMetadata} jobData
 * @param {Reducer} reducer
 * @param {Callback} callback
 */
function reduce(jobData, reducer, callback = (_e, _) => {}) {
  const { gid, jobID } = jobData;
  groups.get(gid, (e, neighbors) => {
    if (e) return callback(e);

    assert(neighbors);
    const neighborNIDNodeMap = new Map(
      Object.values(neighbors).map((node) => [id.getNID(node), node]),
    );
    store.getAll(jobID, (e, mapResults) => {
      if (e) return callback(e);

      assert(mapResults);
      const organizedMapResults = mapResults.reduce((accumulator, obj) => {
        const [key, val] = Object.entries(obj)[0];

        if (key in accumulator) {
          accumulator[key] = accumulator[key].concat(val);
        } else {
          accumulator[key] = val;
        }
        return accumulator;
      }, {});

      try {
        const reduceResult = Object.entries(organizedMapResults).map(
          ([key, val]) => reducer(key, val),
        );
        const key = jobID + id.getSID(global.nodeConfig);
        storeOnRemote(
          gid,
          key,
          key,
          reduceResult,
          neighborNIDNodeMap,
          callback,
        );
      } catch (e) {
        return callback(e);
      }
    });
  });
}

module.exports = { map, reduce };
