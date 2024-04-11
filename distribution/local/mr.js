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
const id = require("../util/id");
const util = require("../util/util");
const { writeFileSync } = require("node:fs");

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

/**
 * Waits until a certain number of notifications have been received.
 * It will then call the next action.
 * @param {MRJobMetadata} jobData
 * @param {number} numNotifs
 * @param {Callback} callback
 * @return {Callback}
 */
function notificationBarrier(jobData, numNotifs, callback) {
  const { jobID, supervisor } = jobData;
  const notifyRemote = {
    node: supervisor,
    service: `mr-${jobID}`,
    method: "call",
  };
  return util.waitAll(numNotifs, (e, _) => {
    comm.send([e], notifyRemote, (err, _) => {
      if (err) {
        return callback(err);
      }
    });
  });
}

/**
 * @param {MRJobMetadata} jobData
 * @param {Map<string, Array>} mapperRes
 * @param {number} numKeys
 * @param {Map<string, NodeInfo>} neighborNIDNodeMap
 * @param {Callback} callback
 * @return {Callback}
 */
function storeBarrier(
  jobData,
  mapperRes,
  numKeys,
  neighborNIDNodeMap,
  callback,
) {
  return util.waitAll(numKeys, (e) => {
    if (e) return callback(e);

    const entries = Array.from(mapperRes.entries());
    const notif = notificationBarrier(jobData, entries.length, callback);
    entries.forEach((entry) => {
      const [key, val] = entry;
      const uniqueKey = id.getID(entry) + id.getSID(global.nodeConfig);
      storeOnRemote(
        jobData.jobID,
        key,
        uniqueKey,
        { [key]: val },
        neighborNIDNodeMap,
        notif,
      );
    });
  });
}

/**
 * @param {MRJobMetadata} jobData
 * @param {Mapper} mapper
 * @param {Callback} callback
 */
function map(jobData, mapper, callback = () => {}) {
  const { gid } = jobData;
  store.get({ gid: gid, key: null }, (e, keys) => {
    if (e) return callback(e);

    groups.get(gid, (e, neighbors) => {
      if (e) return callback(e);

      assert(neighbors);
      const neighborNIDNodeMap = new Map(
        Object.values(neighbors).map((node) => [id.getNID(node), node]),
      );

      const mapperRes = new Map();
      const storeBar = storeBarrier(
        jobData,
        mapperRes,
        keys.length,
        neighborNIDNodeMap,
        callback,
      );
      keys.forEach((/** @type {store.LocalKey} */ storeKey) => {
        store.get({ gid: gid, key: storeKey }, (e, data) => {
          if (e) return storeBar(e);

          try {
            const result = mapper(storeKey, data);
            if (result instanceof Array) {
              result.forEach((res) => {
                Object.entries(res).forEach(([key, val]) => {
                  if (mapperRes.has(key)) {
                    mapperRes.get(key).push(val);
                  } else {
                    mapperRes.set(key, [val]);
                  }
                });
              });
            } else {
              Object.entries(result).forEach(([key, val]) => {
                if (mapperRes.has(key)) {
                  mapperRes.get(key).push(val);
                } else {
                  mapperRes.set(key, [val]);
                }
              });
            }
          } catch (e) {
            return storeBar(e);
          }
          storeBar(null);
        });
      });
    });
  });
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
        const key = Object.keys(obj)[0];
        const val = obj[key];

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
