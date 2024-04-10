// @ts-check

const assert = require("node:assert");
const store = require("./store");
const comm = require("./comm");
const groups = require("./groups");
const id = require("../util/id");
const types = require("../types");
const util = require("../util/util");

/**
 * Sends the result of the map operation to the corresponding node to reduce
 * @param {id.ID} jobID
 * @param {Array} results
 * @param {Map.<string, types.NodeInfo>} neighborNIDs
 * @param {id.HashFunc} hash
 * @param {types.Callback} callback
 */

/**
 * Waits until a certain number of notifications have been received.
 * It will then call the next action.
 * @param {id.ID} jobID
 * @param {types.NodeInfo} supervisor
 * @param {number} numNotifs
 * @param {types.Callback} callback
 */
function notificationBarrier(jobID, supervisor, numNotifs, callback) {
  const notifyRemote = {
    node: supervisor,
    service: `mr-${jobID}`,
    method: "call",
  };
  return util.waitAll(numNotifs, (e, _) => {
    if (e) return callback(e);

    comm.send([], notifyRemote, (err, _) => {
      if (err) {
        return callback(err);
      }
    });
  });
}

function storeBarrier(
  jobID,
  supervisor,
  mapperRes,
  numKeys,
  neighborNIDNodeMap,
  callback,
) {
  const neighborNIDS = Array.from(neighborNIDNodeMap.keys());
  return util.waitAll(numKeys, (e) => {
    if (e) return callback(e);

    const entries = Array.from(mapperRes.entries());
    const notif = notificationBarrier(
      jobID,
      supervisor,
      entries.length,
      callback,
    );
    entries.forEach(([key, val]) => {
      const storeID = id.getID(key) + id.getSID(global.nodeConfig);
      const destinationNID = id.consistentHash(storeID, neighborNIDS);
      const destinationNode = neighborNIDNodeMap.get(destinationNID);
      assert(destinationNode);
      comm.send(
        [{ [key]: val }, { gid: jobID, key: storeID }],
        {
          node: destinationNode,
          service: "store",
          method: "put",
        },
        notif,
      );
    });
  });
}

/**
 * @param {string} gid
 * @param {id.ID} jobID
 * @param {types.NodeInfo} supervisor
 * @param {types.Mapper} mapper
 * @param {types.Callback} callback
 */
function map(gid, supervisor, jobID, mapper, callback = () => {}) {
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
        jobID,
        supervisor,
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
            assert(Object.entries(result).length === 1);
            const [key, val] = Object.entries(result)[0];
            if (mapperRes.has(key)) {
              mapperRes.get(key).push(val);
            } else {
              mapperRes.set(key, [val]);
            }
          } catch (e) {
            return storeBar(e);
          }
          storeBar();
        });
      });
    });
  });
}

/**
 * @param {id.ID} jobID
 * @param {types.Reducer} reducer
 * @param {types.Callback} callback
 */
function reduce(jobID, reducer, callback = (_e, _) => {}) {
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
      return callback(null, reduceResult);
    } catch (e) {
      return callback(e);
    }
  });
}

module.exports = { map, reduce };
