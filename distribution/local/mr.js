// @ts-check

const assert = require("node:assert");
const store = require("./store");
const comm = require("./comm");
const groups = require("./groups");
const id = require("../util/id");
const types = require("../types");
const util = require("../util/util");
const { randomInt } = require("node:crypto");

/**
 * Sends the result of the map operation to the corresponding node to reduce
 * @param {id.ID} jobID
 * @param {Array} results
 * @param {Map.<string, types.NodeInfo>} neighborNIDs
 * @param {id.HashFunc} hash
 * @param {types.Callback} callback
 */
function sendForGrouping(jobID, results, neighborNIDs, hash, callback) {
  const barrier = util.waitAll(results.length, callback);
  results.forEach((res) => {
    const mapKey = Object.keys(res)[0];
    const destinationNID = hash(
      id.getID(mapKey),
      Array.from(neighborNIDs.keys()),
    );
    const destinationNode = neighborNIDs.get(destinationNID);
    assert(destinationNode);
    const key = id.getID(res) + id.getSID(global.nodeConfig) + randomInt(1000);
    comm.send(
      [res, { gid: jobID, key }],
      { node: destinationNode, service: "store", method: "put" },
      barrier,
    );
  });
}

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

      const notif = notificationBarrier(
        jobID,
        supervisor,
        keys.length,
        callback,
      );
      keys.forEach((/** @type {store.LocalKey} */ key) => {
        store.get({ gid: gid, key: key }, (e, val) => {
          if (e) return callback(e);

          let mapperRes = mapper(key, val);
          if (!(mapperRes instanceof Array)) {
            mapperRes = [mapperRes];
          }
          sendForGrouping(
            jobID,
            mapperRes,
            neighborNIDNodeMap,
            id.consistentHash,
            notif,
          );
          try {
          } catch (e) {
            callback(e);
          }
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
    const organizedMapResults = mapResults.reduce((accumulator, obj, _) => {
      const key = Object.keys(obj)[0];
      const val = obj[key];

      if (key in accumulator) {
        accumulator[key].push(val);
      } else {
        accumulator[key] = [val];
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
