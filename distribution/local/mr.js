// @ts-check

const assert = require("node:assert");
const store = require("./store");
const comm = require("./comm");
const groups = require("./groups");
const id = require("../util/id");
const types = require("../types");

/**
 * Sends the result of the map operation to the corresponding node to reduce
 * @param {id.ID} jobID
 * @param {any} result
 * @param {Map.<string, types.NodeInfo>} neighborNIDs
 * @param {id.HashFunc} hash
 * @param {types.Callback} callback
 */
function sendForGrouping(jobID, result, neighborNIDs, hash, callback) {
  const mapKey = Object.keys(result)[0];
  const destinationNID = hash(
    id.getID(mapKey),
    Array.from(neighborNIDs.keys()),
  );
  const destinationNode = neighborNIDs.get(destinationNID);
  assert(destinationNode !== undefined);
  comm.send(
    [result, { gid: jobID, key: null }],
    { node: destinationNode, service: "store", method: "put" },
    callback,
  );
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
  let counter = 0;
  let failed = false;
  const notifyRemote = {
    node: supervisor,
    service: `mr-${jobID}`,
    method: "call",
  };
  const barrier = (e, _) => {
    if (failed) return;
    if (e) {
      failed = true;
      return callback(e);
    }

    if (++counter == numNotifs) {
      comm.send([], notifyRemote, (e, _) => {
        if (e) {
          failed = true;
          return callback(e);
        }
      });
    }
  };
  return barrier;
}

/**
 * @param {string} gid
 * @param {id.ID} jobID
 * @param {types.NodeInfo} supervisor
 * @param {types.Mapper} mapper
 * @param {id.HashFunc} hash
 * @param {types.Callback} callback
 */
function map(gid, supervisor, jobID, mapper, hash, callback = (_e, _) => {}) {
  store.get({ gid: gid, key: null }, (e, keys) => {
    if (e) return callback(e);

    groups.get(gid, (e, neighbors) => {
      if (e) return callback(e);

      /** @type {types.Group} */
      assert(neighbors !== undefined);
      const neighborNIDNodeMap = new Map(
        Object.values(neighbors).map((node) => [id.getNID(node), node]),
      );

      keys.forEach((key) => {
        store.get({ gid: gid, key: key }, (e, val) => {
          if (e) return callback(e);

          try {
            sendForGrouping(
              jobID,
              mapper(key, val),
              neighborNIDNodeMap,
              hash,
              notificationBarrier(jobID, supervisor, keys.length, callback),
            );
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
  store.hasGID(jobID, (e, exists) => {
    if (e) return callback(e);
    // nothing was assigned to this node
    if (!exists) return callback(null, {});

    // Below is NOT a race condition since only this reduce task has access to
    // the group store with jobID as gid
    store.get({ gid: jobID, key: null }, (e, keys) => {
      if (e) return callback(e);

      const mapResults = [];
      keys.forEach((key) => {
        store.get({ gid: jobID, key: key }, (e, v) => {
          if (e) return callback(e);

          vals.push(v);
        });
      });

      const key = Object.keys(mapResults[0])[0];
      const vals = mapResults.map((res) => Object.values(res)[0]);
      try {
        const result = reducer(key, vals);
        return callback(null, result);
      } catch (e) {
        return callback(e);
      }
    });
  });
}

module.exports = { map, reduce };
