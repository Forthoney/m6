const store = require("./store");
const comm = require("./comm");
const id = require("../util/id");

/*
 * Sends the result of the map operation to the corresponding node to reduce
 */
function sendForGrouping(jobID, result, neighborNIDs, callback) {
  const mapKey = Object.keys(result)[0];
  const destinationNID = id.consistentHash(
    id.getID(mapKey),
    Object.keys(neighborNIDs),
  );
  const destinationNode = neighborNIDs[destinationNID];
  comm.send(
    [result, { gid: jobID, key: null }],
    { node: destinationNode, service: "store", method: "put" },
    callback,
  );
}

function notificationBarrier(jobID, supervisor, length, callback) {
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

    if (++counter == length) {
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

const mr = {
  map: (
    gid,
    jobID,
    mapper,
    keys,
    supervisor,
    neighborNIDs,
    callback = () => {},
  ) => {
    keys.forEach((key) => {
      store.get({ gid: gid, key: key }, (e, val) => {
        if (e) return callback(e);

        try {
          sendForGrouping(
            jobID,
            mapper(key, val),
            neighborNIDs,
            notificationBarrier(jobID, supervisor, keys.length, callback),
          );
        } catch (e) {
          callback(e);
        }
      });
    });
  },

  reduce: (jobID, reducer, callback = () => {}) => {
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
  },
};

module.exports = mr;
